/* Copyright (c) 2014 Sean Pringle sean.pringle@gmail.com

  This program is free software; you can redistribute it and/or modify
  it under the terms of the GNU General Public License as published by
  the Free Software Foundation; version 2 of the License.

  This program is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Public License for more details.

  You should have received a copy of the GNU General Public License
  along with this program; if not, write to the Free Software
  Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

/**
  @file ha_cram.cc
*/

#ifdef USE_PRAGMA_IMPLEMENTATION
#pragma implementation        // gcc: Class implementation
#endif

#include <mysql/plugin.h>
#include "ha_cram.h"
#include "sql_class.h"
#include <pthread.h>
#include <zlib.h>
#include <time.h>

ulonglong cram_checkpoint_duration_usec;

uint cram_table_lists;
uint cram_table_list_hints;
uint cram_compress_boundary;
uint cram_checkpoint_seconds;
uint cram_checkpoint_threads;
uint cram_worker_threads;

list_t *cram_tables;
pthread_mutex_t cram_tables_lock;

bool checkpoint_done;
bool checkpoint_asap;
pthread_t checkpoint_thread;

uint64 cram_seed;
pthread_mutex_t cram_seed_lock;

static handler *cram_create_handler(handlerton *hton, TABLE_SHARE *table, MEM_ROOT *mem_root);

handlerton *cram_hton;

struct ha_table_option_struct{};
struct ha_field_option_struct{};
ha_create_table_option cram_table_option_list[] = { HA_TOPTION_END };
ha_create_table_option cram_field_option_list[] = { HA_FOPTION_END };

static uint cram_verbose;

static void cram_note(const char *format, ...)
{
  char buff[1024];
  snprintf(buff, sizeof(buff), "CRAM: %s", format);
  va_list args;
  va_start(args, format);
  error_log_print(INFORMATION_LEVEL, buff, args);
  va_end(args);
}

static void cram_error(const char *format, ...)
{
  char buff[1024];
  snprintf(buff, sizeof(buff), "CRAM: %s", format);
  va_list args;
  va_start(args, format);
  error_log_print(ERROR_LEVEL, buff, args);
  va_end(args);
}

#define cram_assert(f,...) do { if (!(f)) { cram_error(__VA_ARGS__); abort(); } } while(0)
#define cram_debug(...) if (cram_verbose) cram_note(__VA_ARGS__)

static void* cram_alloc(size_t bytes)
{
  void *ptr = calloc(bytes, 1);
  cram_assert(ptr, "calloc failed %llu bytes", bytes);
  return ptr;
}

static void* cram_realloc(void *ptr, size_t bytes)
{
  void *ptr2 = realloc(ptr, bytes);
  cram_assert(ptr2, "realloc failed %llu bytes", bytes);
  return ptr2;
}

static void cram_free(void *ptr)
{
  free(ptr);
}

/* DJBX33A */
static uint32 cram_hash(const uchar *buffer, size_t length)
{
  uint32 hash = 5381; size_t i = 0;
  for (
    length = length > 1024 ? 1024: length, i = 0;
    i < length;
    hash = hash * 33 + buffer[i++]
  );
  return hash;
}

static uint32 cram_hash_int64(int64 n)
{
  return n < 0 ? n*-1: n;
}

static size_t cram_deflate(uchar *data, size_t width)
{
  uchar *buff = (uchar*) cram_alloc(width);
  uint length = width;

  z_stream stream;
  int err;

  stream.next_in = (Bytef*)data;
  stream.avail_in = (uInt)width;
  stream.next_out = (Bytef*)buff;
  stream.avail_out = (uInt)length;
  stream.zalloc = Z_NULL;
  stream.zfree = Z_NULL;
  stream.opaque = Z_NULL;

  err = deflateInit2(&stream, Z_BEST_SPEED, Z_DEFLATED, 15, 9, Z_DEFAULT_STRATEGY);
  if (err != Z_OK) goto oops;

  err = deflate(&stream, Z_FINISH);
  if (err != Z_STREAM_END) goto oops;

  length = stream.total_out;
  deflateEnd(&stream);
  memmove(data, buff, length);
  cram_free(buff);
  return length;

oops:
  deflateEnd(&stream);
  length = UINT_MAX;
  cram_free(buff);
  return length;
}

static size_t cram_inflate(uchar *data, size_t width, size_t limit)
{
  size_t length = limit;
  uchar *buff = (uchar*) cram_alloc(limit);
  int err = uncompress(buff, &length, data, width);
  if (err != Z_OK) length = UINT_MAX;
  else memmove(data, buff, length);
  cram_free(buff);
  return length;
}

static str_t* str_alloc(size_t limit)
{
  str_t *str = (str_t*) cram_alloc(sizeof(str_t));
  str->buffer = (char*) cram_alloc(limit);
  str->limit  = limit;
  str->length = 0;
  return str;
}

static void str_free(str_t *str)
{
  cram_free(str->buffer);
  cram_free(str);
}

static void str_reset(str_t *str)
{
  str->length = 0;
}

static void str_cat(str_t *str, char *buffer, size_t length)
{
  if (buffer && length > 0)
  {
    if (str->length + length + 1 > str->limit)
    {
      str->limit = str->length + length + 1;
      str->buffer = (char*) cram_realloc(str->buffer, str->limit);
    }
    memmove(str->buffer + str->length, buffer, length);
    str->length += length;
    str->buffer[str->length] = 0;
  }
}

static void str_print(str_t *str, const char *format, ...)
{
  char buff[1024];
  va_list args;
  va_start(args, format);
  int length = vsnprintf(buff, sizeof(buff), format, args);
  va_end(args);
  if (length >= 0)
    str_cat(str, buff, length);
}

static size_t bmp_size(uint width)
{
  return width % 8 ? width/8+1: width/8;
}

static void bmp_all_clr(bmp_t *bmp, size_t width)
{
  memset(bmp, 0, bmp_size(width));
}

static bmp_t* bmp_alloc(size_t width)
{
  bmp_t *bmp = (bmp_t*) cram_alloc(bmp_size(width));
  bmp_all_clr(bmp, width);
  return bmp;
}

static void bmp_free(bmp_t *bmp)
{
  cram_free(bmp);
}

static bool bmp_chk(bmp_t *bmp, uint bit)
{
  return bmp[bit/8] & (1 << (bit%8)) ? TRUE: FALSE;
}

static void bmp_set(bmp_t *bmp, uint bit)
{
  bmp[bit/8] |= (1 << (bit%8));
}
/*
static void bmp_clr(bmp_t *bmp, uint bit)
{
  bmp[bit/8] &= ~(1 << (bit%8));
}
*/
static list_t* list_alloc()
{
  list_t *list = (list_t*) cram_alloc(sizeof(list_t));
  return list;
}

static void list_free(list_t *list)
{
  cram_free(list);
}

static bool list_is_empty(list_t *list)
{
  return list->head ? FALSE: TRUE;
}

static void list_insert_head(list_t *list, void *item)
{
  node_t *node = (node_t*) cram_alloc(sizeof(node_t));
  node->payload = item;
  node->next = list->head;
  list->head = node;
  list->length++;
}

static void* list_remove_node(list_t *list, node_t *node)
{
  node_t **prev = &list->head;
  while (prev && *prev != node)
    prev = &(*prev)->next;

  void *payload = node->payload;

  *prev = node->next;
  list->length--;
  cram_free(node);

  return payload;
}

static void* list_remove_head(list_t *list)
{
  return (list->head) ? list_remove_node(list, list->head): NULL;
}

static node_t* list_locate(list_t *list, void *item)
{
  node_t *node = list->head;
  while (node && item != node->payload)
    node = node->next;
  return node;
}

static bool list_delete(list_t *list, void *item)
{
  node_t **prev = &list->head;
  while (prev && (*prev) && (*prev)->payload != item)
    prev = &(*prev)->next;

  if (prev && *prev)
  {
    node_t *node = *prev;
    *prev = node->next;
    list->length--;
    cram_free(node);
    return TRUE;
  }
  return FALSE;
}

static uchar* cram_field(CramTable *table, uchar *row, uint field)
{
  cram_assert(field < table->width, "impossible field offset");

  uint offset = 0;
  for (uint col = 0; col < field; col++)
  {
    uchar type = row[offset++];
    switch (type) {
      case CRAM_NULL:
        break;
      case CRAM_STRING:
        offset += *((uint*)&row[offset]) + sizeof(uint) + sizeof(uint);
        break;
      case CRAM_TINYSTRING:
        offset += *((uint8_t*)&row[offset]) + sizeof(uint8_t);
        break;
      case CRAM_INT64:
        offset += sizeof(int64_t);
        break;
      case CRAM_INT32:
        offset += sizeof(int32_t);
        break;
      case CRAM_INT08:
        offset += sizeof(int8_t);
        break;
    }
  }
  return &row[offset];
}

static uchar cram_field_type(uchar *row)
{
  return *row;
}

static uchar* cram_field_buffer(uchar *row)
{
  uchar type = *row++;
  switch (type) {
    case CRAM_STRING:
      row += sizeof(uint) + sizeof(uint);
      break;
    case CRAM_TINYSTRING:
      row += sizeof(uint8_t);
      break;
    case CRAM_NULL:
    case CRAM_INT64:
    case CRAM_INT32:
    case CRAM_INT08:
      break;
  }
  return row;
}

static uint cram_field_length(uchar *row)
{
  uint length = 0;
  uchar type = *row++;
  switch (type) {
    case CRAM_NULL:
      break;
    case CRAM_STRING:
      length = *((uint*)row);
      break;
    case CRAM_TINYSTRING:
      length = *((uint8_t*)row);
      break;
    case CRAM_INT64:
      length = sizeof(int64_t);
      break;
    case CRAM_INT32:
      length = sizeof(int32_t);
      break;
    case CRAM_INT08:
      length = sizeof(int8_t);
      break;
  }
  return length;
}

static uint cram_field_length_string(uchar *row)
{
  return *((uint*)(row+1+sizeof(uint)));
}

static uint64 cram_field_width(uchar *row)
{
  uint64 length = 1;
  uchar type = *row++;
  switch (type) {
    case CRAM_NULL:
      break;
    case CRAM_STRING:
      length += *((uint*)row) + sizeof(uint) + sizeof(uint);
      break;
    case CRAM_TINYSTRING:
      length += *((uint8_t*)row) + sizeof(uint8_t);
      break;
    case CRAM_INT64:
      length += sizeof(int64_t);
      break;
    case CRAM_INT32:
      length += sizeof(int32_t);
      break;
    case CRAM_INT08:
      length += sizeof(int8_t);
      break;
  }
  return length;
}

static bool cram_field_int64(uchar *row, int64 *res)
{
  uchar type    = cram_field_type(row);
  uchar *buffer = cram_field_buffer(row);
  uint length   = cram_field_length(row);

  char *err = NULL;
  char pad[1024];

  switch (type) {

    case CRAM_INT64:
      *res = *((int64_t*)buffer);
      return TRUE;

    case CRAM_INT32:
      *res = *((int32_t*)buffer);
      return TRUE;

    case CRAM_INT08:
      *res = *((int8_t*)buffer);
      return TRUE;

    case CRAM_TINYSTRING:
      memmove(pad, buffer, length);
      pad[length] = 0;
      *res = strtoll(pad, &err, 10);
      return err == pad+length;

    default:
      return FALSE;
  }
  return FALSE;
}

static void cram_row_index(CramTable *table, uint list, CramRow *row)
{
  uchar *field = cram_field(table, row, 0);

  for (uint col = 0; col < table->width; field += cram_field_width(field), col++)
  {
    uint     type = cram_field_type(field);
    uchar *buffer = cram_field_buffer(field);
    uint   length = cram_field_length(field);

    if (length > 0)
    {
      uint hashval = 0;
      switch (type)
      {
        case CRAM_INT08:
        case CRAM_INT32:
        case CRAM_INT64:
          int64 ni64; cram_field_int64(field, &ni64);
          hashval = cram_hash_int64(ni64);
          bmp_set(table->hints[list][col], hashval % table->hints_width);
          break;
        case CRAM_TINYSTRING:
          hashval = cram_hash(buffer, length);
          bmp_set(table->hints[list][col], hashval % table->hints_width);
          break;
        default: break;
      }
    }
  }
}

static CramTable* cram_table_open(const char *name, uint width)
{
  node_t *node = cram_tables->head;
  while (node && strcmp(((CramTable*)node->payload)->name, name) != 0)
    node = node->next;

  CramTable *table = node ? (CramTable*) node->payload: NULL;

  if (!table && width)
  {
    table = (CramTable*) cram_alloc(sizeof(CramTable));

    table->name = (char*) cram_alloc(strlen(name)+1);
    strcpy(table->name, name);

    table->width = width;
    table->lists_count = cram_table_lists;
    table->hints_width = cram_table_list_hints;
    table->compress_boundary = cram_compress_boundary;

    char fname[1024];
    snprintf(fname, sizeof(fname), "%s.cram", table->name);
    FILE *data = fopen(fname, "rb");

    if (data)
    {
      fread(&table->width, 1, sizeof(size_t), data);
      fread(&table->lists_count, 1, sizeof(size_t), data);
      fread(&table->hints_width, 1, sizeof(size_t), data);
      fread(&table->compress_boundary, 1, sizeof(size_t), data);
    }

    table->lists   = (list_t**) cram_alloc(sizeof(list_t*) * table->lists_count);
    table->hints   = (bmp_t***) cram_alloc(sizeof(bmp_t**) * table->lists_count);
    table->changes = (uint*)    cram_alloc(sizeof(uint)    * table->lists_count);
    table->locks   = (pthread_mutex_t*) cram_alloc(sizeof(pthread_mutex_t) * table->lists_count);

    for (uint i = 0; i < table->lists_count; i++)
    {
      pthread_mutex_init(&table->locks[i], NULL);
      table->lists[i] = list_alloc();
      table->hints[i] = (bmp_t**) cram_alloc(sizeof(bmp_t*) * width);

      for (uint j = 0; j < width; j++)
        table->hints[i][j] = bmp_alloc(table->hints_width);
    }

    list_insert_head(cram_tables, table);

    uint64 meta_size = sizeof(CramTable) + strlen(table->name) + 1;
    uint64 data_size = 0;
    uint64 row_count = 0;

    meta_size += sizeof(list_t*) * table->lists_count;
    meta_size += (sizeof(bmp_t*) + bmp_size(table->hints_width)) * table->lists_count * table->width;
    meta_size += sizeof(uint) * table->lists_count; // changes
    meta_size += sizeof(pthread_mutex_t) * table->lists_count; // locks

    if (data)
    {
      uint list = 0; uint64 width = 0;
      while (fread(&width, 1, sizeof(uint64), data) == sizeof(uint64))
      {
        CramRow *row = (uchar*) cram_alloc(width);

        if (fread(row, 1, width, data) < width)
        {
          cram_error("read failed %s", fname);
          break;
        }

        list_insert_head(table->lists[list], row);
        cram_row_index(table, list, row);

        list++;
        if (list == table->lists_count)
          list = 0;

        meta_size += sizeof(node_t);
        data_size += width;
        row_count++;
      }
      fclose(data);
    }

    table->meta_size = meta_size;
    table->data_size = data_size;
    table->row_count = row_count;

    thr_lock_init(&table->mysql_lock);
  }

  return table;
}

static void cram_table_drop(CramTable *table, bool hard)
{
  if (hard)
  {
    char fname[1024];
    snprintf(fname, sizeof(fname), "%s.cram", table->name);
    remove(fname);
  }

  for (uint i = 0; i < table->lists_count; i++)
  {
    pthread_mutex_destroy(&table->locks[i]);
    while (!list_is_empty(table->lists[i]))
      cram_free(list_remove_head(table->lists[i]));
    list_free(table->lists[i]);

    for (uint j = 0; j < table->width; j++)
      bmp_free(table->hints[i][j]);

    cram_free(table->hints[i]);
  }

  thr_lock_delete(&table->mysql_lock);

  cram_free(table->name);
  list_delete(cram_tables, table);
  cram_free(table);
}

static void* cram_checkpointer(void *p)
{
  CramTable *table = (CramTable*) p;

  char nname[1024], fname[1024];
  snprintf(fname, sizeof(fname), "%s.cram", table->name);
  snprintf(nname, sizeof(nname), "%s.new.cram", table->name);
  FILE *data = fopen(nname, "wb");

  fwrite(&table->width, 1, sizeof(size_t), data);
  fwrite(&table->lists_count, 1, sizeof(size_t), data);
  fwrite(&table->hints_width, 1, sizeof(size_t), data);
  fwrite(&table->compress_boundary, 1, sizeof(size_t), data);

  uint64 meta_size = sizeof(CramTable) + strlen(table->name) + 1;
  uint64 data_size = 0;
  uint64 row_count = 0;

  meta_size += sizeof(list_t*) * table->lists_count;
  meta_size += (sizeof(bmp_t*) + bmp_size(table->hints_width)) * table->lists_count * table->width;
  meta_size += sizeof(uint) * table->lists_count; // changes
  meta_size += sizeof(pthread_mutex_t) * table->lists_count; // locks

  // Walk lists in reverse order. Theory is this makes it harder for
  // queries to fetch up in line behind the checkpoint threads.
  for (int li = table->lists_count-1; li >= 0; li--)
  {
    list_t *list = table->lists[li];
    pthread_mutex_lock(&table->locks[li]);

    meta_size += sizeof(node_t) * list->length;

    row_count += list->length;

    for (node_t *node = list->head; node; node = node->next)
    {
      CramRow *row = (CramRow*) node->payload;

      uchar *field = row;

      for (uint col = 0; col < table->width; col++)
        field += cram_field_width(field);

      uint64 width = field - row;

      fwrite(&width, 1, sizeof(uint64), data);
      fwrite(row, 1, width, data);

      data_size += width;
    }

    pthread_mutex_unlock(&table->locks[li]);
  }
  fclose(data);
  rename(nname, fname);

  table->meta_size = meta_size;
  table->data_size = data_size;
  table->row_count = row_count;

  return NULL;
}

static void* cram_checkpoint(void *p)
{
  list_t *tables = list_alloc();

  while (!checkpoint_done)
  {
    int64 delay = cram_checkpoint_seconds * 1000000;
    while (delay > 0 && !checkpoint_done && !checkpoint_asap)
    {
      int mu = delay > 10000 ? 10000: delay;
      usleep(mu); delay -= mu;
    }
    if (checkpoint_done) break;
    if (checkpoint_asap) checkpoint_asap = FALSE;

    struct timeval time_start, time_stop;
    gettimeofday(&time_start, NULL);

    pthread_mutex_lock(&cram_tables_lock);

    for (node_t *tnode = cram_tables->head; tnode; tnode = tnode->next)
    {
      CramTable *table = (CramTable*) tnode->payload;
      if (!table->dropping)
      {
        list_insert_head(tables, table);
        table->users++;
      }
    }

    pthread_mutex_unlock(&cram_tables_lock);

    uint spawned = 0;
    pthread_t writers[cram_checkpoint_threads];

    for (node_t *tnode = tables->head; tnode; tnode = tnode->next)
    {
      CramTable *table = (CramTable*) tnode->payload;

      if (spawned == cram_checkpoint_threads)
      {
        for (uint i = 0; i < spawned; i++)
          pthread_join(writers[i], NULL);
        spawned = 0;
      }

      pthread_create(&writers[spawned++], NULL, cram_checkpointer, table);
    }

    for (uint i = 0; i < spawned; i++)
      pthread_join(writers[i], NULL);

    pthread_mutex_lock(&cram_tables_lock);

    while (tables->length)
    {
      CramTable *table = (CramTable*) list_remove_head(tables);
      table->users--;
    }

    pthread_mutex_unlock(&cram_tables_lock);

    gettimeofday(&time_stop, NULL);
    cram_checkpoint_duration_usec = (time_stop.tv_sec * 1000000 + time_stop.tv_usec)
      - (time_start.tv_sec * 1000000 + time_start.tv_usec);
  }
  return NULL;
}

static bool cram_show_status(handlerton* hton, THD* thd, stat_print_fn* stat_print, enum ha_stat_type stat_type)
{
  str_t *str = str_alloc(100);

  uint64 meta = 0, data = 0;

  pthread_mutex_lock(&cram_tables_lock);

  node_t *node = cram_tables->head;

  while (node)
  {
    CramTable *table = (CramTable*) node->payload;
    meta += table->meta_size;
    data += table->data_size;
    node = node->next;
  }

  pthread_mutex_unlock(&cram_tables_lock);

  str_print(str, "metadata: %llu MB, data: %llu MB", meta/1024/1024, data/1024/1024);

  stat_print(thd, STRING_WITH_LEN("CRAM"), STRING_WITH_LEN("memory"), str->buffer, str->length);

  pthread_mutex_lock(&cram_tables_lock);

  node = cram_tables->head;

  while (node)
  {
    str_reset(str);

    CramTable *table = (CramTable*) node->payload;
    table->users++;

    pthread_mutex_unlock(&cram_tables_lock);

    str_print(str, "%s width %u lists %u hints %u compress %u", table->name, table->width, table->lists_count, table->hints_width, table->compress_boundary);

    uint min = UINT_MAX, max = 0, avg, tot = 0;
    for (uint i = 0; i < table->lists_count; i++)
    {
      pthread_mutex_lock(&table->locks[i]);

      uint len = table->lists[i]->length;
      if (len < min) min = len;
      if (len > max) max = len;
      tot += len;

      pthread_mutex_unlock(&table->locks[i]);
    }
    avg = tot / table->lists_count;
    str_print(str, " min %llu max %llu mean %llu total %llu", min, max, avg, tot);

    stat_print(thd, STRING_WITH_LEN("CRAM"), STRING_WITH_LEN("table"), str->buffer, str->length);

    pthread_mutex_lock(&table->locks[0]);

    for (uint col = 0; col < table->width; col++)
    {
      str_reset(str);
      str_print(str, "%s [%3u] ", table->name, col);

      for (uint i = 0; i < table->hints_width; i++)
        str_print(str, "%u", bmp_chk(table->hints[0][col], i));

      stat_print(thd, STRING_WITH_LEN("CRAM"), STRING_WITH_LEN("table"), str->buffer, str->length);
    }

    pthread_mutex_unlock(&table->locks[0]);

    pthread_mutex_lock(&cram_tables_lock);
    table->users--;

    node = node->next;
  }

  pthread_mutex_unlock(&cram_tables_lock);

  str_free(str);

  return FALSE; // success
}

static int cram_init_func(void *p)
{
  cram_hton = (handlerton*)p;

  cram_hton->state  = SHOW_OPTION_YES;
  cram_hton->create = cram_create_handler;
  cram_hton->flags  = HTON_CAN_RECREATE;
  cram_hton->table_options = cram_table_option_list;
  cram_hton->field_options = cram_field_option_list;
  cram_hton->show_status = cram_show_status;

  cram_seed = 1;

  pthread_mutex_init(&cram_tables_lock, NULL);
  pthread_mutex_init(&cram_seed_lock, NULL);
  pthread_create(&checkpoint_thread, NULL, cram_checkpoint, NULL);

  cram_tables = list_alloc();

  return 0;
}

static int cram_done_func(void *p)
{
  checkpoint_done = TRUE;
  pthread_join(checkpoint_thread, NULL);

  pthread_mutex_destroy(&cram_tables_lock);
  pthread_mutex_destroy(&cram_seed_lock);

  while (!list_is_empty(cram_tables))
    cram_table_drop((CramTable*)list_remove_head(cram_tables), FALSE);
  list_free(cram_tables);

  return 0;
}

static handler* cram_create_handler(handlerton *hton, TABLE_SHARE *table, MEM_ROOT *mem_root)
{
  return new (mem_root) ha_cram(hton, table);
}

ha_cram::ha_cram(handlerton *hton, TABLE_SHARE *table_arg)
  :handler(hton, table_arg)
{
  cram_debug("%s", __func__);
  cram_table = NULL;
  cram_conds = NULL;
  cram_trash = NULL;
  cram_lists_done = NULL;
  bulk_insert = FALSE;
  clear_state();
}

static const char *ha_cram_exts[] = {
  NullS
};

const char **ha_cram::bas_ext() const
{
  cram_debug("%s", __func__);
  return ha_cram_exts;
}

void ha_cram::empty_trash()
{
  if (cram_trash)
  {
    while (cram_trash->length)
      cram_free(list_remove_head(cram_trash));

    list_free(cram_trash);
    cram_trash = NULL;
  }
}

void ha_cram::use_trash()
{
  if (!cram_trash)
    cram_trash = list_alloc();
}

void ha_cram::empty_conds()
{
  if (cram_conds)
  {
    while (cram_conds->length)
    {
      CramCondition *cc = (CramCondition*) list_remove_head(cram_conds);

      while (cc->items && cc->items->length)
        cram_free(list_remove_head(cc->items));
      cram_free(cc);
    }

    list_free(cram_conds);
    cram_conds = NULL;
  }
}

void ha_cram::use_conds()
{
  if (!cram_conds)
    cram_conds = list_alloc();
}

int ha_cram::open(const char *name, int mode, uint test_if_locked)
{
  cram_debug("%s %s", __func__, name);
  reset();

  pthread_mutex_lock(&cram_tables_lock);

  cram_table = cram_table_open(name, table->s->fields);
  thr_lock_data_init(&cram_table->mysql_lock, &lock, NULL);
  cram_table->users++;

  pthread_mutex_unlock(&cram_tables_lock);

  ref_length = sizeof(CramPosition);

  return cram_table ? 0: -1;
}

int ha_cram::close(void)
{
  cram_debug("%s", __func__);

  pthread_mutex_lock(&cram_tables_lock);

  cram_table->users--;
  cram_table = NULL;

  pthread_mutex_unlock(&cram_tables_lock);

  empty_trash();
  empty_conds();

  return 0;
}

void ha_cram::start_bulk_insert(ha_rows rows, uint flags)
{
  cram_debug("%s", __func__);
  bulk_insert = TRUE;
}

int ha_cram::end_bulk_insert()
{
  cram_debug("%s", __func__);
  bulk_insert = FALSE;
  return 0;
}

int ha_cram::record_store(uchar *buf)
{
//  cram_debug("%s", __func__);
  if (!cram_result)
    return HA_ERR_END_OF_FILE;

  memset(buf, 0, table->s->null_bytes);
  // Avoid asserts in ::store() for columns that are not going to be updated
  my_bitmap_map *org_bitmap = dbug_tmp_use_all_columns(table, table->write_set);

  uchar *row  = (uchar*) cram_result->payload;
  uchar *buff = row;

  for (uint col = 0; col < cram_table->width; col++)
  {
    Field *field = table->field[col];

    uchar  type   = cram_field_type(buff);
    uchar *buffer = cram_field_buffer(buff);
    uint   length = cram_field_length(buff);
    uint  slength = length;

    uchar *tmp;

    switch (type) {
      case CRAM_NULL:
        field->set_null();
        break;
      case CRAM_STRING:
        slength = cram_field_length_string(buff);
        if (length != slength)
        {
          tmp = (uchar*)cram_alloc(slength);
          memmove(tmp, buffer, length);
          cram_inflate(tmp, length, slength);
          field->store((char*)tmp, slength, &my_charset_bin, CHECK_FIELD_WARN);
          cram_free(tmp);
        }
        else
        {
          field->store((char*)buffer, length, &my_charset_bin, CHECK_FIELD_WARN);
        }
        break;
      case CRAM_TINYSTRING:
        field->store((char*)buffer, length, &my_charset_bin, CHECK_FIELD_WARN);
        break;
      case CRAM_INT64:
        field->store(*((int64_t*)buffer), FALSE);
        break;
      case CRAM_INT32:
        field->store(*((int32_t*)buffer), FALSE);
        break;
      case CRAM_INT08:
        field->store(*((int8_t*)buffer), FALSE);
        break;
    }

    buff += cram_field_width(buff);
  }
  dbug_tmp_restore_column_map(table->write_set, org_bitmap);
  return 0;
}

uchar* ha_cram::record_place(uchar *buf)
{
//  cram_debug("%s", __func__);
  size_t length = 0;

  uint real_lengths[table->s->fields];
  memset(real_lengths, 0, sizeof(real_lengths));

  uint comp_lengths[table->s->fields];
  memset(comp_lengths, 0, sizeof(comp_lengths));

  uchar *compressed[table->s->fields];
  memset(compressed, 0, sizeof(compressed));

  for (uint col = 0; col < table->s->fields; col++)
  {
    Field *field = table->field[col];

    length += 1;

    if (field->is_null())
    {
      // nop
    }
    else
    if (field->result_type() == INT_RESULT)
    {
      if (field->val_int() < 128 && field->val_int() > -128)
        length += sizeof(int8_t);
      else
        length += sizeof(int64);
    }
    else
    {
      char pad[1024];
      String tmp(pad, sizeof(pad), &my_charset_bin);
      field->val_str(&tmp, &tmp);

      if (tmp.length() < cram_table->compress_boundary)
      {
        real_lengths[col] = tmp.length();
        length += tmp.length() + sizeof(uint8_t);
      }
      else
      {
        real_lengths[col] = tmp.length();
        compressed[col] = (uchar*) cram_alloc(real_lengths[col]);
        memmove(compressed[col], tmp.ptr(), real_lengths[col]);
        comp_lengths[col] = cram_deflate(compressed[col], real_lengths[col]);
        if (comp_lengths[col] == UINT_MAX)
        {
          comp_lengths[col] = real_lengths[col];
          memmove(compressed[col], tmp.ptr(), real_lengths[col]);
        }
        length += comp_lengths[col] + sizeof(uint) + sizeof(uint);
      }
    }
  }

  uchar *row = (uchar*) cram_alloc(length);

  for (uint col = 0; col < table->s->fields; col++)
  {
    Field *field = table->field[col];
    uchar *buff = cram_field(cram_table, row, col);

    if (field->is_null())
    {
      *buff = CRAM_NULL;
    }
    else
    if (field->result_type() == INT_RESULT)
    {
      if (field->val_int() < 128 && field->val_int() > -128)
      {
        *buff++ = CRAM_INT08;
        *((int8_t*)buff) = field->val_int();
      }
      else
      if (field->val_int() < INT_MAX && field->val_int() > INT_MIN)
      {
        *buff++ = CRAM_INT32;
        *((int32_t*)buff) = field->val_int();
      }
      else
      {
        *buff++ = CRAM_INT64;
        *((int64_t*)buff) = field->val_int();
      }
    }
    else
    {
      if (!compressed[col])
      {
        char pad[1024];
        String tmp(pad, sizeof(pad), &my_charset_bin);
        field->val_str(&tmp, &tmp);

        *buff++ = CRAM_TINYSTRING;
        *((uint8_t*)buff) = tmp.length();
        buff += sizeof(uint8_t);
        memmove(buff, tmp.ptr(), tmp.length());
      }
      else
      {
        *buff++ = CRAM_STRING;
        *((uint*)buff) = comp_lengths[col];
        buff += sizeof(uint);
        *((uint*)buff) = real_lengths[col];
        buff += sizeof(uint);
        memmove(buff, compressed[col], comp_lengths[col]);
      }
    }

    if (compressed[col])
      cram_free(compressed[col]);
  }

  return row;
}

void ha_cram::update_list_hints(uint list, CramRow *row)
{
  cram_row_index(cram_table, list, row);
  counter_rows_indexed++;
}

int ha_cram::write_row(uchar *buf)
{
  //cram_debug("%s", __func__);
  // Avoid asserts in val_str() for columns that are not going to be updated
  my_bitmap_map *org_bitmap = dbug_tmp_use_all_columns(table, table->read_set);

  uchar *row = record_place(buf);

  long r; lrand48_r(&cram_rand, &r);
  uint list = r % cram_table->lists_count;

  pthread_mutex_lock(&cram_table->locks[list]);
  list_insert_head(cram_table->lists[list], row);
  update_list_hints(list, row);
  pthread_mutex_unlock(&cram_table->locks[list]);

  dbug_tmp_restore_column_map(table->read_set, org_bitmap);
  counter_rows_written++;
  return 0;
}

int ha_cram::update_row(const uchar *old_data, uchar *new_data)
{
  //cram_debug("%s", __func__);
  // Avoid asserts in val_str() for columns that are not going to be updated
  my_bitmap_map *org_bitmap = dbug_tmp_use_all_columns(table, table->read_set);

  uchar *new_row = record_place(new_data);
  delete_row(old_data);

  list_insert_head(cram_table->lists[cram_list], new_row);
  update_list_hints(cram_list, new_row);

  dbug_tmp_restore_column_map(table->read_set, org_bitmap);
  counter_rows_updated++;
  return 0;
}

int ha_cram::delete_row(const uchar *buf)
{
  //cram_debug("%s", __func__);

  uchar *row = (uchar*) cram_result->payload;

  // We're about to delete cram_result, which would make any
  // later call to rnd_next() perform an invalid ->next read.
  memmove(&cram_node, cram_result, sizeof(node_t));
  cram_result = &cram_node;

  list_delete(cram_table->lists[cram_list], row);
  cram_table->changes[cram_list]++;
  cram_free(row);

  counter_rows_deleted++;
  return 0;
}

bool ha_cram::next_list()
{
  if (cram_list < UINT_MAX)
    pthread_mutex_unlock(&cram_table->locks[cram_list]);

  for (uint i = cram_list+1; i < cram_table->lists_count; i++)
  {
    if (!bmp_chk(cram_lists_done, i) && pthread_mutex_trylock(&cram_table->locks[i]) == 0)
    {
      cram_list = i;
      cram_results = cram_table->lists[i];
      cram_result = cram_results->head;
      bmp_set(cram_lists_done, i);
      return TRUE;
    }
  }
  for (uint i = 0; i < cram_table->lists_count; i++)
  {
    if (!bmp_chk(cram_lists_done, i) && pthread_mutex_trylock(&cram_table->locks[i]) == 0)
    {
      cram_list = i;
      cram_results = cram_table->lists[i];
      cram_result = cram_results->head;
      bmp_set(cram_lists_done, i);
      return TRUE;
    }
  }
  for (uint i = 0; i < cram_table->lists_count; i++)
  {
    if (!bmp_chk(cram_lists_done, i))
    {
      pthread_mutex_lock(&cram_table->locks[i]);
      cram_list = i;
      cram_results = cram_table->lists[i];
      cram_result = cram_results->head;
      bmp_set(cram_lists_done, i);
      return TRUE;
    }
  }
  cram_list    = UINT_MAX;
  cram_result  = NULL;
  cram_results = NULL;
  return FALSE;
}

int ha_cram::rnd_init(bool scan)
{
  cram_debug("%s", __func__);
  rnd_end();

  cram_lists_done = bmp_alloc(cram_table->lists_count);

  return 0;
}

int ha_cram::rnd_end()
{
  cram_debug("%s", __func__);

  if (cram_list < UINT_MAX)
    pthread_mutex_unlock(&cram_table->locks[cram_list]);

  cram_list    = UINT_MAX;
  cram_result  = NULL;
  cram_results = NULL;

  bmp_free(cram_lists_done);
  cram_lists_done = NULL;

  return 0;
}

static bool ecp_check_row(list_t *conds, CramTable *table, CramRow *row)
{
  bool select = TRUE;

  for (node_t *node = conds->head; select && node; node = node->next)
  {
    CramCondition *cc = (CramCondition*) node->payload;

    uchar *field  = cram_field(table, row, cc->column);
    uchar *buffer = cram_field_buffer(field);
    uint length   = cram_field_length(field);

    int64 ni64;

    CramItem *ci = cc->items && cc->items->length ? (CramItem*) cc->items->head->payload: NULL;

    switch (cc->cond)
    {
      case CRAM_ISNULL:
        select = cram_field_type(field) == CRAM_NULL;
        break;

      case CRAM_ISNOTNULL:
        select = cram_field_type(field) != CRAM_NULL;
        break;

      case CRAM_EQ:

        select = FALSE;
        switch (ci->type)
        {
          case CRAM_INT64:
            select = cram_field_int64(field, &ni64) && ni64 == ci->bigint;
            break;

          case CRAM_TINYSTRING:
            select = length == ci->length
              && memcmp(buffer, ci->buffer, length) == 0;
            break;
        }
        break;

      case CRAM_NE:

        select = FALSE;
        switch (ci->type)
        {
          case CRAM_INT64:
            select = cram_field_int64(field, &ni64) && ni64 != ci->bigint;
            break;

          case CRAM_TINYSTRING:
            select = length != ci->length
              || memcmp(buffer, ci->buffer, length < ci->length ? length: ci->length) != 0;
            break;
        }
        break;

      case CRAM_LT:

        select = FALSE;
        switch (ci->type)
        {
          case CRAM_INT64:
            select = cram_field_int64(field, &ni64) && ni64 < ci->bigint;
            break;

          case CRAM_TINYSTRING:
            ni64 = memcmp(buffer, ci->buffer, length < ci->length ? length: ci->length);
            if (ni64 < 0 || (ni64 == 0 && length < ci->length)) select = TRUE;
            break;
        }
        break;

      case CRAM_GT:

        select = FALSE;
        switch (ci->type)
        {
          case CRAM_INT64:
            select = cram_field_int64(field, &ni64) && ni64 > ci->bigint;
            break;

          case CRAM_TINYSTRING:
            ni64 = memcmp(buffer, ci->buffer, length < ci->length ? length: ci->length);
            if (ni64 > 0 || (ni64 == 0 && length > ci->length)) select = TRUE;
            break;
        }
        break;

      case CRAM_LE:

        select = FALSE;
        switch (ci->type)
        {
          case CRAM_INT64:
            select = cram_field_int64(field, &ni64) && ni64 <= ci->bigint;
            break;

          case CRAM_TINYSTRING:
            ni64 = memcmp(buffer, ci->buffer, length < ci->length ? length: ci->length);
            if (ni64 <= 0) select = TRUE;
            break;
        }
        break;

      case CRAM_GE:

        select = FALSE;
        switch (ci->type)
        {
          case CRAM_INT64:
            select = cram_field_int64(field, &ni64) && ni64 >= ci->bigint;
            break;

          case CRAM_TINYSTRING:
            ni64 = memcmp(buffer, ci->buffer, length < ci->length ? length: ci->length);
            if (ni64 >= 0) select = TRUE;
            break;
        }
        break;

      case CRAM_IN:

        select = FALSE;
        for (node_t *inode = cc->items->head; !select && inode; inode = inode->next)
        {
          ci = (CramItem*) inode->payload;

          switch (ci->type)
          {
            case CRAM_INT64:
              select = cram_field_int64(field, &ni64) && ni64 == ci->bigint;
              break;

            case CRAM_TINYSTRING:
              select = length == ci->length
                && memcmp(buffer, ci->buffer, length) == 0;
              break;
          }
        }
        break;
    }
  }
  return select;
}

int ha_cram::rnd_next(uchar *buf)
{
  //cram_debug("%s", __func__);
  for (;;)
  {
    if (cram_result)
      cram_result = cram_result->next;

    if (!cram_result)
    {
      next_list();

      if (cram_results && cram_table->changes[cram_list] > cram_results->length/4)
      {
        for (uint i = 0; i < cram_table->width; i++)
        {
          bmp_t *bmp = cram_table->hints[cram_list][i];
          bmp_all_clr(bmp, cram_table->hints_width);
        }
        for (node_t *node = cram_results->head; node; node = node->next)
        {
          CramRow *row = (CramRow*) node->payload;
          update_list_hints(cram_list, row);
        }
        cram_table->changes[cram_list] = 0;
      }

      if (cram_results && cram_conds && cram_conds->length)
      {
        bool select = TRUE;
        for (node_t *node = cram_conds->head; select && node; node = node->next)
        {
          CramCondition *cc = (CramCondition*) node->payload;
          if (cc->cond == CRAM_EQ || cc->cond == CRAM_IN)
          {
            select = FALSE;
            for (node_t *inode = cc->items->head; !select && inode; inode = inode->next)
            {
              CramItem *ci = (CramItem*) inode->payload;
              switch (ci->type)
              {
                case CRAM_INT64:
                case CRAM_TINYSTRING:
                  select = bmp_chk(cram_table->hints[cram_list][cc->column], ci->hashval % cram_table->hints_width);
                  break;
              }
            }
          }
        }
        if (!select)
        {
          cram_result = NULL;
          continue;
        }
      }
    }

    if (!cram_result)
      break;

    counter_rows_touched++;

    if (cram_conds && cram_conds->length)
    {
      CramRow *row = (CramRow*) cram_result->payload;
      if (!ecp_check_row(cram_conds, cram_table, row))
        continue;
    }

    break;
  }

  if (cram_result)
    counter_rows_selected++;

  return record_store(buf);
}

int ha_cram::index_init(uint idx, bool sorted)
{
  cram_debug("%s", __func__);
  return HA_ERR_WRONG_COMMAND;
}

int ha_cram::index_read(uchar * buf, const uchar * key, uint key_len, enum ha_rkey_function find_flag)
{
  cram_debug("%s", __func__);
  return HA_ERR_WRONG_COMMAND;
}

int ha_cram::index_end()
{
  cram_debug("%s", __func__);
  return HA_ERR_WRONG_COMMAND;
}

void ha_cram::position(const uchar *record)
{
  //cram_debug("%s", __func__);

  use_trash();

  CramPosition *cp = (CramPosition*) ref;

  cp->list = cram_list;
  cp->row  = (CramRow*) cram_result->payload;
}

int ha_cram::rnd_pos(uchar *buf, uchar *pos)
{
  //cram_debug("%s", __func__);

  if (cram_list < UINT_MAX)
    pthread_mutex_unlock(&cram_table->locks[cram_list]);

  CramPosition *cp = (CramPosition*)pos;

  cram_list = cp->list;
  pthread_mutex_lock(&cram_table->locks[cram_list]);
  cram_results = cram_table->lists[cram_list];
  cram_result = list_locate(cram_results, cp->row);

  if (!cram_result)
  {
    pthread_mutex_unlock(&cram_table->locks[cram_list]);
    cram_list    = UINT_MAX;
    cram_results = NULL;
    cram_result  = NULL;
    return HA_ERR_RECORD_DELETED;
  }

  return record_store(buf);
}

int ha_cram::info(uint flag)
{
  cram_debug("%s", __func__);

  if (flag & HA_STATUS_VARIABLE)
  {
    stats.records = cram_table->row_count;
    stats.deleted = 0;
    stats.data_file_length  = cram_table->data_size;
    stats.index_file_length = cram_table->meta_size;
    stats.mean_rec_length   = cram_table->row_count ? cram_table->data_size / cram_table->row_count: 0;
  }

  return 0;
}

void ha_cram::clear_state()
{
  cram_results = NULL;
  cram_result  = NULL;
  cram_list    = UINT_MAX;
  counter_rows_touched  = 0;
  counter_rows_selected = 0;
  counter_rows_indexed  = 0;
  counter_rows_written  = 0;
  counter_rows_updated  = 0;
  counter_rows_deleted  = 0;
  pthread_mutex_lock(&cram_seed_lock);
  srand48_r(cram_seed++, &cram_rand);
  pthread_mutex_unlock(&cram_seed_lock);
}

int ha_cram::reset()
{
  cram_debug("%s rows t %llu s %llu i %llu w %llu u %llu d %llu",
    __func__,
    counter_rows_touched,
    counter_rows_selected,
    counter_rows_indexed,
    counter_rows_written,
    counter_rows_updated,
    counter_rows_deleted);
  empty_trash();
  empty_conds();
  clear_state();
  return 0;
}

int ha_cram::external_lock(THD *thd, int lock_type)
{
  cram_debug("%s", __func__);
  return 0;
}

int ha_cram::delete_table(const char *name)
{
  cram_debug("%s %s", __func__, name);

  pthread_mutex_lock(&cram_tables_lock);

  CramTable *table = cram_table_open(name, 0);

  if (table && !table->dropping)
  {
    table->users++;
    table->dropping = TRUE;
    while (table->users > 1)
    {
      pthread_mutex_unlock(&cram_tables_lock);
      usleep(1000);
      pthread_mutex_lock(&cram_tables_lock);
    }
    cram_table_drop(table, TRUE);
  }

  pthread_mutex_unlock(&cram_tables_lock);

  return 0;
}

int ha_cram::rename_table(const char *from, const char *to)
{
  cram_debug("%s %s %s", __func__, from, to);

  pthread_mutex_lock(&cram_tables_lock);

  CramTable *table = cram_table_open(from, 0);

  if (table)
  {
    if (cram_table != table)
      table->users++;

    while (table->users > 1)
    {
      pthread_mutex_unlock(&cram_tables_lock);
      usleep(1000);
      pthread_mutex_lock(&cram_tables_lock);
    }

    cram_free(table->name);
    table->name = (char*) cram_alloc(strlen(to)+1);
    strcpy(table->name, to);

    char oname[1024], nname[1024];
    snprintf(oname, sizeof(oname), "%s.cram", from);
    snprintf(nname, sizeof(nname), "%s.cram", to);
    rename(oname, nname);

    if (cram_table != table)
      table->users--;
  }

  pthread_mutex_unlock(&cram_tables_lock);
  checkpoint_asap = TRUE;
  return 0;
}

int ha_cram::create(const char *name, TABLE *table_arg, HA_CREATE_INFO *create_info)
{
  cram_debug("%s %s", __func__, name);
  return 0;
}

bool ha_cram::check_if_incompatible_data(HA_CREATE_INFO *info, uint table_changes)
{
  cram_debug("%s", __func__);
  return COMPATIBLE_DATA_NO;
}

void ha_cram::check_condition ( const COND * cond )
{
  char pad[1024];
  String *str, tmp(pad, sizeof(pad), &my_charset_bin);

  if (cond->type() == COND::FUNC_ITEM)
  {
    Item_func *func = (Item_func*)cond;
    Item **args = func->arguments();

    if (func->argument_count() == 1
      && (func->functype() == Item_func::ISNULL_FUNC || func->functype() == Item_func::ISNOTNULL_FUNC))
    {
      Item_field *fld = (Item_field*)args[0];

      CramCondition *cc = (CramCondition*) cram_alloc(sizeof(CramCondition));
      list_insert_head(cram_conds, cc);

      cc->cond   = func->functype() == Item_func::ISNULL_FUNC ? CRAM_ISNULL: CRAM_ISNOTNULL;
      cc->column = fld->field->field_index;
      cc->items  = NULL;

      cram_debug("%s ECP [IS NOT] NULL", __func__);
    }

    else
    if ( func->argument_count() == 2
      && args[0]->type() == COND::FIELD_ITEM
      && args[1]->const_item()
      && !args[1]->is_null()
      && (args[1]->result_type() == INT_RESULT || args[1]->result_type() == STRING_RESULT)
      && ( func->functype() == Item_func::EQ_FUNC
        || func->functype() == Item_func::NE_FUNC
        || func->functype() == Item_func::LT_FUNC
        || func->functype() == Item_func::GT_FUNC
        || func->functype() == Item_func::LE_FUNC
        || func->functype() == Item_func::GE_FUNC
      ))
    {
      Item *arg = args[1];
      Item_field *fld = (Item_field*)args[0];

      CramCondition *cc = (CramCondition*) cram_alloc(sizeof(CramCondition));
      list_insert_head(cram_conds, cc);

      cc->column = fld->field->field_index;
      cc->items  = list_alloc();

      switch (func->functype()) {
        case Item_func::EQ_FUNC: cc->cond = CRAM_EQ; break;
        case Item_func::NE_FUNC: cc->cond = CRAM_NE; break;
        case Item_func::LT_FUNC: cc->cond = CRAM_LT; break;
        case Item_func::GT_FUNC: cc->cond = CRAM_GT; break;
        case Item_func::LE_FUNC: cc->cond = CRAM_LE; break;
        case Item_func::GE_FUNC: cc->cond = CRAM_GE; break;
        default: break;
      }

      CramItem *ci = (CramItem*) cram_alloc(sizeof(CramItem));
      list_insert_head(cc->items, ci);

      if (arg->result_type() == INT_RESULT)
      {
        ci->type    = CRAM_INT64;
        ci->bigint  = arg->val_int();
        ci->hashval = cram_hash_int64(ci->bigint);
        cram_debug("%s ECP EQ/NE/LT/GT/LE/GE INT %lld", __func__, ci->bigint);
      }
      else
      if ((str = arg->val_str(&tmp)) && str->length() < cram_table->compress_boundary)
      {
        ci->type = CRAM_TINYSTRING;
        ci->length = str->length();
        memmove(ci->buffer, str->ptr(), str->length());
        ci->hashval = cram_hash(ci->buffer, ci->length);
        cram_debug("%s ECP EQ/NE/LT/GT/LE/GE STR %lld", __func__, ci->length);
      }
    }

    else
    if (func->argument_count() > 1
      && args[0]->type() == COND::FIELD_ITEM
      && func->functype() == Item_func::IN_FUNC)
    {
      Item_field *fld = (Item_field*)args[0];

      for (uint ai = 1; ai < func->argument_count(); ai++)
      {
        if (!args[ai]->const_item())
          return;
        if (args[ai]->result_type() != INT_RESULT && args[ai]->result_type() != STRING_RESULT)
          return;
      }

      CramCondition *cc = (CramCondition*) cram_alloc(sizeof(CramCondition));
      list_insert_head(cram_conds, cc);

      cc->column = fld->field->field_index;
      cc->items  = list_alloc();
      cc->cond   = CRAM_IN;

      for (uint ai = 1; ai < func->argument_count(); ai++)
      {
        Item *arg = args[ai];

        if (arg->is_null())
          continue;

        CramItem *ci = (CramItem*) cram_alloc(sizeof(CramItem));
        list_insert_head(cc->items, ci);

        if (arg->result_type() == INT_RESULT)
        {
          ci->type    = CRAM_INT64;
          ci->bigint  = arg->val_int();
          ci->hashval = cram_hash_int64(ci->bigint);
        }
        else
        if ((str = arg->val_str(&tmp)) && str->length() < cram_table->compress_boundary)
        {
          ci->type = CRAM_TINYSTRING;
          ci->length = str->length();
          memmove(ci->buffer, str->ptr(), str->length());
          ci->hashval = cram_hash(ci->buffer, ci->length);
        }
      }
      cram_debug("%s ECP IN %lld", __func__, cc->items->length);
    }
  }
}

const COND * ha_cram::cond_push ( const COND * cond )
{
  cram_debug("%s", __func__);

  empty_conds();
  use_conds();

  if (cond->type() == COND::COND_ITEM)
  {
    Item_cond *ic = (Item_cond*)cond;
    cram_debug("ECP %s", ic->func_name());

    if (ic->functype() == Item_func::COND_AND_FUNC)
    {
      List<Item>* arglist= ic->argument_list();
      List_iterator<Item> li(*arglist);

      for (uint i = 0; i < arglist->elements; i++)
        check_condition(li++);
    }
  }
  else
  {
    check_condition(cond);
  }

  return cram_conds->length ? NULL: cond;
}

THR_LOCK_DATA **ha_cram::store_lock(THD *thd, THR_LOCK_DATA **to, enum thr_lock_type lock_type)
{
  if (lock_type != TL_IGNORE && lock.type == TL_UNLOCK)
  {
    /*
      If TL_UNLOCK is set
      If we are not doing a LOCK TABLE or DISCARD/IMPORT
      TABLESPACE, then allow multiple writers
    */

    if ((lock_type >= TL_WRITE_CONCURRENT_INSERT &&
         lock_type <= TL_WRITE) && !thd->in_lock_tables
        && !thd->tablespace_op)
      lock_type = TL_WRITE_ALLOW_WRITE;

    /*
      In queries of type INSERT INTO t1 SELECT ... FROM t2 ...
      MySQL would use the lock TL_READ_NO_INSERT on t2, and that
      would conflict with TL_WRITE_ALLOW_WRITE, blocking all inserts
      to t2. Convert the lock to a normal read lock to allow
      concurrent inserts to t2.
    */

    if (lock_type == TL_READ_NO_INSERT && !thd->in_lock_tables)
      lock_type = TL_READ;
  }
  *to++ = &lock;
  return to;
}

struct st_mysql_storage_engine cram_storage_engine= { MYSQL_HANDLERTON_INTERFACE_VERSION };

static void cram_verbose_update(THD * thd, struct st_mysql_sys_var *sys_var, void *var, const void *save)
{
  uint n = *((uint*)save);
  *((uint*)var) = n;
  cram_verbose = n;
}

static void cram_table_lists_update(THD * thd, struct st_mysql_sys_var *sys_var, void *var, const void *save)
{
  uint n = *((uint*)save);
  *((uint*)var) = n;
  cram_table_lists = n;
}

static void cram_table_list_hints_update(THD * thd, struct st_mysql_sys_var *sys_var, void *var, const void *save)
{
  uint n = *((uint*)save);
  *((uint*)var) = n;
  cram_table_list_hints = n;
}

static void cram_compress_boundary_update(THD * thd, struct st_mysql_sys_var *sys_var, void *var, const void *save)
{
  uint n = *((uint*)save);
  *((uint*)var) = n;
  cram_compress_boundary = n;
}

static void cram_checkpoint_seconds_update(THD * thd, struct st_mysql_sys_var *sys_var, void *var, const void *save)
{
  uint n = *((uint*)save);
  *((uint*)var) = n;
  cram_checkpoint_seconds = n;
}

static void cram_checkpoint_threads_update(THD * thd, struct st_mysql_sys_var *sys_var, void *var, const void *save)
{
  uint n = *((uint*)save);
  *((uint*)var) = n;
  cram_checkpoint_threads = n;
}

static void cram_worker_threads_update(THD * thd, struct st_mysql_sys_var *sys_var, void *var, const void *save)
{
  uint n = *((uint*)save);
  *((uint*)var) = n;
  cram_worker_threads = n;
}

static MYSQL_SYSVAR_UINT(verbose, cram_verbose, 0,
  "Debug noise to stderr.", 0, cram_verbose_update, 0, 0, 1, 1);

static MYSQL_SYSVAR_UINT(table_lists, cram_table_lists, 0,
  "Partitions per table.", 0, cram_table_lists_update, 1024, 8, UINT_MAX, 1);

static MYSQL_SYSVAR_UINT(table_list_hints, cram_table_list_hints, 0,
  "Width of table list hints bitmap.", 0, cram_table_list_hints_update, 512, 128, UINT_MAX, 1);

static MYSQL_SYSVAR_UINT(compress_boundary, cram_compress_boundary, 0,
  "Compress strings longer than N bytes.", 0, cram_compress_boundary_update, 128, 32, 256, 1);

static MYSQL_SYSVAR_UINT(checkpoint_interval, cram_checkpoint_seconds, 0,
  "Checkpoint interval.", 0, cram_checkpoint_seconds_update, 60, 10, 300, 1);

static MYSQL_SYSVAR_UINT(checkpoint_threads, cram_checkpoint_threads, 0,
  "Checkpoint threads.", 0, cram_checkpoint_threads_update, 4, 2, 32, 1);

static MYSQL_SYSVAR_UINT(worker_threads, cram_worker_threads, 0,
  "Workers threads per connection.", 0, cram_worker_threads_update, 2, 2, 32, 1);

static struct st_mysql_sys_var *cram_system_variables[] = {
    MYSQL_SYSVAR(verbose),
    MYSQL_SYSVAR(table_lists),
    MYSQL_SYSVAR(table_list_hints),
    MYSQL_SYSVAR(compress_boundary),
    MYSQL_SYSVAR(checkpoint_interval),
    MYSQL_SYSVAR(checkpoint_threads),
    MYSQL_SYSVAR(worker_threads),
    NULL
};

static struct st_mysql_show_var func_status[]=
{
  { "cram_checkpoint_duration_usec", (char*)&cram_checkpoint_duration_usec, SHOW_ULONGLONG },
  { 0,0,SHOW_UNDEF }
};

struct st_mysql_daemon unusable_cram=
{ MYSQL_DAEMON_INTERFACE_VERSION };

mysql_declare_plugin(cram)
{
  MYSQL_STORAGE_ENGINE_PLUGIN,
  &cram_storage_engine,
  "CRAM",
  "Sean Pringle, Wikimedia Foundation",
  "Cram everything into memory!",
  PLUGIN_LICENSE_GPL,
  cram_init_func,                               /* Plugin Init */
  cram_done_func,                               /* Plugin Deinit */
  0x0001 /* 0.1 */,
  func_status,                                  /* status variables */
  cram_system_variables,                        /* system variables */
  NULL,                                         /* config options */
  0,                                            /* flags */
}
mysql_declare_plugin_end;
maria_declare_plugin(cram)
{
  MYSQL_STORAGE_ENGINE_PLUGIN,
  &cram_storage_engine,
  "CRAM",
  "Sean Pringle, Wikimedia Foundation",
  "Cram everything into memory!",
  PLUGIN_LICENSE_GPL,
  cram_init_func,                               /* Plugin Init */
  cram_done_func,                               /* Plugin Deinit */
  0x0001,                                       /* version number (0.1) */
  func_status,                                  /* status variables */
  cram_system_variables,                        /* system variables */
  "0.1",                                        /* string version */
  MariaDB_PLUGIN_MATURITY_EXPERIMENTAL          /* maturity */
},
{
  MYSQL_DAEMON_PLUGIN,
  &unusable_cram,
  "CRAM UNUSABLE",
  "Sean Pringle",
  "Unusable Engine",
  PLUGIN_LICENSE_GPL,
  NULL,                                         /* Plugin Init */
  NULL,                                         /* Plugin Deinit */
  0x0100,                                       /* version number (1.00) */
  NULL,                                         /* status variables */
  NULL,                                         /* system variables */
  "1.00",                                       /* version, as a string */
  MariaDB_PLUGIN_MATURITY_EXPERIMENTAL          /* maturity */
}
maria_declare_plugin_end;
