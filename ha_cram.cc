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

static handler *cram_create_handler(handlerton *hton, TABLE_SHARE *table, MEM_ROOT *mem_root);

handlerton *cram_hton;

struct ha_table_option_struct{};
struct ha_field_option_struct{};
ha_create_table_option cram_table_option_list[] = { HA_TOPTION_END };
ha_create_table_option cram_field_option_list[] = { HA_FOPTION_END };

uint cram_verbose;

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

#define cram_debug(...) if (cram_verbose) cram_note(__VA_ARGS__)

static void* cram_alloc(size_t bytes)
{
  void *ptr = malloc(bytes);
  if (!ptr)
  {
    cram_error("malloc failed %llu bytes", bytes);
    abort();
  }
  memset(ptr, 0, bytes);
  return ptr;
}

static void cram_free(void *ptr)
{
  free(ptr);
}

static size_t cram_deflate(uchar *data, size_t width)
{
  size_t length = width, comp_len = 0;
  return (my_compress(data, &length, &comp_len) == 0 && length < width) ? length: UINT_MAX;
}

static size_t cram_inflate(uchar *data, size_t width, size_t limit)
{
  size_t a = width, b = limit;
  return my_uncompress(data, a, &b) != 0 ? UINT_MAX: limit;
}

static THR_LOCK mysql_lock;
static uint64 cram_epoch;
static FILE *cram_epoch_file;
static uint64 cram_epoch_eof;
static pthread_mutex_t cram_epoch_mutex;

/* A global hash table that holds actual data BLOBs */
static CramHash cram_hash;
static uint cram_hash_chains;
static uint cram_hash_locks;

/* List of open tables */
static CramTable *cram_tables;
static CramWorker *cram_workers;
static pthread_mutex_t cram_tables_mutex;

/* Every table and record gets an id */
static uint64 cram_sequence;
static pthread_spinlock_t cram_sequence_spinlock;

/* General config vars */
static uint cram_worker_threads;
static uint cram_force_start;
static uint cram_strict_write;
static uint cram_flush_level;
static uint cram_page_rows;
static uint cram_compress_log;

/* Stats counters passed up to mysqld as SHOW_ULONGLONG */
static pthread_spinlock_t cram_stats_spinlock;
static ulonglong cram_ecp_steps;
static ulonglong cram_ecp_matches;
static ulonglong cram_tables_created;
static ulonglong cram_tables_deleted;
static ulonglong cram_tables_renamed;
static ulonglong cram_pages_created;
static ulonglong cram_pages_deleted;
static ulonglong cram_rows_created;
static ulonglong cram_rows_deleted;
static ulonglong cram_rows_inserted;
static ulonglong cram_rows_appended;
static ulonglong cram_hash_reads;
static ulonglong cram_hash_inserts;
static ulonglong cram_hash_deletes;
static ulonglong cram_log_writes_queued;
static ulonglong cram_log_writes_completed;

/* Background worker thread job list */
static CramJob *cram_job;
static pthread_mutex_t cram_job_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t cram_job_cond = PTHREAD_COND_INITIALIZER;

static int cram_log_create(CramTable *table);
static int cram_init_create(uchar *data);
static int cram_log_rename(const char *old_name, const char *new_name);
static int cram_init_rename(uchar *data);
static int cram_log_drop(const char *name);
static int cram_init_drop(uchar *data);
static int cram_log_insert(CramTable *table, CramRow *row);
static int cram_init_insert(uchar *data);
static int cram_log_delete(CramTable *table, uint64 id);
static int cram_init_delete(uchar *data);

static void cram_stat_add(pthread_spinlock_t *spin, ulonglong *var, ulonglong n)
{
  pthread_spin_lock(spin);
  *var += n;
  pthread_spin_unlock(spin);
}

static void cram_stat_inc(pthread_spinlock_t *spin, ulonglong *var)
{
  cram_stat_add(spin, var, 1);
}

/* DJBX33A */
static uint32_t cram_hash_calc(uchar *buffer, uint32 length)
{
  uint32 hash = 5381, i = 0;
  for (
    length = length > 1024 ? 1024: length, i = 0;
    i < length;
    hash = hash * 33 + buffer[i++]
  );
  return hash;
}

static CramBlob* cram_get(uchar *buffer, uint32 length)
{
  uint32_t hash = cram_hash_calc(buffer, length);
  ulonglong chain = hash % cram_hash_chains;
  ulonglong lock  = hash % cram_hash_locks;

  pthread_mutex_lock(&cram_hash.mutexes[lock]);

  CramBlob *n = cram_hash.chains[chain];

  while (n && (length != n->length || memcmp(buffer, n->buffer, length)))
    n = n->next;

  pthread_mutex_unlock(&cram_hash.mutexes[lock]);

  cram_stat_inc(&cram_stats_spinlock, &cram_hash_reads);

  return n;
}

static CramBlob* cram_incref(uchar *buffer, uint32 length)
{
  bool created = FALSE;

  uint32_t hash = cram_hash_calc(buffer, length);
  ulonglong chain = hash % cram_hash_chains;
  ulonglong lock  = hash % cram_hash_locks;

  pthread_mutex_lock(&cram_hash.mutexes[lock]);

  CramBlob *n = cram_hash.chains[chain];

  while (n && (length != n->length || memcmp(buffer, n->buffer, length)))
    n = n->next;

  if (!n)
  {
    n = (CramBlob*) cram_alloc(sizeof(CramBlob));

    n->buffer = (uchar*) cram_alloc(length);
    memmove(n->buffer, buffer, length);

    n->length = length;

    n->next = cram_hash.chains[chain];
    cram_hash.chains[chain] = n;
    created = TRUE;
  }

  n->count++;

  pthread_mutex_unlock(&cram_hash.mutexes[lock]);

  cram_stat_inc(&cram_stats_spinlock, &cram_hash_reads);
  if (created) cram_stat_inc(&cram_stats_spinlock, &cram_hash_inserts);

  return n;
}

static int cram_decref(uchar *buffer, uint32 length)
{
  bool deleted = FALSE;
  uint32_t hash = cram_hash_calc(buffer, length);
  ulonglong chain = hash % cram_hash_chains;
  ulonglong lock  = hash % cram_hash_locks;

  pthread_mutex_lock(&cram_hash.mutexes[lock]);

  CramBlob **n = &cram_hash.chains[chain];

  while (n && (*n) && (length != (*n)->length || memcmp(buffer, (*n)->buffer, length)))
    n = &(*n)->next;

  int rc = -1;

  if (n && *n)
  {
    (*n)->count--;
    if ((*n)->count == 0)
    {
      CramBlob *f = (*n);
      *n = (*n)->next;
      cram_free(f->buffer);
      cram_free(f);
      deleted = TRUE;
    }
    rc = 0;
  }

  pthread_mutex_unlock(&cram_hash.mutexes[lock]);

  cram_stat_inc(&cram_stats_spinlock, &cram_hash_reads);
  if (deleted) cram_stat_inc(&cram_stats_spinlock, &cram_hash_deletes);

  return rc;
}

void cram_field_set(CramBlob **blobs, uint index, Field *field)
{
  if (blobs[index])
  {
    cram_decref(blobs[index]->buffer, blobs[index]->length);
  }

  if (field->is_null())
  {
    blobs[index] = NULL;
  }
  else
  if (field->result_type() == INT_RESULT)
  {
    int64 n = field->val_int();
    blobs[index] = cram_incref((uchar*)(&n), sizeof(int64));
  }
  else
  {
    char pad[1024];
    String tmp(pad, sizeof(pad), &my_charset_bin);
    field->val_str(&tmp, &tmp);
    blobs[index] = cram_incref((uchar*)tmp.ptr(), tmp.length());
  }
}

static void cram_page_free(CramTable *table, CramPage *page)
{
  if (page->prev)
  {
    page->prev->next = page->next;
  }
  for (uint i = 0; i < CRAM_JOBS; i++)
  {
    if (table->lists[i].first == page)
    {
      table->lists[i].first = page->next;
    }
    if (table->lists[i].last == page)
    {
      table->lists[i].last = page->prev;
    }
  }
  pthread_rwlock_destroy(&page->lock);
  cram_free(page);

  cram_stat_inc(&cram_stats_spinlock, &cram_pages_deleted);
}

static CramRow* cram_row_create(CramTable *table, uint64 id, CramBlob **blobs, bool log_entry, uint insert_mode)
{
  CramRow *row = NULL;
  CramPage *page = NULL;
  CramList *list = NULL;
  bool page_created = FALSE;

  if (!id)
  {
    pthread_spin_lock(&cram_sequence_spinlock);
    id = cram_sequence++;
    pthread_spin_unlock(&cram_sequence_spinlock);
  }

  if (!blobs)
  {
    blobs = (CramBlob**) cram_alloc(sizeof(CramBlob*) * table->columns);
  }

  uint i, l = 0;
  pthread_spin_lock(&table->lists[0].spinlock);
  uint64 rows = table->lists[0].rows;
  pthread_spin_unlock(&table->lists[0].spinlock);

  for (i = 1; i < CRAM_JOBS; i++)
  {
    pthread_spin_lock(&table->lists[i].spinlock);
    if (table->lists[i].rows < rows)
    {
      l = i;
      rows = table->lists[i].rows;
    }
    pthread_spin_unlock(&table->lists[i].spinlock);
  }
  list = &table->lists[l];

  pthread_rwlock_rdlock(&list->lock);

  CramPage *p = list->first;

  for (; p && !row; p = p->next)
  {
    if (pthread_rwlock_trywrlock(&p->lock) == 0)
    {
      if (p->count < cram_page_rows)
      {
        for (uint j = 0; !row && j < cram_page_rows; j++)
        {
          if (!p->rows[j].blobs)
          {
            page = p;
            row = &page->rows[j];
            goto done;
          }
        }
      }
      pthread_rwlock_unlock(&p->lock);
    }
    if (insert_mode == CRAM_APPEND)
    {
      break;
    }
  }

  pthread_rwlock_unlock(&list->lock);
  pthread_rwlock_wrlock(&list->lock);

  page = (CramPage*) cram_alloc(sizeof(CramPage));
  pthread_rwlock_init(&page->lock, NULL);
  pthread_rwlock_wrlock(&page->lock);
  page_created = TRUE;

  if (!list->last) list->last = page;
  page->next = list->first;
  list->first = page;
  list->pages++;
  if (page->next) page->next->prev = page;

  page->list = list - table->lists;

  cram_stat_inc(&cram_stats_spinlock, &cram_pages_created);

  row = &page->rows[0];

done:

  page->count++;
  row->id = id;
  row->blobs = blobs;

  if (log_entry)
  {
    cram_log_insert(table, row);
  }

  pthread_rwlock_unlock(&page->lock);
  pthread_rwlock_unlock(&list->lock);

  pthread_spin_lock(&list->spinlock);
  list->rows++;
  if (page_created) list->pages++;
  pthread_spin_unlock(&list->spinlock);

  cram_stat_inc(&cram_stats_spinlock, &cram_rows_created);

  if (insert_mode == CRAM_INSERT) cram_stat_inc(&cram_stats_spinlock, &cram_rows_inserted);
  if (insert_mode == CRAM_APPEND) cram_stat_inc(&cram_stats_spinlock, &cram_rows_appended);

  return row;
}

static int cram_row_free(CramTable *table, CramPage *page, CramRow *row, bool log_entry)
{
  int rc = HA_ERR_RECORD_DELETED;

  CramList *list = &table->lists[page->list];

  uint64 id = 0;
  CramBlob **blobs = NULL;

  pthread_rwlock_rdlock(&list->lock);
  pthread_rwlock_wrlock(&page->lock);

  if (row->blobs)
  {
    id = row->id;
    blobs = row->blobs;
    row->blobs = NULL;
    page->count--;
    pthread_spin_lock(&list->spinlock);
    list->rows--;
    pthread_spin_unlock(&list->spinlock);
  }

  pthread_rwlock_unlock(&page->lock);
  pthread_rwlock_unlock(&list->lock);

  if (blobs)
  {
    if (log_entry)
    {
      cram_log_delete(table, id);
    }
    for (uint i = 0; i < table->columns; i++)
    {
      if (blobs[i])
      {
        cram_decref(blobs[i]->buffer, blobs[i]->length);
      }
    }
    cram_free(blobs);
    cram_stat_inc(&cram_stats_spinlock, &cram_rows_deleted);
    rc = 0;
  }

  return rc;
}

static CramTable* cram_table_open(const char *name, uint32 columns, uint64 id)
{
  bool created = FALSE;

  if (!id)
  {
    pthread_spin_lock(&cram_sequence_spinlock);
    id = cram_sequence++;
    pthread_spin_unlock(&cram_sequence_spinlock);
  }

  pthread_mutex_lock(&cram_tables_mutex);

  CramTable *table = cram_tables;
  while (table && strcmp(table->name, name))
  {
    table = table->next;
  }

  if (!table)
  {
    table = (CramTable*) cram_alloc(sizeof(CramTable));
    pthread_rwlock_init(&table->lock, NULL);
    pthread_spin_init(&table->spinlock, PTHREAD_PROCESS_PRIVATE);

    table->name = (char*) cram_alloc(strlen(name)+1);
    strcpy(table->name, name);

    table->id = id;
    table->columns = columns;

    for (uint i = 0; i < CRAM_JOBS; i++)
    {
      pthread_rwlock_init(&table->lists[i].lock, NULL);
      pthread_spin_init(&table->lists[i].spinlock, PTHREAD_PROCESS_PRIVATE);
    }

    table->next = cram_tables;
    cram_tables = table;
    created = TRUE;
  }

  pthread_mutex_unlock(&cram_tables_mutex);

  if (created)
  {
    cram_stat_inc(&cram_stats_spinlock, &cram_tables_created);
  }

  return table;
}

static int cram_table_drop(const char *name)
{
  int rc = -1;

  pthread_mutex_lock(&cram_tables_mutex);
  CramTable **table = &cram_tables;
  while (table && *table && strcmp(name, (*table)->name) != 0)
  {
    table = &((*table)->next);
  }

  if (table && *table)
  {
    CramTable *t = (*table);
    *table = (*table)->next;
    pthread_rwlock_wrlock(&t->lock);

    for (uint i = 0; i < CRAM_JOBS; i++)
    {
      while (t->lists[i].first)
      {
        for (uint j = 0; j < cram_page_rows; j++)
        {
          if (t->lists[i].first->rows[j].blobs)
          {
            cram_row_free(t, t->lists[i].first, &t->lists[i].first->rows[j], CRAM_NO_LOG);
          }
        }
        cram_page_free(t, t->lists[i].first);
      }
    }

    for (uint i = 0; i < CRAM_JOBS; i++)
    {
      pthread_rwlock_destroy(&t->lists[i].lock);
      pthread_spin_destroy(&t->lists[i].spinlock);
    }

    pthread_rwlock_destroy(&t->lock);
    cram_free(t->name);
    cram_free(t);

    rc = 0;
  }
  pthread_mutex_unlock(&cram_tables_mutex);

  cram_stat_inc(&cram_stats_spinlock, &cram_tables_deleted);

  return rc;
}

static int cram_table_rename(const char *from, const char *to)
{
  pthread_mutex_lock(&cram_tables_mutex);
  CramTable *table = cram_tables;
  while (table && strcmp(table->name, from))
  {
    table = table->next;
  }
  if (table)
  {
    pthread_rwlock_wrlock(&table->lock);
    cram_free(table->name);
    table->name = (char*) cram_alloc(strlen(to)+1);
    strcpy(table->name, to);
    pthread_rwlock_unlock(&table->lock);
  }
  pthread_mutex_unlock(&cram_tables_mutex);

  cram_stat_inc(&cram_stats_spinlock, &cram_tables_renamed);

  return table ? 0: -1;
}

static CramTable* cram_table_by_id(uint64 id)
{
  CramTable *table = cram_tables;
  while (table && table->id != id)
  {
    table = table->next;
  }
  return table;
}

static int cram_epoch_create()
{
  char name[1024];
  snprintf(name, sizeof(name), CRAM_FILE, cram_epoch++);

  if (cram_epoch_file)
  {
    fclose(cram_epoch_file);
  }
  cram_epoch_file = fopen(name, "ab");
  cram_epoch_eof  = 0;

  return cram_epoch_file ? 0: -1;
}

static pthread_t cram_writer_thread;
static pthread_spinlock_t cram_log_events_spinlock;
static CramLogEvent *cram_log_events;
static bool cram_writer_done;

static void* cram_writer(void *p)
{
  cram_note("writer started");

  for (;;)
  {
    pthread_spin_lock(&cram_log_events_spinlock);
    CramLogEvent *event = cram_log_events;
    cram_log_events = NULL;
    pthread_spin_unlock(&cram_log_events_spinlock);

    for (; event && event->next; event = event->next);

    if (!event)
    {
      fflush(cram_epoch_file);
      usleep(1000);
    }

    if (!event && cram_writer_done)
    {
      break;
    }

    while (event)
    {
      uchar *data  = event->data;
      size_t width = event->width;

      size_t cwidth_offset  = 0;
      size_t uwidth_offset  = cwidth_offset + sizeof(uint64);
      size_t payload_offset = uwidth_offset + sizeof(uint64);
      size_t write_bytes    = payload_offset + width;

      uchar *buffer  = (uchar*) cram_alloc(write_bytes),
            *payload = &buffer[payload_offset];

      memmove(payload, data, width);
      *((uint64*)&buffer[cwidth_offset]) = 0;
      *((uint64*)&buffer[uwidth_offset]) = width;

      size_t length;
      if (cram_compress_log && (length = cram_deflate(payload, width)) && length < width)
      {
        *((uint64*)&buffer[cwidth_offset]) = length;
        write_bytes = payload_offset + length;
      }
      else
      {
        memmove(payload, data, width);
      }

      pthread_mutex_lock(&cram_epoch_mutex);

      if (cram_epoch_eof + write_bytes > CRAM_EPOCH)
      {
        cram_epoch_create();
      }

      size_t written; uint tries;
      for (written = 0, tries = 0; written < write_bytes && tries < 5;
        written += fwrite(&buffer[written], 1, write_bytes - written, cram_epoch_file), tries++
      );

      if (written < write_bytes)
      {
        cram_error("failed to write log entry, error %llu", errno);
        if (cram_strict_write) abort();
      }

      cram_epoch_eof += write_bytes;

      if (cram_flush_level > 0 && (cram_log_writes_completed % cram_flush_level) == 0)
      {
        fflush(cram_epoch_file);
      }

      pthread_mutex_unlock(&cram_epoch_mutex);

      CramLogEvent *prev = event->prev;
      cram_free(buffer);
      cram_free(event->data);
      cram_free(event);
      event = prev;

      cram_stat_inc(&cram_stats_spinlock, &cram_log_writes_completed);
    }
  }

  fflush(cram_epoch_file);
  cram_note("writer stopped");
  return NULL;
}

static void cram_writer_start()
{
  pthread_create(&cram_writer_thread, NULL, cram_writer, NULL);
}

static void cram_writer_stop()
{
  cram_writer_done = TRUE;
  pthread_join(cram_writer_thread, NULL);
}

static int cram_log_entry(uchar *data, size_t width)
{
  CramLogEvent *event = (CramLogEvent*) cram_alloc(sizeof(CramLogEvent));
  event->data = (uchar*) cram_alloc(width);
  memmove(event->data, data, width);
  event->width = width;

  pthread_spin_lock(&cram_log_events_spinlock);
  event->next = cram_log_events;
  cram_log_events = event;
  if (event->next) event->next->prev = event;
  pthread_spin_unlock(&cram_log_events_spinlock);

  cram_stat_inc(&cram_stats_spinlock, &cram_log_writes_queued);

  return 0;
}

static uint cram_log_string_counted(uchar *data, const char *str)
{
  data[0] = strlen(str);
  memmove(&data[1], str, data[0]);
  return data[0] + sizeof(uchar);
}

static uint cram_init_string_counted(uchar *data, char *str)
{
  uchar len = *data++;
  memmove(str, data, len);
  str[len] = 0;
  return len+1;
}

/*
uchar type
uint64 columns
uint64 table id
uchar length
char* name
*/

static int cram_log_create(CramTable *table)
{
  uchar data[1024];
  uint64 offset = 0;

  *(( uchar*)&data[offset]) = CRAM_ENTRY_CREATE;
  offset += sizeof(uchar);

  *((uint32*)&data[offset]) = table->columns;
  offset += sizeof(uint32);

  *((uint64*)&data[offset]) = table->id;
  offset += sizeof(uint64);

  offset += cram_log_string_counted(&data[offset], table->name);

  return cram_log_entry(data, offset);
}

static int cram_init_create(uchar *data)
{
  uint32 columns = *((uint32*)data);
  data += sizeof(uint32);

  uint64 id = *((uint64*)data);
  data += sizeof(uint64);

  char name[256];
  data += cram_init_string_counted(data, name);

  cram_table_open(name, columns, id);

  return 0;
}

/*
uchar type
uchar old_length
char* old_name
uchar new_length
char* new_name
*/

static int cram_log_rename(const char *old_name, const char *new_name)
{
  uchar data[1024];
  uint64 offset = 0;

  *(( uchar*)&data[offset]) = CRAM_ENTRY_RENAME;
  offset += sizeof(uchar);

  offset += cram_log_string_counted(&data[offset], old_name);
  offset += cram_log_string_counted(&data[offset], new_name);

  return cram_log_entry(data, offset);
}

static int cram_init_rename(uchar *data)
{
  char old_name[256];
  data += cram_init_string_counted(data, old_name);

  char new_name[256];
  data += cram_init_string_counted(data, new_name);

  cram_table_rename(old_name, new_name);

  return 0;
}

/*
uchar type
uchar length
char* name
*/

static int cram_log_drop(const char *name)
{
  uchar data[1024];
  uint64 offset = 0;

  *(( uchar*)&data[offset]) = CRAM_ENTRY_DROP;
  offset += sizeof(uchar);

  offset += cram_log_string_counted(&data[1], name);

  return cram_log_entry(data, offset);
}

static int cram_init_drop(uchar *data)
{
  char name[256];
  cram_init_string_counted(data, name);
  return cram_table_drop(name);
}

/*
uchar type
uint64 row id
uint64 nulls bitmap
uint64 types bitmap
uint64 table id
fields
*/

static int cram_log_insert(CramTable *table, CramRow *row)
{
  size_t width = sizeof(uchar) + sizeof(uint64) + sizeof(uint64) + sizeof(uint64) + sizeof(uint64);

  for (uint column = 0; column < table->columns; column++)
  {
    if (row->blobs[column])
    {
      width += row->blobs[column]->length;
      width += (row->blobs[column]->length < 256) ? sizeof(uchar): sizeof(uint32);
    }
  }

  uchar *data = (uchar*) cram_alloc(width);
  size_t offset = 0;

  *(( uchar*)&data[offset]) = CRAM_ENTRY_INSERT;
  offset += sizeof(uchar);

  *((uint64*)&data[offset]) = row->id;
  offset += sizeof(uint64);

  uint64 nulls = 0;
  size_t nulls_offset = offset;
  offset += sizeof(uint64);

  uint64 types = 0;
  size_t types_offset = offset;
  offset += sizeof(uint64);

  *((uint64*)&data[offset]) = table->id;
  offset += sizeof(uint64);

  for (uint column = 0; column < table->columns; column++)
  {
    CramBlob *blob = row->blobs[column];

    if (!blob)
    {
      nulls |= 1<<column;
    }
    else
    if (blob->length < 256)
    {
      types |= 1<<column;
      *((uchar*)&data[offset]) = blob->length;
      offset += sizeof(uchar);
      memmove(&data[offset], blob->buffer, blob->length);
      offset += blob->length;
    }
    else
    {
      *((uint32*)&data[offset]) = blob->length;
      offset += sizeof(uint32);
      memmove(&data[offset], blob->buffer, blob->length);
      offset += blob->length;
    }
  }

  *((uint64*)&data[nulls_offset]) = nulls;
  *((uint64*)&data[types_offset]) = types;

  int rc = cram_log_entry(data, width);

  cram_free(data);
  return rc;
}

static int cram_init_insert(uchar *data)
{
  uint64 id = *((uint64*)data);
  data += sizeof(uint64);

  uint64 nulls = *((uint64*)data);
  data += sizeof(uint64);

  uint64 types = *((uint64*)data);
  data += sizeof(uint64);

  uint64 tid = *((uint64*)data);
  data += sizeof(uint64);

  CramTable *table = cram_table_by_id(tid);
  if (!table)
  {
    cram_error("unknown table id: %llu", id);
    if (!cram_force_start) abort(); else return 0;
  }

  CramRow *row = cram_row_create(table, id, NULL, CRAM_NO_LOG, CRAM_APPEND);

  uint64 used = 0;

  for (uint32 column = 0; column < table->columns; column++)
  {
    uint32 length = 0;

    if (nulls & 1<<column)
    {
      row->blobs[column] = NULL;
    }
    else
    if (types & 1<<column)
    {
      length = *((uchar*)data);
      data += sizeof(uchar);
      used += sizeof(uchar);

      row->blobs[column] = cram_incref(data, length);
    }
    else
    {
      length = *((uint32*)data);
      data += sizeof(uint32);
      used += sizeof(uint32);

      row->blobs[column] = cram_incref(data, length);
    }
    data += length;
    used += length;
  }

  return 0;
}

/*
uchar type
uint64 row id
uint64 table id
*/

static int cram_log_delete(CramTable *table, uint64 id)
{
  size_t width = sizeof(uchar) + sizeof(uint64) + sizeof(uint64);

  uchar data[1024];
  size_t offset = 0;

  *(( uchar*)&data[offset]) = CRAM_ENTRY_DELETE;
  offset += sizeof(uchar);

  *((uint64*)&data[offset]) = id;
  offset += sizeof(uint64);

  *((uint64*)&data[offset]) = table->id;
  offset += sizeof(uint64);

  return cram_log_entry(data, width);
}

static int cram_init_delete(uchar *data)
{
  uint64 id = *((uint64*)data);
  data += sizeof(uint64);

  uint64 tid = *((uint64*)data);
  data += sizeof(uint64);

  CramTable *table = cram_table_by_id(tid);

  if (!table)
  {
    cram_error("unknown table id: %llu", tid);
    if (!cram_force_start) abort(); else return 0;
  }

  CramPage *page = NULL;
  CramRow *row = NULL;
  for (uint i = 0; !page && i < CRAM_JOBS; i++)
  {
    CramPage *list = table->lists[i].first;
    while (list && !page)
    {
      for (uint j = 0; j < cram_page_rows; j++)
      {
        if (list->rows[j].id == id)
        {
          page = list;
          row = &page->rows[j];
          break;
        }
      }
      list = list->next;
    }
  }
  if (page)
  {
    cram_row_free(table, page, row, CRAM_NO_LOG);
  }

  return 0;
}

static void cram_job_queue(CramJob *job)
{
  pthread_mutex_lock(&cram_job_mutex);

  cram_debug("... queue job %s", job->table->name);

  CramJob *prev = cram_job;
  while (prev && prev->next)
  {
    prev = prev->next;
  }
  if (prev)
  {
    prev->next = job;
  }
  else
  {
    cram_job = job;
  }
  pthread_cond_signal(&cram_job_cond);
  pthread_mutex_unlock(&cram_job_mutex);
}

static CramJob* cram_job_create(CramTable *table, CramCondition *condition, uint position)
{
  CramJob *job = (CramJob*) cram_alloc(sizeof(CramJob));

  job->table     = table;
  job->result    = NULL;
  job->list      = position;
  job->condition = condition;

  pthread_mutex_init(&job->mutex, NULL);

  cram_job_queue(job);

  return job;
}

static void cram_job_free(CramJob *job)
{
  pthread_mutex_destroy(&job->mutex);
  cram_free(job);
}

static bool cram_job_row(CramJob *job, CramPage *page, CramRow *row)
{
  CramCondition *con = job->condition;

  if (!row->blobs)
  {
    return FALSE;
  }

  job->steps++;

  bool match = TRUE;

  if (con)
  {
    match = FALSE;

    if (con->type == CRAM_COND_NULL)
    {
      match = row->blobs[con->index] ? FALSE: TRUE;
    }

    else
    if (con->type == CRAM_COND_NOTNULL)
    {
      match = row->blobs[con->index] ? TRUE: FALSE;
    }

    else
    if (con->type == CRAM_COND_EQ)
    {
      match = row->blobs[con->index] == con->blobs[0] ? TRUE: FALSE;
    }

    else
    if (con->type == CRAM_COND_LT)
    {
      if (row->blobs[con->index]->length == sizeof(uint64))
      {
        match = *((int64*)row->blobs[con->index]->buffer) < con->number ? TRUE: FALSE;
      }
    }

    else
    if (con->type == CRAM_COND_LT_STR)
    {
      uint32 length = con->length < row->blobs[con->index]->length
        ? con->length : row->blobs[con->index]->length;
      match = memcmp(row->blobs[con->index]->buffer, con->buffer, length) < 0 ? TRUE: FALSE;
    }

    else
    if (con->type == CRAM_COND_GT)
    {
      if (row->blobs[con->index]->length == sizeof(uint64))
      {
        match = *((int64*)row->blobs[con->index]->buffer) > con->number ? TRUE: FALSE;
      }
    }

    else
    if (con->type == CRAM_COND_GT_STR)
    {
      uint32 length = con->length < row->blobs[con->index]->length
        ? con->length : row->blobs[con->index]->length;
      match = memcmp(row->blobs[con->index]->buffer, con->buffer, length) > 0 ? TRUE: FALSE;
    }

    else
    if (con->type == CRAM_COND_IN)
    {
      for (uint k = 0; !match && k < con->count; k++)
      {
        match = row->blobs[con->index] == con->blobs[k] ? TRUE: FALSE;
      }
    }

    else
    if (con->type == CRAM_COND_LEADING)
    {
      if (row->blobs[con->index])
      {
        uchar *blob_buf = row->blobs[con->index]->buffer;
        uint blob_len   = row->blobs[con->index]->length;

        match = blob_len >= con->like_len
          && memcmp(con->like, blob_buf + blob_len - con->like_len, con->like_len) == 0
          ? TRUE: FALSE;
      }
    }

    else
    if (con->type == CRAM_COND_TRAILING)
    {
      if (row->blobs[con->index])
      {
        uchar *blob_buf = row->blobs[con->index]->buffer;
        uint blob_len   = row->blobs[con->index]->length;

        match = blob_len >= con->like_len
          && memcmp(con->like, blob_buf, con->like_len) == 0
          ? TRUE: FALSE;
      }
    }

    else
    if (con->type == CRAM_COND_CONTAINS)
    {
      if (row->blobs[con->index])
      {
        uchar *blob_buf = row->blobs[con->index]->buffer;
        uint blob_len   = row->blobs[con->index]->length;

        if (blob_len >= con->like_len)
        {
          for (uint k = 0; !match && k < blob_len && blob_len - k >= con->like_len; k++)
          {
            match = memcmp(con->like, &blob_buf[k], con->like_len) == 0 ? TRUE: FALSE;
          }
        }
      }
    }
  }
  if (match)
  {
    CramResult *res = (CramResult*) cram_alloc(sizeof(CramResult));
    res->row = row;
    res->page = page;
    res->next = job->result;
    job->result = res;
    job->matches++;
  }
  return match;
}

static bool cram_job_page(CramJob *job, CramPage *page)
{
  bool flag = FALSE;
  pthread_rwlock_rdlock(&page->lock);
  for (uint i = 0; i < cram_page_rows; i++)
  {
    CramRow *row = &page->rows[i];
    cram_job_row(job, page, row);
  }
  pthread_rwlock_unlock(&page->lock);
  flag = TRUE;
  return flag;
}

static void cram_job_execute(uint id, CramJob *job)
{
  CramTable *table = job->table;

  pthread_rwlock_rdlock(&table->lock);

  CramList *list = &table->lists[job->list];

  pthread_rwlock_rdlock(&list->lock);
  CramPage *page = list->first;

  for (; page && !job->complete; page = page->next)
  {
    cram_job_page(job, page);
  }

  job->complete = TRUE;

  pthread_rwlock_unlock(&list->lock);
  pthread_rwlock_unlock(&table->lock);

  cram_debug("%u worker matches %llu steps %llu", id, job->matches, job->steps);

  cram_stat_add(&cram_stats_spinlock, &cram_ecp_steps,   job->steps);
  cram_stat_add(&cram_stats_spinlock, &cram_ecp_matches, job->matches);
}

static void* cram_worker(void *p)
{
  CramWorker *self = (CramWorker*) p;
  uint id = self - cram_workers;

  pthread_mutex_lock(&cram_job_mutex);
  cram_note("%u worker started", id);
  while (self->run)
  {
    if (!cram_job)
    {
      cram_debug("%u worker wait", id);
      pthread_cond_wait(&cram_job_cond, &cram_job_mutex);
      cram_debug("%u worker awoke", id);
    }
    if (cram_job)
    {
      CramJob *job = cram_job;
      cram_job = job->next;
      pthread_mutex_lock(&job->mutex);
      pthread_mutex_unlock(&cram_job_mutex);
      cram_debug("%u working job %s", id, job->table->name);
      cram_job_execute(id, job);
      //pthread_cond_signal(&job->cond);
      pthread_mutex_unlock(&job->mutex);
      pthread_mutex_lock(&cram_job_mutex);
    }
  }
  self->done = TRUE;
  pthread_mutex_unlock(&cram_job_mutex);
  cram_note("%u worker stopped", id);
  return NULL;
}

static void cram_workers_start()
{
  cram_workers = (CramWorker*) cram_alloc(sizeof(CramWorker) * cram_worker_threads);

  for (uint i = 0; i < cram_worker_threads; i++)
  {
    cram_workers[i].run = TRUE;
    pthread_create(&cram_workers[i].thread, NULL, cram_worker, &cram_workers[i]);
  }
}

static void cram_workers_stop()
{
  for (uint i = 0; i < cram_worker_threads; i++)
  {
    cram_workers[i].run = FALSE;
  }
  for (uint i = 0; i < cram_worker_threads; i++)
  {
    while (!cram_workers[i].done)
    {
      pthread_mutex_lock(&cram_job_mutex);
      pthread_cond_signal(&cram_job_cond);
      pthread_mutex_unlock(&cram_job_mutex);
      usleep(1000);
    }
    pthread_join(cram_workers[i].thread, NULL);
  }

  cram_free(cram_workers);
}

static void* cram_consolidator(void *p)
{
  CramConsolidateJob *job = (CramConsolidateJob*)p;

  CramPage *page = job->table->lists[job->list].first;
  while (page)
  {
    for (uint j = 0; j < cram_page_rows; j++)
    {
      if (page->rows[j].blobs)
      {
        cram_log_insert(job->table, &page->rows[j]);
      }
    }
    page = page->next;
  }

  pthread_mutex_lock(&job->mutex);
  job->complete = TRUE;
  pthread_mutex_unlock(&job->mutex);
  return NULL;
}

static void cram_consolidate()
{
  cram_note("waiting for writer...");

  for (;;)
  {
    pthread_spin_lock(&cram_stats_spinlock);
    ulonglong queued = cram_log_writes_queued;
    ulonglong completed = cram_log_writes_completed;
    pthread_spin_unlock(&cram_stats_spinlock);

    if (completed == queued)
    {
      break;
    }
    usleep(1000);
  }

  cram_note("consolidating data files...");

  pthread_mutex_lock(&cram_epoch_mutex);
  fclose(cram_epoch_file);

  for (uint64 i = 0; i < cram_epoch; i++)
  {
    char name[1024];
    snprintf(name, sizeof(name), CRAM_FILE, i);
    remove(name);
  }

  cram_epoch = 1;
  cram_epoch_file = NULL;
  cram_flush_level = 0;
  cram_epoch_create();
  pthread_mutex_unlock(&cram_epoch_mutex);

  CramTable *table = cram_tables;
  while (table)
  {
    cram_note("%s", table->name);
    cram_log_create(table);

    CramConsolidateJob jobs[CRAM_JOBS];

    for (uint i = 0; i < CRAM_JOBS; i++)
    {
      jobs[i].table = table;
      jobs[i].list  = i;
      jobs[i].complete = FALSE;
      pthread_mutex_init(&jobs[i].mutex, NULL);
      pthread_create(&jobs[i].thread, NULL, cram_consolidator, &jobs[i]);
    }

    for (uint i = 0; i < CRAM_JOBS; i++)
    {
      for (;;)
      {
        usleep(1000);
        pthread_mutex_lock(&jobs[i].mutex);
        bool complete = jobs[i].complete;
        pthread_mutex_unlock(&jobs[i].mutex);
        if (complete)
        {
          pthread_join(jobs[i].thread, NULL);
          pthread_mutex_destroy(&jobs[i].mutex);
          break;
        }
      }
    }
    table = table->next;
  }
}

static int cram_init_func(void *p)
{
  cram_hton = (handlerton*)p;

  cram_hton->state  = SHOW_OPTION_YES;
  cram_hton->create = cram_create_handler;
  cram_hton->flags  = HTON_CAN_RECREATE;
  cram_hton->table_options = cram_table_option_list;
  cram_hton->field_options = cram_field_option_list;

  thr_lock_init(&mysql_lock);

  cram_hash.mutexes  = (pthread_mutex_t*) cram_alloc(sizeof(pthread_mutex_t) * cram_hash_locks);
  cram_hash.chains = (CramBlob**) cram_alloc(sizeof(CramBlob*) * cram_hash_chains);

  for (uint i = 0; i < cram_hash_locks; i++)
  {
    pthread_mutex_init(&cram_hash.mutexes[i], NULL);
  }

  pthread_mutex_init(&cram_tables_mutex, NULL);
  pthread_spin_init(&cram_sequence_spinlock, PTHREAD_PROCESS_PRIVATE);
  pthread_mutex_init(&cram_epoch_mutex, NULL);
  pthread_spin_init(&cram_log_events_spinlock, PTHREAD_PROCESS_PRIVATE);
  pthread_spin_init(&cram_stats_spinlock, PTHREAD_PROCESS_PRIVATE);
  cram_note("Time to cram!");

  pthread_mutex_init(&cram_job_mutex, NULL);
  pthread_cond_init(&cram_job_cond, NULL);

  char name[1024];
  uchar *data = (uchar*) cram_alloc(CRAM_EPOCH);

  for (cram_epoch = 1; ; cram_epoch++)
  {
    snprintf(name, sizeof(name), CRAM_FILE, cram_epoch);

    if (!(cram_epoch_file = fopen(name, "r")))
    {
      break;
    }

    cram_note("reading %s", name);
    cram_epoch_eof = fread(data, 1, CRAM_EPOCH, cram_epoch_file);

    for (size_t offset = 0; offset < cram_epoch_eof; )
    {
      size_t position = offset;

      size_t compressed_width = *((uint64*)&data[offset]);
      offset += sizeof(uint64);

      size_t uncompressed_width = *((uint64*)&data[offset]);
      offset += sizeof(uint64);

      uchar *buffer = (uchar*) cram_alloc(uncompressed_width);

      if (compressed_width > 0)
      {
        memmove(buffer, &data[offset], compressed_width);
        if (cram_inflate(buffer, compressed_width, uncompressed_width) != uncompressed_width)
        {
          cram_error("decompression failed");
          if (!cram_force_start) abort();
          cram_free(buffer);
          break;
        }
        offset += compressed_width;
      }
      else
      {
        memmove(buffer, &data[offset], uncompressed_width);
        offset += uncompressed_width;
      }

      uchar entry = buffer[0];

      switch (entry)
      {
        case CRAM_ENTRY_CREATE:
          cram_init_create(&buffer[1]);
          break;
        case CRAM_ENTRY_RENAME:
          cram_init_rename(&buffer[1]);
          break;
        case CRAM_ENTRY_DROP:
          cram_init_drop(&buffer[1]);
          break;
        case CRAM_ENTRY_INSERT:
          cram_init_insert(&buffer[1]);
          break;
        case CRAM_ENTRY_DELETE:
          cram_init_delete(&buffer[1]);
          break;
        default:
          cram_error("invalid entry type %u at %llu", entry, position);
          if (!cram_force_start) abort();
      }

      cram_free(buffer);
    }

    fclose(cram_epoch_file);
    cram_epoch_file = NULL;
  }

  cram_free(data);

  cram_epoch_create();
  cram_writer_start();
  cram_workers_start();

  return 0;
}

static int cram_done_func(void *p)
{
  cram_workers_stop();
  cram_consolidate();
  cram_writer_stop();

  for (uint i = 0; i < cram_hash_locks; i++)
  {
    pthread_mutex_destroy(&cram_hash.mutexes[i]);
  }

  pthread_mutex_destroy(&cram_tables_mutex);
  pthread_spin_destroy(&cram_sequence_spinlock);
  pthread_mutex_destroy(&cram_epoch_mutex);
  pthread_spin_destroy(&cram_log_events_spinlock);
  pthread_spin_destroy(&cram_stats_spinlock);
  pthread_mutex_destroy(&cram_job_mutex);

  pthread_cond_destroy(&cram_job_cond);

  return 0;
}

static handler* cram_create_handler(handlerton *hton, TABLE_SHARE *table, MEM_ROOT *mem_root)
{
  return new (mem_root) ha_cram(hton, table);
}

ha_cram::ha_cram(handlerton *hton, TABLE_SHARE *table_arg)
  :handler(hton, table_arg)
{
  cram_table  = NULL;
  counter_insert = 0;
  counter_update = 0;
  counter_delete = 0;
  counter_rnd_next = 0;
  counter_rnd_pos  = 0;
  counter_position = 0;
}

int ha_cram::record_store(uchar *buf)
{
  if (!cram_result)
  {
    return HA_ERR_END_OF_FILE;
  }

  pthread_rwlock_rdlock(&cram_result->page->lock);

  CramRow *row = cram_result->row;

  if (!row->blobs)
  {
    pthread_rwlock_unlock(&cram_result->page->lock);
    return HA_ERR_RECORD_DELETED;
  }

  memset(buf, 0, table->s->null_bytes);
  // Avoid asserts in ::store() for columns that are not going to be updated
  my_bitmap_map *org_bitmap = dbug_tmp_use_all_columns(table, table->write_set);

  for (Field **field = table->field ; *field ; field++)
  {
    if (bitmap_is_set(table->read_set, (*field)->field_index))
    {
      uint index = field - table->field;

      if (!row->blobs[index])
      {
        (*field)->set_null();
      }
      else
      if ((*field)->result_type() == INT_RESULT)
      {
        CramBlob *blob = row->blobs[index];
        (*field)->store(*((int64*)blob->buffer), FALSE);
      }
      else
      {
        CramBlob *blob = row->blobs[index];
        (*field)->store((char*)blob->buffer, blob->length, &my_charset_bin, CHECK_FIELD_WARN);
      }
    }
  }

  dbug_tmp_restore_column_map(table->write_set, org_bitmap);

  pthread_rwlock_unlock(&cram_result->page->lock);

  return 0;
}


static const char *ha_cram_exts[] = {
  NullS
};

const char **ha_cram::bas_ext() const
{
  cram_debug("%s", __func__);
  return ha_cram_exts;
}

int ha_cram::open(const char *name, int mode, uint test_if_locked)
{
  cram_debug("%s %s", __func__, name);

  cram_table = cram_table_open(name, table->s->fields, 0);
  pthread_rwlock_rdlock(&cram_table->lock);

  thr_lock_data_init(&mysql_lock, &lock, NULL);
  cram_condition = NULL;
  cram_result = NULL;
  cram_rnd_results = NULL;
  cram_pos_results = NULL;
  counter_insert = 0;
  counter_update = 0;
  counter_delete = 0;
  counter_rnd_next = 0;
  counter_rnd_pos  = 0;
  counter_position = 0;
  return cram_table ? 0: -1;
}

int ha_cram::close(void)
{
  cram_debug("%s %s", __func__, cram_table->name);
  pthread_rwlock_unlock(&cram_table->lock);
  return 0;
}

int ha_cram::write_row(uchar *buf)
{
  CramBlob **blobs = (CramBlob**) cram_alloc(sizeof(CramBlob*) * cram_table->columns);

  // Avoid asserts in val_str() for columns that are not going to be updated
  my_bitmap_map *org_bitmap = dbug_tmp_use_all_columns(table, table->read_set);

  for (Field **field = table->field ; *field ; field++)
  {
    uint index = field - table->field;
    cram_field_set(blobs, index, *field);
  }

  dbug_tmp_restore_column_map(table->read_set, org_bitmap);

  cram_row_create(cram_table, 0, blobs, CRAM_LOG, CRAM_INSERT);

  counter_insert++;
  return 0;
}

int ha_cram::update_row(const uchar *old_data, uchar *new_data)
{
  int rc = HA_ERR_RECORD_DELETED;

  pthread_rwlock_wrlock(&cram_result->page->lock);

  CramBlob **blobs = cram_result->row->blobs;

  if (blobs)
  {
    // Avoid asserts in val_str() for columns that are not going to be updated
    my_bitmap_map *org_bitmap = dbug_tmp_use_all_columns(table, table->read_set);

    for (uint index = 0; index < cram_table->columns; index++)
    {
      Field *field = table->field[index];

      if (bitmap_is_set(table->write_set, field->field_index))
      {
        cram_field_set(blobs, index, field);
      }
    }

    dbug_tmp_restore_column_map(table->read_set, org_bitmap);

    cram_log_delete(cram_table, cram_result->row->id);
    cram_log_insert(cram_table, cram_result->row);

    rc = 0;
  }

  pthread_rwlock_unlock(&cram_result->page->lock);

  counter_update++;
  return rc;
}

int ha_cram::delete_row(const uchar *buf)
{
  counter_delete++;
  return cram_row_free(cram_table, cram_result->page, cram_result->row, CRAM_LOG);
}

int ha_cram::rnd_init(bool scan)
{
  cram_debug("%s", __func__);

  // rnd_init can be called multiple times followed by a single explicit rnd_end
  rnd_end();

  cram_rnd_started = FALSE;

  return 0;
}

void ha_cram::rnd_map()
{
  // Equality. If no blob was found in the data dictionary then nothing can possibly equal it.
  if (cram_condition && cram_condition->type == CRAM_COND_EQ && !cram_condition->blobs[0])
  {
    return;
  }

  CramJob *jobs[CRAM_JOBS];

  for (uint i = 0; i < CRAM_JOBS; i++)
  {
    jobs[i] = cram_job_create(cram_table, cram_condition, i);
  }

  for (uint i = 0; i < CRAM_JOBS; i++)
  {
    CramJob *job = jobs[i];

    pthread_mutex_lock(&job->mutex);

    // Seems to scale better than pthread_cond_wait
    while (!job->complete)
    {
      pthread_mutex_unlock(&job->mutex);
      usleep(10000);
      pthread_mutex_lock(&job->mutex);
    }

    cram_debug("job %u done", i);

    CramResult *res = job->result;
    while (res && res->next)
    {
      res = res->next;
    }
    if (res)
    {
      res->next = cram_result;
    }
    if (job->result)
    {
      cram_result = job->result;
    }

    pthread_mutex_unlock(&job->mutex);
    cram_job_free(job);
  }

  cram_rnd_results = cram_result;
}

int ha_cram::rnd_end()
{
  cram_debug("%s", __func__);

  cram_result = cram_rnd_results;
  while (cram_result)
  {
    CramResult *next = cram_result->next;
    cram_free(cram_result);
    cram_result = next;
  }
  cram_rnd_results = NULL;

  return 0;
}

int ha_cram::rnd_next(uchar *buf)
{
  counter_rnd_next++;
  if (!cram_rnd_started)
  {
    rnd_map();
    cram_result = cram_rnd_results;
    cram_rnd_started = TRUE;
  }
  else
  if (cram_result)
  {
    cram_result = cram_result->next;
  }
  return record_store(buf);
}

void ha_cram::position(const uchar *record)
{
  CramResult *res = (CramResult*) cram_alloc(sizeof(CramResult));
  memmove(res, cram_result, sizeof(CramResult));

  res->next = cram_pos_results;
  cram_pos_results = res;

  *((CramResult**)ref) = res;
  ref_length = sizeof(CramResult*);

  counter_position++;
}

int ha_cram::rnd_pos(uchar *buf, uchar *pos)
{
  cram_result = *((CramResult**)pos);
  counter_rnd_pos++;
  return record_store(buf);
}

int ha_cram::info(uint flag)
{
  cram_debug("%s", __func__);

  if (flag & HA_STATUS_VARIABLE)
  {
    uint64 rows = 0;
    for (uint i = 0; i < CRAM_JOBS; i++)
    {
      pthread_rwlock_rdlock(&cram_table->lists[i].lock);
      pthread_spin_lock(&cram_table->lists[i].spinlock);
      rows += cram_table->lists[i].rows;
      pthread_spin_unlock(&cram_table->lists[i].spinlock);
      pthread_rwlock_unlock(&cram_table->lists[i].lock);
    }

    cram_debug("... HA_STATUS_VARIABLE");
    stats.records = rows;
    stats.deleted = 0;
    stats.data_file_length = 0;
    stats.index_file_length = 0;
    stats.mean_rec_length = cram_table->columns * sizeof(uint64);
  }

  return 0;
}

int ha_cram::reset()
{
  cram_debug("%s", __func__);

  if (cram_condition)
  {
    cram_free(cram_condition->buffer);
    cram_free(cram_condition->like);
    cram_free(cram_condition->blobs);
    cram_free(cram_condition);
    cram_condition = NULL;
  }
  return 0;
}

int ha_cram::external_lock(THD *thd, int lock_type)
{
  cram_result = cram_pos_results;
  while (cram_result)
  {
    CramResult *next = cram_result->next;
    cram_free(cram_result);
    cram_result = next;
  }
  cram_pos_results = NULL;

  counter_insert = 0;
  counter_update = 0;
  counter_delete = 0;
  counter_rnd_next = 0;
  counter_rnd_pos  = 0;
  counter_position = 0;

  return 0;
}

int ha_cram::delete_table(const char *name)
{
  cram_debug("%s %s", __func__, name);

  int rc = cram_table_drop(name);
  if (rc == 0) cram_log_drop(name);

  return rc;
}

int ha_cram::rename_table(const char *from, const char *to)
{
  cram_debug("%s %s %s", __func__, from, to);

  int rc = cram_table_rename(from, to);
  if (rc == 0) cram_log_rename(from, to);

  return rc;
}

int ha_cram::create(const char *name, TABLE *table_arg, HA_CREATE_INFO *create_info)
{
  cram_debug("%s %s", __func__, name);
/*
  for (uint i = 0; i < table_arg->s->fields; i++)
  {
    Field* field = table_share->field[i];
    enum_field_types mysql_type = field->real_type();

    switch (mysql_type) {
      case MYSQL_TYPE_TINY:
      case MYSQL_TYPE_SHORT:
      case MYSQL_TYPE_INT24:
      case MYSQL_TYPE_LONG:
      case MYSQL_TYPE_LONGLONG:
      case MYSQL_TYPE_STRING:
      case MYSQL_TYPE_VARCHAR:
      case MYSQL_TYPE_TINY_BLOB:
      case MYSQL_TYPE_MEDIUM_BLOB:
      case MYSQL_TYPE_BLOB:
      case MYSQL_TYPE_LONG_BLOB:
      case MYSQL_TYPE_DOUBLE:
        continue;

      case MYSQL_TYPE_DATE:
      case MYSQL_TYPE_YEAR:
      case MYSQL_TYPE_NEWDATE:
      case MYSQL_TYPE_TIME:
      case MYSQL_TYPE_DATETIME:
      case MYSQL_TYPE_TIMESTAMP:
      case MYSQL_TYPE_BIT:
      case MYSQL_TYPE_ENUM:
      case MYSQL_TYPE_SET:
      case MYSQL_TYPE_FLOAT:
      case MYSQL_TYPE_NEWDECIMAL:
#if 50600 <= MYSQL_VERSION_ID && MYSQL_VERSION_ID <= 50699
      case MYSQL_TYPE_DATETIME2:
      case MYSQL_TYPE_TIMESTAMP2:
      case MYSQL_TYPE_TIME2:
#endif
      case MYSQL_TYPE_GEOMETRY:
      case MYSQL_TYPE_DECIMAL:
      case MYSQL_TYPE_VAR_STRING:
      case MYSQL_TYPE_NULL:
      default:
        return HA_ERR_UNSUPPORTED;
    }
  }
*/
  cram_table = cram_table_open(name, table_arg->s->fields, 0);

  if (cram_table)
  {
    cram_log_create(cram_table);
  }

  return cram_table ? 0: -1;
}

bool ha_cram::check_if_incompatible_data(HA_CREATE_INFO *info, uint table_changes)
{
  cram_debug("%s", __func__);
  return COMPATIBLE_DATA_NO;
}

const COND * ha_cram::cond_push ( const COND * cond )
{
  cram_debug("%s", __func__);

  char pad[1024];
  String *str, tmp(pad, sizeof(pad), &my_charset_bin);

  reset();

  // If we're passed an AND condition, just grab the first clause and scan on that.
  if (cond->type() == COND::COND_ITEM && ((Item_cond*) cond)->functype() == Item_func::COND_AND_FUNC)
  {
    cond = ((Item_cond*) cond)->argument_list()->head();
  }

  if (cond->type() == COND::FUNC_ITEM)
  {
    Item_func *func = (Item_func*)cond;
    Item **args = func->arguments();

    // IS NULL
    // IS NOT NULL
    if (func->argument_count() == 1
      && (func->functype() == Item_func::ISNULL_FUNC || func->functype() == Item_func::ISNOTNULL_FUNC))
    {
      Item_field *ff = (Item_field*)args[0];

      cram_debug("ECP %s %llu %llu",
        func->functype() == Item_func::ISNULL_FUNC ? "NULL": "NOT NULL",
        ff->field->field_index, func->argument_count()-1);

      CramCondition *sc = (CramCondition*) cram_alloc(sizeof(CramCondition));

      sc->type  = func->functype() == Item_func::ISNULL_FUNC ? CRAM_COND_NULL: CRAM_COND_NOTNULL;
      sc->count = 1;
      sc->index = ff->field->field_index;

      cram_condition = sc;
    }

    if (func->argument_count() == 2 && args[0]->type() == COND::FIELD_ITEM)
    {
      Item *arg = args[1];
      Item_field *ff = (Item_field*)args[0];

      cram_debug("... cond %llu", args[0]->type());

      // =
      if (func->functype() == Item_func::EQ_FUNC)
      {
        cram_debug("ECP EQ %llu %llu", ff->field->field_index, func->argument_count()-1);

        CramCondition *sc = (CramCondition*) cram_alloc(sizeof(CramCondition));

        sc->type  = CRAM_COND_EQ;
        sc->count = 1;
        sc->index = ff->field->field_index;

        sc->blobs = (CramBlob**) cram_alloc(sizeof(CramBlob*) * sc->count);

        if (arg->is_null())
        {
          cram_debug("... null");
          sc->blobs[0] = NULL;
        }
        else
        if (arg->result_type() == INT_RESULT)
        {
          int64 n = arg->val_int();
          cram_debug("... int %lld", n);
          sc->blobs[0] = cram_get((uchar*)(&n), sizeof(int64));
        }
        else
        {
          str = arg->val_str(&tmp);
          cram_debug("... str %s", str->c_ptr());
          sc->blobs[0] = cram_get((uchar*)str->ptr(), str->length());
        }

        cram_condition = sc;
      }

      // <
      if (func->functype() == Item_func::LT_FUNC && !arg->is_null())
      {
        cram_debug("ECP LT %llu %llu", ff->field->field_index, func->argument_count()-1);

        CramCondition *sc = (CramCondition*) cram_alloc(sizeof(CramCondition));

        sc->type  = CRAM_COND_LT;
        sc->count = 0;
        sc->index = ff->field->field_index;

        sc->blobs = (CramBlob**) cram_alloc(sizeof(CramBlob*) * sc->count);

        if (arg->result_type() == INT_RESULT)
        {
          cram_debug("... int %lld", arg->val_int());
          sc->number = arg->val_int();
        }
        else
        {
          sc->type = CRAM_COND_LT_STR;
          str = arg->val_str(&tmp);
          cram_debug("... str %s", str->c_ptr());
          sc->buffer = (uchar*) cram_alloc(str->length());
          memmove(sc->buffer, str->ptr(), str->length());
          sc->length = str->length();
        }

        cram_condition = sc;
      }

      // >
      if (func->functype() == Item_func::GT_FUNC && !arg->is_null())
      {
        cram_debug("ECP GT %llu %llu", ff->field->field_index, func->argument_count()-1);

        CramCondition *sc = (CramCondition*) cram_alloc(sizeof(CramCondition));

        sc->type  = CRAM_COND_GT;
        sc->count = 0;
        sc->index = ff->field->field_index;

        sc->blobs = (CramBlob**) cram_alloc(sizeof(CramBlob*) * sc->count);

        if (arg->result_type() == INT_RESULT)
        {
          cram_debug("... int %lld", arg->val_int());
          sc->number = arg->val_int();
        }
        else
        {
          sc->type = CRAM_COND_GT_STR;
          str = arg->val_str(&tmp);
          cram_debug("... str %s", str->c_ptr());
          sc->buffer = (uchar*) cram_alloc(str->length());
          memmove(sc->buffer, str->ptr(), str->length());
          sc->length = str->length();
        }

        cram_condition = sc;
      }

      // LIKE
      if (func->functype() == Item_func::LIKE_FUNC && args[0]->type() == COND::FIELD_ITEM)
      {
        str = arg->val_str(&tmp);

        int left   = str->length() > 1 && str->c_ptr()[0] == '%';
        int right  = str->length() > 1 && str->c_ptr()[str->length()-1] == '%';

        char *pos = strchr(str->c_ptr()+1, '%');
        int within = str->length() > 1 && pos && pos < str->c_ptr() + str->length() - 1;

        if (left && !right && !within)
        {
          cram_debug("ECP LEADING %llu %s", ff->field->field_index, str->c_ptr());

          CramCondition *sc = (CramCondition*) cram_alloc(sizeof(CramCondition));

          sc->type  = CRAM_COND_LEADING;
          sc->count = 1;
          sc->index = ff->field->field_index;

          sc->like = (char*) cram_alloc(str->length());
          strcpy(sc->like, str->c_ptr()+1);
          sc->like_len = str->length()-1;

          cram_condition = sc;
        }

        if (right && !left && !within)
        {
          cram_debug("ECP TRAILING %llu %s", ff->field->field_index, str->c_ptr());

          CramCondition *sc = (CramCondition*) cram_alloc(sizeof(CramCondition));

          sc->type  = CRAM_COND_TRAILING;
          sc->count = 1;
          sc->index = ff->field->field_index;

          sc->like = (char*) cram_alloc(str->length());
          strncpy(sc->like, str->c_ptr(), str->length()-1);
          sc->like[str->length()-1] = 0;
          sc->like_len = str->length()-1;

          cram_condition = sc;
        }

        if (left && right && !within)
        {
          cram_debug("ECP CONTAINS %llu %s", ff->field->field_index, str->c_ptr());

          CramCondition *sc = (CramCondition*) cram_alloc(sizeof(CramCondition));

          sc->type  = CRAM_COND_CONTAINS;
          sc->count = 1;
          sc->index = ff->field->field_index;

          sc->like = (char*) cram_alloc(str->length());
          strncpy(sc->like, str->c_ptr()+1, str->length()-2);
          sc->like[str->length()-2] = 0;
          sc->like_len = str->length()-2;

          cram_debug("... [%s]", sc->like);

          cram_condition = sc;
        }

        // Equality
        if (!left && !right && !within)
        {
          cram_debug("ECP LIKE EQ %llu %s", ff->field->field_index, str->c_ptr());

          CramCondition *sc = (CramCondition*) cram_alloc(sizeof(CramCondition));

          sc->type  = CRAM_COND_EQ;
          sc->count = 1;
          sc->index = ff->field->field_index;

          sc->blobs = (CramBlob**) cram_alloc(sizeof(CramBlob*) * sc->count);
          sc->blobs[0] = cram_get((uchar*)str->ptr(), str->length());

          cram_condition = sc;
        }
      }
    }

    if (func->argument_count() > 1 && args[0]->type() == COND::FIELD_ITEM)
    {
      Item_field *ff = (Item_field*)args[0];

      if (func->functype() == Item_func::IN_FUNC)
      {
        cram_debug("ECP IN %llu %llu", ff->field->field_index, func->argument_count()-1);

        CramCondition *sc = (CramCondition*) cram_alloc(sizeof(CramCondition));

        sc->type  = CRAM_COND_IN;
        sc->count = func->argument_count() - 1;
        sc->index = ff->field->field_index;

        sc->blobs = (CramBlob**) cram_alloc(sizeof(CramBlob*) * sc->count);

        for (uint i = 1; i < func->argument_count(); i++)
        {
          Item *arg = args[i];

          if (arg->is_null())
          {
            cram_debug("... null");
            sc->blobs[i-1] = NULL;
          }
          else
          if (arg->result_type() == INT_RESULT)
          {
            int64 n = arg->val_int();
            cram_debug("... int %lld", n);
            sc->blobs[i-1] = cram_get((uchar*)(&n), sizeof(int64));
          }
          else
          {
            str = arg->val_str(&tmp);
            cram_debug("... str %s", str->c_ptr());
            sc->blobs[i-1] = cram_get((uchar*)str->ptr(), str->length());
          }
        }

        cram_condition = sc;
      }
    }
  }

  return NULL;
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

struct st_mysql_storage_engine cram_storage_engine=
{ MYSQL_HANDLERTON_INTERFACE_VERSION };


static void cram_worker_threads_update(THD * thd, struct st_mysql_sys_var *sys_var, void *var, const void *save)
{
  cram_workers_stop();

  uint n = *((uint*)save);
  *((uint*)var) = n;

  cram_worker_threads = n;

  cram_workers_start();
}

static void cram_flush_level_update(THD * thd, struct st_mysql_sys_var *sys_var, void *var, const void *save)
{
  uint n = *((uint*)save);
  *((uint*)var) = n;

  cram_flush_level = n;
}

static void cram_compress_log_update(THD * thd, struct st_mysql_sys_var *sys_var, void *var, const void *save)
{
  uint n = *((uint*)save);
  *((uint*)var) = n;

  cram_compress_log = n;
}

static void cram_verbose_update(THD * thd, struct st_mysql_sys_var *sys_var, void *var, const void *save)
{
  uint n = *((uint*)save);
  *((uint*)var) = n;

  cram_verbose = n;
}

static MYSQL_SYSVAR_UINT(worker_threads, cram_worker_threads, 0,
  "Size of the worker thread pool.", 0, cram_worker_threads_update, CRAM_WORKERS, 2, 32, 1);

static MYSQL_SYSVAR_UINT(hash_chains, cram_hash_chains, PLUGIN_VAR_READONLY,
  "Width of the values hash-table.", 0, NULL, CRAM_CHAINS, 1000, UINT_MAX, 1);

static MYSQL_SYSVAR_UINT(hash_locks, cram_hash_locks, PLUGIN_VAR_READONLY,
  "Size of hash chain locks array.", 0, NULL, CRAM_LOCKS, 100, UINT_MAX, 1);

static MYSQL_SYSVAR_UINT(force_start, cram_force_start, PLUGIN_VAR_READONLY,
  "Skip startup errors.", 0, NULL, 0, 0, 1, 1);

static MYSQL_SYSVAR_UINT(strict_write, cram_strict_write, PLUGIN_VAR_READONLY,
  "Abort on file access problems.", 0, NULL, 0, 0, 1, 1);

static MYSQL_SYSVAR_UINT(flush_level, cram_flush_level, 0,
  "Data file flush frequency.", 0, cram_flush_level_update, 1, 0, UINT_MAX, 1);

static MYSQL_SYSVAR_UINT(page_rows, cram_page_rows, PLUGIN_VAR_READONLY,
  "Number of rows per page.", 0, NULL, CRAM_PAGE, 1, UINT_MAX, 1);

static MYSQL_SYSVAR_UINT(compress_log, cram_compress_log, 0,
  "Compress log entries.", 0, cram_compress_log_update, 1, 0, 1, 1);

static MYSQL_SYSVAR_UINT(verbose, cram_verbose, 0,
  "Debug noise to stderr.", 0, cram_verbose_update, 0, 0, 1, 1);

static struct st_mysql_sys_var *cram_system_variables[] = {
    MYSQL_SYSVAR(worker_threads),
    MYSQL_SYSVAR(hash_chains),
    MYSQL_SYSVAR(hash_locks),
    MYSQL_SYSVAR(force_start),
    MYSQL_SYSVAR(strict_write),
    MYSQL_SYSVAR(flush_level),
    MYSQL_SYSVAR(page_rows),
    MYSQL_SYSVAR(compress_log),
    MYSQL_SYSVAR(verbose),
    NULL
};

static struct st_mysql_show_var func_status[]=
{
  { "cram_ecp_steps", (char*) &cram_ecp_steps, SHOW_ULONGLONG },
  { "cram_ecp_matches", (char*) &cram_ecp_matches, SHOW_ULONGLONG },
  { "cram_tables_created", (char*) &cram_tables_created, SHOW_ULONGLONG },
  { "cram_tables_deleted", (char*) &cram_tables_deleted, SHOW_ULONGLONG },
  { "cram_tables_renamed", (char*) &cram_tables_renamed, SHOW_ULONGLONG },
  { "cram_pages_created", (char*) &cram_pages_created, SHOW_ULONGLONG },
  { "cram_pages_deleted", (char*) &cram_pages_deleted, SHOW_ULONGLONG },
  { "cram_rows_created", (char*) &cram_rows_created, SHOW_ULONGLONG },
  { "cram_rows_deleted", (char*) &cram_rows_deleted, SHOW_ULONGLONG },
  { "cram_rows_inserted", (char*) &cram_rows_inserted, SHOW_ULONGLONG },
  { "cram_rows_appended", (char*) &cram_rows_appended, SHOW_ULONGLONG },
  { "cram_hash_reads", (char*) &cram_hash_reads, SHOW_ULONGLONG },
  { "cram_hash_inserts", (char*) &cram_hash_inserts, SHOW_ULONGLONG },
  { "cram_hash_deletes", (char*) &cram_hash_deletes, SHOW_ULONGLONG },
  { "cram_log_writes_queued", (char*) &cram_log_writes_queued, SHOW_ULONGLONG },
  { "cram_log_writes_completed", (char*) &cram_log_writes_completed, SHOW_ULONGLONG },
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
  cram_init_func,                            /* Plugin Init */
  cram_done_func,                            /* Plugin Deinit */
  0x0001 /* 0.1 */,
  func_status,                                  /* status variables */
  cram_system_variables,                     /* system variables */
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
  cram_init_func,                            /* Plugin Init */
  cram_done_func,                            /* Plugin Deinit */
  0x0001,                                       /* version number (0.1) */
  func_status,                                  /* status variables */
  cram_system_variables,                     /* system variables */
  "0.1",                                        /* string version */
  MariaDB_PLUGIN_MATURITY_EXPERIMENTAL          /* maturity */
},
{
  MYSQL_DAEMON_PLUGIN,
  &unusable_cram,
  "UNUSABLE",
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
