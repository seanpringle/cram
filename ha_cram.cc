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

/*
TODO
cram_page_free check janitor queue for pages
find a way to defrag/purge empty pages without ALTER/OPTIMIZE
cram_row_create speed up empty row scan (or always append and do defrag/purge)
table/page ref counts?
*/

#ifdef USE_PRAGMA_IMPLEMENTATION
#pragma implementation        // gcc: Class implementation
#endif

#include <mysql/plugin.h>
#include "ha_cram.h"
#include "sql_class.h"
#include <pthread.h>
#include <zlib.h>

static handler *cram_create_handler(handlerton *hton, TABLE_SHARE *table, MEM_ROOT *mem_root);

handlerton *cram_hton;

struct ha_table_option_struct{};
struct ha_field_option_struct{};
ha_create_table_option cram_table_option_list[] = { HA_TOPTION_END };
ha_create_table_option cram_field_option_list[] = { HA_FOPTION_END };

static THR_LOCK mysql_lock;
static uint64 cram_epoch;
static FILE *cram_epoch_file;
static uint64 cram_epoch_eof;
static pthread_mutex_t cram_epoch_mutex;

/* A global hash table that holds actual data BLOBs */
static CramDict *cram_dict;
static uint cram_dict_chains;
static uint cram_dict_locks;

/* List of open tables */
static CramList *cram_tables;
static CramWorker *cram_workers;

/* Every table and record gets an id */
static uint64 cram_sequence;
static pthread_spinlock_t cram_sequence_spinlock;

/* General config vars */
static uint cram_worker_threads;
static uint cram_loader_threads;
static uint cram_force_start;
static uint cram_strict_write;
static uint cram_flush_level;
static uint cram_page_rows;
static uint cram_compress_log;
static uint cram_precompress_log; // not exposed to MariaDB
static uint cram_verbose;
static uint cram_table_lists;
static uint cram_job_queue_size;
static uint cram_write_queue_size;
static uint cram_check_queue_size;
static uint cram_index_weight;

/* Stats counters passed up to mysqld as SHOW_ULONGLONG */
static pthread_spinlock_t cram_stats_spinlock;
static ulonglong cram_worker_rows_touched;
static ulonglong cram_worker_pages_touched;
static ulonglong cram_worker_rows_matched;
static ulonglong cram_tables_created;
static ulonglong cram_tables_deleted;
static ulonglong cram_tables_renamed;
static ulonglong cram_pages_created;
static ulonglong cram_pages_deleted;
static ulonglong cram_rows_created;
static ulonglong cram_rows_deleted;
static ulonglong cram_rows_inserted;
static ulonglong cram_rows_appended;
static ulonglong cram_blobs_created;
static ulonglong cram_blobs_deleted;
static ulonglong cram_worker_jobs_queued;
static ulonglong cram_worker_jobs_processed;
static ulonglong cram_worker_jobs_stalled;
static ulonglong cram_worker_jobs_failed;
static ulonglong cram_log_events_queued;
static ulonglong cram_log_events_processed;
static ulonglong cram_log_events_stalled;
static ulonglong cram_log_events_failed;
static ulonglong cram_check_events_queued;
static ulonglong cram_check_events_processed;
static ulonglong cram_check_events_stalled;
static ulonglong cram_check_events_failed;

/* Background worker thread job list */
static CramQueue *cram_job_queue;

/* Background log-writer thread */
static CramQueue *cram_write_queue;
static pthread_t cram_writer_thread;
static bool cram_writer_done;

/* Background janitor thread */
static CramQueue *cram_check_queue;
static pthread_t cram_janitor_thread;
static bool cram_janitor_done;

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

static bool cram_bitmap_chk(CramBitMap bitmap, uint bit)
{
  return bitmap[bit/8] & (1 << (bit%8));
}

static void cram_bitmap_set(CramBitMap bitmap, uint bit)
{
  bitmap[bit/8] |= (1 << (bit%8));
}

static void cram_bitmap_clr(CramBitMap bitmap, uint bit)
{
  bitmap[bit/8] &= ~(1 << (bit%8));
}

static CramBitMap cram_bitmap_create(uint bits)
{
  return (CramBitMap) cram_alloc((bits / 8) + 1);
}

static void cram_bitmap_free(CramBitMap bitmap)
{
  cram_free(bitmap);
}

static void cram_string_init(CramString *string)
{
  string->length = 0;
  string->limit  = 1024;
  string->buffer = (uchar*) cram_alloc(string->limit);
}

static void cram_string_destroy(CramString *string)
{
  cram_free(string->buffer);
}

static CramString* cram_string_create()
{
  CramString *string = (CramString*) cram_alloc(sizeof(CramString));
  cram_string_init(string);
  return string;
}

static void cram_string_free(CramString *string)
{
  cram_string_destroy(string);
  cram_free(string);
}

static void cram_string_reset(CramString *string)
{
  string->length = 0;
}

static void cram_string_push(CramString *string, uchar *buffer, size_t length)
{
  if (buffer && length > 0)
  {
    if (string->length + length + 1 > string->limit)
    {
      string->limit = string->length + length + 1;
      string->buffer = (uchar*) cram_realloc(string->buffer, string->limit);
    }
    memmove(string->buffer + string->length, buffer, length);
    string->length += length;
    string->buffer[string->length] = 0;
  }
}

static void cram_string_printf(CramString *string, const char *format, ...)
{
  char buff[1024];
  va_list args;
  va_start(args, format);
  int length = vsnprintf(buff, sizeof(buff), format, args);
  va_end(args);
  if (length >= 0)
    cram_string_push(string, (uchar*)buff, length);
}

/*
static void cram_string_dump(CramString *string, const uchar *buffer, size_t length)
{
  for (size_t i = 0; i < length; i++)
    cram_string_printf(string, "%02x ", buffer[i]);
}
*/

static void cram_queue_init(CramQueue *queue, size_t width)
{
  memset(queue, 0, sizeof(CramQueue));
  pthread_mutex_init(&queue->mutex, NULL);
  pthread_cond_init(&queue->read_cond, NULL);
  pthread_cond_init(&queue->write_cond, NULL);
  pthread_cond_init(&queue->wait_cond, NULL);
  queue->items = (void**) cram_alloc(sizeof(void*) * width);
  queue->width = width;
}

static void cram_queue_destroy(CramQueue *queue)
{
  pthread_cond_destroy(&queue->read_cond);
  pthread_cond_destroy(&queue->write_cond);
  pthread_cond_destroy(&queue->wait_cond);
  pthread_mutex_destroy(&queue->mutex);
  cram_free(queue->items);
}

static CramQueue* cram_queue_create(size_t width)
{
  CramQueue *queue = (CramQueue*) cram_alloc(sizeof(CramQueue));
  cram_queue_init(queue, width);
  return queue;
}

static void cram_queue_free(CramQueue *queue)
{
  cram_queue_destroy(queue);
  cram_free(queue);
}

static void cram_queue_submit(CramQueue *queue, void *ptr)
{
  pthread_mutex_lock(&queue->mutex);

  while (!queue->halt && queue->count == queue->width)
  {
    queue->stalls++;
    pthread_cond_wait(&queue->write_cond, &queue->mutex);
  }
  if (!queue->halt) {
    if (queue->write == queue->width)
      queue->write = 0;
    queue->items[queue->write++] = ptr;
    queue->count++;
    queue->total++;
    pthread_cond_signal(&queue->read_cond);
  }
  pthread_mutex_unlock(&queue->mutex);
}

static bool cram_queue_trysubmit(CramQueue *queue, void *ptr)
{
  bool rc = FALSE;
  pthread_mutex_lock(&queue->mutex);
  if (!queue->halt && queue->count < queue->width)
  {
    rc = TRUE;
    if (queue->write == queue->width)
      queue->write = 0;
    queue->items[queue->write++] = ptr;
    queue->count++;
    queue->total++;
    pthread_cond_signal(&queue->read_cond);
  }
  pthread_mutex_unlock(&queue->mutex);
  return rc;
}

static void* cram_queue_accept(CramQueue *queue)
{
  void *ptr = NULL;
  pthread_mutex_lock(&queue->mutex);

  while (!queue->halt && queue->count == 0)
    pthread_cond_wait(&queue->read_cond, &queue->mutex);

  if (!queue->halt) {
    if (queue->read == queue->width)
      queue->read = 0;
    ptr = queue->items[queue->read++];
    queue->count--;
    pthread_cond_signal(&queue->write_cond);
  }
  pthread_mutex_unlock(&queue->mutex);
  return ptr;
}

static void cram_queue_bump(CramQueue *queue)
{
  pthread_mutex_lock(&queue->mutex);
  queue->complete++;
  pthread_cond_broadcast(&queue->wait_cond);
  pthread_mutex_unlock(&queue->mutex);
}

static void cram_queue_halt(CramQueue *queue)
{
  pthread_mutex_lock(&queue->mutex);
  queue->halt = TRUE;
  pthread_cond_broadcast(&queue->read_cond);
  pthread_cond_broadcast(&queue->write_cond);
  pthread_mutex_unlock(&queue->mutex);
}

static void cram_queue_resume(CramQueue *queue)
{
  pthread_mutex_lock(&queue->mutex);
  queue->halt = FALSE;
  pthread_cond_broadcast(&queue->read_cond);
  pthread_cond_broadcast(&queue->write_cond);
  pthread_mutex_unlock(&queue->mutex);
}

static void cram_queue_wait(CramQueue *queue)
{
  pthread_mutex_lock(&queue->mutex);
  if (queue->total > queue->complete) {
    uint64 events = queue->total;
    while (!queue->halt && queue->complete < events)
      pthread_cond_wait(&queue->wait_cond, &queue->mutex);
  }
  pthread_mutex_unlock(&queue->mutex);
}

static void cram_list_init(CramList *list)
{
  pthread_rwlock_init(&list->rwlock, NULL);
}

static void cram_list_destroy(CramList *list)
{
  while (list->first) {
    CramNode *node = list->first->next;
    cram_free(node);
    list->first = node;
  }
  pthread_rwlock_destroy(&list->rwlock);
}

static CramList* cram_list_create()
{
  CramList *list = (CramList*) cram_alloc(sizeof(CramList));
  cram_list_init(list);
  return list;
}

static void cram_list_free(CramList *list)
{
  cram_list_destroy(list);
  cram_free(list);
}

static void cram_list_open_read(CramList *list)
{
  pthread_rwlock_rdlock(&list->rwlock);
}

static void cram_list_open_write(CramList *list)
{
  pthread_rwlock_wrlock(&list->rwlock);
}

static void cram_list_close(CramList *list)
{
  pthread_rwlock_unlock(&list->rwlock);
}

static void cram_list_push(CramList *list, void *item)
{
  CramNode *node = (CramNode*) cram_alloc(sizeof(CramNode));
  if (list->last) {
    list->last->next = node;
    list->last = node;
  } else {
    list->first = node;
    list->last  = node;
  }
  list->length++;
  node->item = item;
}

static void cram_list_shunt(CramList *list, void *item)
{
  CramNode *node = (CramNode*) cram_alloc(sizeof(CramNode));
  node->next = list->first;
  list->first = node;
  if (!list->last)
    list->last = node;
  list->length++;
  node->item = item;
}

static bool cram_list_eq(void *item, void *context)
{
  return item == context;
}

static void* cram_list_remove(CramList *list, CramEqual equal, void *context)
{
  void *item = NULL;
  CramNode *prev = NULL, *node = list->first;
  for (; node && !equal(node->item, context); prev = node, node = node->next);
  if (node) {
    item = node->item;
    if (node == list->first) list->first = node->next;
    if (node == list->last)  list->last  = prev;
    if (prev) prev->next = node->next;
    cram_free(node);
    list->length--;
  }
  return item;
}

static void* cram_list_walk(CramList *list, CramEqual equal, void *context)
{
  CramNode *node = list->first;
  while (node && !equal(node->item, context))
    node = node->next;
  return node ? node->item: NULL;
}

static void cram_list_join(CramList *listA, CramList *listB)
{
  if (listA->first && listB->first)
  {
    listA->last->next = listB->first;
    listA->last = listB->last;
    listA->length += listB->length;
  }
  else
  if (listB->first)
  {
    listA->first = listB->first;
    listA->last  = listB->last;
    listA->length = listB->length;
  }
  listB->first = NULL;
  listB->last  = NULL;
  listB->length = 0;
}

static void cram_dict_init(CramDict *hash, size_t width, size_t locks)
{
  hash->width = width;
  hash->locks = locks;
  hash->chains  = (CramNode**) cram_alloc(sizeof(CramNode*) * width);
  hash->rwlocks = (pthread_rwlock_t*) cram_alloc(sizeof(pthread_rwlock_t) * locks);
  for (size_t i = 0; i < locks; i++)
    pthread_rwlock_init(&hash->rwlocks[i], NULL);
}

static void cram_dict_destroy(CramDict *hash)
{
  for (size_t i = 0; i < hash->width; i++) {
    while (hash->chains[i]) {
      CramNode *next = hash->chains[i]->next;
      cram_free(hash->chains[i]);
      hash->chains[i] = next;
  }}
  for (size_t i = 0; i < hash->locks; i++)
    pthread_rwlock_destroy(&hash->rwlocks[i]);
  cram_free(hash->rwlocks);
  cram_free(hash->chains);
}

static CramDict* cram_dict_create(size_t width, size_t locks)
{
  CramDict *hash = (CramDict*) cram_alloc(sizeof(CramDict));
  cram_dict_init(hash, width, locks);
  return hash;
}

static void cram_dict_free(CramDict *hash)
{
  cram_dict_destroy(hash);
  cram_free(hash);
}

static void cram_dict_open_read(CramDict *hash, uint hashval)
{
  uint lock = hashval % hash->locks;
  pthread_rwlock_rdlock(&hash->rwlocks[lock]);
}

static void cram_dict_open_write(CramDict *hash, uint hashval)
{
  uint lock = hashval % hash->locks;
  pthread_rwlock_wrlock(&hash->rwlocks[lock]);
}

static void cram_dict_close(CramDict *hash, uint hashval)
{
  uint lock = hashval % hash->locks;
  pthread_rwlock_unlock(&hash->rwlocks[lock]);
}

static void* cram_dict_get(CramDict *hash, uint hashval, CramEqual equal, void *context)
{
  uint chain = hashval % hash->width;
  CramNode *n = hash->chains[chain];
  while (n && !equal(n->item, context))
    n = n->next;
  return n ? n->item: NULL;
}

static void* cram_dict_incref(CramDict *hash, uint hashval, CramEqual equal, void *context, CramCreate create)
{
  uint chain = hashval % hash->width;
  CramNode *n = hash->chains[chain];
  while (n && !equal(n->item, context))
    n = n->next;
  if (!n) {
    n = (CramNode*) cram_alloc(sizeof(CramNode));
    n->count = 0;
    n->item = create(n, context);
    n->next = hash->chains[chain];
    hash->chains[chain] = n;
  }
  n->count++;
  return n->item;
}

static void cram_dict_decref(CramDict *hash, uint hashval, CramEqual equal, void *context, CramDestroy destroy)
{
  uint chain = hashval % hash->width;
  CramNode **n = &hash->chains[chain];
  while (n && (*n) && !equal((*n)->item, context))
    n = &(*n)->next;
  if (n && (*n)) {
    (*n)->count--;
    if ((*n)->count == 0) {
      CramNode *f = (*n);
      *n = (*n)->next;
      if (destroy) destroy(f->item, context);
      cram_free(f);
    }
  }
}

static void cram_dict_store(CramDict *hash, uint hashval, void *item, bool check)
{
  uint chain = hashval % hash->width;
  CramNode *n = hash->chains[chain];
  while (check && n && n->item != item)
    n = n->next;
  if (!check || !n) {
    n = (CramNode*) cram_alloc(sizeof(CramNode));
    n->item = item;
    n->next = hash->chains[chain];
    hash->chains[chain] = n;
  }
}

static void cram_dict_purge(CramDict *hash, uint hashval, void *item)
{
  uint chain = hashval % hash->width;
  CramNode **n = &hash->chains[chain];
  while (n && (*n) && (*n)->item != item)
    n = &(*n)->next;
  if (n && (*n)) {
    CramNode *f = (*n);
    *n = (*n)->next;
    cram_free(f);
  }
}

static size_t cram_deflate(uchar *data, size_t width)
{
  uchar *buff = (uchar*) cram_alloc(width);
  uint length = width;

  z_stream stream;
  int err;

  stream.next_in   = (Bytef*)data;
  stream.avail_in  = (uInt)width;
  stream.next_out  = (Bytef*)buff;
  stream.avail_out = (uInt)length;
  stream.zalloc    = Z_NULL;
  stream.zfree     = Z_NULL;
  stream.opaque    = Z_NULL;

  err = deflateInit2(&stream, Z_BEST_SPEED, Z_DEFLATED, 15, 9, Z_HUFFMAN_ONLY);
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

static uint64 cram_id_create()
{
  pthread_spin_lock(&cram_sequence_spinlock);
  uint64 id = cram_sequence++;
  pthread_spin_unlock(&cram_sequence_spinlock);
  return id;
}

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
static uint32 cram_dict33(const uchar *buffer, size_t length)
{
  uint32 hash = 5381; size_t i = 0;
  for (
    length = length > 1024 ? 1024: length, i = 0;
    i < length;
    hash = hash * 33 + buffer[i++]
  );
  return hash;
}

struct cram_dict_context {
  uint32 hashval;
  const uchar *buffer;
  size_t length;
  bool deleted;
  bool created;
};

static bool cram_dict_cmp(void *item, void *context)
{
  CramBlob *blob = (CramBlob*) item;
  struct cram_dict_context *st = (struct cram_dict_context*) context;
  return blob && st->length == blob->length && memcmp(st->buffer, blob->buffer, st->length) == 0;
}

static void* cram_blob_create(void *item, void *context)
{
  struct cram_dict_context *st = (struct cram_dict_context*) context;
  CramBlob *blob = (CramBlob*) cram_alloc(sizeof(CramBlob));
  blob->buffer = (uchar*) cram_alloc(st->length);
  memmove(blob->buffer, st->buffer, st->length);
  blob->length = st->length;
  blob->hashval = st->hashval;
  st->created = TRUE;
  return blob;
}

static void cram_blob_free(void *item, void *context)
{
  struct cram_dict_context *st = (struct cram_dict_context*) context;
  CramBlob *blob = (CramBlob*) item;
  cram_free(blob->buffer);
  cram_free(blob);
  st->deleted = TRUE;
}

static CramBlob* cram_blob_inc(const uchar *buffer, size_t length)
{
  uint hashval = cram_dict33(buffer, length);
  struct cram_dict_context st;
  memset(&st, 0, sizeof(struct cram_dict_context));
  st.buffer = buffer; st.length = length; st.hashval = hashval;
  cram_dict_open_write(cram_dict, hashval);
  CramBlob *blob = (CramBlob*) cram_dict_incref(cram_dict, hashval, cram_dict_cmp, &st, cram_blob_create);
  cram_dict_close(cram_dict, hashval);
  if (st.created) cram_stat_inc(&cram_stats_spinlock, &cram_blobs_created);
  return blob;
}

static void cram_blob_dec(const uchar *buffer, size_t length)
{
  uint hashval = cram_dict33(buffer, length);
  struct cram_dict_context st;
  memset(&st, 0, sizeof(struct cram_dict_context));
  st.buffer = buffer; st.length = length; st.hashval = hashval;
  cram_dict_open_write(cram_dict, hashval);
  cram_dict_decref(cram_dict, hashval, cram_dict_cmp, &st, cram_blob_free);
  cram_dict_close(cram_dict, hashval);
  if (st.deleted) cram_stat_inc(&cram_stats_spinlock, &cram_blobs_deleted);
}

static CramBlob* cram_field_to_blob(Field *field)
{
  CramBlob *blob = NULL;
  if (field->is_null())
  {
    blob = NULL;
  }
  else
  if (field->result_type() == INT_RESULT)
  {
    int64 n = field->val_int();
    blob = cram_blob_inc((uchar*)(&n), sizeof(int64));
  }
  else
  {
    char pad[1024];
    String tmp(pad, sizeof(pad), &my_charset_bin);
    field->val_str(&tmp, &tmp);
    blob = cram_blob_inc((uchar*)tmp.ptr(), tmp.length());
  }
  return blob;
}

static void cram_field_set(CramBlob **blobs, uint index, Field *field)
{
  if (blobs[index])
    cram_blob_dec(blobs[index]->buffer, blobs[index]->length);
  blobs[index] = cram_field_to_blob(field);
}

static void cram_page_free(CramTable *table, CramPage *page)
{
  cram_free(page->rows);
  cram_bitmap_free(page->bitmap);
  pthread_rwlock_destroy(&page->lock);
  cram_free(page);
  cram_stat_inc(&cram_stats_spinlock, &cram_pages_deleted);
}

static void cram_row_index(CramTable *table, CramPage *page, CramRow *row)
{
  uint li = 0;
  for (; table->lists[li] != page->list; li++);
  for (uint i = 0; i < table->columns; i++)
  {
    uint hashval = (row->blobs[i]) ? row->blobs[i]->hashval: 0;
    uint chain   = hashval % table->index_width;
    cram_bitmap_set(page->bitmap, chain);
  }
}

static void cram_page_deindex(CramTable *table, CramPage *page)
{
  for (uint i = 0; i < table->index_width; i++)
  {
    uint chain = i % table->index_width;
    cram_bitmap_clr(page->bitmap, chain);
  }
}

static void cram_page_index(CramTable *table, CramPage *page)
{
  for (uint i = 0; i < cram_page_rows; i++)
    if (page->rows[i].blobs)
      cram_row_index(table, page, &page->rows[i]);
}

static void* cram_janitor(void *p)
{
  cram_note("janitor started");

  for (;;)
  {
    CramCheckEvent *event = (CramCheckEvent*) cram_queue_accept(cram_check_queue);

    if (!event && cram_janitor_done)
      break;

    if (event)
    {
      CramTable *table = event->table;
      CramPage *page   = event->page;
      CramList *list   = page->list;

      cram_list_open_write(list);
      pthread_rwlock_wrlock(&page->lock);

      if (page->count == 0)
      {
        cram_page_deindex(table, page);
        cram_list_remove(page->list, cram_list_eq, page);
        cram_page_free(table, page);
        pthread_rwlock_unlock(&page->lock);
      }
      else
      if (page->changes > cram_page_rows/4)
      {
        cram_page_deindex(table, page);
        cram_page_index(table, page);
        page->queued = FALSE;
        pthread_rwlock_unlock(&page->lock);
      }
      else
      {
        pthread_rwlock_unlock(&page->lock);
      }

      cram_list_close(list);

      cram_queue_bump(cram_check_queue);
      cram_free(event);

      cram_stat_inc(&cram_stats_spinlock, &cram_check_events_processed);
    }
  }

  cram_note("janitor stopped");
  return NULL;
}

static void cram_janitor_start()
{
  cram_queue_resume(cram_check_queue);
  pthread_create(&cram_janitor_thread, NULL, cram_janitor, NULL);
}

static void cram_janitor_stop()
{
  cram_janitor_done = TRUE;
  cram_queue_halt(cram_check_queue);
  pthread_join(cram_janitor_thread, NULL);
}

static void cram_check_entry(CramTable *table, CramPage *page)
{
  CramCheckEvent *event = (CramCheckEvent*) cram_alloc(sizeof(CramCheckEvent));
  event->table = table;
  event->page = page;
  bool submit = cram_queue_trysubmit(cram_check_queue, event);
  if (!submit) cram_free(event);

  pthread_spin_lock(&cram_stats_spinlock);
  if (submit) cram_check_events_queued++; else cram_check_events_failed++;
  cram_check_events_stalled = cram_check_queue->stalls;
  pthread_spin_unlock(&cram_stats_spinlock);
}

static void cram_page_changed(CramTable *table, CramPage *page)
{
  pthread_rwlock_wrlock(&page->lock);
  bool dirty = ++page->changes > cram_page_rows/4 || page->count == 0;
  bool rebuild = dirty && !page->queued;
  if (rebuild) page->queued = TRUE;
  pthread_rwlock_unlock(&page->lock);
  if (rebuild) cram_check_entry(table, page);
}

static CramRow* cram_row_create(CramTable *table, uint64 id, CramBlob **blobs, bool log_entry, uint insert_mode)
{
  CramRow *row = NULL;
  CramPage *page = NULL;
  CramList *list = NULL;
  bool page_created = FALSE;

  if (!id)
    id = cram_id_create();

  if (!blobs)
    blobs = (CramBlob**) cram_alloc(sizeof(CramBlob*) * table->columns);

  list = table->lists[id % cram_table_lists];

  cram_list_open_read(list);
  CramNode *n = list->first; CramPage *p;

  for (uint i = 0; n && !row; n = n->next, i++)
  {
    p = (CramPage*) n->item;
    // If someone else has a lock, just skip onward because we
    // really don't care where rows are inserted. This reduces
    // stalls for worker threads *especially*.
    if (pthread_rwlock_trywrlock(&p->lock) == 0)
    {
      if (p->count < cram_page_rows && p->row_free)
      {
        page = p;
        row = p->row_free;
        p->row_free = row->next;
        goto done;
      }
      pthread_rwlock_unlock(&p->lock);
    }
    // Append mode just means "don't scan the whole page list".
    if (insert_mode == CRAM_APPEND && i > cram_loader_threads)
      break;
  }

  cram_list_close(list);

  page = (CramPage*) cram_alloc(sizeof(CramPage));
  pthread_rwlock_init(&page->lock, NULL);
  pthread_rwlock_wrlock(&page->lock);
  page->rows = (CramRow*) cram_alloc(sizeof(CramRow) * cram_page_rows);
  page->bitmap = cram_bitmap_create(table->index_width);
  page_created = TRUE;
  row = &page->rows[0];
  page->list = list;

  for (uint i = 1; i < cram_page_rows; i++)
  {
    page->rows[i].next = page->row_free;
    page->row_free = &page->rows[i];
  }

  cram_list_open_write(list);
  cram_list_shunt(list, page);

done:
  cram_list_close(list);

  page->count++;
  row->id = id;
  row->blobs = blobs;

  if (log_entry)
    cram_log_insert(table, row);

  cram_row_index(table, page, row);
  pthread_rwlock_unlock(&page->lock);

  pthread_spin_lock(&cram_stats_spinlock);
  cram_rows_created++;
  if (page_created) cram_pages_created++;
  if (insert_mode == CRAM_INSERT) cram_rows_inserted++;
  if (insert_mode == CRAM_APPEND) cram_rows_appended++;
  pthread_spin_unlock(&cram_stats_spinlock);

  return row;
}

static int cram_row_free(CramTable *table, CramPage *page, CramRow *row, bool log_entry)
{
  int rc = HA_ERR_RECORD_DELETED;

  CramList *list = page->list;

  uint64 id = 0;
  CramBlob **blobs = NULL;

  cram_list_open_read(list);
  pthread_rwlock_wrlock(&page->lock);

  if (row->blobs)
  {
    id = row->id;
    blobs = row->blobs;
    row->blobs = NULL;
    page->count--;
    row->next = page->row_free;
    page->row_free = row;
  }

  pthread_rwlock_unlock(&page->lock);
  cram_list_close(list);

  if (blobs)
  {
    if (log_entry)
      cram_log_delete(table, id);

    for (uint i = 0; i < table->columns; i++)
    {
      if (blobs[i])
        cram_blob_dec(blobs[i]->buffer, blobs[i]->length);
    }
    cram_free(blobs);
    cram_stat_inc(&cram_stats_spinlock, &cram_rows_deleted);
    rc = 0;
  }
  return rc;
}

static bool cram_table_by_name_cb(void *item, void *context)
{
  return strcmp(((CramTable*)item)->name, (char*)context) == 0;
}

static bool cram_table_by_id_cb(void *item, void *context)
{
  return ((CramTable*)item)->id == *((uint64*)context);
}

static CramTable* cram_table_construct(const char *name, uint32 columns, uint64 id)
{
  cram_debug("creating %s", name);

  if (!id)
    id = cram_id_create();

  CramTable *table = (CramTable*) cram_alloc(sizeof(CramTable));
  pthread_rwlock_init(&table->lock, NULL);

  table->name = (char*) cram_alloc(strlen(name)+1);
  strcpy(table->name, name);

  table->id = id;
  table->columns = columns;
  table->opened = 0;

  table->lists = (CramList**) cram_alloc(sizeof(CramList*) * cram_table_lists);
  table->index_width = cram_page_rows * columns * cram_index_weight;

  for (uint i = 0; i < cram_table_lists; i++)
  {
    table->lists[i] = cram_list_create();
  }

  cram_stat_inc(&cram_stats_spinlock, &cram_tables_created);

  return table;
}

static CramTable* cram_table_prepare(const char *name, uint32 columns, uint64 id)
{
  cram_list_open_write(cram_tables);
  CramTable *table = (CramTable*) cram_list_walk(cram_tables, cram_table_by_name_cb, (void*)name);

  if (!table)
  {
    table = cram_table_construct(name, columns, id);
    cram_list_push(cram_tables, table);
  }

  cram_list_close(cram_tables);
  return table;
}

static CramTable* cram_table_open(const char *name, uint32 columns, uint64 id)
{
  cram_list_open_write(cram_tables);
  CramTable *table = (CramTable*) cram_list_walk(cram_tables, cram_table_by_name_cb, (void*)name);

  if (!table)
  {
    table = cram_table_construct(name, columns, id);
    cram_list_push(cram_tables, table);
  }

  pthread_rwlock_wrlock(&table->lock);
  table->opened++;
  pthread_rwlock_unlock(&table->lock);

  cram_list_close(cram_tables);
  return table;
}

static void cram_table_close(CramTable *table)
{
  pthread_rwlock_wrlock(&table->lock);
  table->opened--;
  pthread_rwlock_unlock(&table->lock);
}

static int cram_table_drop(const char *name)
{
  int rc = -1;
  cram_list_open_write(cram_tables);
  CramTable *t = (CramTable*) cram_list_remove(cram_tables, cram_table_by_name_cb, (void*)name);
  cram_list_close(cram_tables);

  if (t)
  {
    cram_debug("dropping %s", name);

    pthread_rwlock_wrlock(&t->lock);
    while (t->opened > 0)
    {
      pthread_rwlock_unlock(&t->lock);
      usleep(100);
      pthread_rwlock_wrlock(&t->lock);
    }

    for (uint i = 0; i < cram_table_lists; i++)
    {
      for (CramNode *n = t->lists[i]->first; n; n = n->next)
      {
        CramPage *p = (CramPage*) n->item;
        for (uint j = 0; j < cram_page_rows; j++)
        {
          if (p->rows[j].blobs)
            cram_row_free(t, p, &p->rows[j], CRAM_NO_LOG);
        }
        cram_page_free(t, p);
      }
      cram_list_free(t->lists[i]);
    }

    pthread_rwlock_destroy(&t->lock);
    cram_free(t->lists);
    cram_free(t->name);
    cram_free(t);

    rc = 0;
  }
  cram_stat_inc(&cram_stats_spinlock, &cram_tables_deleted);
  return rc;
}

static int cram_table_rename(const char *from, const char *to)
{
  cram_list_open_write(cram_tables);
  CramTable *table = (CramTable*) cram_list_walk(cram_tables, cram_table_by_name_cb, (void*)from);
  if (table)
  {
    cram_debug("renaming %s %s", from, to);
    pthread_rwlock_wrlock(&table->lock);
    cram_free(table->name);
    table->name = (char*) cram_alloc(strlen(to)+1);
    strcpy(table->name, to);
    pthread_rwlock_unlock(&table->lock);
  }
  cram_list_close(cram_tables);
  cram_stat_inc(&cram_stats_spinlock, &cram_tables_renamed);
  return table ? 0: -1;
}

static CramTable* cram_table_by_id(uint64 id)
{
  cram_list_open_read(cram_tables);
  CramTable *table = (CramTable*) cram_list_walk(cram_tables, cram_table_by_id_cb, &id);
  cram_list_close(cram_tables);
  return table;
}

static int cram_epoch_create()
{
  char name[1024];
  snprintf(name, sizeof(name), CRAM_FILE, cram_epoch++);

  if (cram_epoch_file)
  {
    fflush(cram_epoch_file);
    fclose(cram_epoch_file);
  }
  cram_epoch_file = fopen(name, "ab");
  if (!cram_epoch_file)
  {
    cram_error("failed to create %s", name);
    abort();
  }
  cram_epoch_eof  = 0;
  return cram_epoch_file ? 0: -1;
}

// The writer thread writes log events to disk.

static void* cram_writer(void *p)
{
  cram_note("writer started");

  for (;;)
  {
    CramLogEvent *event = (CramLogEvent*) cram_queue_accept(cram_write_queue);

    if (!event && cram_writer_done)
      break;

    if (event)
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
      // Client thread precompressed payload?
      if (cram_precompress_log && event->cdata)
      {
        *((uint64*)&buffer[cwidth_offset]) = event->cwidth;
        memmove(payload, event->cdata, event->cwidth);
        write_bytes = payload_offset + event->cwidth;
      }
      else
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

      if (cram_flush_level > 0 && (cram_log_events_processed % cram_flush_level) == 0)
      {
        fflush(cram_epoch_file);
      }

      pthread_mutex_unlock(&cram_epoch_mutex);

      cram_free(buffer);
      cram_free(event->cdata);
      cram_free(event->data);
      cram_free(event);

      cram_stat_inc(&cram_stats_spinlock, &cram_log_events_processed);
      cram_queue_bump(cram_write_queue);
    }
  }

  pthread_mutex_lock(&cram_epoch_mutex);
  fflush(cram_epoch_file);
  pthread_mutex_unlock(&cram_epoch_mutex);
  cram_note("writer stopped");
  return NULL;
}

static void cram_writer_start()
{
  cram_queue_resume(cram_write_queue);
  pthread_create(&cram_writer_thread, NULL, cram_writer, NULL);
}

static void cram_writer_stop()
{
  cram_writer_done = TRUE;
  cram_queue_halt(cram_write_queue);
  pthread_join(cram_writer_thread, NULL);
}

static void cram_writer_wait()
{
  cram_queue_wait(cram_write_queue);
}

// Send a log entry to the writer thread. This function does not
// guarantee that data has been flushed to disk! That requires:
// - cram_flush_level
// - cram_writer_wait()

static int cram_log_entry(uchar *data, size_t width)
{
  CramLogEvent *event = (CramLogEvent*) cram_alloc(sizeof(CramLogEvent));
  event->data = (uchar*) cram_alloc(width);
  memmove(event->data, data, width);
  event->width = width;

  if (cram_precompress_log)
  {
    uchar *payload = (uchar*) cram_alloc(width);
    memmove(payload, data, width);

    size_t length;
    if (cram_compress_log && (length = cram_deflate(payload, width)) && length < width)
    {
      event->cwidth = length;
      event->cdata  = payload;
    }
    else
    {
      cram_free(payload);
    }
  }
  cram_queue_submit(cram_write_queue, event);

  pthread_spin_lock(&cram_stats_spinlock);
  cram_log_events_queued++;
  cram_log_events_stalled = cram_write_queue->stalls;
  pthread_spin_unlock(&cram_stats_spinlock);
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

  cram_table_prepare(name, columns, id);

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

  CramBlob **blobs = (CramBlob**) cram_alloc(sizeof(CramBlob*) * table->columns);
  uint64 used = 0;

  for (uint32 column = 0; column < table->columns; column++)
  {
    uint32 length = 0;

    if (nulls & 1<<column)
    {
      blobs[column] = NULL;
    }
    else
    if (types & 1<<column)
    {
      length = *((uchar*)data);
      data += sizeof(uchar);
      used += sizeof(uchar);
      blobs[column] = cram_blob_inc(data, length);
    }
    else
    {
      length = *((uint32*)data);
      data += sizeof(uint32);
      used += sizeof(uint32);
      blobs[column] = cram_blob_inc(data, length);
    }
    data += length;
    used += length;
  }

  cram_row_create(table, id, blobs, CRAM_NO_LOG, CRAM_APPEND);

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
  for (uint i = 0; !page && i < cram_table_lists; i++)
  {
    for (CramNode *n = table->lists[i]->first; !page && n; n = n->next)
    {
      CramPage *p = (CramPage*) n->item;
      for (uint j = 0; j < cram_page_rows; j++)
      {
        if (p->rows[j].id == id && p->rows[j].blobs)
        {
          page = p;
          row = &page->rows[j];
          break;
        }
      }
    }
  }
  if (page)
  {
    cram_row_free(table, page, row, CRAM_NO_LOG);
  }

  return 0;
}

static CramJob* cram_job_create(CramTable *table, CramCondition *condition, uint position)
{
  CramJob *job = (CramJob*) cram_alloc(sizeof(CramJob));

  job->table     = table;
  job->results   = cram_list_create();
  job->list      = position;
  job->condition = condition;

  pthread_mutex_init(&job->mutex, NULL);
  pthread_cond_init(&job->cond, NULL);
  cram_queue_submit(cram_job_queue, job);

  pthread_spin_lock(&cram_stats_spinlock);
  cram_worker_jobs_queued++;
  cram_worker_jobs_stalled = cram_job_queue->stalls;
  pthread_spin_unlock(&cram_stats_spinlock);
  return job;
}

static void cram_job_free(CramJob *job)
{
  pthread_mutex_destroy(&job->mutex);
  pthread_cond_destroy(&job->cond);
  cram_list_free(job->results);
  cram_free(job);
  cram_stat_inc(&cram_stats_spinlock, &cram_worker_jobs_processed);
}

static bool cram_job_check(CramRow *row, CramCondition *con)
{
  bool match = FALSE;
  uint k; uint32 length;
  uchar *blob_buf; uint32 blob_len;

  switch (con->type)
  {
    case CRAM_COND_NULL:
      match = row->blobs[con->index] ? FALSE: TRUE;
      break;

    case CRAM_COND_NOTNULL:
      match = row->blobs[con->index] ? TRUE: FALSE;
      break;

    case CRAM_COND_EQ:
      match = row->blobs[con->index] == con->blobs[0] ? TRUE: FALSE;
      break;

    case CRAM_COND_NE:
      match = row->blobs[con->index] != con->blobs[0] ? TRUE: FALSE;
      break;

    case CRAM_COND_LT:
      if (row->blobs[con->index]->length == sizeof(int64))
        match = *((int64*)row->blobs[con->index]->buffer) < con->number ? TRUE: FALSE;
      break;

    case CRAM_COND_LT_STR:
      length = con->length < row->blobs[con->index]->length
        ? con->length : row->blobs[con->index]->length;
      match = memcmp(row->blobs[con->index]->buffer, con->buffer, length) < 0 ? TRUE: FALSE;
      break;

    case CRAM_COND_GT:
      if (row->blobs[con->index]->length == sizeof(int64))
        match = *((int64*)row->blobs[con->index]->buffer) > con->number ? TRUE: FALSE;
      break;

    case CRAM_COND_GT_STR:
      length = con->length < row->blobs[con->index]->length
        ? con->length : row->blobs[con->index]->length;
      match = memcmp(row->blobs[con->index]->buffer, con->buffer, length) > 0 ? TRUE: FALSE;
      break;

    case CRAM_COND_LE:
      if (row->blobs[con->index]->length == sizeof(int64))
        match = *((int64*)row->blobs[con->index]->buffer) <= con->number ? TRUE: FALSE;
      break;

    case CRAM_COND_LE_STR:
      length = con->length < row->blobs[con->index]->length
        ? con->length : row->blobs[con->index]->length;
      match = memcmp(row->blobs[con->index]->buffer, con->buffer, length) <= 0 ? TRUE: FALSE;
      break;

    case CRAM_COND_GE:
      if (row->blobs[con->index]->length == sizeof(int64))
        match = *((int64*)row->blobs[con->index]->buffer) >= con->number ? TRUE: FALSE;
      break;

    case CRAM_COND_GE_STR:
      length = con->length < row->blobs[con->index]->length
        ? con->length : row->blobs[con->index]->length;
      match = memcmp(row->blobs[con->index]->buffer, con->buffer, length) >= 0 ? TRUE: FALSE;
      break;

    case CRAM_COND_IN:
      for (k = 0; !match && k < con->count; k++)
        match = row->blobs[con->index] == con->blobs[k] ? TRUE: FALSE;
      break;

    case CRAM_COND_LEADING:
      if (row->blobs[con->index])
      {
        blob_buf = row->blobs[con->index]->buffer;
        blob_len   = row->blobs[con->index]->length;

        match = blob_len >= con->like_len
          && memcmp(con->like, blob_buf + blob_len - con->like_len, con->like_len) == 0
          ? TRUE: FALSE;
      }
      break;

    case CRAM_COND_TRAILING:
      if (row->blobs[con->index])
      {
        blob_buf = row->blobs[con->index]->buffer;
        blob_len   = row->blobs[con->index]->length;

        match = blob_len >= con->like_len
          && memcmp(con->like, blob_buf, con->like_len) == 0
          ? TRUE: FALSE;
      }
      break;

    case CRAM_COND_CONTAINS:
      if (row->blobs[con->index])
      {
        blob_buf = row->blobs[con->index]->buffer;
        blob_len   = row->blobs[con->index]->length;

        if (blob_len >= con->like_len)
        {
          for (k = 0; !match && k < blob_len && blob_len - k >= con->like_len; k++)
            match = memcmp(con->like, &blob_buf[k], con->like_len) == 0 ? TRUE: FALSE;
        }
      }
      break;
  }

  return match;
}

static bool cram_job_row(CramJob *job, CramPage *page, CramRow *row)
{
  CramCondition *con = job->condition;

  job->rows++;

  if (!row->blobs)
    return FALSE;

  bool match = TRUE;

  if (con)
  {
    match = FALSE;
    do {
      match = cram_job_check(row, con);
      con = con->next;
    }
    while (match && con);
  }
  if (match)
  {
    CramResult *res = (CramResult*) cram_alloc(sizeof(CramResult));
    res->row = row;
    res->page = page;
    cram_list_push(job->results, res);
    job->matches++;
  }
  return match;
}

static bool cram_job_page(CramJob *job, CramPage *page)
{
  bool pass = TRUE;
  job->pages++;
  pthread_rwlock_rdlock(&page->lock);
  CramCondition *con = job->condition;
  for (; pass && con && con->type == CRAM_COND_EQ; con = con->next)
  {
    if (con->count == 1 && con->blobs[0])
      pass = cram_bitmap_chk(page->bitmap, con->blobs[0]->hashval % job->table->index_width);
  }
  if (pass) for (uint i = 0; i < cram_page_rows; i++)
  {
    CramRow *row = &page->rows[i];
    cram_job_row(job, page, row);
  }
  pthread_rwlock_unlock(&page->lock);
  return TRUE;
}

static void cram_job_execute(uint id, CramJob *job)
{
  CramTable *table = job->table;
  pthread_rwlock_rdlock(&table->lock);

  CramList *list = table->lists[job->list];
  cram_list_open_read(list);
  for (CramNode *node = list->first; node && !job->complete; node = node->next)
    cram_job_page(job, (CramPage*)node->item);
  cram_list_close(list);

  job->complete = TRUE;
  pthread_rwlock_unlock(&table->lock);
  cram_debug("%u worker matches %llu rows %llu pages %llu", id, job->matches, job->rows, job->pages);

  pthread_spin_lock(&cram_stats_spinlock);
  cram_worker_rows_touched += job->rows;
  cram_worker_pages_touched += job->pages;
  cram_worker_rows_matched += job->matches;
  pthread_spin_unlock(&cram_stats_spinlock);
}

static void* cram_worker(void *p)
{
  CramWorker *self = (CramWorker*) p;
  uint id = self - cram_workers;

  cram_note("%u worker started", id);
  while (self->run)
  {
    CramJob *job = (CramJob*) cram_queue_accept(cram_job_queue);

    if (job)
    {
      pthread_mutex_lock(&job->mutex);
      cram_debug("%u working job %s", id, job->table->name);
      cram_job_execute(id, job);
      pthread_cond_signal(&job->cond);
      pthread_mutex_unlock(&job->mutex);
      cram_queue_bump(cram_job_queue);
    }
  }
  self->done = TRUE;
  cram_note("%u worker stopped", id);
  return NULL;
}

static void cram_workers_start()
{
  cram_queue_resume(cram_job_queue);
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
    cram_workers[i].run = FALSE;

  cram_queue_halt(cram_job_queue);

  for (uint i = 0; i < cram_worker_threads; i++)
    pthread_join(cram_workers[i].thread, NULL);

  cram_free(cram_workers);
}

static CramResult* cram_row_find_one(CramTable *table, uint32 column, const uchar *key, uint32 key_len)
{
  CramResult *result = NULL;
  CramBlob *blob = cram_blob_inc(key, key_len);

  CramList *results = cram_list_create();
  CramJob *jobs[cram_table_lists];

  CramCondition *cc = (CramCondition*) cram_alloc(sizeof(CramCondition));
  cc->type  = CRAM_COND_EQ;
  cc->count = 1;
  cc->index = 0;
  cc->blobs = (CramBlob**) cram_alloc(sizeof(CramBlob*) * cc->count);
  cc->blobs[0] = blob;

  for (uint i = 0; i < cram_table_lists; i++)
  {
    cram_debug("job %u create", i);
    jobs[i] = cram_job_create(table, cc, i);
  }
  for (uint i = 0; i < cram_table_lists; i++)
  {
    CramJob *job = jobs[i];
    pthread_mutex_lock(&job->mutex);
    if (!job->complete)
      pthread_cond_wait(&job->cond, &job->mutex);
    cram_debug("job %u done", i);
    cram_list_join(results, job->results);
    pthread_mutex_unlock(&job->mutex);
    cram_job_free(job);
  }
  if (results->first)
    result = (CramResult*) cram_list_remove(results, cram_list_eq, results->first->item);

  while (results->first)
    cram_free(cram_list_remove(results, cram_list_eq, results->first->item));

  cram_list_free(results);
  cram_free(cc->blobs);
  cram_free(cc);
  cram_blob_dec(key, key_len);
  return result;
}

static void* cram_consolidator(void *p)
{
  CramConsolidateJob *job = (CramConsolidateJob*)p;

  for (CramNode *n = job->table->lists[job->list]->first; n; n = n->next)
  {
    CramPage *page = (CramPage*) n->item;
    for (uint j = 0; j < cram_page_rows; j++)
    {
      if (page->rows[j].blobs)
        cram_log_insert(job->table, &page->rows[j]);
    }
    cram_writer_wait();
  }

  pthread_mutex_lock(&job->mutex);
  job->complete = TRUE;
  pthread_mutex_unlock(&job->mutex);
  return NULL;
}

static void cram_consolidate()
{
  cram_note("waiting for writer...");
  cram_writer_wait();

  cram_note("consolidating data files...");

  pthread_mutex_lock(&cram_epoch_mutex);
  fclose(cram_epoch_file);
  remove("cram");
  for (uint64 i = 0; i < cram_epoch; i++)
  {
    char name[1024];
    snprintf(name, sizeof(name), CRAM_FILE, i);
    remove(name);
  }
  cram_epoch = 1;
  cram_epoch_file = NULL;
  cram_flush_level = 0;
  if (cram_compress_log) cram_precompress_log = 1;
  cram_epoch_create();
  pthread_mutex_unlock(&cram_epoch_mutex);

  cram_list_open_read(cram_tables);

  CramNode *node = cram_tables->first;
  while (node)
  {
    cram_log_create((CramTable*)node->item);
    node = node->next;
  }
  cram_writer_wait();

  pthread_mutex_lock(&cram_epoch_mutex);
  cram_epoch_create();
  pthread_mutex_unlock(&cram_epoch_mutex);

  node = cram_tables->first;
  while (node)
  {
    CramTable *table = (CramTable*) node->item;
    cram_note("%s", table->name);

    CramConsolidateJob jobs[cram_table_lists];

    for (uint i = 0; i < cram_table_lists; i++)
    {
      jobs[i].table = table;
      jobs[i].list  = i;
      jobs[i].complete = FALSE;
      pthread_mutex_init(&jobs[i].mutex, NULL);
      pthread_create(&jobs[i].thread, NULL, cram_consolidator, &jobs[i]);
    }

    for (uint i = 0; i < cram_table_lists; i++)
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
    cram_writer_wait();
    node = node->next;
  }

  cram_list_close(cram_tables);
  cram_writer_wait();

  uchar ok = 1;
  FILE *state = fopen("cram", "wb");
  fwrite(&ok, 1, 1, state);
  fclose(state);
}

static void* cram_loader(void *p)
{
  CramLoadJob *job = (CramLoadJob*)p;

  FILE *file = job->file;
  uchar *data = (uchar*) cram_alloc(CRAM_EPOCH);

  uint64 eof = fread(data, 1, CRAM_EPOCH, file);

  for (size_t offset = 0; offset < eof; )
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

  cram_free(data);
  fclose(file);

  job->complete = TRUE;
  job->success = TRUE;

  return NULL;
}

struct cram_status_st {
  uint64 memory;
  uint64 dict_clen;
  uint64 dict_cmax;
  uint64 dict_cmin;
  CramTable *table;
};

static bool cram_show_status_cb1(void *ptr, void *context)
{
  CramBlob *blob = (CramBlob*) ptr;
  struct cram_status_st *st = (struct cram_status_st*) context;
  st->memory += sizeof(CramNode) + sizeof(CramBlob) + blob->length;
  st->dict_clen++;
  return FALSE; // continue
}

static bool cram_show_status_cb3(void *ptr, void *context)
{
  CramPage *page = (CramPage*) ptr;
  struct cram_status_st *st = (struct cram_status_st*) context;
  st->memory += sizeof(CramPage)
    + (sizeof(CramRow) * cram_page_rows)
    + (sizeof(CramBlob*) * st->table->columns * page->count);
  return FALSE; // continue
}

static bool cram_show_status_cb2(void *ptr, void *context)
{
  CramTable *table = (CramTable*) ptr;
  struct cram_status_st *st = (struct cram_status_st*) context;
  st->memory += sizeof(CramNode) + sizeof(CramTable) + strlen(table->name);
  st->table = table;

  for (uint i = 0; i < cram_table_lists; i++)
  {
    cram_list_open_read(table->lists[i]);
    cram_list_walk(table->lists[i], cram_show_status_cb3, st);
    cram_list_close(table->lists[i]);
  }
  return FALSE; // continue
}

static bool cram_show_status(handlerton* hton, THD* thd, stat_print_fn* stat_print, enum ha_stat_type stat_type)
{
  CramString *string = cram_string_create();

  struct cram_status_st _st, *st = &_st;

  st->dict_clen = 0;
  st->dict_cmin = 0;
  st->dict_cmax = 0;
  st->memory = sizeof(CramDict) + (sizeof(CramNode*) * cram_dict_chains) + (sizeof(pthread_rwlock_t) * cram_dict_locks);

  for (uint i = 0; i < cram_dict_chains; i++)
  {
    cram_dict_open_read(cram_dict, i);
    cram_dict_get(cram_dict, i, cram_show_status_cb1, st);
    cram_dict_close(cram_dict, i);
    if (st->dict_clen < st->dict_cmin)
      st->dict_cmin = st->dict_clen;
    if (st->dict_clen > st->dict_cmax)
      st->dict_cmax = st->dict_clen;
  }

  cram_string_reset(string);
  cram_string_printf(string, "%llu MB, shortest chain: %llu, longest: %llu",
    st->memory/1024/1024, st->dict_cmin, st->dict_cmax);
  stat_print(thd, STRING_WITH_LEN("CRAM"), STRING_WITH_LEN("dictionary"), (char*)string->buffer, string->length);

  st->memory = sizeof(CramList);

  cram_list_open_read(cram_tables);
  cram_list_walk(cram_tables, cram_show_status_cb2, st);
  cram_list_close(cram_tables);

  cram_string_reset(string);
  cram_string_printf(string, "%llu MB", st->memory/1024/1024);
  stat_print(thd, STRING_WITH_LEN("CRAM"), STRING_WITH_LEN("tables"), (char*)string->buffer, string->length);

  cram_string_free(string);
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

  thr_lock_init(&mysql_lock);

  pthread_spin_init(&cram_sequence_spinlock, PTHREAD_PROCESS_PRIVATE);
  pthread_mutex_init(&cram_epoch_mutex, NULL);
  pthread_spin_init(&cram_stats_spinlock, PTHREAD_PROCESS_PRIVATE);

  cram_job_queue   = cram_queue_create(cram_job_queue_size);
  cram_write_queue = cram_queue_create(cram_write_queue_size);
  cram_check_queue = cram_queue_create(cram_check_queue_size);
  cram_dict = cram_dict_create(cram_dict_chains, cram_dict_locks);
  cram_tables = cram_list_create();

  cram_note("Time to cram!");

  bool clean_shutdown = FALSE;

  char name[1024];
  uchar state[1024];

  FILE *cram_state = fopen("cram", "r");
  if (cram_state)
  {
    if (fread(state, 1, sizeof(state), cram_state) > 0)
      clean_shutdown = state[0] != 0 ? TRUE: FALSE;
    fclose(cram_state);
  }

  cram_janitor_start();

  // An unclean shutdown may mean the order of log events is important,
  // so it has to be a single-threaded reload. A clean shutdown means
  // the log events can be replayed in parallel.
  int threads = clean_shutdown ? cram_loader_threads: 1;
  CramLoadJob loaders[threads], *job;
  memset(loaders, 0, sizeof(loaders));
  FILE *epoch_file = NULL;

  cram_note("log state is %s %d threads", clean_shutdown
    ? "clean; reloading...": "unclean; attempting reload...", threads);

  // First file contains tables after a clean shutdown. Ensure it is
  // loaded by a single thread.
  snprintf(name, sizeof(name), CRAM_FILE, 1LL);
  if ((epoch_file = fopen(name, "r")))
  {
    cram_note("loader %u %s", 0, name);
    job = &loaders[0];
    job->file = epoch_file;
    job->running = TRUE;
    job->epoch = 1;
    pthread_create(&job->thread, NULL, cram_loader, job);
    pthread_join(job->thread, NULL);
    memset(job, 0, sizeof(CramLoadJob));

    // Remaining files can probably be loaded in parallel
    for (cram_epoch = 2; ; cram_epoch++)
    {
      snprintf(name, sizeof(name), CRAM_FILE, cram_epoch);
      if (!(epoch_file = fopen(name, "r"))) break;

      // Scan for completed jobs
      for (int i = 0; i < threads; i++)
      {
        job = &loaders[i];
        if (job->running && job->complete)
        {
          pthread_join(job->thread, NULL);
          memset(job, 0, sizeof(CramLoadJob));
        }
      }
      // Find a free slot for the next job
      int thread = -1;
      for (int i = 0; thread < 0 && i < threads; i++)
      {
        job = &loaders[i];
        if (!job->running) thread = i;
      }
      // Nothing free? Wait for the oldest...
      if (thread < 0)
      {
        thread = 0;
        for (int i = 1; i < threads; i++)
        {
          job = &loaders[i];
          if (job->running && job->epoch < loaders[thread].epoch)
            thread = i;
        }
        job = &loaders[thread];
        pthread_join(job->thread, NULL);
        memset(job, 0, sizeof(CramLoadJob));
      }

      cram_note("loader %u %s", thread, name);
      job = &loaders[thread];
      job->file = epoch_file;
      job->running = TRUE;
      pthread_create(&job->thread, NULL, cram_loader, job);

      // Epoch log files after a clean shutdon can be quite homogeneous,
      // causing the loading threads to finish almot simultaneousy. Stagger
      // things just a bit.
      usleep(1000);
    }

    // Wait for all jobs to complete
    for (int i = 0; i < threads; i++)
    {
      job = &loaders[i];
      if (job->running)
      {
        pthread_join(job->thread, NULL);
        memset(job, 0, sizeof(CramLoadJob));
      }
    }
  }
  remove("cram");

  cram_epoch_file = NULL;
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
  cram_janitor_stop();

  cram_queue_free(cram_job_queue);
  cram_queue_free(cram_write_queue);
  cram_queue_free(cram_check_queue);
  cram_dict_free(cram_dict);
  cram_list_free(cram_tables);

  pthread_spin_destroy(&cram_sequence_spinlock);
  pthread_mutex_destroy(&cram_epoch_mutex);
  pthread_spin_destroy(&cram_stats_spinlock);

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

void ha_cram::update_state(const char *format, ...)
{
  va_list args;
  va_start(args, format);
  vsnprintf(status_msg, sizeof(status_msg), format, args);
  va_end(args);
  thd_proc_info(ha_thd(), status_msg);
}

int ha_cram::record_store(uchar *buf, CramResult *result)
{
  if (!result)
    return HA_ERR_END_OF_FILE;

  pthread_rwlock_rdlock(&result->page->lock);
  CramRow *row = result->row;

  if (!row->blobs)
  {
    pthread_rwlock_unlock(&result->page->lock);
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
  pthread_rwlock_unlock(&result->page->lock);
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

  thr_lock_data_init(&mysql_lock, &lock, NULL);
  cram_condition = NULL;
  cram_result = NULL;
  cram_rnd_node = NULL;
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

  cram_table_close(cram_table);
  cram_table = NULL;

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

int ha_cram::write_row(uchar *buf)
{
  int rc = 0;

  // Avoid asserts in val_str() for columns that are not going to be updated
  my_bitmap_map *org_bitmap = dbug_tmp_use_all_columns(table, table->read_set);

  // Duplicate key check
  if (table->s->keys > 0)
  {
    uint column = table->key_info[0].key_part->field->field_index;
    Field *field = table->field[column];
    CramBlob *blob = cram_field_to_blob(field);

    if (blob)
    {
      CramResult *result = cram_row_find_one(cram_table, column, blob->buffer, blob->length);
      if (result)
        rc = HA_ERR_FOUND_DUPP_KEY;
      cram_blob_dec(blob->buffer, blob->length);
      cram_free(result);
    }
  }

  if (rc == 0)
  {
    CramBlob **blobs = (CramBlob**) cram_alloc(sizeof(CramBlob*) * cram_table->columns);

    for (Field **field = table->field ; *field ; field++)
    {
      uint index = field - table->field;
      cram_field_set(blobs, index, *field);
    }
    cram_row_create(cram_table, 0, blobs, CRAM_LOG, bulk_insert ? CRAM_APPEND: CRAM_INSERT);
  }

  dbug_tmp_restore_column_map(table->read_set, org_bitmap);
  counter_insert++;
  return rc;
}

int ha_cram::update_row(const uchar *old_data, uchar *new_data)
{
  int rc = 0;

  // Avoid asserts in val_str() for columns that are not going to be updated
  my_bitmap_map *org_bitmap = dbug_tmp_use_all_columns(table, table->read_set);

  // Duplicate key check
  if (table->s->keys > 0)
  {
    uint column = table->key_info[0].key_part->field->field_index;
    Field *field = table->field[column];
    CramBlob *blob = NULL;

    if (bitmap_is_set(table->write_set, column) && (blob = cram_field_to_blob(field)))
    {
      CramResult *result = cram_row_find_one(cram_table, column, blob->buffer, blob->length);
      if (result && result->row != cram_result->row)
        rc = HA_ERR_FOUND_DUPP_KEY;
      cram_blob_dec(blob->buffer, blob->length);
      cram_free(result);
    }
  }

  if (rc == 0)
  {
    pthread_rwlock_wrlock(&cram_result->page->lock);
    CramBlob **blobs = cram_result->row->blobs;

    if (blobs)
    {
      for (uint index = 0; index < cram_table->columns; index++)
      {
        Field *field = table->field[index];
        if (bitmap_is_set(table->write_set, field->field_index))
          cram_field_set(blobs, index, field);
      }

      cram_log_delete(cram_table, cram_result->row->id);
      cram_log_insert(cram_table, cram_result->row);
      cram_row_index(cram_table, cram_result->page, cram_result->row);
    }
    else
    {
      rc = HA_ERR_RECORD_DELETED;
    }
    pthread_rwlock_unlock(&cram_result->page->lock);

    if (rc == 0)
      cram_page_changed(cram_table, cram_result->page);
  }

  dbug_tmp_restore_column_map(table->read_set, org_bitmap);
  counter_update++;
  return rc;
}

int ha_cram::delete_row(const uchar *buf)
{
  counter_delete++;
  int rc = cram_row_free(cram_table, cram_result->page, cram_result->row, CRAM_LOG);
  if (rc == 0) cram_page_changed(cram_table, cram_result->page);
  return rc;
}

int ha_cram::rnd_init(bool scan)
{
  cram_debug("%s", __func__);
  // rnd_init can be called multiple times followed by a single explicit rnd_end
  rnd_end();
  cram_rnd_started = FALSE;
  cram_rnd_node = NULL;
  return 0;
}

void ha_cram::rnd_map()
{
  cram_debug("%s", __func__);
  update_state("%s", __func__);

  cram_rnd_results = cram_list_create();

  CramJob *jobs[cram_table_lists];

  for (uint i = 0; i < cram_table_lists; i++)
  {
    cram_debug("job %u create", i);
    jobs[i] = cram_job_create(cram_table, cram_condition, i);
  }

  for (uint i = 0; i < cram_table_lists; i++)
  {
    CramJob *job = jobs[i];
    pthread_mutex_lock(&job->mutex);
    if (!job->complete)
      pthread_cond_wait(&job->cond, &job->mutex);
    cram_debug("job %u done", i);
    cram_list_join(cram_rnd_results, job->results);
    pthread_mutex_unlock(&job->mutex);
    cram_job_free(job);
  }
}

int ha_cram::rnd_end()
{
  cram_debug("%s", __func__);
  if (cram_rnd_results)
  {
    while (cram_rnd_results->first)
      cram_free(cram_list_remove(cram_rnd_results, cram_list_eq, cram_rnd_results->first->item));
    cram_list_free(cram_rnd_results);
    cram_rnd_results = NULL;
  }
  cram_rnd_node = NULL;
  return 0;
}

int ha_cram::rnd_next(uchar *buf)
{
  counter_rnd_next++;

  if (!cram_rnd_started)
  {
    rnd_map();
    cram_rnd_node = cram_rnd_results->first;
    cram_rnd_started = TRUE;
  }
  else
  if (cram_rnd_node)
  {
    cram_rnd_node = cram_rnd_node->next;
  }

  cram_result = (CramResult*) (cram_rnd_node ? cram_rnd_node->item: NULL);

  return record_store(buf, cram_result);
}

int ha_cram::index_init(uint idx, bool sorted)
{
  cram_debug("%s %u %d", __func__, idx, sorted ? 1:0);
  active_index = idx;
  return 0;
}

int ha_cram::index_read(uchar * buf, const uchar * key, uint key_len, enum ha_rkey_function find_flag)
{
  cram_debug("%s", __func__);
  CramResult *result = cram_row_find_one(cram_table, active_index, key, key_len);
  int rc = record_store(buf, result);
  cram_free(result);
  return rc;
}

int ha_cram::index_end()
{
  cram_debug("%s", __func__);
  return 0;
}

void ha_cram::position(const uchar *record)
{
  if (!cram_pos_results)
    cram_pos_results = cram_list_create();

  CramResult *res = (CramResult*) cram_alloc(sizeof(CramResult));
  memmove(res, cram_result, sizeof(CramResult));

  cram_list_push(cram_pos_results, res);

  *((CramResult**)ref) = res;
  ref_length = sizeof(CramResult*);

  counter_position++;
}

int ha_cram::rnd_pos(uchar *buf, uchar *pos)
{
  cram_result = *((CramResult**)pos);
  counter_rnd_pos++;
  return record_store(buf, cram_result);
}

int ha_cram::info(uint flag)
{
  cram_debug("%s", __func__);

  if (flag & HA_STATUS_VARIABLE)
  {
    uint64 rows = 0;
    for (uint i = 0; i < cram_table_lists; i++)
    {
      cram_list_open_read(cram_table->lists[i]);
      rows += cram_table->lists[i]->length * cram_page_rows;
      cram_list_close(cram_table->lists[i]);
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

  while (cram_condition)
  {
    for (uint i = 0; i < cram_condition->count; i++)
    {
      if (cram_condition->blobs && cram_condition->blobs[i])
        cram_blob_dec(cram_condition->blobs[i]->buffer, cram_condition->blobs[i]->length);
    }
    cram_free(cram_condition->buffer);
    cram_free(cram_condition->like);
    cram_free(cram_condition->blobs);
    CramCondition *c = cram_condition;
    cram_condition = c->next;
    cram_free(c);
  }
  return 0;
}

int ha_cram::external_lock(THD *thd, int lock_type)
{
  if (cram_pos_results)
  {
    while (cram_pos_results->first)
      cram_free(cram_list_remove(cram_pos_results, cram_list_eq, cram_pos_results->first->item));
    cram_list_free(cram_pos_results);
    cram_pos_results = NULL;
  }

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
  CramTable *table = NULL;
  if ((table = cram_table_construct(name, table_arg->s->fields, 0)))
    cram_log_create(table);
  return table ? 0: -1;
}

bool ha_cram::check_if_incompatible_data(HA_CREATE_INFO *info, uint table_changes)
{
  cram_debug("%s", __func__);
  return COMPATIBLE_DATA_NO;
}

static CramCondition* check_condition(const COND *cond)
{
  char pad[1024];
  String *str, tmp(pad, sizeof(pad), &my_charset_bin);
  CramCondition *cc = NULL;

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

      cc = (CramCondition*) cram_alloc(sizeof(CramCondition));

      cc->type  = func->functype() == Item_func::ISNULL_FUNC ? CRAM_COND_NULL: CRAM_COND_NOTNULL;
      cc->count = 1;
      cc->index = ff->field->field_index;
    }

    if (func->argument_count() == 2 && args[0]->type() == COND::FIELD_ITEM)
    {
      Item *arg = args[1];
      Item_field *ff = (Item_field*)args[0];

      cram_debug("... cond %llu", args[0]->type());

      // = !=
      if (func->functype() == Item_func::EQ_FUNC || func->functype() == Item_func::NE_FUNC)
      {
        cram_debug("ECP EQ/NE %llu %llu", ff->field->field_index, func->argument_count()-1);

        cc = (CramCondition*) cram_alloc(sizeof(CramCondition));

        cc->type  = func->functype() == Item_func::EQ_FUNC ? CRAM_COND_EQ: CRAM_COND_NE;
        cc->count = 1;
        cc->index = ff->field->field_index;

        cc->blobs = (CramBlob**) cram_alloc(sizeof(CramBlob*) * cc->count);

        if (arg->is_null())
        {
          cram_debug("... null");
          cc->blobs[0] = NULL;
        }
        else
        if (arg->result_type() == INT_RESULT)
        {
          int64 n = arg->val_int();
          cram_debug("... int %lld", n);
          cc->blobs[0] = cram_blob_inc((uchar*)(&n), sizeof(int64));
        }
        else
        {
          str = arg->val_str(&tmp);
          cram_debug("... str %s", str->c_ptr());
          cc->blobs[0] = cram_blob_inc((uchar*)str->ptr(), str->length());
        }
      }

      // < > <= >=
      if (!arg->is_null() && (
        func->functype() == Item_func::LT_FUNC ||
        func->functype() == Item_func::GT_FUNC ||
        func->functype() == Item_func::LE_FUNC ||
        func->functype() == Item_func::GE_FUNC))
      {
        cram_debug("ECP LT/GT/LE/GE %llu %llu", ff->field->field_index, func->argument_count()-1);

        cc = (CramCondition*) cram_alloc(sizeof(CramCondition));
        cc->count = 0;
        cc->index = ff->field->field_index;

        if (arg->result_type() == INT_RESULT)
        {
          cram_debug("... int %lld", arg->val_int());
          switch (func->functype())
          {
            case Item_func::LT_FUNC: cc->type = CRAM_COND_LT; break;
            case Item_func::GT_FUNC: cc->type = CRAM_COND_GT; break;
            case Item_func::LE_FUNC: cc->type = CRAM_COND_LE; break;
            case Item_func::GE_FUNC: cc->type = CRAM_COND_GE; break;
            default: break;
          }
          cc->number = arg->val_int();
        }
        else
        {
          str = arg->val_str(&tmp);
          cram_debug("... str %s", str->c_ptr());
          switch (func->functype())
          {
            case Item_func::LT_FUNC: cc->type = CRAM_COND_LT_STR; break;
            case Item_func::GT_FUNC: cc->type = CRAM_COND_GT_STR; break;
            case Item_func::LE_FUNC: cc->type = CRAM_COND_LE_STR; break;
            case Item_func::GE_FUNC: cc->type = CRAM_COND_GE_STR; break;
            default: break;
          }
          cc->buffer = (uchar*) cram_alloc(str->length());
          memmove(cc->buffer, str->ptr(), str->length());
          cc->length = str->length();
        }
      }

      // LIKE
      if (!arg->is_null() && func->functype() == Item_func::LIKE_FUNC && args[0]->type() == COND::FIELD_ITEM)
      {
        str = arg->val_str(&tmp);

        int left   = str->length() > 1 && str->c_ptr()[0] == '%';
        int right  = str->length() > 1 && str->c_ptr()[str->length()-1] == '%';

        char *pos = strchr(str->c_ptr()+1, '%');
        int within = str->length() > 1 && pos && pos < str->c_ptr() + str->length() - 1;

        if (left && !right && !within)
        {
          cram_debug("ECP LEADING %llu %s", ff->field->field_index, str->c_ptr());

          cc = (CramCondition*) cram_alloc(sizeof(CramCondition));

          cc->type  = CRAM_COND_LEADING;
          cc->count = 1;
          cc->index = ff->field->field_index;

          cc->like = (char*) cram_alloc(str->length());
          strcpy(cc->like, str->c_ptr()+1);
          cc->like_len = str->length()-1;
        }

        if (right && !left && !within)
        {
          cram_debug("ECP TRAILING %llu %s", ff->field->field_index, str->c_ptr());

          cc = (CramCondition*) cram_alloc(sizeof(CramCondition));

          cc->type  = CRAM_COND_TRAILING;
          cc->count = 1;
          cc->index = ff->field->field_index;

          cc->like = (char*) cram_alloc(str->length());
          strncpy(cc->like, str->c_ptr(), str->length()-1);
          cc->like[str->length()-1] = 0;
          cc->like_len = str->length()-1;
        }

        if (left && right && !within)
        {
          cram_debug("ECP CONTAINS %llu %s", ff->field->field_index, str->c_ptr());

          cc = (CramCondition*) cram_alloc(sizeof(CramCondition));

          cc->type  = CRAM_COND_CONTAINS;
          cc->count = 1;
          cc->index = ff->field->field_index;

          cc->like = (char*) cram_alloc(str->length());
          strncpy(cc->like, str->c_ptr()+1, str->length()-2);
          cc->like[str->length()-2] = 0;
          cc->like_len = str->length()-2;

          cram_debug("... [%s]", cc->like);
        }

        // Equality
        if (!left && !right && !within)
        {
          cram_debug("ECP LIKE EQ %llu %s", ff->field->field_index, str->c_ptr());

          cc = (CramCondition*) cram_alloc(sizeof(CramCondition));

          cc->type  = CRAM_COND_EQ;
          cc->count = 1;
          cc->index = ff->field->field_index;

          cc->blobs = (CramBlob**) cram_alloc(sizeof(CramBlob*) * cc->count);
          cc->blobs[0] = cram_blob_inc((uchar*)str->ptr(), str->length());
        }
      }
    }

    if (func->argument_count() > 1 && args[0]->type() == COND::FIELD_ITEM)
    {
      Item_field *ff = (Item_field*)args[0];

      // IN (const,...)
      if (func->functype() == Item_func::IN_FUNC)
      {
        cram_debug("ECP IN %llu %llu", ff->field->field_index, func->argument_count()-1);

        cc = (CramCondition*) cram_alloc(sizeof(CramCondition));

        cc->type  = CRAM_COND_IN;
        cc->count = func->argument_count() - 1;
        cc->index = ff->field->field_index;

        cc->blobs = (CramBlob**) cram_alloc(sizeof(CramBlob*) * cc->count);

        for (uint i = 1; i < func->argument_count(); i++)
        {
          Item *arg = args[i];

          if (arg->is_null())
          {
            cram_debug("... null");
            cc->blobs[i-1] = NULL;
          }
          else
          if (arg->result_type() == INT_RESULT)
          {
            int64 n = arg->val_int();
            cram_debug("... int %lld", n);
            cc->blobs[i-1] = cram_blob_inc((uchar*)(&n), sizeof(int64));
          }
          else
          {
            str = arg->val_str(&tmp);
            cram_debug("... str %s", str->c_ptr());
            cc->blobs[i-1] = cram_blob_inc((uchar*)str->ptr(), str->length());
          }
        }
      }
    }
  }

  return cc;
}

const COND * ha_cram::cond_push ( const COND * cond )
{
  cram_debug("%s", __func__);

  reset();

  if (cond->type() == COND::COND_ITEM)
  {
    Item_cond *ic = (Item_cond*)cond;
    cram_debug("ECP %s", ic->func_name());

    if (ic->functype() == Item_func::COND_AND_FUNC)
    {
      List<Item>* arglist= ic->argument_list();
      List_iterator<Item> li(*arglist);
      CramCondition *cc;
      for (uint i = 0; i < arglist->elements; i++)
      {
        cc = check_condition(li++);
        if (cc)
        {
          cc->next = cram_condition;
          cram_condition = cc;
        }
      }
      // Prioritize simpler conditions
      CramCondition **cp = &cram_condition;
      while (cp && (*cp))
      {
        if ((*cp)->next && (*cp)->type > (*cp)->next->type)
        {
          CramCondition *ct = (*cp);
          CramCondition *cn = ct->next;
          *cp = cn;
          ct->next = cn->next;
          cn->next = ct;
        }
        else
        {
          cp = &((*cp)->next);
        }
      }
    }
  }
  else
  {
    cram_condition = check_condition(cond);
  }
  // We default to saying "yes" to ECP, regardless.
  // TODO: Decide if this is actually a good approach?
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

struct st_mysql_storage_engine cram_storage_engine= { MYSQL_HANDLERTON_INTERFACE_VERSION };

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

static void cram_index_weight_update(THD * thd, struct st_mysql_sys_var *sys_var, void *var, const void *save)
{
  uint n = *((uint*)save);
  *((uint*)var) = n;
  cram_index_weight = n;
}

static MYSQL_SYSVAR_UINT(table_lists, cram_table_lists, PLUGIN_VAR_READONLY,
  "Partitions per table.", 0, NULL, CRAM_LISTS, 2, 100, 1);

static MYSQL_SYSVAR_UINT(worker_threads, cram_worker_threads, 0,
  "Size of the worker thread pool.", 0, cram_worker_threads_update, CRAM_WORKERS, 2, 100, 1);

static MYSQL_SYSVAR_UINT(loader_threads, cram_loader_threads, PLUGIN_VAR_READONLY,
  "Size of the loader thread pool.", 0, NULL, CRAM_LOADERS, 1, 100, 1);

static MYSQL_SYSVAR_UINT(hash_chains, cram_dict_chains, PLUGIN_VAR_READONLY,
  "Width of the values hash-table.", 0, NULL, CRAM_CHAINS, 1000, UINT_MAX, 1);

static MYSQL_SYSVAR_UINT(hash_locks, cram_dict_locks, PLUGIN_VAR_READONLY,
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

static MYSQL_SYSVAR_UINT(job_queue_size, cram_job_queue_size, PLUGIN_VAR_READONLY,
  "Max pending worker jobs.", 0, NULL, CRAM_QUEUE, CRAM_QUEUE, UINT_MAX, 1);

static MYSQL_SYSVAR_UINT(write_queue_size, cram_write_queue_size, PLUGIN_VAR_READONLY,
  "Max pending writes to disk.", 0, NULL, CRAM_QUEUE, CRAM_QUEUE, UINT_MAX, 1);

static MYSQL_SYSVAR_UINT(index_queue_size, cram_check_queue_size, PLUGIN_VAR_READONLY,
  "Max pages pending reindexing.", 0, NULL, CRAM_QUEUE, CRAM_QUEUE, UINT_MAX, 1);

static MYSQL_SYSVAR_UINT(index_weight, cram_index_weight, 0,
  "Bigger value means faster but more memory hungry.", 0, cram_index_weight_update, CRAM_WEIGHT, 1, UINT_MAX, 1);

static struct st_mysql_sys_var *cram_system_variables[] = {
    MYSQL_SYSVAR(table_lists),
    MYSQL_SYSVAR(worker_threads),
    MYSQL_SYSVAR(loader_threads),
    MYSQL_SYSVAR(hash_chains),
    MYSQL_SYSVAR(hash_locks),
    MYSQL_SYSVAR(force_start),
    MYSQL_SYSVAR(strict_write),
    MYSQL_SYSVAR(flush_level),
    MYSQL_SYSVAR(page_rows),
    MYSQL_SYSVAR(compress_log),
    MYSQL_SYSVAR(verbose),
    MYSQL_SYSVAR(job_queue_size),
    MYSQL_SYSVAR(write_queue_size),
    MYSQL_SYSVAR(index_queue_size),
    MYSQL_SYSVAR(index_weight),
    NULL
};

static struct st_mysql_show_var func_status[]=
{
  { "cram_worker_rows_touched", (char*) &cram_worker_rows_touched, SHOW_ULONGLONG },
  { "cram_worker_pages_touched", (char*) &cram_worker_pages_touched, SHOW_ULONGLONG },
  { "cram_worker_rows_matched", (char*) &cram_worker_rows_matched, SHOW_ULONGLONG },
  { "cram_tables_created", (char*) &cram_tables_created, SHOW_ULONGLONG },
  { "cram_tables_deleted", (char*) &cram_tables_deleted, SHOW_ULONGLONG },
  { "cram_tables_renamed", (char*) &cram_tables_renamed, SHOW_ULONGLONG },
  { "cram_pages_created", (char*) &cram_pages_created, SHOW_ULONGLONG },
  { "cram_pages_deleted", (char*) &cram_pages_deleted, SHOW_ULONGLONG },
  { "cram_rows_created", (char*) &cram_rows_created, SHOW_ULONGLONG },
  { "cram_rows_deleted", (char*) &cram_rows_deleted, SHOW_ULONGLONG },
  { "cram_rows_inserted", (char*) &cram_rows_inserted, SHOW_ULONGLONG },
  { "cram_rows_appended", (char*) &cram_rows_appended, SHOW_ULONGLONG },
  { "cram_blobs_created", (char*) &cram_blobs_created, SHOW_ULONGLONG },
  { "cram_blobs_deleted", (char*) &cram_blobs_deleted, SHOW_ULONGLONG },
  { "cram_log_events_queued", (char*) &cram_log_events_queued, SHOW_ULONGLONG },
  { "cram_log_events_processed", (char*) &cram_log_events_processed, SHOW_ULONGLONG },
  { "cram_log_events_stalled", (char*) &cram_log_events_stalled, SHOW_ULONGLONG },
  { "cram_log_events_failed", (char*) &cram_log_events_failed, SHOW_ULONGLONG },
  { "cram_check_events_queued", (char*) &cram_check_events_queued, SHOW_ULONGLONG },
  { "cram_check_events_processed", (char*) &cram_check_events_processed, SHOW_ULONGLONG },
  { "cram_check_events_stalled", (char*) &cram_check_events_stalled, SHOW_ULONGLONG },
  { "cram_check_events_failed", (char*) &cram_check_events_failed, SHOW_ULONGLONG },
  { "cram_worker_jobs_queued", (char*) &cram_worker_jobs_queued, SHOW_ULONGLONG },
  { "cram_worker_jobs_processed", (char*) &cram_worker_jobs_processed, SHOW_ULONGLONG },
  { "cram_worker_jobs_stalled", (char*) &cram_worker_jobs_stalled, SHOW_ULONGLONG },
  { "cram_worker_jobs_failed", (char*) &cram_worker_jobs_failed, SHOW_ULONGLONG },
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
