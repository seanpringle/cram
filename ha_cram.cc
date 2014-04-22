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

static THR_LOCK mysql_lock;
static uint64 cram_epoch;
static FILE *cram_epoch_file;
static uint64 cram_epoch_eof;
static pthread_mutex_t cram_epoch_mutex;

/* A global hash table that holds actual data BLOBs */
static CramHash cram_hash;
static uint cram_hash_chains;
static uint cram_hash_locks;
static CramBlob cram_hash_uints[CRAM_UINTS];
static uint64 cram_hash_uint_bufs[CRAM_UINTS];

/* List of open tables */
static CramTable *cram_tables;
static CramWorker *cram_workers;
static pthread_mutex_t cram_tables_mutex;

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
static uint cram_verbose;
static uint cram_table_lists;
static uint cram_indexing;

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

/* Background log-writer thread */
static pthread_t cram_writer_thread;
static pthread_spinlock_t cram_log_events_spinlock;
static CramLogEvent *cram_log_events;
static bool cram_writer_done;

/* Background indexer thread */
static pthread_t cram_indexer_thread;
static pthread_spinlock_t cram_index_events_spinlock;
static CramIndexEvent *cram_index_events;
static bool cram_indexer_done;

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
  void *ptr = calloc(bytes, 1);
  if (!ptr)
  {
    cram_error("malloc failed %llu bytes", bytes);
    abort();
  }
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
static void cram_schedule_index(CramTable *table, CramPage *page);

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

static void cram_stat2_add(pthread_spinlock_t *spin, ulonglong *a, ulonglong *b, ulonglong an, ulonglong bn)
{
  pthread_spin_lock(spin);
  *a += an;
  *b += bn;
  pthread_spin_unlock(spin);
}

/* DJBX33A */
static uint32 cram_hash_calc(uchar *buffer, uint32 length)
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
  if (length == sizeof(uint64) && *((uint64*)buffer) < CRAM_UINTS)
  {
    return &cram_hash_uints[*((uint64*)buffer)];
  }

  uint32 hash = cram_hash_calc(buffer, length);
  uint chain = hash % cram_hash_chains;
  uint lock  = hash % cram_hash_locks;

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
  if (length == sizeof(uint64) && *((uint64*)buffer) < CRAM_UINTS)
  {
    return &cram_hash_uints[*((uint64*)buffer)];
  }

  bool created = FALSE;

  uint32 hash = cram_hash_calc(buffer, length);
  uint chain = hash % cram_hash_chains;
  uint lock  = hash % cram_hash_locks;

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

  cram_stat2_add(&cram_stats_spinlock, &cram_hash_reads, &cram_hash_inserts, 1, created ? 1:0);
  return n;
}

static int cram_decref(uchar *buffer, uint32 length)
{
  if (length == sizeof(uint64) && *((uint64*)buffer) < CRAM_UINTS)
  {
    return 0;
  }

  bool deleted = FALSE;
  uint32 hash = cram_hash_calc(buffer, length);
  uint chain = hash % cram_hash_chains;
  uint lock  = hash % cram_hash_locks;

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

  cram_stat2_add(&cram_stats_spinlock, &cram_hash_reads, &cram_hash_deletes, 1, deleted ? 1:0);
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

static uint cram_index_width(CramTable *table)
{
  return table->columns * cram_page_rows;
}

static void cram_row_index(CramTable *table, CramPage *page, CramBlob **blobs)
{
  CramList *list = &table->lists[page->list];
  pthread_mutex_lock(&list->mutex);
  uint count = 0, chains[table->columns];
  for (uint i = 0; i < table->columns; i++)
  {
    uint chain = 0;
    if (blobs[i] != NULL)
    {
      uint32 hash = cram_hash_calc(blobs[i]->buffer, blobs[i]->length);
      chain = (hash % cram_index_width(table));
    }
    if (page->indexed[chain/8U] & (1U<<(chain%8U)))
      continue;

    bool is_new = TRUE;
    for (uint j = 0; is_new && j < count; j++)
    {
      if (chains[j] == chain)
        is_new = FALSE;
    }
    if (is_new)
      chains[count++] = chain;
  }
  for (uint i = 0; i < count; i++)
  {
    uint chain = chains[i];
    CramIndex *idx = (CramIndex*) cram_alloc(sizeof(CramIndex));
    idx->page = page;
    idx->next = list->index[chain];
    list->index[chain] = idx;
    page->indexed[chain/8U] |= (1U<<(chain%8U));
  }
  pthread_mutex_unlock(&list->mutex);
}

static void cram_page_deindex(CramTable *table, CramPage *page)
{
  CramList *list = &table->lists[page->list];
  pthread_mutex_lock(&list->mutex);
  for (uint chain = 0; chain < cram_index_width(table); chain++)
  {
    if (!(page->indexed[chain/8U] & (1U<<(chain%8U))))
      continue;

    CramIndex **idx = &list->index[chain];
    while (idx && (*idx))
    {
      if ((*idx)->page == page)
      {
        CramIndex *drop = (*idx);
        *idx = (*idx)->next;
        cram_free(drop);
        break;
      }
      idx = &(*idx)->next;
    }
  }
  pthread_mutex_unlock(&list->mutex);
  memset(page->indexed, 0, (cram_index_width(table) / 8) + 1);
}

static void cram_page_index(CramTable *table, CramPage *page)
{
  cram_page_deindex(table, page);

  for (uint i = 0; i < cram_page_rows; i++)
  {
    if (page->rows[i].blobs)
      cram_row_index(table, page, page->rows[i].blobs);
  }

  page->changes = 0;
}

static void cram_page_changed(CramTable *table, CramPage *page)
{
  if (++page->changes > (cram_page_rows/10))
  {
    cram_schedule_index(table, page);
    page->changes = 0;
  }
}

static void cram_page_free(CramTable *table, CramPage *page)
{
  cram_page_deindex(table, page);
  if (page->prev)
  {
    page->prev->next = page->next;
  }
  for (uint i = 0; i < cram_table_lists; i++)
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
  cram_free(page->indexed);
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
    id = cram_id_create();
  }

  if (!blobs)
  {
    blobs = (CramBlob**) cram_alloc(sizeof(CramBlob*) * table->columns);
  }

  if (insert_mode == CRAM_APPEND)
  {
    pthread_mutex_lock(&table->mutex);
    list = &table->lists[table->next_list++];
    if (table->next_list == cram_table_lists)
      table->next_list = 0;
    pthread_mutex_unlock(&table->mutex);
  }
  else
  {
    // Choose a page list to scan. Prefer one with fewest rows so
    // as to keep them all roughly in balance.
    uint i, l = 0;
    pthread_mutex_lock(&table->lists[0].mutex);
    uint64 rows = table->lists[0].rows;
    pthread_mutex_unlock(&table->lists[0].mutex);

    for (i = 1; i < cram_table_lists; i++)
    {
      pthread_mutex_lock(&table->lists[i].mutex);
      if (table->lists[i].rows < rows)
      {
        l = i;
        rows = table->lists[i].rows;
      }
      pthread_mutex_unlock(&table->lists[i].mutex);
    }
    list = &table->lists[l];
  }

  pthread_rwlock_rdlock(&list->lock);
  CramPage *p = list->first;

  for (uint i = 0; p && !row; p = p->next, i++)
  {
    // If someone else has a lock, just skip onward because we
    // really don't care where rows are inserted. This reduces
    // stalls for worker threads *especially*.
    if (pthread_rwlock_trywrlock(&p->lock) == 0)
    {
      if (p->count < cram_page_rows)
      {
        // During log replay at startup rows fill up nicely without
        // gaps so it's worth trying to halve page scan time. During
        // normal activity this probably makes no difference.
        if (p->count < cram_page_rows/2)
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
        else
        {
          for (uint j = cram_page_rows-1; !row && j >= 0; j--)
          {
            if (!p->rows[j].blobs)
            {
              page = p;
              row = &page->rows[j];
              goto done;
            }
          }
        }
      }
      pthread_rwlock_unlock(&p->lock);
    }
    // Append mode just means "don't scan the whole page list".
    if (insert_mode == CRAM_APPEND && i > 0)
      break;
  }

  pthread_rwlock_unlock(&list->lock);

  page = (CramPage*) cram_alloc(sizeof(CramPage));
  pthread_rwlock_init(&page->lock, NULL);
  pthread_rwlock_wrlock(&page->lock);
  page->indexed = (uchar*) cram_alloc((cram_index_width(table) / 8) + 1);
  page_created = TRUE;
  row = &page->rows[0];
  page->list = list - table->lists;

  pthread_rwlock_wrlock(&list->lock);

  if (!list->last) list->last = page;
  page->next = list->first;
  list->first = page;
  list->pages++;
  if (page->next) page->next->prev = page;

done:

  page->count++;
  row->id = id;
  row->blobs = blobs;

  if (log_entry)
  {
    cram_log_insert(table, row);
  }

  cram_row_index(table, page, row->blobs);
  pthread_rwlock_unlock(&page->lock);

  pthread_mutex_lock(&list->mutex);
  list->rows++;
  if (page_created) list->pages++;
  pthread_mutex_unlock(&list->mutex);

  pthread_rwlock_unlock(&list->lock);

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
    pthread_mutex_lock(&list->mutex);
    list->rows--;
    pthread_mutex_unlock(&list->mutex);

    cram_page_changed(table, page);
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
    id = cram_id_create();
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
    pthread_mutex_init(&table->mutex, NULL);

    table->name = (char*) cram_alloc(strlen(name)+1);
    strcpy(table->name, name);

    table->id = id;
    table->columns = columns;

    table->lists = (CramList*) cram_alloc(sizeof(CramList) * cram_table_lists);

    for (uint i = 0; i < cram_table_lists; i++)
    {
      pthread_rwlock_init(&table->lists[i].lock, NULL);
      pthread_mutex_init(&table->lists[i].mutex, NULL);

      table->lists[i].index = (CramIndex**) cram_alloc(sizeof(CramIndex*) * cram_index_width(table));
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

// The indexer thread updates page/blob hash tables for CRAM_COND_EQ.

static void* cram_indexer(void *p)
{
  cram_note("indexer started");

  while (!cram_indexer_done)
  {
    pthread_spin_lock(&cram_index_events_spinlock);
    CramIndexEvent *event = cram_index_events;
    cram_index_events = NULL;
    pthread_spin_unlock(&cram_index_events_spinlock);

    if (!event)
      usleep(1000);

    while (event)
    {
      CramTable *table = event->table;
      CramPage *page = event->page;
      CramList *list = &table->lists[page->list];

      cram_debug("indexer %s %llx", table->name, (ulonglong)page);

      pthread_rwlock_rdlock(&list->lock);
      pthread_rwlock_wrlock(&page->lock);
      cram_page_index(event->table, event->page);
      pthread_rwlock_unlock(&page->lock);
      pthread_rwlock_unlock(&list->lock);

      CramIndexEvent *next = event->next;
      cram_free(event);
      event = next;
    }
  }

  cram_note("indexer stopped");
  return NULL;
}

static void cram_indexer_start()
{
  pthread_create(&cram_indexer_thread, NULL, cram_indexer, NULL);
}

static void cram_indexer_stop()
{
  cram_indexer_done = TRUE;
  pthread_join(cram_indexer_thread, NULL);
}

static void cram_schedule_index(CramTable *table, CramPage *page)
{
  CramIndexEvent *event = (CramIndexEvent*) cram_alloc(sizeof(CramIndexEvent));
  event->table = table;
  event->page = page;

  pthread_spin_lock(&cram_index_events_spinlock);
  event->next = cram_index_events;
  cram_index_events = event;
  pthread_spin_unlock(&cram_index_events_spinlock);
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

    for (uint i = 0; i < cram_table_lists; i++)
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

    for (uint i = 0; i < cram_table_lists; i++)
    {
      for (uint j = 0; j < (t->columns * cram_page_rows); j++)
      {
        while (t->lists[i].index[j])
        {
          CramIndex *drop = t->lists[i].index[j];
          t->lists[i].index[j] = drop->next;
          cram_free(drop);
        }
      }
      cram_free(t->lists[i].index);
      pthread_rwlock_destroy(&t->lists[i].lock);
      pthread_mutex_destroy(&t->lists[i].mutex);
    }

    pthread_rwlock_destroy(&t->lock);
    pthread_mutex_destroy(&t->mutex);
    cram_free(t->lists);
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
  if (!cram_epoch_file)
  {
    cram_error("failed to create %s", name);
    abort();
  }
  cram_epoch_eof  = 0;
  return cram_epoch_file ? 0: -1;
}

// The writer thread writes log events to disk.
// If compression is turned on, it happens here. Don't be tempted to
// move compression up into other threads because it *really* messes
// with concurrency by causing locks to be held for too long!

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

static void cram_writer_wait()
{
  for (;;)
  {
    pthread_spin_lock(&cram_stats_spinlock);
    ulonglong queued = cram_log_writes_queued;
    ulonglong completed = cram_log_writes_completed;
    pthread_spin_unlock(&cram_stats_spinlock);

    if (completed == queued)
      break;
    usleep(1000);
  }
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

      blobs[column] = cram_incref(data, length);
    }
    else
    {
      length = *((uint32*)data);
      data += sizeof(uint32);
      used += sizeof(uint32);

      blobs[column] = cram_incref(data, length);
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
  pthread_cond_init(&job->cond, NULL);

  cram_job_queue(job);

  return job;
}

static void cram_job_free(CramJob *job)
{
  pthread_mutex_destroy(&job->mutex);
  pthread_cond_destroy(&job->cond);
  cram_free(job);
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
      if (row->blobs[con->index]->length == sizeof(uint64))
        match = *((int64*)row->blobs[con->index]->buffer) < con->number ? TRUE: FALSE;
      break;

    case CRAM_COND_LT_STR:
      length = con->length < row->blobs[con->index]->length
        ? con->length : row->blobs[con->index]->length;
      match = memcmp(row->blobs[con->index]->buffer, con->buffer, length) < 0 ? TRUE: FALSE;
      break;

    case CRAM_COND_GT:
      if (row->blobs[con->index]->length == sizeof(uint64))
        match = *((int64*)row->blobs[con->index]->buffer) > con->number ? TRUE: FALSE;
      break;

    case CRAM_COND_GT_STR:
      length = con->length < row->blobs[con->index]->length
        ? con->length : row->blobs[con->index]->length;
      match = memcmp(row->blobs[con->index]->buffer, con->buffer, length) > 0 ? TRUE: FALSE;
      break;

    case CRAM_COND_LE:
      if (row->blobs[con->index]->length == sizeof(uint64))
        match = *((int64*)row->blobs[con->index]->buffer) <= con->number ? TRUE: FALSE;
      break;

    case CRAM_COND_LE_STR:
      length = con->length < row->blobs[con->index]->length
        ? con->length : row->blobs[con->index]->length;
      match = memcmp(row->blobs[con->index]->buffer, con->buffer, length) <= 0 ? TRUE: FALSE;
      break;

    case CRAM_COND_GE:
      if (row->blobs[con->index]->length == sizeof(uint64))
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

  if (!row->blobs)
  {
    return FALSE;
  }

  job->steps++;

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

  if (cram_indexing && job->condition && job->condition->type == CRAM_COND_EQ
    && job->condition->blobs[0])
  {
    cram_debug("worker %u using index", id);

    CramBlob *blob = job->condition->blobs[0];
    uint32 hash  = cram_hash_calc(blob->buffer, blob->length);
    uint chain = hash % cram_index_width(job->table);

    pthread_mutex_lock(&list->mutex);
    CramIndex *idx = list->index[chain];
    pthread_mutex_unlock(&list->mutex);

    for (; idx && !job->complete; idx = idx->next)
    {
      cram_job_page(job, idx->page);
    }
  }
  else
  {
    pthread_mutex_lock(&list->mutex);
    CramPage *page = list->first;
    pthread_mutex_unlock(&list->mutex);

    for (; page && !job->complete; page = page->next)
    {
      cram_job_page(job, page);
    }
  }

  job->complete = TRUE;

  pthread_rwlock_unlock(&list->lock);
  pthread_rwlock_unlock(&table->lock);

  cram_debug("%u worker matches %llu steps %llu", id, job->matches, job->steps);

  cram_stat2_add(&cram_stats_spinlock, &cram_ecp_steps, &cram_ecp_matches, job->steps, job->matches);
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
      pthread_cond_signal(&job->cond);
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
        cram_log_insert(job->table, &page->rows[j]);
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
  cram_epoch_create();
  pthread_mutex_unlock(&cram_epoch_mutex);

  CramTable *table = cram_tables;
  while (table)
  {
    cram_log_create(table);
    table = table->next;
  }
  cram_writer_wait();

  pthread_mutex_lock(&cram_epoch_mutex);
  cram_epoch_create();
  pthread_mutex_unlock(&cram_epoch_mutex);

  table = cram_tables;
  while (table)
  {
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
    table = table->next;
  }
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
    pthread_mutex_init(&cram_hash.mutexes[i], NULL);

  pthread_mutex_init(&cram_tables_mutex, NULL);
  pthread_spin_init(&cram_sequence_spinlock, PTHREAD_PROCESS_PRIVATE);
  pthread_mutex_init(&cram_epoch_mutex, NULL);
  pthread_spin_init(&cram_log_events_spinlock, PTHREAD_PROCESS_PRIVATE);
  pthread_spin_init(&cram_index_events_spinlock, PTHREAD_PROCESS_PRIVATE);
  pthread_spin_init(&cram_stats_spinlock, PTHREAD_PROCESS_PRIVATE);
  cram_note("Time to cram!");

  pthread_mutex_init(&cram_job_mutex, NULL);
  pthread_cond_init(&cram_job_cond, NULL);

  for (uint i = 0; i < CRAM_UINTS; i++)
  {
    cram_hash_uints[i].length = sizeof(uint64);
    cram_hash_uints[i].buffer = (uchar*) &cram_hash_uint_bufs[i];
    cram_hash_uint_bufs[i] = i;
  }

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

  cram_epoch_file = NULL;
  cram_epoch_create();
  cram_writer_start();
  cram_indexer_start();
  cram_workers_start();

  return 0;
}

static int cram_done_func(void *p)
{
  cram_workers_stop();
  cram_indexer_stop();
  cram_consolidate();
  cram_writer_stop();

  for (uint i = 0; i < cram_hash_locks; i++)
    pthread_mutex_destroy(&cram_hash.mutexes[i]);

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
    cram_row_index(cram_table, cram_result->page, cram_result->row->blobs);
    cram_page_changed(cram_table, cram_result->page);

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
    return;

  CramJob *jobs[cram_table_lists];

  for (uint i = 0; i < cram_table_lists; i++)
  {
    jobs[i] = cram_job_create(cram_table, cram_condition, i);
  }

  for (uint i = 0; i < cram_table_lists; i++)
  {
    CramJob *job = jobs[i];

    pthread_mutex_lock(&job->mutex);
    if (!job->complete)
    {
      pthread_cond_wait(&job->cond, &job->mutex);
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
    for (uint i = 0; i < cram_table_lists; i++)
    {
      pthread_mutex_lock(&cram_table->lists[i].mutex);
      rows += cram_table->lists[i].rows;
      pthread_mutex_unlock(&cram_table->lists[i].mutex);
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

  if ((cram_table = cram_table_open(name, table_arg->s->fields, 0)))
    cram_log_create(cram_table);

  return cram_table ? 0: -1;
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
          cc->blobs[0] = cram_get((uchar*)(&n), sizeof(int64));
        }
        else
        {
          str = arg->val_str(&tmp);
          cram_debug("... str %s", str->c_ptr());
          cc->blobs[0] = cram_get((uchar*)str->ptr(), str->length());
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
          cc->blobs[0] = cram_get((uchar*)str->ptr(), str->length());
        }
      }
    }

    if (func->argument_count() > 1 && args[0]->type() == COND::FIELD_ITEM)
    {
      Item_field *ff = (Item_field*)args[0];

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
            cc->blobs[i-1] = cram_get((uchar*)(&n), sizeof(int64));
          }
          else
          {
            str = arg->val_str(&tmp);
            cram_debug("... str %s", str->c_ptr());
            cc->blobs[i-1] = cram_get((uchar*)str->ptr(), str->length());
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

static void cram_indexing_update(THD * thd, struct st_mysql_sys_var *sys_var, void *var, const void *save)
{
  uint n = *((uint*)save);
  *((uint*)var) = n;

  cram_indexing = n;
}

static MYSQL_SYSVAR_UINT(table_lists, cram_table_lists, PLUGIN_VAR_READONLY,
  "Partitions per table.", 0, NULL, CRAM_LISTS, 2, 100, 1);

static MYSQL_SYSVAR_UINT(worker_threads, cram_worker_threads, 0,
  "Size of the worker thread pool.", 0, cram_worker_threads_update, CRAM_WORKERS, 2, 100, 1);

static MYSQL_SYSVAR_UINT(loader_threads, cram_loader_threads, PLUGIN_VAR_READONLY,
  "Size of the loader thread pool.", 0, NULL, CRAM_LOADERS, 1, 100, 1);

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

static MYSQL_SYSVAR_UINT(indexing, cram_indexing, 0,
  "Use table auto-indexing.", 0, cram_indexing_update, 0, 0, 1, 1);

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
    MYSQL_SYSVAR(indexing),
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
