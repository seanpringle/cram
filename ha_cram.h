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
  Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
*/

#ifdef USE_PRAGMA_INTERFACE
#pragma interface			/* gcc class implementation */
#endif

#define MYSQL_SERVER 1 // required for THD class
#include <sql_table.h>
#include <sql_class.h>
#include <probes_mysql.h>
#include "thr_lock.h" /* THR_LOCK, THR_LOCK_DATA */

#define CRAM_CHAINS 10000000
#define CRAM_LOCKS 10000
#define CRAM_WORKERS 8
#define CRAM_LOADERS 4
#define CRAM_LISTS 4
#define CRAM_EPOCH 1000000000
#define CRAM_PAGE 100
#define CRAM_FILE "cram%06llx"
#define CRAM_QUEUE 1000
#define CRAM_WEIGHT 16

#define CRAM_LOG TRUE
#define CRAM_NO_LOG FALSE

#define CRAM_INSERT 1
#define CRAM_APPEND 2

typedef bool (*CramEqual)(void*, void*);
typedef void* (*CramCreate)(void*, void*);
typedef void (*CramDestroy)(void*, void*);

typedef uchar* CramBitMap;

typedef struct _CramString {
  uchar *buffer;
  size_t length;
  size_t limit;
} CramString;

typedef struct _CramQueue {
  bool halt;
  uint64 total, count, complete, stalls;
  pthread_mutex_t mutex;
  pthread_cond_t read_cond, write_cond, wait_cond;
  size_t width, read, write;
  void **items;
} CramQueue;

typedef struct _CramNode {
  void *item;
  uint64 count;
  struct _CramNode *next;
} CramNode;

typedef struct _CramDict {
  size_t width, locks;
  CramNode **chains;
  pthread_rwlock_t *rwlocks;
} CramDict;

typedef struct _CramList {
  size_t length;
  pthread_rwlock_t rwlock;
  CramNode *first, *last;
} CramList;

enum {
  CRAM_ENTRY_CREATE=1,
  CRAM_ENTRY_RENAME,
  CRAM_ENTRY_DROP,
  CRAM_ENTRY_INSERT,
  CRAM_ENTRY_DELETE,
};

typedef struct _CramBlob {
  uint32 hashval;
  uint32 length;
  uchar *buffer;
} CramBlob;

typedef struct _CramRow {
  union {
    uint64 id;
    struct _CramRow *next;
  };
  CramBlob **blobs;
} CramRow;

typedef struct _CramPage {
  pthread_rwlock_t lock;
  uint16 count;
  uint32 changes;
  CramList *list;
  CramRow *rows, *row_free;
  CramBitMap bitmap;
  bool queued;
} CramPage;

typedef struct _CramTable {
  pthread_rwlock_t lock;
  char *name;
  uint64 id;
  uint32 columns;
  CramList **lists;
  uint opened;
  uint index_width;
} CramTable;

typedef struct _CramLogEvent {
  uchar *data;
  size_t width;
  uchar *cdata;
  size_t cwidth;
} CramLogEvent;

typedef struct _CramCheckEvent {
  CramTable *table;
  CramPage *page;
} CramCheckEvent;

enum {
  CRAM_COND_EQ=1,
  CRAM_COND_NE,
  CRAM_COND_IN,
  CRAM_COND_LT,
  CRAM_COND_GT,
  CRAM_COND_LE,
  CRAM_COND_GE,
  CRAM_COND_NULL,
  CRAM_COND_NOTNULL,
  CRAM_COND_LT_STR,
  CRAM_COND_GT_STR,
  CRAM_COND_LE_STR,
  CRAM_COND_GE_STR,
  CRAM_COND_LEADING,
  CRAM_COND_TRAILING,
  CRAM_COND_CONTAINS,
};

typedef struct _CramCondition {
  uint32 type;
  uint32 count;
  uint32 index;
  CramBlob **blobs;
  char *like;
  uint32 like_len;
  int64 number;
  uchar *buffer;
  uint32 length;
  struct _CramCondition *next;
} CramCondition;

typedef struct _CramResult {
  CramPage *page;
  CramRow *row;
} CramResult;

typedef struct _CramJob {
  CramTable *table;
  CramList *results;
  bool complete;
  uint list;
  uint64 pages, rows, matches;
  CramCondition *condition;
  pthread_mutex_t mutex;
  pthread_cond_t cond;
} CramJob;

typedef struct _CramWorker {
  pthread_t thread;
  bool run;
  bool done;
} CramWorker;

typedef struct _CramConsolidateJob {
  pthread_t thread;
  pthread_mutex_t mutex;
  CramTable *table;
  uint list;
  bool complete;
} CramConsolidateJob;

typedef struct _CramLoadJob {
  FILE *file;
  bool running;
  bool complete;
  bool success;
  uint64 epoch;
  pthread_t thread;
} CramLoadJob;

/** @brief
  Class definition for the storage engine
*/
class ha_cram: public handler
{
  CramTable *cram_table;
  CramList *cram_rnd_results, *cram_pos_results;
  CramNode *cram_rnd_node;
  CramResult *cram_result;
  CramCondition *cram_condition;
  bool cram_rnd_started;
  THR_LOCK_DATA lock;
  uint active_index;
  bool bulk_insert;
  char status_msg[256];

  uint64 counter_insert;
  uint64 counter_update;
  uint64 counter_delete;
  uint64 counter_rnd_next;
  uint64 counter_rnd_pos;
  uint64 counter_position;

public:
  ha_cram(handlerton *hton, TABLE_SHARE *table_arg);
  ~ha_cram()
  {
  }

  const char *table_type() const { return "CRAM"; }
  const char **bas_ext() const;

  ulonglong table_flags() const
  {
    return (
        HA_NO_TRANSACTIONS
      | HA_NO_AUTO_INCREMENT
      | HA_REC_NOT_IN_SEQ
      | HA_PARTIAL_COLUMN_READ
      | HA_BINLOG_ROW_CAPABLE
      | HA_BINLOG_STMT_CAPABLE
      | HA_DO_INDEX_COND_PUSHDOWN
      | HA_MUST_USE_TABLE_CONDITION_PUSHDOWN
    );
  }

  ulong index_flags(uint inx, uint part, bool all_parts) const
  {
    return HA_DO_INDEX_COND_PUSHDOWN;
  }

  uint max_supported_record_length() const { return HA_MAX_REC_LENGTH; }
  uint max_supported_keys()          const { return 1; }
  uint max_supported_key_parts()     const { return 1; }
  uint max_supported_key_length()    const { return UINT_MAX; }
  virtual double scan_time() { return (double) DBL_MIN; }
  virtual double read_time(uint, uint, ha_rows rows) { return (double) DBL_MAX/2; }
  int open(const char *name, int mode, uint test_if_locked);    // required
  int close(void);                                              // required
  int write_row(uchar *buf);
  int update_row(const uchar *old_data, uchar *new_data);
  int delete_row(const uchar *buf);
  int rnd_init(bool scan);                                      //required
  void rnd_map();
  int rnd_end();
  int rnd_next(uchar *buf);                                     ///< required
  int rnd_pos(uchar *buf, uchar *pos);                          ///< required
  int index_init(uint idx, bool sorted);
  int index_read(uchar * buf, const uchar * key, uint key_len, enum ha_rkey_function find_flag);
  int index_end();
  void position(const uchar *record);                           ///< required
  int info(uint);                                               ///< required
  int reset();
  int external_lock(THD *thd, int lock_type);                   ///< required
  int delete_table(const char *from);
  int rename_table(const char *from, const char *to);
  int create(const char *name, TABLE *form, HA_CREATE_INFO *create_info);                      ///< required
  bool check_if_incompatible_data(HA_CREATE_INFO *info, uint table_changes);
  THR_LOCK_DATA **store_lock(THD *thd, THR_LOCK_DATA **to, enum thr_lock_type lock_type);     ///< required
  const COND * cond_push ( const COND * cond );
  void start_bulk_insert(ha_rows rows, uint flags);
  int end_bulk_insert();
  int record_store(uchar *buf, CramResult *result);
  void update_state(const char *format, ...);
};
