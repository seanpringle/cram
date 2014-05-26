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
#include "my_global.h"
#include <sql_table.h>
#include <sql_class.h>
#include <probes_mysql.h>
#include "thr_lock.h" /* THR_LOCK, THR_LOCK_DATA */

typedef bool (*map_fn)(void*, void*);
typedef int (*cmp_fn)(void*, void*);

typedef struct str_st {
  char *buffer;
  size_t length, limit;
} str_t;

typedef struct node_st {
  void *payload;
  struct node_st *next;
} node_t;

typedef struct list_st {
  node_t *head;
  uint64 length;
} list_t;

typedef uchar CramRow;
typedef uchar bmp_t;

typedef struct _CramTable {
  char *name;
  uint users;
  size_t width;
  size_t lists_count;
  size_t hints_width;
  size_t compress_boundary;
  pthread_mutex_t *locks;
  list_t **lists;
  bmp_t ***hints;
  uint *changes;
  bool dropping;
  uint64 row_count;
  uint64 meta_size;
  uint64 data_size;
  THR_LOCK mysql_lock;
  uint64 rows_indexed;
  uint64 rows_touched;
  uint64 rows_selected;
  uint64 rows_inserted;
  uint64 rows_updated;
  uint64 rows_deleted;
} CramTable;

typedef struct _CramPosition {
  uint list;
  CramRow *row;
} CramPosition;

typedef struct _CramItem {
  uint type;
  int64 bigint;
  uchar buffer[256];
  uint length;
  uint hashval;
} CramItem;

typedef struct _CramCondition {
  uint cond;
  uint column;
  list_t *items;
} CramCondition;

typedef struct _CramCheckpoint {
  CramTable *table;
  bool done;
  bool idle;
  pthread_t thread;
  pthread_mutex_t mutex;
} CramCheckpoint;

enum {
  CRAM_NULL=0,
  CRAM_INT08,
  CRAM_INT32,
  CRAM_INT64,
  CRAM_STRING,
  CRAM_TINYSTRING,
};

enum {
  CRAM_EQ=1,
  CRAM_NE,
  CRAM_LT,
  CRAM_GT,
  CRAM_LE,
  CRAM_GE,
  CRAM_ISNULL,
  CRAM_ISNOTNULL,
  CRAM_IN,
};

enum {
  CRAM_INSERT=1,
  CRAM_DELETE,
};

/** @brief
  Class definition for the storage engine
*/
class ha_cram: public handler
{
  THR_LOCK_DATA lock;
  CramTable *cram_table;

  bool cram_rnd;
  uint cram_list;
  list_t *cram_results;
  node_t *cram_result;

  bool bulk_insert;

  bmp_t *cram_lists_done;
  list_t *cram_trash;
  list_t *cram_conds;
  node_t cram_node;

  uint64 counter_rows_indexed;
  uint64 counter_rows_touched;
  uint64 counter_rows_selected;
  uint64 counter_rows_inserted;
  uint64 counter_rows_updated;
  uint64 counter_rows_deleted;

  struct drand48_data cram_rand;

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
  uint max_supported_keys()          const { return 0; }
  uint max_supported_key_parts()     const { return 0; }
  uint max_supported_key_length()    const { return UINT_MAX; }
  virtual double scan_time() { return (double) DBL_MIN; }
  virtual double read_time(uint, uint, ha_rows rows) { return (double) DBL_MAX/2; }
  int open(const char *name, int mode, uint test_if_locked);    // required
  int close(void);                                              // required
  int write_row(uchar *buf);
  int update_row(const uchar *old_data, uchar *new_data);
  int delete_row(const uchar *buf);
  int rnd_init(bool scan);                                      //required
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
  int record_store(uchar *buf);
  uchar* record_place(uchar *buf);

  bool next_list();
  void empty_trash();
  void use_trash();
  void empty_conds();
  void use_conds();
  void check_condition ( const COND * cond );
  void clear_state();
  void log_state();
  void update_list_hints(uint part, CramRow *row);
};
