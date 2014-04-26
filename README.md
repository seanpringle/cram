# CRAM

An experimental storage engine for MariaDB 10 intended for OLAP.

* Dataset in memory
* In-memory and on-disk compression
* Engine Condition Pushdown
* Multithreaded queries (SQL > MapReduce > SQL)
* Page-level locking
* Automatic indexing

## Config Variables

#### cram_compress_log = 0|1

Default: 0

Use huffman encoding for log entries.

#### cram_flush_level = N

Default: 1

Flush the log after every N entries. 0 means disabled, where flushing will occur only when the writer thread sleeps, when a new epoch is started, or during log consolidation.

#### cram_force_start = 0|1

Default: 0

Whether to continue if log corruption is found during startup. If this is used then application traffic should probably be disabled until any problems are resolved.

#### cram_hash_chains = N

Default: 1000000

Width of the global blob hash-table that stores all values and reference counts. Wider means faster at the cost of memory.

#### cram_hash_locks = N

Default: 1000

Width of the global blob locks-array that controls access to hash chains. Does not make sense to exceed 1:1 ratio with cram_hash_chains.

#### cram_index_queue_size = N

Default: 1000

Page-level indexes need to be updated immediately on INSERT and UPDATE with new field values, but can be lazy for DELETE and purging old field values. Once a page hits 25% of rows changed, lazy reindexing is scheduled via a background *indexer thread*. Limiting the size of the index event queue is useful if heavy write activity causes index lag.

#### cram_index_weight = N

Default: 1

Page-level indexes determine whether a blob is referenced somewhere on a page but not the specific row or field. The width of a table's index is calculated like this:

    width = cram_page_rows * columns * cram_index_weight

A larger weight means a wider index and faster lookups at the cost of memory.

#### cram_job_queue_size = N

Default: 1000

The background worker threads process mapping jobs on behalf of MariaDB client threads. Each query generates N=*cram_table_lists* jobs. Increasing the job queue size generally won't process jobs faster but it can reduce context switches.

#### cram_loader_threads = N

Default: 4

The number of threads to spawn when loading a clean transaction log at startup. It usually isn't worth making this more than *cram_table_lists*.

#### cram_page_rows = N

Default: 100

Number of rows per page defines the locking granularity.

#### cram_strict_write = 1|0

Default: 1

Whether to continue or abort on disk write failure. Since the data set is in memory it's possible to keep running during a storage failure, allowing time to dump data elsewhere. Obviously only useful if mysqld itself stays alive.

#### cram_table_lists = N

Default: 4

Number of page lists per table, and by extension the number of mapping jobs created per query and thus the number of cores potentially used. Increasing this improves individual query speed only if sufficient worker threads are free. Increasing this too much can result in lower concurrency.

#### cram_worker_threads = N

Default: 8

Number of background worker threads processing mapping jobs. To avoid query starvation, generally:

    cram_table_lists < cram_worker_threads < CPU cores

#### cram_write_queue_size = N

Default: 1000

The log is written asynchronously by a background *writer thread*. If write activity is high and sustained it may be necessary to reduce the queue size and make clients wait.
