# CRAM

An experimental storage engine for MariaDB 10 intended for OLAP.

* Dataset in memory
* In-memory and on-disk compression
* Engine Condition Pushdown
* Multithreaded queries (SQL > MapReduce > SQL)
* Page-level locking
* Automatic indexing

## Config Variables

### cram_compress_log = 0|1

Use huffman compression for log entries.

### cram_flush_level = N

Flush the log after every N entries. 0 means disabled, where flushing will occur only when the writer thread sleeps, when a new epoch is started, or during log consolidation.

### cram_force_start = 0|1

Whether to continue if log corruption is found during startup. If this is used then application traffic should probably be disabled until any problems are resolved.

### cram_hash_chains = N

Width of the blob hash-table. Wider means faster at the cost of memory. Ideally proprotional to the number of unique values in the data set.

### cram_hash_locks = N

Width of the blob hash-table locks array. Does not make sense to exceed 1:1 ratio with cram_hash_chains.

### cram_index_queue_size = N

Page-level indexes need to be updated immediately for on INSERT and UPDATE for new field values, but can be lazy for DELETE and purging old field values. Once a page hits 25% of rows changed, reindexing is scheduled via a background thread. Limiting the size of the index event queue is useful if heavy write activity causes index lag.

### cram_index_weight = N

Page-level indexes determine whether a blob is referenced somewhere on a page, but not the specific row or field. The width of a table's index is calculated like this:

    width = cram_page_rows * columns * cram_index_weight

A larger weight means a wider index and faster lookups at the cost of memory.

### cram_job_queue_size = N

The background worker threads process mapping jobs on behalf of the MariaDB clients. Each query generates *cram_table_lists*jobs. Increasing the job queue size won't make jobs get processed faster but it can reduce thread context switches.

### cram_loader_threads = N

The number of threads to spawn when loading a clean transaction log at startup. It usually isn't worth making this more than *cram_table_lists*.

### cram_page_rows = N

Number of rows per page defines the locking granularity.

### cram_strict_write = 1|0

Whether to abort on disk write failure. Since the data set is in memory it's possible to keep running during a storage failure, allowing time to dump data elsewhere. Obviously only useful if mysqld itself stays alive.

### cram_table_lists = N

Number of page lists per table, and by extension the number of mapping jobs created per query and thus the number of cores potentially used. Increasing this improves individual query speed only if sufficient worker threads are free. Increasing this too much can result in lower concurrency if a single query can use all available workers.

### cram_worker_threads = N

Number of background worker threads processing mapping jobs. To avoid query starvation, generally:

    cram_table_lists <= cram_worker_threads <= CPU cores

### cram_write_queue_size = N

The log is written asynchronously by a background *writer thread*. If write activity is high and sustained it may be necessary to reduce the queue size and make clients wait.
