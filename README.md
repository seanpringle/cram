# CRAM

CRAM is an experimental storage engine for MariaDB 10 with some design elements originally chosen just for the fun of it and later discovered to be actually useful in some situations, particularly OLAP.

## No indexes

Indexes take up space on disk and in memory, increase locking overhead, and slow down writes. Admittedly they also speed up a bunch of stuff, but what's life without a challenge? :-)

CRAM avoids traditional indexes and concentrates on using MariaDB's *Engine Condition Pushdown* (ECP) functionality, along with some simple internal things like implicit column hashing and parallel processing.

## Dataset fits in memory

Without indexes to consider, quite a bit of data can be crammed into memory on a commodity server. Judicious use of compression helps, plus reference counting ensures each value exists only once and that records are simple fixed-length arrays of pointers. Very roughly speaking: a dataset takes about as much memory as a gzipped SQL dump of the same.

On disk the data lives in a transaction log, optionally compressed. Upon MariaDB restart the log is replayed. During a clean shutdown the log is consolidated into a stream of bulk inserts to allow rapid replay. After an unclean shutdown the log can still be replayed but will take longer.

## Page-level locking

Row-level locking is ideal for query concurrency but has significant overhead, and of course table-level locking isn't very friendly to heavy write loads. Page-level locking with a configurable page size can emulate either approach to suit the dataset and traffic.

## Multiple CPU cores per query

Each query is converted to multiple jobs executed in parallel by a pool of worker threads. A job scans a section of a table and uses ECP to pre-filter records as much as possible. The MariaDB client thread combines the results from all jobs and performs any final filtering and sorting not handled by ECP. Think of this like *MapReduce* across cores rather than nodes.

## MariaDB "Block-Based Join Algorithms"

These work well with CRAM tables:

https://mariadb.com/kb/en/block-based-join-algorithms/

## Future Ideas

### Investigate locality and contention

Almost nothing objective done in this regard yet. Subjective review of systems under load seems good, but, yeah...

### Combining duplicate access patterns

If two or more queries access a table concurrently with compatible ECP it seems logical to service them from a single set of worker jobs, providing the overhead of identifying candidates isn't prohibitive. Maybe useful for reducing impact of query storms in OLTP?

### Combine CRAM with CONNECT

The MariaDB CONNECT engine also understands some ECP. It's possible to chain CONNECT and CRAM together to spread worker load over multiple servers for joins. Remains to be seen if this poor-mans-cluster is of any real use.

### Skip Lists

Pages are tracked with linked lists and it seems possible to extend it to a form of skip-list to speed up some ECP filtering. Maybe also presorting records? Needs thought.