# CRAM

An experimental storage engine for MariaDB.

* In-memory tables
* Checkpoint to disk
* Concurrent reads and writes
* Engine Condition Pushdown
* List-level locking
* Non-transactional
* No indexes
* Table scans, baby!

## List-level locking?

Each table stores records in a configurable number of linked lists.

## No indexes?

Right! The aims are:

1. Keep memory usage low and writes really fast
2. Use MariaDB's block-based join algorithms (these are cool)
3. See how fast in-memory table scans can be done
4. Use bitmap hints for vague indexing

A bitmap is maintained per column per list. When writing a record each field is hashed, modulo calculated, and a bit set. When scanning for records *Engine Condition Pushdown* with equality *=* or *IN()* conditions allows the engine to check bitmaps and skip irrelevant lists.

The astute reader can probably predict the potential pitfalls with this approach. By configuring the number of lists and the width of the bitmaps a table can be tuned for a specific combination of memory usage, concurrency, and speed.