# spark-multi-delta-writer

A DataSource V2 sink that fans **one DataFrame out to many tables in a single
Spark job / single pass over the data**, routing each row to a target table by a
routing column. Supports **Delta** and **pure Parquet** sinks.

## Why this exists

Calling `df.write` once per table is slow because each call is a separate Spark
action: the upstream DAG is recomputed, jobs run sequentially, and (even cached)
the data is re-scanned once per table. This sink scans/computes the source
**once** and writes every table's files from the same task set.

Trade-off, accepted by design: writes are **not atomic across tables**. Each
Delta table is committed with its own transaction; a mid-commit failure leaves
partial state. Make upstream writes idempotent (e.g. `MERGE` on a key, or Delta
`txnAppId`/`txnVersion`) if you need convergence on retry.

## Usage

```scala
df.write.format("multiDelta")
  .option("routeColumn", "target_table")  // required: value of this column picks the table
  .option("basePath", "/mnt/warehouse")   // required: table dir = basePath/<routeValue>
  .option("sinkFormat", "delta")          // "delta" (default) or "parquet"
  .option("dropRouteColumn", "true")      // default true: strip routing column from output
  .option("partitionBy", "dt,country")    // Hive-style partition columns applied to every table
  .option("maxRecordsPerFile", "1000000") // 0 (default) = unbounded; >0 rolls files at this row count
  .option("sortWithinPartitions", "true") // local-sort by route+partition cols; 1 open writer at a time
  .mode("append")                         // or "overwrite" (replaces only touched tables)
  .save()
```

- `sinkFormat=delta` → commits `AddFile` actions to each table's `_delta_log`.
- `sinkFormat=parquet` → writes parquet straight into `basePath/<table>` and drops
  a `_SUCCESS` marker; the files *are* the table (no transaction log).

### Partitioning

`partitionBy` writes standard Hive-style `col=value/` subdirectories under each
table and records the values in `AddFile.partitionValues` (delta) — so reads
prune partitions normally, and the parquet sink is discovered by Spark's usual
partition inference. Partition columns live in the path, not the data files.
Supported partition types: string / boolean / integral (date & timestamp need
custom value formatting and are rejected up-front).

### Overwrite mode

`mode("overwrite")` replaces the contents of **only the tables that receive rows
in this write** — untouched tables are left alone. Delta does it transactionally
(`RemoveFile` for the current files + the new `AddFile`s in one commit); the
parquet sink deletes pre-existing files not written by this commit.

## Output layout & file sizing

Files are organized one directory per routing value:

```
basePath/
├── us/   _delta_log/ (delta) or _SUCCESS (parquet) + part-<pid>-<tid>-<uuid>.parquet
├── eu/   ...
└── apac/ ...
```

**File count is driven by input partitioning, not by table count.** Each Spark
task holds one open Parquet writer per table, so a table gets **one file per
input partition that carries its rows**. Rows scattered across `P` partitions →
`P` files per table (the small-file trap). Two controls:

- `df.repartition(col("routeColumn"))` before writing → collapses each table to
  ~1 file per partition (measured: 300k rows, 4 partitions → 12 files dropped to 3).
  **Only safe when the routing key is not skewed** — hash-partitioning a skewed
  key funnels the hot value into one task/file (straggler + OOM risk). Prefer the
  `REBALANCE` hint below, which splits hot keys; see *Handling skew*.
- `maxRecordsPerFile` → caps how many rows land in each file, rolling to a new
  file (and a new `AddFile`) past the limit. Measured on 300k single-partition
  rows: unbounded = 1 file (3.9 MiB); `maxRecordsPerFile=50000` = 6 even files
  (~650 KiB each). Use it to bound file size when a partition is large, or to
  guarantee no single giant file regardless of input partitioning.

For the delta sink you can also compact after the fact with `OPTIMIZE <table>`.

### Handling skew in the routing column

Routing skew only hurts if you pre-shuffle by the routing key:

- **Default write (no repartition):** unaffected. Tasks are sized by *input*
  partitions, so a hot routing value just yields larger files, not a straggler.
- **`repartition($"routeColumn")`:** dangerous under skew — all hot-value rows
  hash to one partition, so one task writes the whole hot table (straggler; the
  task also holds that table's full Parquet row-group buffer → OOM risk).
- **`REBALANCE(<routeCol>)` + AQE (recommended):** AQE's
  `optimizeSkewsInRebalancePartitions` (default on) *splits* the hot key into
  several even, target-sized partitions written by different tasks in parallel.
  Measured through this sink at 98% skew: `repartition` produced one 8.3 MiB hot
  file from a single task; `REBALANCE` produced 8 even ~1 MiB files and finished
  ~2.4× faster. Set `spark.sql.adaptive.advisoryPartitionSizeInBytes` to your
  target file size.

Two related cautions: don't set `maxRecordsPerFile` so low that a huge hot table
emits an enormous number of `AddFile`s (bloats `_delta_log`); and note the memory
ceiling is driven by *routing cardinality*, not skew — each task holds one open
Parquet writer per `(table, partition)` it currently sees, so hundreds of distinct
routing values per task means hundreds of row-group buffers.

### `sortWithinPartitions` — bound writer memory for high cardinality

Set `sortWithinPartitions=true` and the sink asks Spark (via DSv2
`RequiresDistributionAndOrdering`) for a **local** sort by `[routeColumn,
partitionCols]` before the write — no shuffle, so it does not reintroduce skew.
Every `(table, partition)` group then arrives contiguously, and the writer keeps
just **one** open Parquet writer at a time (closing each group as the key changes).
Memory is bounded to a single row-group buffer regardless of routing cardinality,
so it stays safe even without the `REBALANCE` hint. Cost: a local sort (CPU, and
possible spill). Prefer it when a single task may see many distinct routing values;
skip it when `REBALANCE`/repartition already clusters the data. Verified through
the sink: interleaved 5-table input stays grouped (not fragmented) with all rows
preserved across the group boundaries.

### Interop with Synapse / Databricks "Optimized Write"

Optimized Write (`spark.microsoft.delta.optimizeWrite.enabled`,
`delta.autoOptimize.optimizeWrite`) is implemented **inside Delta's write path** —
an adaptive shuffle injected into `TransactionalWrite.writeFiles`. This sink
supplies its **own** write path and uses only Delta's **commit** path
(`txn.commit`), so the `optimizeWrite` flag/table-property has **no effect here**.
To get the same even, target-sized files in a single pass:

- **`REBALANCE` hint + AQE** — `SELECT /*+ REBALANCE(<routeCol>[, <partCols>]) */`
  before the write, with `spark.sql.adaptive.advisoryPartitionSizeInBytes` set to
  your target file size (e.g. `128m`). AQE coalesces small tables to one file and
  splits large ones into even chunks. Measured through this sink (skewed 400k
  rows): the small tables dropped from 8 tiny files to 1 target-sized file each,
  while the large table stayed evenly split — Optimized-Write-equivalent layout.
- **`maxRecordsPerFile`** as a hard per-file cap (see above).
- **Auto Compact** (`delta.autoOptimize.autoCompact`) is a *post-commit hook*, and
  this sink does call `txn.commit`, so — unlike Optimized Write — it may still fire
  for the delta sink. Enable and verify on your Synapse runtime.

## How it works

See [ARCHITECTURE.md](ARCHITECTURE.md) for component/sequence/data-flow diagrams.

| Stage | Where | What |
|-------|-------|------|
| `MultiDeltaSource` / `Table` / `WriteBuilder` | driver | DSv2 plumbing; pulls the query schema |
| `MultiDeltaBatchWrite` | driver | builds the serializable Parquet `OutputWriterFactory`, coordinates commit |
| `MultiDeltaDataWriter` | executor | **one open Parquet writer per (table, partition dir)**, routes each row (single pass), rolls files at `maxRecordsPerFile` |
| `DeltaCommitter` / `ParquetCommitter` | driver | finalizes each table (append/overwrite; log commit vs `_SUCCESS`) |

## Build & test

```bash
sbt package   # produces a jar to add with --jars
sbt test      # runs MultiDeltaWriterSuite against a local SparkSession
```

## Validation status

Compiled and executed end-to-end against **Spark 3.5.1 + Delta 3.2.0**
(Scala 2.12.18). All 13 cases in `MultiDeltaWriterSuite` pass — delta & parquet
routing, append accumulation, `dropRouteColumn`, `maxRecordsPerFile` rolling,
**partitioned** tables (delta + parquet), **overwrite** mode (selective replace
+ stale-file removal), `sortWithinPartitions` (bounded writers, data preserved),
partition-type validation, fail-fast on missing options, and the **single-pass**
guarantee (4 input rows across 3 target tables trigger exactly 4 row-visits, not 12).

> Note: on JDK 17+ the `--add-opens` flags in `build.sbt` are required for Spark
> to run (they're wired into `Test / javaOptions`). Spark 3.5 targets JDK 8/11/17.

Pin `sparkVersion` / `deltaVersion` in `build.sbt` to match your cluster
**exactly** — this touches Spark internal datasource classes and Delta internal
transaction APIs, neither of which is source-stable across major versions.

## Known limitations (extension points)

- **No per-file Delta stats.** `AddFile.stats` is left null (data skipping still
  works via file pruning, just less selectively). Populate it for better skipping.
- **Sequential commits.** Tables commit one-by-one on the driver; wrap
  `DeltaCommitter` in a `Future` pool if commit latency matters.
- **String routing column** assumed; extend `routeValue` for other types.
- **Partition types limited** to string / boolean / integral. Date & timestamp
  need value formatting that matches Delta's expectations (extend
  `rawPartitionValue`).
- **Overwrite is per-table, not per-partition.** `mode("overwrite")` replaces a
  touched table entirely; dynamic partition overwrite (replace only the written
  partitions) would need `RemoveFile`s scoped to the affected partitions.
