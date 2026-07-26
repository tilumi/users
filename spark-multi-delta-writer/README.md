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
  .option("maxRecordsPerFile", "1000000") // 0 (default) = unbounded; >0 rolls files at this row count
  .mode("append")
  .save()
```

- `sinkFormat=delta` → commits `AddFile` actions to each table's `_delta_log`.
- `sinkFormat=parquet` → writes parquet straight into `basePath/<table>` and drops
  a `_SUCCESS` marker; the files *are* the table (no transaction log).

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
- `maxRecordsPerFile` → caps how many rows land in each file, rolling to a new
  file (and a new `AddFile`) past the limit. Measured on 300k single-partition
  rows: unbounded = 1 file (3.9 MiB); `maxRecordsPerFile=50000` = 6 even files
  (~650 KiB each). Use it to bound file size when a partition is large, or to
  guarantee no single giant file regardless of input partitioning.

For the delta sink you can also compact after the fact with `OPTIMIZE <table>`.

## How it works

| Stage | Where | What |
|-------|-------|------|
| `MultiDeltaSource` / `Table` / `WriteBuilder` | driver | DSv2 plumbing; pulls the query schema |
| `MultiDeltaBatchWrite` | driver | builds the serializable Parquet `OutputWriterFactory`, coordinates commit |
| `MultiDeltaDataWriter` | executor | **one open Parquet writer per target table**, routes each row (single pass) |
| `DeltaCommitter` / `ParquetCommitter` | driver | finalizes each table (log commit vs `_SUCCESS`) |

## Build & test

```bash
sbt package   # produces a jar to add with --jars
sbt test      # runs MultiDeltaWriterSuite against a local SparkSession
```

## Validation status

Compiled and executed end-to-end against **Spark 3.5.1 + Delta 3.2.0**
(Scala 2.12.18). All six cases in `MultiDeltaWriterSuite` pass — delta routing,
append accumulation, `dropRouteColumn`, the pure-parquet sink (`_SUCCESS`, no
`_delta_log`), fail-fast on missing options, and the **single-pass** guarantee
(4 input rows across 3 target tables trigger exactly 4 row-visits, not 12).

> Note: on JDK 17+ the `--add-opens` flags in `build.sbt` are required for Spark
> to run (they're wired into `Test / javaOptions`). Spark 3.5 targets JDK 8/11/17.

Pin `sparkVersion` / `deltaVersion` in `build.sbt` to match your cluster
**exactly** — this touches Spark internal datasource classes and Delta internal
transaction APIs, neither of which is source-stable across major versions.

## Known limitations (extension points)

- **Append only.** `overwrite` needs `RemoveFile` actions (delta) or dir
  clearing (parquet) before the commit.
- **Unpartitioned target tables.** Partitioned output requires routing rows to
  `col=val/` subdirs and populating `AddFile.partitionValues`.
- **No per-file Delta stats.** `AddFile.stats` is left null (data skipping still
  works via file pruning, just less selectively). Populate it for better skipping.
- **Sequential commits.** Tables commit one-by-one on the driver; wrap
  `DeltaCommitter` in a `Future` pool if commit latency matters.
- **String routing column** assumed; extend `routeValue` for other types.
