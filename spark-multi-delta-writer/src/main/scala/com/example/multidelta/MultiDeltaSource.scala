package com.example.multidelta

import java.util

import org.apache.hadoop.mapreduce.Job

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.{SupportsWrite, Table, TableCapability, TableProvider}
import org.apache.spark.sql.connector.distributions.{Distribution, Distributions}
import org.apache.spark.sql.connector.expressions.{Expressions, NullOrdering, SortDirection, SortOrder, Transform}
import org.apache.spark.sql.connector.write._
import org.apache.spark.sql.execution.datasources.OutputWriterFactory
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types._
import org.apache.spark.util.SerializableConfiguration
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * A DataSource V2 sink that fans a single DataFrame out to many tables in ONE
 * Spark job / ONE pass over the data, routing each row to a target table by the
 * value of a routing column.
 *
 * Usage:
 * {{{
 *   df.write.format("multiDelta")
 *     .option("routeColumn", "target_table")   // column whose value picks the table
 *     .option("basePath", "/mnt/warehouse")    // table dir = basePath/<routeValue>
 *     .option("sinkFormat", "delta")           // "delta" (default) or "parquet"
 *     .option("dropRouteColumn", "true")       // drop the routing column from output
 *     .option("partitionBy", "dt,country")     // Hive-style partition columns per table
 *     .option("maxRecordsPerFile", "1000000")  // 0 = unbounded; else roll files
 *     .mode("append")                          // or "overwrite" (per touched table)
 *     .save()
 * }}}
 *
 * Semantics: NOT atomic across tables (accepted trade-off). Each Delta table is
 * committed independently on the driver; a failure after some tables committed
 * leaves partial state. Make each write idempotent upstream if you need
 * convergence on retry. `overwrite` replaces only the tables that receive rows
 * in this write; tables not touched are left untouched.
 */
class MultiDeltaSource extends TableProvider with DataSourceRegister {

  override def shortName(): String = "multiDelta"

  // We rely on Spark passing the query schema into getTable (external metadata),
  // so inferSchema is never the source of truth for writes.
  override def inferSchema(options: CaseInsensitiveStringMap): StructType = new StructType()

  override def supportsExternalMetadata(): Boolean = true

  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: util.Map[String, String]): Table =
    new MultiDeltaTable(schema)
}

class MultiDeltaTable(tableSchema: StructType) extends Table with SupportsWrite {
  override def name(): String = "multiDelta"

  override def schema(): StructType = tableSchema

  override def capabilities(): util.Set[TableCapability] =
    // TRUNCATE enables mode("overwrite") (OverwriteByExpression with a `true` filter).
    util.EnumSet.of(
      TableCapability.BATCH_WRITE,
      TableCapability.TRUNCATE,
      TableCapability.ACCEPT_ANY_SCHEMA)

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder =
    new MultiDeltaWriteBuilder(info)
}

/** SupportsTruncate is what routes mode("overwrite") to us. */
class MultiDeltaWriteBuilder(info: LogicalWriteInfo) extends WriteBuilder with SupportsTruncate {
  private var overwrite = false
  override def truncate(): WriteBuilder = { overwrite = true; this }
  override def build(): Write = new MultiDeltaWrite(info, overwrite)
}

/**
 * When `sortWithinPartitions` is set, we ask Spark (via RequiresDistributionAndOrdering)
 * for a LOCAL sort by [routeColumn, partitionCols] before the write — no shuffle, so it
 * doesn't reintroduce skew. That makes every (table, partition) group contiguous in the
 * row stream, letting the writer keep just ONE open Parquet writer at a time (bounded
 * memory regardless of routing cardinality). When unset, all methods are no-ops.
 */
class MultiDeltaWrite(info: LogicalWriteInfo, overwrite: Boolean)
    extends Write with RequiresDistributionAndOrdering {

  private val opts = info.options()
  private val sortEnabled = opts.getBoolean("sortWithinPartitions", false)
  private val routeColumn = opts.get("routeColumn")
  private val partitionCols =
    Option(opts.get("partitionBy")).map(_.split(",").map(_.trim).filter(_.nonEmpty)).getOrElse(Array.empty[String])

  override def toBatch: BatchWrite = new MultiDeltaBatchWrite(info, overwrite)

  override def requiredDistribution(): Distribution = Distributions.unspecified() // no shuffle
  override def requiredNumPartitions(): Int = 0
  override def distributionStrictlyRequired(): Boolean = false

  override def requiredOrdering(): Array[SortOrder] =
    if (!sortEnabled || routeColumn == null) Array.empty
    else (routeColumn +: partitionCols.toSeq).map { c =>
      Expressions.sort(Expressions.column(c), SortDirection.ASCENDING, NullOrdering.NULLS_FIRST)
    }.toArray
}

/**
 * Driver-side coordinator. Builds the (serializable) Parquet OutputWriterFactory
 * once, ships it to executors, and on commit finalizes each table.
 */
class MultiDeltaBatchWrite(info: LogicalWriteInfo, overwrite: Boolean) extends BatchWrite {

  private val opts        = info.options()
  private val fullSchema  = info.schema()
  private val routeColumn = required("routeColumn")
  private val basePath    = required("basePath").stripSuffix("/")
  private val sinkFormat  = Option(opts.get("sinkFormat")).getOrElse("delta").toLowerCase
  private val dropRoute   = opts.getBoolean("dropRouteColumn", true)
  private val maxRecordsPerFile = opts.getLong("maxRecordsPerFile", 0L)
  // When true, input is locally sorted by [routeColumn, partitionCols] (see MultiDeltaWrite),
  // so the writer holds one open file at a time.
  private val sortWithinPartitions = opts.getBoolean("sortWithinPartitions", false)
  private val partitionCols: Seq[String] =
    Option(opts.get("partitionBy"))
      .map(_.split(",").map(_.trim).filter(_.nonEmpty).toSeq).getOrElse(Nil)

  private val routeIdx = fullSchema.fieldIndex(routeColumn)

  // Validate partition columns up-front (fail on the driver, not mid-job).
  partitionCols.foreach { c =>
    val i = fullSchema.fieldNames.indexOf(c)
    if (i < 0) throw new IllegalArgumentException(s"partitionBy column '$c' not found in schema")
    if (i == routeIdx)
      throw new IllegalArgumentException(s"partitionBy column '$c' cannot be the routeColumn")
    fullSchema(i).dataType match {
      case StringType | BooleanType | ByteType | ShortType | IntegerType | LongType => // ok
      case dt => throw new IllegalArgumentException(
        s"partitionBy column '$c' has unsupported type $dt (use string/boolean/integral; " +
          "date/timestamp partitions need custom value formatting)")
    }
  }

  // tableSchema = what Delta records as the table schema (route col optionally removed,
  //               partition columns INCLUDED — Delta keeps them in the schema).
  private val tableSchema: StructType =
    if (dropRoute) StructType(fullSchema.fields.zipWithIndex.collect { case (f, i) if i != routeIdx => f })
    else fullSchema

  private val partitionSet = partitionCols.toSet
  // dataSchema = what the Parquet files actually contain (partition columns live in the
  //              directory path, not the file — standard Hive/Delta layout).
  private val dataSchema: StructType =
    StructType(tableSchema.fields.filterNot(f => partitionSet.contains(f.name)))

  private val partitionIdxInFull: Array[Int] = partitionCols.map(fullSchema.fieldIndex).toArray

  @transient private val spark = SparkSession.active
  @transient private val hadoopConf = spark.sessionState.newHadoopConf()

  // prepareWrite MUTATES the job's Configuration with the Parquet write-support class +
  // schema; executors must receive THAT configuration (built from dataSchema).
  private val job = Job.getInstance(hadoopConf)
  private val parquetFactory: OutputWriterFactory =
    new ParquetFileFormat().prepareWrite(spark, job, Map.empty[String, String], dataSchema)
  private val serConf = new SerializableConfiguration(job.getConfiguration)

  private def required(key: String): String =
    Option(opts.get(key)).getOrElse(throw new IllegalArgumentException(s"Option '$key' is required"))

  override def createBatchWriterFactory(pInfo: PhysicalWriteInfo): DataWriterFactory =
    new MultiDeltaWriterFactory(
      fullSchema, dataSchema, routeIdx, dropRoute, partitionCols.toArray, partitionIdxInFull,
      basePath, parquetFactory, serConf, maxRecordsPerFile, sortWithinPartitions)

  override def useCommitCoordinator(): Boolean = false

  override def commit(messages: Array[WriterCommitMessage]): Unit = {
    val addsByTable: Map[String, Seq[WrittenFile]] =
      messages.collect { case m: MultiDeltaCommitMessage => m.files }
        .flatten
        .groupBy(_.table)
        .map { case (t, fs) => t -> fs.toSeq }

    sinkFormat match {
      case "delta" =>
        DeltaCommitter.commitAll(spark, basePath, tableSchema, partitionCols, addsByTable, overwrite)
      case "parquet" =>
        ParquetCommitter.finalizeAll(serConf, basePath, addsByTable, overwrite)
      case other =>
        throw new IllegalArgumentException(s"Unsupported sinkFormat '$other' (use 'delta' or 'parquet')")
    }
  }

  override def abort(messages: Array[WriterCommitMessage]): Unit = {
    messages.collect { case m: MultiDeltaCommitMessage => m.files }.flatten.foreach { wf =>
      try {
        val p = new org.apache.hadoop.fs.Path(s"$basePath/${wf.table}/${wf.relPath}")
        p.getFileSystem(serConf.value).delete(p, false)
      } catch { case _: Throwable => /* best effort */ }
    }
  }
}
