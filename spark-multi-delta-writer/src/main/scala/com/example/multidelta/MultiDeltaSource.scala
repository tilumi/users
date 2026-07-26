package com.example.multidelta

import java.util

import org.apache.hadoop.mapreduce.Job

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.{SupportsWrite, Table, TableCapability, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.write._
import org.apache.spark.sql.execution.datasources.OutputWriterFactory
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.StructType
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
 *     .mode("append")
 *     .save()
 * }}}
 *
 * Semantics: NOT atomic across tables (accepted trade-off). Each Delta table is
 * committed independently on the driver; a failure after some tables committed
 * leaves partial state. Make each write idempotent upstream if you need
 * convergence on retry.
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
    util.EnumSet.of(TableCapability.BATCH_WRITE, TableCapability.ACCEPT_ANY_SCHEMA)

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder =
    new MultiDeltaWriteBuilder(info)
}

class MultiDeltaWriteBuilder(info: LogicalWriteInfo) extends WriteBuilder {
  override def build(): Write = new MultiDeltaWrite(info)
}

class MultiDeltaWrite(info: LogicalWriteInfo) extends Write {
  override def toBatch: BatchWrite = new MultiDeltaBatchWrite(info)
}

/**
 * Driver-side coordinator. Builds the (serializable) Parquet OutputWriterFactory
 * once, ships it to executors, and on commit finalizes each table.
 */
class MultiDeltaBatchWrite(info: LogicalWriteInfo) extends BatchWrite {

  private val opts        = info.options()
  private val fullSchema  = info.schema()
  private val routeColumn = required("routeColumn")
  private val basePath    = required("basePath").stripSuffix("/")
  private val sinkFormat  = Option(opts.get("sinkFormat")).getOrElse("delta").toLowerCase
  private val dropRoute   = opts.getBoolean("dropRouteColumn", true)

  private val routeIdx = fullSchema.fieldIndex(routeColumn)

  /** Schema actually written to each target table (route column optionally removed). */
  private val writeSchema: StructType =
    if (dropRoute) StructType(fullSchema.fields.zipWithIndex.collect {
      case (f, i) if i != routeIdx => f
    })
    else fullSchema

  @transient private val spark = SparkSession.active
  @transient private val hadoopConf = spark.sessionState.newHadoopConf()

  // prepareWrite must run on the driver; the returned factory is serializable.
  private val parquetFactory: OutputWriterFactory = {
    val job = Job.getInstance(hadoopConf)
    new ParquetFileFormat().prepareWrite(spark, job, Map.empty[String, String], writeSchema)
  }
  private val serConf = new SerializableConfiguration(hadoopConf)

  private def required(key: String): String =
    Option(opts.get(key)).getOrElse(throw new IllegalArgumentException(s"Option '$key' is required"))

  override def createBatchWriterFactory(pInfo: PhysicalWriteInfo): DataWriterFactory =
    new MultiDeltaWriterFactory(fullSchema, writeSchema, routeIdx, dropRoute, basePath, parquetFactory, serConf)

  override def useCommitCoordinator(): Boolean = false

  override def commit(messages: Array[WriterCommitMessage]): Unit = {
    // table -> all files written for it across every task
    val addsByTable: Map[String, Seq[WrittenFile]] =
      messages.collect { case m: MultiDeltaCommitMessage => m.files }
        .flatten
        .groupBy(_.table)
        .map { case (t, fs) => t -> fs.toSeq }

    sinkFormat match {
      case "delta" =>
        DeltaCommitter.commitAll(spark, basePath, writeSchema, addsByTable)
      case "parquet" =>
        // Files were written straight into basePath/<table>; they ARE the table.
        // Nothing to commit — just drop a _SUCCESS marker per touched table.
        ParquetCommitter.finalizeAll(serConf, basePath, addsByTable.keys.toSeq)
      case other =>
        throw new IllegalArgumentException(s"Unsupported sinkFormat '$other' (use 'delta' or 'parquet')")
    }
  }

  override def abort(messages: Array[WriterCommitMessage]): Unit = {
    // Best-effort cleanup of orphaned files. For the delta sink these were never
    // committed to the log, so deleting the parquet files is sufficient.
    messages.collect { case m: MultiDeltaCommitMessage => m.files }.flatten.foreach { wf =>
      try {
        val p = new org.apache.hadoop.fs.Path(s"$basePath/${wf.table}/${wf.name}")
        p.getFileSystem(serConf.value).delete(p, false)
      } catch { case _: Throwable => /* best effort */ }
    }
  }
}
