package com.example.multidelta

import org.apache.hadoop.fs.Path

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration

// Delta internal APIs — version-sensitive. Targets Delta 3.2 (Spark 3.5).
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.DeltaOperations
import org.apache.spark.sql.delta.actions.{AddFile, Format, Metadata}
import org.apache.spark.sql.SaveMode

/**
 * Driver-side Delta commit. For each target table we open an independent
 * OptimisticTransaction and commit that table's AddFile actions. This is where
 * the (accepted) non-atomicity lives: N separate transactions, committed in
 * sequence. Parallelize with a Future pool if commit latency dominates.
 */
object DeltaCommitter {

  def commitAll(
      spark: SparkSession,
      basePath: String,
      writeSchema: StructType,
      addsByTable: Map[String, Seq[WrittenFile]]): Unit = {

    addsByTable.foreach { case (table, files) =>
      val tablePath = s"$basePath/$table"
      val log = DeltaLog.forTable(spark, tablePath)
      val txn = log.startTransaction()

      // First write to a brand-new table: stamp schema + format as the metadata.
      if (txn.readVersion < 0) {
        txn.updateMetadata(
          Metadata(
            schemaString    = writeSchema.json,
            format          = Format("parquet"),
            partitionColumns = Nil))
      }

      val adds: Seq[AddFile] = files.map { f =>
        AddFile(
          path             = f.name,          // relative to the table root (unpartitioned)
          partitionValues  = Map.empty[String, String],
          size             = f.size,
          modificationTime = f.modificationTime,
          dataChange       = true)
      }

      txn.commit(adds, DeltaOperations.Write(SaveMode.Append))
    }
  }
}

/**
 * "Pure parquet" sink: the executors already wrote the parquet files directly
 * into basePath/<table>, so those files ARE the table. There is no transaction
 * log to update — we only drop a _SUCCESS marker per touched directory so
 * downstream readers/tools see a conventional parquet output.
 */
object ParquetCommitter {

  def finalizeAll(serConf: SerializableConfiguration, basePath: String, tables: Seq[String]): Unit = {
    val conf = serConf.value
    tables.foreach { table =>
      val dir = new Path(s"$basePath/$table")
      val fs  = dir.getFileSystem(conf)
      val marker = new Path(dir, "_SUCCESS")
      val out = fs.create(marker, true)
      try out.close() catch { case _: Throwable => }
    }
  }
}
