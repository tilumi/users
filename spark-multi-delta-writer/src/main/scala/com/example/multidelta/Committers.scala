package com.example.multidelta

import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration

// Delta internal APIs — version-sensitive. Targets Delta 3.2 (Spark 3.5).
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.DeltaOperations
import org.apache.spark.sql.delta.actions.{Action, AddFile, Format, Metadata}
import org.apache.spark.sql.SaveMode

/**
 * Driver-side Delta commit. For each target table we open an independent
 * OptimisticTransaction and commit that table's actions. Non-atomicity across
 * tables lives here (accepted trade-off): N separate transactions in sequence.
 *
 * append    -> commit new AddFiles.
 * overwrite -> RemoveFile every currently-visible file in the table, plus the new
 *              AddFiles, in a single transaction (replaces the table's contents).
 *              Only tables that receive rows in this write are touched.
 */
object DeltaCommitter {

  def commitAll(
      spark: SparkSession,
      basePath: String,
      tableSchema: StructType,
      partitionColumns: Seq[String],
      addsByTable: Map[String, Seq[WrittenFile]],
      overwrite: Boolean): Unit = {

    addsByTable.foreach { case (table, files) =>
      val tablePath = s"$basePath/$table"
      val log = DeltaLog.forTable(spark, tablePath)
      val txn = log.startTransaction()

      if (txn.readVersion < 0) {
        txn.updateMetadata(
          Metadata(
            schemaString     = tableSchema.json,
            format           = Format("parquet"),
            partitionColumns = partitionColumns))
      }

      val adds: Seq[AddFile] = files.map { f =>
        AddFile(
          path             = f.relPath,          // relative to the table root
          partitionValues  = f.partitionValues,
          size             = f.size,
          modificationTime = f.modificationTime,
          dataChange       = true,
          stats            = f.stats)            // null when collectStats is off
      }

      val removes: Seq[Action] =
        if (overwrite && txn.readVersion >= 0)
          txn.snapshot.allFiles.collect().toSeq.map(_.remove)
        else Nil

      val op = if (overwrite) DeltaOperations.Write(SaveMode.Overwrite)
               else DeltaOperations.Write(SaveMode.Append)

      txn.commit(removes ++ adds, op)
    }
  }
}

/**
 * "Pure parquet" sink finalize. Executors already wrote parquet straight into
 * basePath/<table>[/<partition dirs>], so those files ARE the table.
 *
 * append    -> just drop a _SUCCESS marker per touched table.
 * overwrite -> delete every pre-existing file under the table dir that we did NOT
 *              write in this commit, then drop _SUCCESS. (We keep exactly the files
 *              named in this commit's WrittenFiles.)
 */
object ParquetCommitter {

  def finalizeAll(
      serConf: SerializableConfiguration,
      basePath: String,
      addsByTable: Map[String, Seq[WrittenFile]],
      overwrite: Boolean): Unit = {

    val conf = serConf.value
    addsByTable.foreach { case (table, files) =>
      val dir = new Path(s"$basePath/$table")
      val fs  = dir.getFileSystem(conf)

      if (overwrite) {
        val keep = files.map(_.relPath).toSet
        deleteExcept(fs, dir, dir, keep)
      }

      val marker = new Path(dir, "_SUCCESS")
      val out = fs.create(marker, true)
      try out.close() catch { case _: Throwable => }
    }
  }

  /** Recursively delete every file under `dir` whose path relative to `root` is
    * not in `keep`. Prunes now-empty directories. The _SUCCESS marker is left to
    * the caller to (re)create. */
  private def deleteExcept(fs: FileSystem, root: Path, dir: Path, keep: Set[String]): Unit = {
    if (!fs.exists(dir)) return
    fs.listStatus(dir).foreach { st =>
      val p = st.getPath
      if (st.isDirectory) {
        deleteExcept(fs, root, p, keep)
        if (fs.listStatus(p).isEmpty) fs.delete(p, false)
      } else {
        val rel = relativize(root, p)
        if (rel != "_SUCCESS" && !keep.contains(rel)) fs.delete(p, false)
      }
    }
  }

  private def relativize(root: Path, file: Path): String = {
    val r = root.toUri.getPath.stripSuffix("/") + "/"
    val f = file.toUri.getPath
    if (f.startsWith(r)) f.substring(r.length) else file.getName
  }
}
