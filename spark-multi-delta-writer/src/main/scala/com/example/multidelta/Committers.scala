package com.example.multidelta

import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Cast, Expression, Literal}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LocalRelation}

// Delta internal APIs — version-sensitive. Targets Delta 3.2 (Spark 3.5).
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.DeltaOperations
import org.apache.spark.sql.delta.actions.{Action, AddFile, Format, Metadata}
import org.apache.spark.sql.SaveMode

/**
 * `replaceWhere` support — restricted to predicates over PARTITION columns.
 *
 * A partition predicate is constant across every row of a file, so a file either lies
 * entirely inside the replace region or entirely outside it: removing the matching
 * files drops only in-region data (no partial-file rewrite, no data loss). Predicates
 * over data columns would need to preserve surviving rows from partially-matching files
 * and are rejected up-front instead of being silently mishandled.
 */
object ReplaceWhere {

  /** Parse + resolve `predicate` against the table schema and require that it references
    * only partition columns. Throws on parse errors, unknown columns, or any data-column
    * reference. Returns the resolved (type-coerced) condition. */
  def resolve(
      spark: SparkSession,
      tableSchema: StructType,
      partitionColumns: Seq[String],
      predicate: String): Expression = {
    val parsed = spark.sessionState.sqlParser.parseExpression(predicate)
    val attrs  = tableSchema.fields.map(f => AttributeReference(f.name, f.dataType, f.nullable)()).toSeq
    val analyzed = spark.sessionState.analyzer.execute(Filter(parsed, LocalRelation(attrs)))
    spark.sessionState.analyzer.checkAnalysis(analyzed) // surfaces unknown columns clearly
    val cond = analyzed.asInstanceOf[Filter].condition
    val nonPartition = cond.references.map(_.name).toSet -- partitionColumns.toSet
    if (nonPartition.nonEmpty)
      throw new IllegalArgumentException(
        "replaceWhere may only reference partition columns " +
          partitionColumns.mkString("[", ", ", "]") +
          s"; found data column(s) ${nonPartition.mkString(", ")}. " +
          "Predicates over data columns are not supported.")
    cond
  }

  /** Evaluate a resolved partition predicate against one file's partition values by
    * substituting each partition column with its (typed) literal value. Null / false
    * both count as "does not match" (SQL WHERE semantics). */
  def matches(cond: Expression, partitionSchema: StructType, values: Map[String, String]): Boolean = {
    val bound = cond.transform {
      case a: AttributeReference =>
        val dt  = partitionSchema(a.name).dataType
        val raw = values.getOrElse(a.name, null)
        val v   = if (raw == null) null else Cast(Literal(raw), dt).eval(null)
        Literal(v, dt)
    }
    bound.eval(null) == true
  }
}

/**
 * Driver-side Delta commit. For each target table we open an independent
 * OptimisticTransaction and commit that table's actions. Non-atomicity across
 * tables lives here (accepted trade-off): N separate transactions in sequence.
 *
 * append    -> commit new AddFiles.
 * overwrite -> RemoveFile every currently-visible file in the table, plus the new
 *              AddFiles, in a single transaction (replaces the table's contents).
 *              Only tables that receive rows in this write are touched.
 * overwrite + replaceWhere -> RemoveFile only the files whose partition matches the
 *              predicate (a scoped, per-table selective overwrite); see ReplaceWhere.
 */
object DeltaCommitter {

  def commitAll(
      spark: SparkSession,
      basePath: String,
      tableSchema: StructType,
      partitionColumns: Seq[String],
      addsByTable: Map[String, Seq[WrittenFile]],
      overwrite: Boolean,
      replaceWhere: Option[String] = None): Unit = {

    val resolvedRW: Option[Expression] =
      replaceWhere.map(rw => ReplaceWhere.resolve(spark, tableSchema, partitionColumns, rw))
    val partitionSchema =
      StructType(tableSchema.fields.filter(f => partitionColumns.contains(f.name)))

    // Pass 1 — enforce the constraint that ALL incoming data satisfies replaceWhere,
    // across every table, BEFORE any commit. A violation aborts the whole write, so we
    // never leave some tables selectively-overwritten and others not.
    resolvedRW.foreach { cond =>
      addsByTable.foreach { case (table, files) =>
        files.map(_.partitionValues).distinct.foreach { pv =>
          if (!ReplaceWhere.matches(cond, partitionSchema, pv))
            throw new IllegalArgumentException(
              s"replaceWhere: table '$table' received data in partition $pv that does not " +
                s"satisfy the condition '${replaceWhere.get}'")
        }
      }
    }

    // Pass 2 — commit each table.
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
        if (overwrite && txn.readVersion >= 0) {
          val existing = txn.snapshot.allFiles.collect().toSeq
          resolvedRW match {
            case Some(cond) => existing.filter(a => ReplaceWhere.matches(cond, partitionSchema, a.partitionValues)).map(_.remove)
            case None       => existing.map(_.remove) // full-table overwrite
          }
        } else Nil

      val op = if (overwrite) DeltaOperations.Write(SaveMode.Overwrite, predicate = replaceWhere)
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
