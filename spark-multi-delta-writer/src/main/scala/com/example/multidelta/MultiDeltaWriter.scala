package com.example.multidelta

import java.util.UUID
import scala.collection.mutable

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.{JobID, TaskAttemptID, TaskID, TaskType}
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.catalog.ExternalCatalogUtils
import org.apache.spark.sql.catalyst.expressions.{BoundReference, UnsafeProjection}
import org.apache.spark.sql.connector.write.{DataWriter, DataWriterFactory, WriterCommitMessage}
import org.apache.spark.sql.execution.datasources.{OutputWriter, OutputWriterFactory}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.SerializableConfiguration

/**
 * A parquet file written for a specific table by one task.
 * `subPath` is the (already path-escaped) Hive-style partition directory, e.g.
 * "dt=2021-01-01/country=US", or "" when the table is unpartitioned.
 * `partitionValues` holds the RAW (unescaped) partition values for the AddFile.
 */
case class WrittenFile(
    table: String,
    subPath: String,
    name: String,
    size: Long,
    modificationTime: Long,
    partitionValues: Map[String, String],
    stats: String = null) {
  def relPath: String = if (subPath.isEmpty) name else s"$subPath/$name"
}

/** Executor -> driver payload: every file this task produced. */
case class MultiDeltaCommitMessage(files: Seq[WrittenFile]) extends WriterCommitMessage

/** Serializable factory shipped to executors. */
class MultiDeltaWriterFactory(
    fullSchema: StructType,
    dataSchema: StructType,
    routeIdx: Int,
    dropRoute: Boolean,
    partitionColNames: Array[String],
    partitionIdxInFull: Array[Int],
    basePath: String,
    parquetFactory: OutputWriterFactory,
    serConf: SerializableConfiguration,
    maxRecordsPerFile: Long,
    sortedMode: Boolean,
    collectStats: Boolean) extends DataWriterFactory {

  override def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow] =
    new MultiDeltaDataWriter(
      partitionId, taskId, fullSchema, dataSchema, routeIdx, dropRoute,
      partitionColNames, partitionIdxInFull, basePath, parquetFactory, serConf,
      maxRecordsPerFile, sortedMode, collectStats)
}

/**
 * The heart of the single-pass design: one instance per Spark task holds an open
 * Parquet writer per (target table, partition dir) and routes each row to the
 * right one. The source is scanned exactly once regardless of how many tables or
 * partitions are targeted.
 */
class MultiDeltaDataWriter(
    partitionId: Int,
    taskId: Long,
    fullSchema: StructType,
    dataSchema: StructType,
    routeIdx: Int,
    dropRoute: Boolean,
    partitionColNames: Array[String],
    partitionIdxInFull: Array[Int],
    basePath: String,
    parquetFactory: OutputWriterFactory,
    serConf: SerializableConfiguration,
    maxRecordsPerFile: Long,
    sortedMode: Boolean,
    collectStats: Boolean) extends DataWriter[InternalRow] {

  private val hadoopConf = serConf.value
  private val routeType  = fullSchema(routeIdx).dataType
  private val hasPartitions = partitionIdxInFull.nonEmpty

  // Projection stripping the routing column (if dropped) AND the partition columns
  // (they live in the path, not the file). Built once per task.
  private val project: InternalRow => InternalRow = {
    val dropSet = (if (dropRoute) Set(routeIdx) else Set.empty[Int]) ++ partitionIdxInFull.toSet
    val refs = fullSchema.fields.zipWithIndex.collect {
      case (f, i) if !dropSet.contains(i) => BoundReference(i, f.dataType, f.nullable)
    }.toSeq
    val proj = UnsafeProjection.create(refs)
    (row: InternalRow) => proj(row)
  }

  // ---- per-file Delta statistics (numRecords / minValues / maxValues / nullCount) ----
  // Only top-level columns of a safe type are indexed, and every encoding is a TRUE
  // bound (min <= all values, max >= all values) so data skipping can never drop a
  // valid row. Unhandled types simply get no min/max (skipping just won't use them).
  // code: 0 = integral(as Long)  1 = float/double  2 = string  3 = date
  private val statCols: Array[(Int, String, Byte)] =
    if (!collectStats) Array.empty
    else dataSchema.fields.zipWithIndex.flatMap { case (f, i) =>
      val code: Byte = f.dataType match {
        case ByteType | ShortType | IntegerType | LongType => 0
        case FloatType | DoubleType                        => 1
        case StringType                                    => 2
        case DateType                                      => 3
        case _                                             => -1
      }
      if (code >= 0) Some((i, f.name, code)) else None
    }

  private def integralAt(row: InternalRow, idx: Int): Long = dataSchema(idx).dataType match {
    case ByteType    => row.getByte(idx).toLong
    case ShortType   => row.getShort(idx).toLong
    case IntegerType => row.getInt(idx).toLong
    case _           => row.getLong(idx)
  }
  private def doubleAt(row: InternalRow, idx: Int): Double =
    if (dataSchema(idx).dataType == FloatType) row.getFloat(idx).toDouble else row.getDouble(idx)

  private def jsonStr(s: String): String = {
    val sb = new StringBuilder(s.length + 2); sb.append('"')
    var i = 0
    while (i < s.length) {
      s.charAt(i) match {
        case '"'  => sb.append("\\\"")
        case '\\' => sb.append("\\\\")
        case '\n' => sb.append("\\n")
        case '\r' => sb.append("\\r")
        case '\t' => sb.append("\\t")
        case '\b' => sb.append("\\b")
        case '\f' => sb.append("\\f")
        case c if c < 0x20 => sb.append("\\u%04x".format(c.toInt))
        case c => sb.append(c)
      }
      i += 1
    }
    sb.append('"').toString
  }

  /** Accumulates Delta stats for one file. Reads values out of the projected row
    * immediately (before the next projection overwrites it); strings are deep-copied. */
  private final class FileStats {
    private val n = statCols.length
    private val nullCount = new Array[Long](n)
    private val seen = new Array[Boolean](n)
    private val bad  = new Array[Boolean](n) // NaN/Inf seen -> drop min/max for the column
    private val lMin = new Array[Long](n);        private val lMax = new Array[Long](n)
    private val dMin = new Array[Double](n);      private val dMax = new Array[Double](n)
    private val sMin = new Array[UTF8String](n);  private val sMax = new Array[UTF8String](n)
    var numRecords = 0L

    def update(row: InternalRow): Unit = {
      numRecords += 1
      var k = 0
      while (k < n) {
        val (idx, _, code) = statCols(k)
        if (row.isNullAt(idx)) nullCount(k) += 1
        else code match {
          case 0 | 3 =>
            val v = if (code == 3) row.getInt(idx).toLong else integralAt(row, idx)
            if (!seen(k)) { lMin(k) = v; lMax(k) = v }
            else { if (v < lMin(k)) lMin(k) = v; if (v > lMax(k)) lMax(k) = v }
            seen(k) = true
          case 1 =>
            val v = doubleAt(row, idx)
            if (v.isNaN || v.isInfinite) bad(k) = true
            else {
              if (!seen(k)) { dMin(k) = v; dMax(k) = v }
              else { if (v < dMin(k)) dMin(k) = v; if (v > dMax(k)) dMax(k) = v }
              seen(k) = true
            }
          case _ => // string
            val c = UTF8String.fromBytes(row.getUTF8String(idx).getBytes) // deep copy
            if (!seen(k)) { sMin(k) = c; sMax(k) = c }
            else { if (c.compareTo(sMin(k)) < 0) sMin(k) = c; if (c.compareTo(sMax(k)) > 0) sMax(k) = c }
            seen(k) = true
        }
        k += 1
      }
    }

    def toJson: String = {
      val sb = new StringBuilder(64)
      sb.append("{\"numRecords\":").append(numRecords)
      appendMap(sb, "minValues", min = true)
      appendMap(sb, "maxValues", min = false)
      sb.append(",\"nullCount\":{")
      var first = true; var k = 0
      while (k < n) {
        if (!first) sb.append(','); first = false
        sb.append(jsonStr(statCols(k)._2)).append(':').append(nullCount(k)); k += 1
      }
      sb.append("}}").toString
    }

    private def appendMap(sb: StringBuilder, field: String, min: Boolean): Unit = {
      sb.append(",\"").append(field).append("\":{")
      var first = true; var k = 0
      while (k < n) {
        if (seen(k) && !bad(k)) {
          if (!first) sb.append(','); first = false
          val (_, name, code) = statCols(k)
          sb.append(jsonStr(name)).append(':')
          code match {
            case 0 => sb.append(if (min) lMin(k) else lMax(k))
            case 3 => sb.append(jsonStr(java.time.LocalDate.ofEpochDay(if (min) lMin(k) else lMax(k)).toString))
            case 1 => sb.append(if (min) dMin(k) else dMax(k))
            case _ => sb.append(jsonStr((if (min) sMin(k) else sMax(k)).toString))
          }
        }
        k += 1
      }
      sb.append('}')
    }
  }

  /** The currently-open Parquet file for one (table, partition) key. */
  private final class OpenFile(
      val writer: OutputWriter,
      val absPath: String,
      val subPath: String,
      val partitionValues: Map[String, String],
      val stats: FileStats) {
    var count: Long = 0L
  }

  private val open   = mutable.Map.empty[(String, String), OpenFile]
  private val closed = mutable.ArrayBuffer.empty[WrittenFile]

  private def routeValue(row: InternalRow): String =
    row.get(routeIdx, routeType) match {
      case u: UTF8String => u.toString
      case null          => throw new IllegalArgumentException("route column value is null")
      case other         => String.valueOf(other)
    }

  private def rawPartitionValue(row: InternalRow, i: Int, dt: DataType): String =
    if (row.isNullAt(i)) ExternalCatalogUtils.DEFAULT_PARTITION_NAME
    else dt match {
      case StringType  => row.getUTF8String(i).toString
      case BooleanType => row.getBoolean(i).toString
      case ByteType    => row.getByte(i).toString
      case ShortType   => row.getShort(i).toString
      case IntegerType => row.getInt(i).toString
      case LongType    => row.getLong(i).toString
      case _           => throw new UnsupportedOperationException(s"Unsupported partition type: $dt")
    }

  /** (escaped partition dir, raw partition value map) for this row. */
  private def computePartition(row: InternalRow): (String, Map[String, String]) = {
    if (!hasPartitions) ("", Map.empty)
    else {
      val dirs = new Array[String](partitionIdxInFull.length)
      val m = Map.newBuilder[String, String]
      var k = 0
      while (k < partitionIdxInFull.length) {
        val i = partitionIdxInFull(k)
        val raw = rawPartitionValue(row, i, fullSchema(i).dataType)
        m += (partitionColNames(k) -> raw)
        dirs(k) = ExternalCatalogUtils.escapePathName(partitionColNames(k)) + "=" +
          ExternalCatalogUtils.escapePathName(raw)
        k += 1
      }
      (dirs.mkString("/"), m.result())
    }
  }

  private def openNew(table: String, subPath: String, partValues: Map[String, String]): OpenFile = {
    val dir = if (subPath.isEmpty) new Path(s"$basePath/$table") else new Path(s"$basePath/$table/$subPath")
    val fs  = dir.getFileSystem(hadoopConf)
    if (!fs.exists(dir)) fs.mkdirs(dir)
    val name    = f"part-$partitionId%05d-$taskId-${UUID.randomUUID()}.parquet"
    val absPath = new Path(dir, name).toString
    val writer  = parquetFactory.newInstance(absPath, dataSchema, newTaskAttemptContext())
    new OpenFile(writer, absPath, subPath, partValues, new FileStats)
  }

  private def closeFile(of: OpenFile, table: String): Unit = {
    of.writer.close()
    val p  = new Path(of.absPath)
    val st = p.getFileSystem(hadoopConf).getFileStatus(p)
    val stats = if (collectStats) of.stats.toJson else null
    closed += WrittenFile(table, of.subPath, p.getName, st.getLen, st.getModificationTime, of.partitionValues, stats)
  }

  override def write(record: InternalRow): Unit = {
    val table = routeValue(record)
    val (subPath, partValues) = computePartition(record)
    val key = (table, subPath)
    // Sorted input: a new key means the previous group is finished, so flush the
    // open writer(s). `open` then holds at most one entry -> memory is bounded to a
    // single Parquet row-group buffer regardless of how many tables/partitions exist.
    if (sortedMode && !open.contains(key) && open.nonEmpty) {
      open.foreach { case ((t, _), o) => closeFile(o, t) }
      open.clear()
    }
    val of = open.getOrElseUpdate(key, openNew(table, subPath, partValues))
    val projected = project(record)
    of.writer.write(projected)
    if (collectStats) of.stats.update(projected)
    of.count += 1
    if (maxRecordsPerFile > 0 && of.count >= maxRecordsPerFile) {
      closeFile(of, table)
      open.remove(key)
    }
  }

  override def commit(): WriterCommitMessage = {
    open.foreach { case ((table, _), of) => closeFile(of, table) }
    open.clear()
    MultiDeltaCommitMessage(closed.toSeq)
  }

  override def abort(): Unit = {
    def del(path: String): Unit =
      try { val p = new Path(path); p.getFileSystem(hadoopConf).delete(p, false) }
      catch { case _: Throwable => }
    open.foreach { case (_, of) => try of.writer.close() catch { case _: Throwable => }; del(of.absPath) }
    closed.foreach(wf => del(s"$basePath/${wf.table}/${wf.relPath}"))
  }

  override def close(): Unit = open.clear()

  private def newTaskAttemptContext(): TaskAttemptContextImpl = {
    val jobId   = new JobID("multidelta", partitionId)
    val tid     = new TaskID(jobId, TaskType.MAP, partitionId)
    val attempt = new TaskAttemptID(tid, (taskId & Int.MaxValue).toInt)
    new TaskAttemptContextImpl(hadoopConf, attempt)
  }
}
