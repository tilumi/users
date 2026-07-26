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
    partitionValues: Map[String, String]) {
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
    sortedMode: Boolean) extends DataWriterFactory {

  override def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow] =
    new MultiDeltaDataWriter(
      partitionId, taskId, fullSchema, dataSchema, routeIdx, dropRoute,
      partitionColNames, partitionIdxInFull, basePath, parquetFactory, serConf,
      maxRecordsPerFile, sortedMode)
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
    sortedMode: Boolean) extends DataWriter[InternalRow] {

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

  /** The currently-open Parquet file for one (table, partition) key. */
  private final class OpenFile(
      val writer: OutputWriter,
      val absPath: String,
      val subPath: String,
      val partitionValues: Map[String, String]) {
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
    new OpenFile(writer, absPath, subPath, partValues)
  }

  private def closeFile(of: OpenFile, table: String): Unit = {
    of.writer.close()
    val p  = new Path(of.absPath)
    val st = p.getFileSystem(hadoopConf).getFileStatus(p)
    closed += WrittenFile(table, of.subPath, p.getName, st.getLen, st.getModificationTime, of.partitionValues)
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
    of.writer.write(project(record))
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
