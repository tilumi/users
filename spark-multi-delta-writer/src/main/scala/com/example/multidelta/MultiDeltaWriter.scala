package com.example.multidelta

import java.util.UUID
import scala.collection.mutable

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.{JobID, TaskAttemptID, TaskID, TaskType}
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{BoundReference, UnsafeProjection}
import org.apache.spark.sql.connector.write.{DataWriter, DataWriterFactory, WriterCommitMessage}
import org.apache.spark.sql.execution.datasources.{OutputWriter, OutputWriterFactory}
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.SerializableConfiguration

/** A parquet file written for a specific table by one task. */
case class WrittenFile(table: String, name: String, size: Long, modificationTime: Long)

/** Executor -> driver payload: every file this task produced, per table. */
case class MultiDeltaCommitMessage(files: Seq[WrittenFile]) extends WriterCommitMessage

/** Serializable factory shipped to executors. */
class MultiDeltaWriterFactory(
    fullSchema: StructType,
    writeSchema: StructType,
    routeIdx: Int,
    dropRoute: Boolean,
    basePath: String,
    parquetFactory: OutputWriterFactory,
    serConf: SerializableConfiguration,
    maxRecordsPerFile: Long) extends DataWriterFactory {

  override def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow] =
    new MultiDeltaDataWriter(
      partitionId, taskId, fullSchema, writeSchema, routeIdx, dropRoute, basePath,
      parquetFactory, serConf, maxRecordsPerFile)
}

/**
 * The heart of the single-pass design: one instance per Spark task holds an open
 * Parquet writer per target table and routes each incoming row to the right one.
 * The source is scanned exactly once regardless of how many tables are targeted.
 */
class MultiDeltaDataWriter(
    partitionId: Int,
    taskId: Long,
    fullSchema: StructType,
    writeSchema: StructType,
    routeIdx: Int,
    dropRoute: Boolean,
    basePath: String,
    parquetFactory: OutputWriterFactory,
    serConf: SerializableConfiguration,
    maxRecordsPerFile: Long) extends DataWriter[InternalRow] {

  private val hadoopConf = serConf.value
  private val routeType  = fullSchema(routeIdx).dataType

  // Projection that strips the routing column from each row (built once per task).
  private val project: InternalRow => InternalRow =
    if (dropRoute) {
      val refs = fullSchema.fields.zipWithIndex.collect {
        case (f, i) if i != routeIdx => BoundReference(i, f.dataType, f.nullable)
      }.toSeq
      val proj = UnsafeProjection.create(refs)
      (row: InternalRow) => proj(row)
    } else identity

  /** The currently-open Parquet file for one table, plus its running row count. */
  private final class OpenFile(val writer: OutputWriter, val absPath: String) {
    var count: Long = 0L
  }

  // table name -> the file currently being written for it
  private val open = mutable.Map.empty[String, OpenFile]
  // files already closed this task (from rolling); reported at commit()
  private val closed = mutable.ArrayBuffer.empty[WrittenFile]

  private def routeValue(row: InternalRow): String =
    row.get(routeIdx, routeType) match {
      case u: UTF8String => u.toString
      case null          => throw new IllegalArgumentException("route column value is null")
      case other         => String.valueOf(other)
    }

  private def openNew(table: String): OpenFile = {
    val dir = new Path(s"$basePath/$table")
    val fs  = dir.getFileSystem(hadoopConf)
    if (!fs.exists(dir)) fs.mkdirs(dir)
    val name    = f"part-$partitionId%05d-$taskId-${UUID.randomUUID()}.parquet"
    val absPath = new Path(dir, name).toString
    val writer  = parquetFactory.newInstance(absPath, writeSchema, newTaskAttemptContext())
    new OpenFile(writer, absPath)
  }

  /** Close one file and record it for the commit message. */
  private def closeFile(table: String, of: OpenFile): Unit = {
    of.writer.close()
    val p  = new Path(of.absPath)
    val st = p.getFileSystem(hadoopConf).getFileStatus(p)
    closed += WrittenFile(table, p.getName, st.getLen, st.getModificationTime)
  }

  override def write(record: InternalRow): Unit = {
    val table = routeValue(record)
    val of = open.getOrElseUpdate(table, openNew(table))
    of.writer.write(project(record))
    of.count += 1
    // Roll to a fresh file once this file hits the cap.
    if (maxRecordsPerFile > 0 && of.count >= maxRecordsPerFile) {
      closeFile(table, of)
      open.remove(table)
    }
  }

  override def commit(): WriterCommitMessage = {
    open.foreach { case (table, of) => closeFile(table, of) }
    open.clear()
    MultiDeltaCommitMessage(closed.toSeq)
  }

  override def abort(): Unit = {
    def del(path: String): Unit =
      try { val p = new Path(path); p.getFileSystem(hadoopConf).delete(p, false) }
      catch { case _: Throwable => }
    open.foreach { case (_, of) =>
      try of.writer.close() catch { case _: Throwable => }
      del(of.absPath)
    }
    closed.foreach(wf => del(s"$basePath/${wf.table}/${wf.name}"))
  }

  override def close(): Unit = open.clear()

  // A Parquet OutputWriter needs a TaskAttemptContext; synthesize a unique one.
  private def newTaskAttemptContext(): TaskAttemptContextImpl = {
    val jobId   = new JobID("multidelta", partitionId)
    val tid     = new TaskID(jobId, TaskType.MAP, partitionId)
    val attempt = new TaskAttemptID(tid, (taskId & Int.MaxValue).toInt)
    new TaskAttemptContextImpl(hadoopConf, attempt)
  }
}
