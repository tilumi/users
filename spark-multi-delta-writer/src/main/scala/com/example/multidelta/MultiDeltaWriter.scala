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
    serConf: SerializableConfiguration) extends DataWriterFactory {

  override def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow] =
    new MultiDeltaDataWriter(
      partitionId, taskId, fullSchema, writeSchema, routeIdx, dropRoute, basePath, parquetFactory, serConf)
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
    serConf: SerializableConfiguration) extends DataWriter[InternalRow] {

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

  // table name -> (open writer, absolute file path)
  private val writers = mutable.Map.empty[String, (OutputWriter, String)]

  private def routeValue(row: InternalRow): String =
    row.get(routeIdx, routeType) match {
      case u: UTF8String => u.toString
      case null          => throw new IllegalArgumentException("route column value is null")
      case other         => String.valueOf(other)
    }

  private def writerFor(table: String): OutputWriter =
    writers.get(table).map(_._1).getOrElse {
      val dir = new Path(s"$basePath/$table")
      val fs  = dir.getFileSystem(hadoopConf)
      if (!fs.exists(dir)) fs.mkdirs(dir)
      val name    = f"part-$partitionId%05d-$taskId-${UUID.randomUUID()}.parquet"
      val absPath = new Path(dir, name).toString
      val writer  = parquetFactory.newInstance(absPath, writeSchema, newTaskAttemptContext())
      writers(table) = (writer, absPath)
      writer
    }

  override def write(record: InternalRow): Unit =
    writerFor(routeValue(record)).write(project(record))

  override def commit(): WriterCommitMessage = {
    val written = writers.map { case (table, (writer, absPath)) =>
      writer.close()
      val p  = new Path(absPath)
      val st = p.getFileSystem(hadoopConf).getFileStatus(p)
      WrittenFile(table, p.getName, st.getLen, st.getModificationTime)
    }.toSeq
    MultiDeltaCommitMessage(written)
  }

  override def abort(): Unit =
    writers.foreach { case (_, (writer, absPath)) =>
      try writer.close() catch { case _: Throwable => }
      try { val p = new Path(absPath); p.getFileSystem(hadoopConf).delete(p, false) }
      catch { case _: Throwable => }
    }

  override def close(): Unit = writers.clear()

  // A Parquet OutputWriter needs a TaskAttemptContext; synthesize a unique one.
  private def newTaskAttemptContext(): TaskAttemptContextImpl = {
    val jobId   = new JobID("multidelta", partitionId)
    val tid     = new TaskID(jobId, TaskType.MAP, partitionId)
    val attempt = new TaskAttemptID(tid, (taskId & Int.MaxValue).toInt)
    new TaskAttemptContextImpl(hadoopConf, attempt)
  }
}
