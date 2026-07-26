package com.example.multidelta

import java.nio.file.Files

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

/** Top-level case class so Spark can derive an Encoder for the single-pass test. */
case class Rec(region: String, id: Int)

class MultiDeltaWriterSuite extends AnyFunSuite with BeforeAndAfterAll {

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .appName("multi-delta-writer-test")
      .master("local[3]")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "3")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
  }

  override def afterAll(): Unit = if (spark != null) spark.stop()

  private def tmpDir(): String = {
    val d = Files.createTempDirectory("multidelta").toFile
    d.deleteOnExit()
    d.getAbsolutePath.stripSuffix("/")
  }

  private def sampleDf() = {
    val ss = spark; import ss.implicits._
    Seq(
      (1, "us", "alice"),
      (2, "eu", "bob"),
      (3, "us", "carol"),
      (4, "apac", "dave"),
      (5, "eu", "erin"),
      (6, "us", "frank")
    ).toDF("id", "region", "name")
  }

  test("delta sink: rows are routed to per-table delta directories") {
    val base = tmpDir()
    sampleDf().repartition(3).write.format("multiDelta")
      .option("routeColumn", "region")
      .option("basePath", base)
      .option("sinkFormat", "delta")
      .mode("append")
      .save()

    val us = spark.read.format("delta").load(s"$base/us")
    val eu = spark.read.format("delta").load(s"$base/eu")
    val apac = spark.read.format("delta").load(s"$base/apac")

    assert(us.count() === 3)
    assert(eu.count() === 2)
    assert(apac.count() === 1)

    // route column dropped by default
    assert(!us.columns.contains("region"))
    assert(us.columns.toSet === Set("id", "name"))

    // a real _delta_log was created
    val fs = new Path(base).getFileSystem(spark.sessionState.newHadoopConf())
    assert(fs.exists(new Path(s"$base/us/_delta_log")))

    val names = us.collect().map(_.getAs[String]("name")).toSet
    assert(names === Set("alice", "carol", "frank"))
  }

  test("delta sink: append accumulates across two writes") {
    val base = tmpDir()
    def writeOnce(): Unit =
      sampleDf().write.format("multiDelta")
        .option("routeColumn", "region").option("basePath", base)
        .mode("append").save()

    writeOnce()
    writeOnce()
    assert(spark.read.format("delta").load(s"$base/us").count() === 6) // 3 + 3
  }

  test("dropRouteColumn=false keeps the routing column in output") {
    val base = tmpDir()
    sampleDf().write.format("multiDelta")
      .option("routeColumn", "region").option("basePath", base)
      .option("dropRouteColumn", "false")
      .mode("append").save()

    val us = spark.read.format("delta").load(s"$base/us")
    assert(us.columns.contains("region"))
    assert(us.select("region").distinct().collect() === Array(Row("us")))
  }

  test("parquet sink: writes plain parquet with a _SUCCESS marker, no _delta_log") {
    val base = tmpDir()
    sampleDf().write.format("multiDelta")
      .option("routeColumn", "region").option("basePath", base)
      .option("sinkFormat", "parquet")
      .mode("append").save()

    val us = spark.read.parquet(s"$base/us")
    assert(us.count() === 3)
    assert(!us.columns.contains("region"))

    val fs = new Path(base).getFileSystem(spark.sessionState.newHadoopConf())
    assert(fs.exists(new Path(s"$base/us/_SUCCESS")))
    assert(!fs.exists(new Path(s"$base/us/_delta_log")))
  }

  test("single-pass: source with a side effect is scanned exactly once") {
    val base = tmpDir()
    val ss = spark; import ss.implicits._
    val counter = ss.sparkContext.longAccumulator("scan_count")
    val counted = Seq(Rec("us", 1), Rec("eu", 2), Rec("us", 3), Rec("apac", 4)).toDS()
      .map { r => counter.add(1L); r } // one side effect per row, evaluated on write

    counted.write.format("multiDelta")
      .option("routeColumn", "region").option("basePath", base)
      .mode("append").save()

    // 4 input rows visited once total — not once per (3) target table.
    assert(counter.value === 4L)
  }

  test("maxRecordsPerFile rolls each table into multiple bounded files") {
    val base = tmpDir()
    val ss = spark; import ss.implicits._
    // 25 us rows in a single partition -> with cap 10 that's 3 files (10,10,5).
    val df = (1 to 25).map(i => ("us", i)).toDF("region", "id").repartition(1)

    df.write.format("multiDelta")
      .option("routeColumn", "region").option("basePath", base)
      .option("maxRecordsPerFile", "10")
      .mode("append").save()

    val conf = spark.sessionState.newHadoopConf()
    val dir = new Path(s"$base/us")
    val parquetFiles = dir.getFileSystem(conf).listStatus(dir)
      .filter(s => s.getPath.getName.endsWith(".parquet"))

    assert(parquetFiles.length === 3, "25 rows / cap 10 should produce 3 files")
    // Every committed AddFile is honored on read-back, and total rows preserved.
    assert(spark.read.format("delta").load(s"$base/us").count() === 25)
  }

  private def fullMessage(t: Throwable): String = {
    val sb = new StringBuilder; var c: Throwable = t
    while (c != null) { sb.append(String.valueOf(c.getMessage)).append(" | "); c = c.getCause }
    sb.toString.toLowerCase
  }

  test("partitioned delta sink: hive-style dirs + reconstructed partition columns") {
    val base = tmpDir()
    val ss = spark; import ss.implicits._
    val df = Seq(("us", "2021", "alice"), ("us", "2022", "bob"), ("eu", "2021", "carol"))
      .toDF("region", "dt", "name")

    df.write.format("multiDelta")
      .option("routeColumn", "region").option("basePath", base)
      .option("partitionBy", "dt").mode("append").save()

    val fs = new Path(base).getFileSystem(spark.sessionState.newHadoopConf())
    assert(fs.exists(new Path(s"$base/us/dt=2021")))
    assert(fs.exists(new Path(s"$base/us/dt=2022")))

    val us = spark.read.format("delta").load(s"$base/us")
    assert(us.columns.toSet === Set("dt", "name")) // route dropped, dt reconstructed from path
    assert(us.count() === 2)
    // partition pruning returns the right row
    assert(us.where($"dt" === "2021").select("name").as[String].collect().toSeq === Seq("alice"))
  }

  test("partitioned parquet sink: partition discovery on read-back") {
    val base = tmpDir()
    val ss = spark; import ss.implicits._
    Seq(("us", "2021", "a"), ("us", "2022", "b")).toDF("region", "dt", "name")
      .write.format("multiDelta").option("routeColumn", "region").option("basePath", base)
      .option("sinkFormat", "parquet").option("partitionBy", "dt").mode("append").save()

    val us = spark.read.parquet(s"$base/us")
    assert(us.columns.toSet === Set("dt", "name"))
    assert(us.count() === 2)
  }

  test("overwrite mode replaces only the tables that receive rows (delta)") {
    val base = tmpDir()
    val ss = spark; import ss.implicits._
    sampleDf().write.format("multiDelta").option("routeColumn", "region")
      .option("basePath", base).mode("append").save() // us=3, eu=2, apac=1

    Seq((10, "us", "zoe"), (11, "us", "yan")).toDF("id", "region", "name")
      .write.format("multiDelta").option("routeColumn", "region")
      .option("basePath", base).mode("overwrite").save() // overwrite only us

    val us = spark.read.format("delta").load(s"$base/us")
    assert(us.count() === 2, "us replaced 3 -> 2")
    assert(us.select("name").as[String].collect().toSet === Set("zoe", "yan"))
    assert(spark.read.format("delta").load(s"$base/eu").count() === 2, "eu untouched")
    assert(spark.read.format("delta").load(s"$base/apac").count() === 1, "apac untouched")
  }

  test("overwrite mode removes stale files (parquet)") {
    val base = tmpDir()
    val ss = spark; import ss.implicits._
    sampleDf().write.format("multiDelta").option("routeColumn", "region")
      .option("basePath", base).option("sinkFormat", "parquet").mode("append").save()

    Seq((10, "us", "zoe")).toDF("id", "region", "name")
      .write.format("multiDelta").option("routeColumn", "region")
      .option("basePath", base).option("sinkFormat", "parquet").mode("overwrite").save()

    val us = spark.read.parquet(s"$base/us")
    assert(us.count() === 1)
    assert(us.select("name").as[String].collect().toSeq === Seq("zoe"))
  }

  test("unsupported partition column type fails fast") {
    val base = tmpDir()
    val ss = spark; import ss.implicits._
    val df = Seq((1.5, "us")).toDF("amount", "region") // double partition -> rejected
    val e = intercept[Exception] {
      df.write.format("multiDelta").option("routeColumn", "region")
        .option("basePath", base).option("partitionBy", "amount").mode("append").save()
    }
    assert(fullMessage(e).contains("partitionby") && fullMessage(e).contains("unsupported"))
  }

  test("missing required options fail fast") {
    val base = tmpDir()
    val e = intercept[Exception] {
      sampleDf().write.format("multiDelta").option("basePath", base).mode("append").save()
    }
    assert(e.getMessage.toLowerCase.contains("routecolumn") ||
      e.getCause != null)
  }
}
