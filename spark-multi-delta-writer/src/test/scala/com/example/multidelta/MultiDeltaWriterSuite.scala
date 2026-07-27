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

  test("sortWithinPartitions=true: local sort + bounded writers, data preserved") {
    val base = tmpDir()
    val ss = spark; import ss.implicits._
    // 5 tables x 3 partitions, interleaved across 4 input partitions -> exercises the
    // sorted close-on-key-change path (Spark local-sorts by [region, dt] first).
    val rows = (0 until 300).map(i => (s"t${i % 5}", s"d${i % 3}", i))
    val df = rows.toDF("region", "dt", "id").repartition(4)

    df.write.format("multiDelta")
      .option("routeColumn", "region").option("basePath", base)
      .option("partitionBy", "dt")
      .option("sortWithinPartitions", "true")
      .option("maxRecordsPerFile", "7") // also roll within a group while sorted
      .mode("append").save()

    var total = 0L
    (0 until 5).foreach { k =>
      val t = spark.read.format("delta").load(s"$base/t$k")
      assert(t.count() === 60, s"t$k should have 60 rows") // 300/5
      assert(t.columns.toSet === Set("dt", "id"))          // route dropped, dt reconstructed
      assert(t.select("dt").distinct().count() === 3)
      total += t.count()
    }
    assert(total === 300, "no rows lost across close-on-key-change")

    // Positive proof the local sort engaged: with keys contiguous, close-on-change
    // rolls only per group (+maxRecordsPerFile). Were the sort a no-op, the
    // interleaved input would fragment into ~300 one-row files. Bound well below that.
    val conf = spark.sessionState.newHadoopConf()
    def countParquet(p: Path): Int = {
      val fs = p.getFileSystem(conf)
      if (!fs.exists(p)) 0
      else fs.listStatus(p).map { s =>
        if (s.isDirectory) countParquet(s.getPath)
        else if (s.getPath.getName.endsWith(".parquet")) 1 else 0
      }.sum
    }
    assert(countParquet(new Path(base)) < 120, "local sort should keep files grouped, not fragmented")
  }

  test("replaceWhere: selective overwrite replaces only the matching partition") {
    val base = tmpDir()
    val ss = spark; import ss.implicits._
    // seed us + eu, each with dt=2021 and dt=2022
    Seq(("us","2021","a1"),("us","2022","b1"),("eu","2021","c1"),("eu","2022","d1"))
      .toDF("region","dt","name")
      .write.format("multiDelta").option("routeColumn","region").option("basePath",base)
      .option("partitionBy","dt").mode("append").save()

    // reload only dt=2021 for us
    Seq(("us","2021","a1_new"),("us","2021","a2_new"))
      .toDF("region","dt","name")
      .write.format("multiDelta").option("routeColumn","region").option("basePath",base)
      .option("partitionBy","dt").option("replaceWhere","dt = '2021'").mode("overwrite").save()

    val us = spark.read.format("delta").load(s"$base/us")
    assert(us.where($"dt" === "2021").select("name").as[String].collect().toSet === Set("a1_new","a2_new"))
    assert(us.where($"dt" === "2022").select("name").as[String].collect().toSeq === Seq("b1")) // untouched
    assert(us.count() === 3)
    // eu received no rows in the 2nd write -> entirely untouched
    assert(spark.read.format("delta").load(s"$base/eu").count() === 2)
  }

  test("replaceWhere: rejects incoming data outside the predicate region (table unchanged)") {
    val base = tmpDir()
    val ss = spark; import ss.implicits._
    Seq(("us","2021","orig")).toDF("region","dt","name")
      .write.format("multiDelta").option("routeColumn","region").option("basePath",base)
      .option("partitionBy","dt").mode("append").save()

    val ex = intercept[Exception] {
      Seq(("us","2021","ok"),("us","2022","outside")) // 2022 violates dt = '2021'
        .toDF("region","dt","name")
        .write.format("multiDelta").option("routeColumn","region").option("basePath",base)
        .option("partitionBy","dt").option("replaceWhere","dt = '2021'").mode("overwrite").save()
    }
    assert(ex.getMessage.contains("replaceWhere") || ex.getCause != null)
    // original data intact — the write aborted before committing
    val us = spark.read.format("delta").load(s"$base/us")
    assert(us.select("name").as[String].collect().toSeq === Seq("orig"))
  }

  test("replaceWhere: a data-column predicate fails fast") {
    val base = tmpDir()
    val ss = spark; import ss.implicits._
    val ex = intercept[Exception] {
      Seq(("us","2021","x")).toDF("region","dt","name")
        .write.format("multiDelta").option("routeColumn","region").option("basePath",base)
        .option("partitionBy","dt").option("replaceWhere","name = 'x'").mode("overwrite").save()
    }
    assert((ex.getMessage + Option(ex.getCause).map(_.getMessage).getOrElse("")).contains("partition"))
  }

  test("replaceWhere without overwrite mode fails") {
    val base = tmpDir()
    val ss = spark; import ss.implicits._
    val ex = intercept[Exception] {
      Seq(("us","2021","x")).toDF("region","dt","name")
        .write.format("multiDelta").option("routeColumn","region").option("basePath",base)
        .option("partitionBy","dt").option("replaceWhere","dt = '2021'").mode("append").save()
    }
    assert((ex.getMessage + Option(ex.getCause).map(_.getMessage).getOrElse("")).contains("overwrite"))
  }

  test("collectStats writes Delta min/max/nullCount; skipping stays correct") {
    val base = tmpDir()
    val ss = spark; import ss.implicits._
    // ids 1..100 into 'us', every 10th name null, across 4 files
    val df = (1 to 100).map(i => (i, "us", if (i % 10 == 0) null.asInstanceOf[String] else s"n$i"))
      .toDF("id", "region", "name").repartition(4)
    df.write.format("multiDelta")
      .option("routeColumn", "region").option("basePath", base).mode("append").save()

    // every committed AddFile carries valid stats JSON with numRecords + min/max on id
    val snap = org.apache.spark.sql.delta.DeltaLog.forTable(spark, s"$base/us").update()
    val files = snap.allFiles.collect()
    assert(files.nonEmpty)
    assert(files.forall(a => a.stats != null &&
      a.stats.contains("numRecords") && a.stats.contains("minValues") && a.stats.contains("\"id\"")))

    // data skipping (driven by those stats) must not drop valid rows
    val us = spark.read.format("delta").load(s"$base/us")
    assert(us.where($"id" === 42).count() === 1)
    assert(us.where($"id" > 90).count() === 10)
    assert(us.where($"name".isNull).count() === 10) // nullCount stat is correct
  }

  test("collectStats=false omits stats") {
    val base = tmpDir()
    val ss = spark; import ss.implicits._
    (1 to 20).map(i => (i, "us")).toDF("id", "region")
      .write.format("multiDelta").option("routeColumn", "region")
      .option("basePath", base).option("collectStats", "false").mode("append").save()
    val files = org.apache.spark.sql.delta.DeltaLog.forTable(spark, s"$base/us").update().allFiles.collect()
    assert(files.nonEmpty && files.forall(_.stats == null))
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
