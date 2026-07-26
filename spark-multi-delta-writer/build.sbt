// Reference implementation — pin these to YOUR cluster's exact versions.
// Delta internal APIs (DeltaLog / OptimisticTransaction / AddFile / Metadata)
// are not source-stable across major versions; this targets Spark 3.5 / Delta 3.2.
name         := "spark-multi-delta-writer"
organization := "com.example"
version      := "0.1.0"
scalaVersion := "2.12.18"

val sparkVersion = "3.5.1"
val deltaVersion = "3.2.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql"    % sparkVersion % Provided,
  "io.delta"         %% "delta-spark"  % deltaVersion % Provided
)

// The writer reaches into Spark internal datasource packages
// (ParquetFileFormat, OutputWriterFactory). Keep it compiled against `Provided`
// jars that match the runtime exactly.
