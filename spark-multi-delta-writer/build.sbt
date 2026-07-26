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
  "io.delta"         %% "delta-spark"  % deltaVersion % Provided,
  "org.scalatest"    %% "scalatest"    % "3.2.18"     % Test
)

// Spark reflects into JDK internals; required to run tests on JDK 17+.
Test / fork := true
Test / javaOptions ++= Seq(
  "--add-opens=java.base/java.lang=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
  "--add-opens=java.base/java.io=ALL-UNNAMED",
  "--add-opens=java.base/java.net=ALL-UNNAMED",
  "--add-opens=java.base/java.nio=ALL-UNNAMED",
  "--add-opens=java.base/java.util=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
  "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
  "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
  "--add-opens=java.base/jdk.internal.ref=ALL-UNNAMED",
  "-Dio.netty.tryReflectionSetAccessible=true"
)

// The writer reaches into Spark internal datasource packages
// (ParquetFileFormat, OutputWriterFactory). Keep it compiled against `Provided`
// jars that match the runtime exactly.
