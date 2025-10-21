name := "SparkSparseMatrix"
version := "0.1"
scalaVersion := "2.12.15"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.3.0" % "provided",
  "org.scalatest" %% "scalatest" % "3.2.15" % "test"
)