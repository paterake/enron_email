name := "enronEmail"

version := "1.0"

scalaVersion := "2.11.11"

val sparkVersion = "2.1.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.scalatest" %% "scalatest" % "3.0.3" % "test",
  "com.databricks" %% "spark-xml" % "0.4.1"
)