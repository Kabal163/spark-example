name := "spark-task1"

version := "0.1"

scalaVersion := "2.11.12"

val sparkVersion = "2.2.1"

resolvers += "Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven"

libraryDependencies ++= Seq (
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "MrPowers" % "spark-fast-tests" % "2.2.0_0.7.0" % "test",
  "org.scalatest" %% "scalatest" % "3.0.6" % "test",
  "org.scalactic" %% "scalactic" % "3.0.6" % "test"
)