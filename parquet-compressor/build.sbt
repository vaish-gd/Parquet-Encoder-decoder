name := "parquet-opt"

version := "1.0"

scalaVersion := "2.11.8"

val sparkVersion = "2.2.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % "2.2.0",
   "com.typesafe" % "config" % "1.3.0",
  // https://mvnrepository.com/artifact/org.apache.spark/spark-graphx

)
//libraryDependencies += "org.apache.spark" %% "spark-graphx" % "2.4.3"

libraryDependencies += "com.outr" %% "hasher" % "1.2.2"
