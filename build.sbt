name := "BioTagME_final"

version := "0.1"

scalaVersion := "2.12.15"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.1.2",
  "org.apache.spark" %% "spark-sql" % "3.1.2",
  "org.apache.httpcomponents" % "httpclient" % "4.5.13",
  "org.scala-lang.modules" %% "scala-xml" % "2.0.1",
  "com.lucidchart" %% "xtract" % "2.2.1",
  "com.databricks" %% "spark-xml" % "0.14.0",
  "commons-io" % "commons-io" % "20030203.000550"
)