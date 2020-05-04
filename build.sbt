name := "BioTagME_final"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.3",
  "org.apache.spark" %% "spark-sql" % "2.4.3",
  "org.apache.httpcomponents" % "httpclient" % "4.5.9",
  "org.scala-lang.modules" %% "scala-xml" % "1.2.0",
  "com.lucidchart" %% "xtract" % "2.0.1"
)