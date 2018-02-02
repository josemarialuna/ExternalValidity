
name := "ExternalValidity"

version := "1.0"

scalaVersion := "2.11.8"


val sparkVersion = "2.2.0"

//LOCAL
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion ,
  "org.apache.spark" %% "spark-mllib" % sparkVersion ,
  "org.apache.spark" %% "spark-hive" % sparkVersion
)

//AWS
//libraryDependencies ++= Seq(
//  "org.apache.spark" %% "spark-core" % sparkVersion% "provided"
//  ,"org.apache.spark" %% "spark-sql" % sparkVersion% "provided"
//  ,"org.apache.spark" %% "spark-mllib" % sparkVersion% "provided"
//  ,"org.apache.spark" %% "spark-hive" % sparkVersion% "provided"
//  ,"org.apache.hadoop" % "hadoop-aws" % "2.7.3"
//)