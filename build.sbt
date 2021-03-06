name := "Learning Scala"

version := "0.1"

scalaVersion := "2.11.12"



libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "2.4.4" exclude("slf4j", "slf4j-log4j12"),
      "org.apache.spark" %% "spark-sql" % "2.4.4"
)