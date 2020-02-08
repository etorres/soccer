organization := "es.eriktorr.samples"
name := "soccer"
version := "1.0"

scalaVersion := "2.12.10"

val SparkVersion = "2.4.3"
val SparkTestingVersion = s"${SparkVersion}_0.12.0"
val ScalaLoggingVersion = "3.9.2"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql"  % SparkVersion % Provided,
  "com.typesafe.scala-logging" %% "scala-logging" % ScalaLoggingVersion,
  "com.holdenkarau" %% "spark-testing-base" % SparkTestingVersion % Test
)

logBuffered in Test := false
fork in Test := true
parallelExecution in Test := false

test in assembly := {}
mainClass in assembly := Some("es.eriktorr.samples.soccer.SoccerApplication")

javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:+CMSClassUnloadingEnabled")
scalacOptions ++= Seq(
  "-deprecation",
  "-encoding", "UTF-8",
  "-language:higherKinds",
  "-language:postfixOps",
  "-Xfatal-warnings")
