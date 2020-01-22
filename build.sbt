name := "akka-consul-lease"

version := "0.1"

scalaVersion := "2.13.1"
organization := "dev.nicu"
val akkaVersion = "2.6.1"
val akkaHttpVersion = "10.1.11"
libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-coordination" % akkaVersion,
  "com.typesafe.akka" %% "akka-stream" % akkaVersion,
  "com.typesafe.akka" %% "akka-http" % akkaHttpVersion,
  "com.typesafe.akka" %% "akka-http-spray-json" % akkaHttpVersion,
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.2",
  "ch.qos.logback" % "logback-classic" % "1.2.3" % "test",
  "org.scalatest" %% "scalatest" % "3.1.0" % "test",
  "org.scalamock" %% "scalamock" % "4.4.0" % "test",
  "com.typesafe.akka" %% "akka-testkit" % akkaVersion % "test"
)
