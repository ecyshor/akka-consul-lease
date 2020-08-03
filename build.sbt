name := "akka-consul-lease"

//enablePlugins(Sonatype, ReleasePlugin, SbtPgp)

scalaVersion := "2.13.1"
organization := "dev.nicu.akka"
val akkaVersion = "2.6.8"
val akkaHttpVersion = "10.1.12"
libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-coordination" % akkaVersion,
  "com.typesafe.akka" %% "akka-stream" % akkaVersion,
  "com.typesafe.akka" %% "akka-http" % akkaHttpVersion,
  "com.typesafe.akka" %% "akka-http-spray-json" % akkaHttpVersion,
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.2",
  "ch.qos.logback" % "logback-classic" % "1.2.3" % "test",
  "org.scalatest" %% "scalatest" % "3.2.0" % "test",
  "org.scalamock" %% "scalamock" % "5.0.0" % "test",
  "com.typesafe.akka" %% "akka-testkit" % akkaVersion % "test"
)

crossScalaVersions := Seq("2.12.9", "2.13.1")

//sonatype config
sonatypeProfileName := "dev.nicu.akka"

publishMavenStyle := true

licenses := Seq("APL2" -> url("http://www.apache.org/licenses/LICENSE-2.0.txt"))

import xerial.sbt.Sonatype._
publishTo := sonatypePublishToBundle.value

sonatypeProjectHosting := Some(GitHubHosting("ecyshor", "akka-consul-lease", "Nicu Reut", "contact@nicu.dev"))

//release config
import ReleaseTransformations._

releaseVersionBump := sbtrelease.Version.Bump.Major
releaseCrossBuild := true // true if you cross-build the project for multiple Scala versions
releaseProcess := Seq[ReleaseStep](
  checkSnapshotDependencies,
  inquireVersions,
  runClean,
  runTest,
  setReleaseVersion,
  commitReleaseVersion,
  tagRelease,
  // For non cross-build projects, use releaseStepCommand("publishSigned")
  releaseStepCommandAndRemaining("+publishSigned"),
  releaseStepCommand("sonatypeBundleRelease"),
  setNextVersion,
  commitNextVersion,
  pushChanges
)