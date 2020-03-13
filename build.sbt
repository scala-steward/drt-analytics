
ThisBuild / scalaVersion := "2.12.7"
ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / organization := "uk.gov.homeoffice"
ThisBuild / organizationName := "drt"

lazy val akkaVersion = "2.5.23"

lazy val akkaPersistenceJdbc = "3.5.0"

lazy val postgres = "42.2.2"

lazy val jodaTime = "2.9.4"

libraryDependencies ++= Seq(
  "ch.qos.logback" % "logback-classic" % "1.2.3",

  "com.typesafe.akka" %% "akka-testkit" % akkaVersion % "test",
  "com.typesafe.akka" %% "akka-stream-testkit" % akkaVersion % "test",
  "com.typesafe.akka" %% "akka-persistence" % akkaVersion,
  "com.typesafe.akka" %% "akka-persistence-query" % akkaVersion,
  "com.github.dnvriend" %% "akka-persistence-jdbc" % akkaPersistenceJdbc,
  "org.postgresql" % "postgresql" % postgres,
  "com.typesafe.akka" %% "akka-stream" % akkaVersion,
  "joda-time" % "joda-time" % jodaTime,
  "org.scalatest" %% "scalatest" % "3.1.0" % Test
  )

lazy val root = (project in file("."))
  .settings(
    name := "drt-analytics",

    PB.targets in Compile := Seq(
      scalapb.gen() -> (sourceManaged in Compile).value / "protobuf"
      ),
    PB.deleteTargetDirectory := false
  )
  .enablePlugins(DockerPlugin)
