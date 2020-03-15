
ThisBuild / scalaVersion := "2.12.8"
ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / organization := "uk.gov.homeoffice"
ThisBuild / organizationName := "drt"

lazy val akkaHttpVersion = "10.1.9"
lazy val akkaVersion = "2.5.23"
lazy val akkaPersistenceJdbcVersion = "3.5.0"
lazy val postgresVersion = "42.2.2"
lazy val jodaTimeVersion = "2.9.4"
lazy val logbackContribVersion = "0.1.5"
lazy val jacksonDatabindVersion = "2.10.0"
lazy val scalaTestVersion = "3.1.0"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-slf4j" % akkaVersion,
  "ch.qos.logback.contrib" % "logback-json-classic" % logbackContribVersion,
  "ch.qos.logback.contrib" % "logback-jackson" % logbackContribVersion,
  "com.fasterxml.jackson.core" % "jackson-databind" % jacksonDatabindVersion,

  "com.typesafe.akka" %% "akka-http" % akkaHttpVersion,
  "com.typesafe.akka" %% "akka-testkit" % akkaVersion % "test",
  "com.typesafe.akka" %% "akka-stream-testkit" % akkaVersion % "test",
  "com.typesafe.akka" %% "akka-persistence" % akkaVersion,
  "com.typesafe.akka" %% "akka-persistence-query" % akkaVersion,
  "com.github.dnvriend" %% "akka-persistence-jdbc" % akkaPersistenceJdbcVersion,
  "org.postgresql" % "postgresql" % postgresVersion,
  "com.typesafe.akka" %% "akka-stream" % akkaVersion,
  "joda-time" % "joda-time" % jodaTimeVersion,
  "org.scalatest" %% "scalatest" % scalaTestVersion % Test
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
  .enablePlugins(AshScriptPlugin)
