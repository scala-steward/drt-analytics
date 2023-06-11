
ThisBuild / scalaVersion := "2.13.11"
ThisBuild / version := "v" + sys.env.getOrElse("DRONE_BUILD_NUMBER", sys.env.getOrElse("BUILD_ID", "DEV"))
ThisBuild / organization := "uk.gov.homeoffice"
ThisBuild / organizationName := "drt"
ThisBuild / scapegoatVersion := "2.1.1"

lazy val drtLib = "v495"

lazy val akkaHttpVersion = "10.4.0"
lazy val akkaVersion = "2.7.0"
lazy val akkaPersistenceJdbcVersion = "5.2.0"
lazy val akkaPersistenceInMemoryVersion = "2.5.15.2"
lazy val postgresVersion = "42.5.2"
lazy val jodaTimeVersion = "2.12.2"
lazy val logbackContribVersion = "0.1.5"
lazy val jacksonDatabindVersion = "2.13.5"
lazy val specs2Version = "4.19.2"
lazy val sparkVersion = "3.3.2"
lazy val scalaTestVersion = "3.2.15"
lazy val catsVersion = "2.9.0"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-slf4j" % akkaVersion,
  "ch.qos.logback.contrib" % "logback-json-classic" % logbackContribVersion,
  "ch.qos.logback.contrib" % "logback-jackson" % logbackContribVersion,
  "com.fasterxml.jackson.core" % "jackson-databind" % jacksonDatabindVersion,
  "org.scalatest" %% "scalatest" % scalaTestVersion % Test,

  "org.apache.spark" %% "spark-mllib" % sparkVersion excludeAll("org.scala-lang.modules", "scala-xml"),
  "org.apache.spark" %% "spark-sql" % sparkVersion excludeAll("org.scala-lang.modules", "scala-xml"),
  "com.typesafe.akka" %% "akka-http" % akkaHttpVersion,
  "com.typesafe.akka" %% "akka-testkit" % akkaVersion % "test",
  "com.typesafe.akka" %% "akka-stream-testkit" % akkaVersion % "test",
  "com.typesafe.akka" %% "akka-persistence" % akkaVersion,
  "com.typesafe.akka" %% "akka-persistence-query" % akkaVersion,
  "com.github.dnvriend" %% "akka-persistence-inmemory" % akkaPersistenceInMemoryVersion % "test",
  "com.lightbend.akka" %% "akka-persistence-jdbc" % akkaPersistenceJdbcVersion,
  "org.postgresql" % "postgresql" % postgresVersion,
  "com.typesafe.akka" %% "akka-stream" % akkaVersion,
  "joda-time" % "joda-time" % jodaTimeVersion,
  "org.specs2" %% "specs2-core" % specs2Version % Test,
  "uk.gov.homeoffice" %% "drt-lib" % drtLib excludeAll("org.scala-lang.modules", "scala-xml"),
  "org.typelevel" %% "cats-core" % catsVersion,
)

lazy val root = (project in file("."))
  .settings(
    name := "drt-analytics",
    trapExit := false,

    resolvers += "Artifactory Realm" at "https://artifactory.digital.homeoffice.gov.uk/",
    resolvers += "Artifactory Realm release local" at "https://artifactory.digital.homeoffice.gov.uk/artifactory/libs-release/",
    credentials += Credentials(Path.userHome / ".ivy2" / ".credentials"),

    dockerBaseImage := "openjdk:11-jre-slim-buster",

    Global / concurrentRestrictions += Tags.limit(Tags.Test, 1)

  )
  .enablePlugins(DockerPlugin)
  .enablePlugins(JavaAppPackaging)
