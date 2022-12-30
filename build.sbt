
ThisBuild / scalaVersion := "2.12.15"
ThisBuild / version := "v" + sys.env.getOrElse("DRONE_BUILD_NUMBER", sys.env.getOrElse("BUILD_ID", "DEV"))
ThisBuild / organization := "uk.gov.homeoffice"
ThisBuild / organizationName := "drt"

lazy val drtLib = "v347"

lazy val akkaHttpVersion = "10.1.9"
lazy val akkaVersion = "2.6.20"
lazy val akkaPersistenceJdbcVersion = "3.5.0"
lazy val akkaPersistenceInMemoryVersion = "2.5.15.2"
lazy val postgresVersion = "42.2.2"
lazy val jodaTimeVersion = "2.9.4"
lazy val logbackContribVersion = "0.1.5"
lazy val jacksonDatabindVersion = "2.12.0"
lazy val specs2Version = "4.6.0"
lazy val sparkVersion = "3.2.0"
lazy val scalaTestVersion = "3.2.9"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-slf4j" % akkaVersion,
  "ch.qos.logback.contrib" % "logback-json-classic" % logbackContribVersion,
  "ch.qos.logback.contrib" % "logback-jackson" % logbackContribVersion,
  "com.fasterxml.jackson.core" % "jackson-databind" % jacksonDatabindVersion,
  "org.scalatest" %% "scalatest" % scalaTestVersion % Test,

  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "com.typesafe.akka" %% "akka-http" % akkaHttpVersion,
  "com.typesafe.akka" %% "akka-testkit" % akkaVersion % "test",
  "com.typesafe.akka" %% "akka-stream-testkit" % akkaVersion % "test",
  "com.typesafe.akka" %% "akka-persistence" % akkaVersion,
  "com.typesafe.akka" %% "akka-persistence-query" % akkaVersion,
  "com.github.dnvriend" %% "akka-persistence-inmemory" % akkaPersistenceInMemoryVersion % "test",
  "com.github.dnvriend" %% "akka-persistence-jdbc" % akkaPersistenceJdbcVersion,
  "org.postgresql" % "postgresql" % postgresVersion,
  "com.typesafe.akka" %% "akka-stream" % akkaVersion,
  "joda-time" % "joda-time" % jodaTimeVersion,
  "org.specs2" %% "specs2-core" % specs2Version % Test,
  "uk.gov.homeoffice" %% "drt-lib" % drtLib,
)

lazy val root = (project in file("."))
  .settings(
    name := "drt-analytics",
    trapExit := false,

    resolvers += "Artifactory Realm" at "https://artifactory.digital.homeoffice.gov.uk/",
    resolvers += "Artifactory Realm release local" at "https://artifactory.digital.homeoffice.gov.uk/artifactory/libs-release/",
    credentials += Credentials(Path.userHome / ".ivy2" / ".credentials"),

    Global / concurrentRestrictions += Tags.limit(Tags.Test, 1)

  )
  .enablePlugins(DockerPlugin)
  .enablePlugins(AshScriptPlugin)
