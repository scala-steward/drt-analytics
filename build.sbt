
ThisBuild / scalaVersion := "2.13.12"
ThisBuild / version := "v" + sys.env.getOrElse("DRONE_BUILD_NUMBER", sys.env.getOrElse("BUILD_ID", "DEV"))
ThisBuild / organization := "uk.gov.homeoffice"
ThisBuild / organizationName := "drt"

lazy val drtLib = "v759"

lazy val akkaHttpVersion = "10.5.3"
lazy val akkaVersion = "2.8.5"
lazy val akkaPersistenceJdbcVersion = "5.2.0"
lazy val postgresVersion = "42.7.0"
lazy val jodaTimeVersion = "2.12.5"
lazy val jacksonDatabindVersion = "2.15.3"
lazy val specs2Version = "4.20.3"
lazy val sparkVersion = "3.5.0"
lazy val scalaTestVersion = "3.2.17"
lazy val catsVersion = "2.10.0"
lazy val awsJava2SdkVersion = "2.13.76"
lazy val sslConfigCoreVersion = "0.6.1"
lazy val scalaXmlVersion = "2.2.0"
lazy val log4jLayoutTemplateJsonVersion = "2.22.1"
lazy val logbackClassicVersion = "1.4.14"
lazy val logbackJsonClassicVersion = "0.1.5"
lazy val logbackJacksonVersion = "0.1.5"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-slf4j" % akkaVersion,

  "ch.qos.logback" % "logback-classic" % logbackClassicVersion,
  "ch.qos.logback.contrib" % "logback-json-classic" % logbackJsonClassicVersion,
  "ch.qos.logback.contrib" % "logback-jackson" % logbackJacksonVersion,

  "com.fasterxml.jackson.core" % "jackson-databind" % jacksonDatabindVersion,
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % jacksonDatabindVersion,
  "org.scalatest" %% "scalatest" % scalaTestVersion % Test,

  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.scala-lang.modules" %% "scala-xml" % scalaXmlVersion,
  "com.typesafe.akka" %% "akka-http" % akkaHttpVersion,
  "com.typesafe.akka" %% "akka-testkit" % akkaVersion % Test,
  "com.typesafe.akka" %% "akka-stream-testkit" % akkaVersion % Test,
  "com.typesafe.akka" %% "akka-persistence" % akkaVersion,
  "com.typesafe.akka" %% "akka-persistence-query" % akkaVersion,
  "com.typesafe.akka" %% "akka-persistence-testkit" % akkaVersion % Test,
  "com.lightbend.akka" %% "akka-persistence-jdbc" % akkaPersistenceJdbcVersion,
  "org.postgresql" % "postgresql" % postgresVersion,
  "com.typesafe.akka" %% "akka-stream" % akkaVersion,
  "joda-time" % "joda-time" % jodaTimeVersion,
  "org.specs2" %% "specs2-core" % specs2Version % Test,
  "uk.gov.homeoffice" %% "drt-lib" % drtLib,
  "org.typelevel" %% "cats-core" % catsVersion,
  "software.amazon.awssdk" % "s3" % awsJava2SdkVersion,
  "com.typesafe" %% "ssl-config-core" % sslConfigCoreVersion,
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
