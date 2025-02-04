
ThisBuild / scalaVersion := "2.13.15"
ThisBuild / version := "v" + sys.env.getOrElse("DRONE_BUILD_NUMBER", sys.env.getOrElse("BUILD_ID", "DEV"))
ThisBuild / organization := "uk.gov.homeoffice"
ThisBuild / organizationName := "drt"

lazy val drtLib = "v1072"

lazy val akkaVersion = "2.9.5" // last version with license key requirement
lazy val akkaHttpVersion = "10.6.3" // last version dependent on akka 2.9.5
lazy val akkaPersistenceJdbcVersion = "5.4.2"

lazy val postgresVersion = "42.7.5"
lazy val jodaTimeVersion = "2.13.1"
lazy val jacksonDatabindVersion = "2.16.1"
lazy val specs2Version = "4.20.9"
lazy val sparkVersion = "3.5.4"
lazy val scalaTestVersion = "3.2.19"
lazy val catsVersion = "2.10.0"
lazy val awsJava2SdkVersion = "2.30.2"
lazy val sslConfigCoreVersion = "0.6.1"
lazy val scalaXmlVersion = "2.2.0"
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

  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.scala-lang.modules" %% "scala-xml" % scalaXmlVersion,
  "com.typesafe.akka" %% "akka-http" % akkaHttpVersion,
  "com.typesafe.akka" %% "akka-persistence" % akkaVersion,
  "com.typesafe.akka" %% "akka-persistence-query" % akkaVersion,
  "com.typesafe.akka" %% "akka-stream" % akkaVersion,
  "com.typesafe.akka" %% "akka-pki" % akkaVersion,
  "com.lightbend.akka" %% "akka-persistence-jdbc" % akkaPersistenceJdbcVersion,
  "org.postgresql" % "postgresql" % postgresVersion,
  "joda-time" % "joda-time" % jodaTimeVersion,
  "uk.gov.homeoffice" %% "drt-lib" % drtLib,
  "org.typelevel" %% "cats-core" % catsVersion,
  "software.amazon.awssdk" % "s3" % awsJava2SdkVersion,
  "com.typesafe" %% "ssl-config-core" % sslConfigCoreVersion,

  "org.scalatest" %% "scalatest" % scalaTestVersion % Test,
  "org.specs2" %% "specs2-core" % specs2Version % Test,
  "com.typesafe.akka" %% "akka-testkit" % akkaVersion % Test,
  "com.typesafe.akka" %% "akka-stream-testkit" % akkaVersion % Test,
  "com.typesafe.akka" %% "akka-persistence-testkit" % akkaVersion % Test,
)

lazy val root = (project in file("."))
  .settings(
    name := "drt-analytics",
    trapExit := false,

    resolvers ++= Seq(
      "Akka library repository".at("https://repo.akka.io/maven"),
      "Artifactory Realm libs release" at "https://artifactory.digital.homeoffice.gov.uk/artifactory/libs-release/",
    ),

    credentials += Credentials(Path.userHome / ".ivy2" / ".credentials"),

    dockerBaseImage := "openjdk:11-jre-slim-buster",

    Global / concurrentRestrictions += Tags.limit(Tags.Test, 1)

  )
  .enablePlugins(DockerPlugin)
  .enablePlugins(JavaAppPackaging)

assembly / assemblyMergeStrategy := {
  case PathList("META-INF", "MANIFEST.MF") =>
    val log = sLog.value
    log.info("discarding MANIFEST.MF")
    MergeStrategy.discard
  case PathList("reference.conf") =>
    val log = sLog.value
    log.info("concatinating reference.conf")
    MergeStrategy.concat
  case PathList("version.conf") =>
    val log = sLog.value
    log.info("concatinating version.conf")
    MergeStrategy.concat
  case default =>
    val log = sLog.value
    log.debug(s"keeping last $default")
    MergeStrategy.last
}
