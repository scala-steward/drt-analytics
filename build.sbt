import net.nmoncho.sbt.dependencycheck.settings.{AnalyzerSettings, NvdApiSettings}

ThisBuild / scalaVersion := "2.13.18"
ThisBuild / version := "v" + sys.env.getOrElse("DRONE_BUILD_NUMBER", sys.env.getOrElse("BUILD_ID", "DEV"))
ThisBuild / organization := "uk.gov.homeoffice"
ThisBuild / organizationName := "drt"

lazy val drtLib = "v1397"


lazy val pekkoVersion = "1.4.0"
lazy val pekkoHttpVersion = "1.3.0"
lazy val pekkoPersistenceJdbcVersion = "1.2.0"

lazy val postgresVersion = "42.7.8"
lazy val jodaTimeVersion = "2.14.0"
lazy val specs2Version = "4.23.0"
lazy val sparkVersion = "4.1.1"
lazy val scalaTestVersion = "3.2.19"
lazy val catsVersion = "2.13.0"
lazy val awsJava2SdkVersion = "2.30.38"
lazy val sslConfigCoreVersion = "0.7.1"
lazy val scalaXmlVersion = "2.4.0"
lazy val logbackClassicVersion = "1.5.24"
lazy val logbackJsonClassicVersion = "0.1.5"
lazy val logbackJacksonVersion = "0.1.5"

libraryDependencies ++= Seq(
  "org.apache.pekko" %% "pekko-slf4j" % pekkoVersion,

  "ch.qos.logback" % "logback-classic" % logbackClassicVersion,
  "ch.qos.logback.contrib" % "logback-json-classic" % logbackJsonClassicVersion,
  "ch.qos.logback.contrib" % "logback-jackson" % logbackJacksonVersion,

  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.scala-lang.modules" %% "scala-xml" % scalaXmlVersion,
  "org.apache.pekko" %% "pekko-http" % pekkoHttpVersion,
  "org.apache.pekko" %% "pekko-persistence" % pekkoVersion,
  "org.apache.pekko" %% "pekko-persistence-query" % pekkoVersion,
  "org.apache.pekko" %% "pekko-stream" % pekkoVersion,
  "org.apache.pekko" %% "pekko-pki" % pekkoVersion,
  "org.apache.pekko" %% "pekko-persistence-jdbc" % pekkoPersistenceJdbcVersion,
  "org.postgresql" % "postgresql" % postgresVersion,
  "joda-time" % "joda-time" % jodaTimeVersion,
  "uk.gov.homeoffice" %% "drt-lib" % drtLib,
  "org.typelevel" %% "cats-core" % catsVersion,
  "software.amazon.awssdk" % "s3" % awsJava2SdkVersion,
  "com.typesafe" %% "ssl-config-core" % sslConfigCoreVersion,
  "dev.ludovic.netlib" % "blas" % "3.0.3",
  "dev.ludovic.netlib" % "lapack" % "3.0.3",
  "dev.ludovic.netlib" % "arpack" % "3.0.3",

  "org.scalatest" %% "scalatest" % scalaTestVersion % Test,
  "org.specs2" %% "specs2-core" % specs2Version % Test,
  "org.apache.pekko" %% "pekko-testkit" % pekkoVersion % Test,
  "org.apache.pekko" %% "pekko-stream-testkit" % pekkoVersion % Test,
  "org.apache.pekko" %% "pekko-persistence-testkit" % pekkoVersion % Test,
)

lazy val root = (project in file("."))
  .settings(
    name := "drt-analytics",
    trapExit := false,

    resolvers ++= Seq(
      "Artifactory Realm libs release" at "https://artifactory.digital.homeoffice.gov.uk/artifactory/libs-release/",
    ),

    credentials += Credentials(Path.userHome / ".ivy2" / ".credentials"),

    dockerBaseImage := "openjdk:11-jre-slim-buster",

    Global / concurrentRestrictions += Tags.limit(Tags.Test, 1)

  )
  .enablePlugins(DockerPlugin)
  .enablePlugins(JavaAppPackaging)
  .settings(SbtUpdatesSettings.sbtUpdatesSettings *)

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

val nvdAPIKey = sys.env.getOrElse("NVD_API_KEY", "")

dependencyCheckNvdApi := NvdApiSettings(apiKey = nvdAPIKey)

ThisBuild / dependencyCheckAnalyzers := dependencyCheckAnalyzers.value.copy(
  ossIndex = AnalyzerSettings.OssIndex(
    enabled = Some(false),
    url = None,
    batchSize = None,
    requestDelay = None,
    useCache = None,
    warnOnlyOnRemoteErrors = None,
    username = None,
    password = None
  )
)
