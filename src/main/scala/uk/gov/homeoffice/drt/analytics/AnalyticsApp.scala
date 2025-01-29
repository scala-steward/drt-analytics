package uk.gov.homeoffice.drt.analytics

import akka.Done
import akka.actor.ActorSystem
import akka.util.Timeout
import com.typesafe.config.{Config, ConfigFactory}
import org.slf4j.{Logger, LoggerFactory}
import uk.gov.homeoffice.drt.analytics.persistence.NoOpPersistence
import uk.gov.homeoffice.drt.analytics.s3.Utils
import uk.gov.homeoffice.drt.analytics.services.JobExecutor
import uk.gov.homeoffice.drt.ports.PortCode
import uk.gov.homeoffice.drt.ports.config.AirportConfigs
import uk.gov.homeoffice.drt.prediction.ModelPersistence
import uk.gov.homeoffice.drt.prediction.persistence.Flight
import uk.gov.homeoffice.drt.time.{SDate, SDateLike}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor, Future}
import scala.language.postfixOps
import scala.util.Try

object AnalyticsApp {
  private val log: Logger = LoggerFactory.getLogger(getClass)
  val config: Config = ConfigFactory.load()

  implicit val system: ActorSystem = ActorSystem("DrtAnalytics")
  implicit val ec: ExecutionContextExecutor = ExecutionContext.global
  implicit val timeout: Timeout = new Timeout(60 seconds)
  implicit val sdateProvider: Long => SDateLike = (ts: Long) => SDate(ts)

  private val portCode = PortCode(config.getString("port-code").toUpperCase)
  private val jobTimeout = config.getInt("options.job-timeout-minutes").minutes

  private val tryWriteToS3: Try[(String, String) => Future[Done]] = for {
    accessKeyId <- Try(config.getString("aws.access-key-id"))
    secretAccessKey <- Try(config.getString("aws.secret-access-key"))
    bucketName <- Try(config.getString("aws.s3.bucket"))
    path <- Try(config.getString("aws.s3.path"))
  } yield {
    log.info(s"S3 persistence of predictions enabled. Bucket name: $bucketName")
    val client = Utils.s3AsyncClient(accessKeyId, secretAccessKey)
    Utils.writeToBucket(client, bucketName, path)
  }

  private val writeToFileSystem = Try(config.getString("options.dump-predictions-file-path"))
    .map { filePath =>
      log.info(s"Writing predictions to file $filePath")
      Utils.writeToFile(filePath)
    }

  private val writePredictions = tryWriteToS3.toOption.toList ++ writeToFileSystem.toOption.toList

  def main(args: Array[String]): Unit = {
    AirportConfigs.confByPort.get(portCode) match {
      case None =>
        log.error(s"Invalid port code '$portCode'")
        system.terminate()
        System.exit(0)

      case Some(portConfig) =>
        log.info(s"Looking for job ${config.getString("options.job-name")}")
        val persistence: ModelPersistence = if (config.getBoolean("options.dry-run")) NoOpPersistence else Flight()
        val executor = JobExecutor(config, portCode, writePredictions, persistence)
        val jobName = config.getString("options.job-name").toLowerCase
        val eventualUpdates = executor.executeJob(portConfig, jobName)

        Await.ready(eventualUpdates, jobTimeout)
        system.terminate()
        System.exit(0)
    }
  }
}
