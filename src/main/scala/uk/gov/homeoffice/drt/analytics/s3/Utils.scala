package uk.gov.homeoffice.drt.analytics.s3

import akka.Done
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.core.async.AsyncRequestBody
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model.PutObjectRequest

import java.io.{File, FileWriter}
import java.nio.file.{Files, Paths}
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.FutureConverters.CompletionStageOps


object Utils {
  def s3AsyncClient(accessKeyId: String, secretKeyId: String): S3AsyncClient = {
    val credentials: AwsBasicCredentials = AwsBasicCredentials.create(accessKeyId, secretKeyId)

    val staticCreds = StaticCredentialsProvider.create(credentials)

    S3AsyncClient.builder()
      .credentialsProvider(staticCreds)
      .build()
  }

  def writeToBucket(client: S3AsyncClient, bucketName: String, path: String)
                   (implicit ec: ExecutionContext): (String, String) => Future[Done] =
    (fileName: String, content: String) => {
      val putObjectRequest = PutObjectRequest.builder()
        .bucket(bucketName)
        .key(s"$path/$fileName")
        .build()

      val asyncRequestBody = AsyncRequestBody.fromString(content)

      client.putObject(putObjectRequest, asyncRequestBody).asScala.map(_ => Done)
    }

  def writeToFile(pathStr: String): (String, String) => Future[Done] =
  (fileName, csvContent) => {
    Files.createDirectories(Paths.get(pathStr))
    val fileWriter = new FileWriter(new File(s"$pathStr/$fileName"))
    fileWriter.write(csvContent)
    fileWriter.close()
    Future.successful(Done)
  }
}
