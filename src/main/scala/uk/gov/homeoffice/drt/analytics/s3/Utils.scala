package uk.gov.homeoffice.drt.analytics.s3

import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.core.async.AsyncRequestBody
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model.{PutObjectRequest, PutObjectResponse}

import scala.concurrent.Future
import scala.jdk.FutureConverters.CompletionStageOps


object Utils {
  def s3AsyncClient(accessKeyId: String, secretKeyId: String): S3AsyncClient = {
    val credentials: AwsBasicCredentials = AwsBasicCredentials.create(accessKeyId, secretKeyId)

    val staticCreds = StaticCredentialsProvider.create(credentials)

    S3AsyncClient.builder()
      .credentialsProvider(staticCreds)
      .build()
  }

  def writeToBucket(client: S3AsyncClient, bucketName: String): (String, String) => Future[PutObjectResponse] =
    (fileName: String, content: String) => {
      val putObjectRequest = PutObjectRequest.builder()
        .bucket(bucketName)
        .key(fileName)
        .build()

      val asyncRequestBody = AsyncRequestBody.fromString(content)

      client.putObject(putObjectRequest, asyncRequestBody).asScala
    }
}
