package uk.gov.homeoffice.drt.analytics.prediction.dump

import akka.Done
import org.apache.spark.ml.regression.LinearRegressionModel
import org.apache.spark.sql.SparkSession
import uk.gov.homeoffice.drt.analytics.prediction.DataSet

import scala.concurrent.Future

trait ModelPredictionsDump {
  def dumpDailyStats(dataSet: DataSet,
                     withIndex: Iterable[(Double, Seq[String], Seq[Double], String)],
                     model: LinearRegressionModel,
                     port: String,
                     terminal: String,
                    )
                    (implicit sparkSession: SparkSession): Future[Done]
}
