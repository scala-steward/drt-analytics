package uk.gov.homeoffice.drt.analytics.persistence

import akka.Done
import org.apache.spark.ml.regression.LinearRegressionModel
import org.slf4j.LoggerFactory
import uk.gov.homeoffice.drt.actor.PredictionModelActor
import uk.gov.homeoffice.drt.prediction.{FeaturesWithOneToManyValues, ModelPersistence}

import scala.concurrent.Future

object NoOpPersistence extends ModelPersistence {
  private val log = LoggerFactory.getLogger(getClass)

  override def getModels(validModelNames: Seq[String]): PredictionModelActor.WithId => Future[PredictionModelActor.Models] =
    _ => Future.successful(PredictionModelActor.Models(Map()))

  override val persist: (PredictionModelActor.WithId, Int, LinearRegressionModel, FeaturesWithOneToManyValues, Int, Double, String) => Future[Done] =
    (_, _, _, _, _, _, _) => {
      log.info(s"NoOpPersistence: not persisting model")
      Future.successful(Done)
    }
  override val clear: (PredictionModelActor.WithId, String) => Future[Done] =
    (_, _) => {
      log.info(s"NoOpPersistence: not clearing model")
      Future.successful(Done)
    }
}
