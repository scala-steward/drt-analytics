package uk.gov.homeoffice.drt.analytics.actors

import org.apache.spark.ml.regression.LinearRegressionModel
import org.slf4j.{Logger, LoggerFactory}
import scalapb.GeneratedMessage
import uk.gov.homeoffice.drt.analytics.actors.TouchdownPredictionActor.ModelAndFeatures
import uk.gov.homeoffice.drt.analytics.prediction.Features
import uk.gov.homeoffice.drt.analytics.time.SDate

object TouchdownPredictionActor {
  case class RegressionModel(coefficients: Iterable[Double], intercept: Double)

  object RegressionModel {
    def apply(lrModel: LinearRegressionModel): RegressionModel = RegressionModel(lrModel.coefficients.toArray, lrModel.intercept)
  }

  case class ModelAndFeatures(model: RegressionModel, features: Features)
}

class TouchdownPredictionActor(val now: () => SDate,
                               terminal: String,
                               number: Int,
                               origin: String
                              ) extends RecoveryActorLike {
  override val log: Logger = LoggerFactory.getLogger(getClass)

  override val recoveryStartMillis: Long = now().millisSinceEpoch

  var state: Option[ModelAndFeatures] = None

  override def persistenceId: String = s"touchdown-prediction-$terminal-$number-$origin"

  override def processRecoveryMessage: PartialFunction[Any, Unit] = {
    case _ =>
  }

  override def processSnapshotMessage: PartialFunction[Any, Unit] = {
    case _ =>
  }

  override def stateToMessage: GeneratedMessage = {
    val modelAndFeatures = state.getOrElse(throw new Exception("No state to convert to serialise to message"))
    ModelAndFeaturesConversion.modelAndFeaturesToMessage(modelAndFeatures, now().millisSinceEpoch)
  }

  override def receiveCommand: Receive = {
    case maf: ModelAndFeatures =>
      persistAndMaybeSnapshot(ModelAndFeaturesConversion.modelAndFeaturesToMessage(maf, now().millisSinceEpoch))
  }
}
