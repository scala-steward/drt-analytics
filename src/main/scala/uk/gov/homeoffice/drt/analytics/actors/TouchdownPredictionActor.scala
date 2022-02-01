package uk.gov.homeoffice.drt.analytics.actors

import org.apache.spark.ml.regression.LinearRegressionModel
import org.slf4j.{Logger, LoggerFactory}
import scalapb.GeneratedMessage
import server.protobuf.messages.ModelAndFeatures.ModelAndFeaturesMessage
import uk.gov.homeoffice.drt.analytics.time.SDate
import uk.gov.homeoffice.drt.prediction.{ModelAndFeatures, RegressionModel}

object TouchdownPredictionActor {
  object RegressionModelFromSpark {
    def apply(lrModel: LinearRegressionModel): RegressionModel = RegressionModel(lrModel.coefficients.toArray, lrModel.intercept)
  }
}

class TouchdownPredictionActor(val now: () => SDate,
                               terminal: String,
                               number: Int,
                               origin: String
                              ) extends RecoveryActorLike {

  import ModelAndFeaturesConversion._

  override val log: Logger = LoggerFactory.getLogger(getClass)

  override val recoveryStartMillis: Long = now().millisSinceEpoch

  var state: Option[ModelAndFeatures] = None

  private val uniqueId = s"$terminal-$number-$origin"

  override def persistenceId: String = s"touchdown-prediction-$uniqueId".toLowerCase

  override def processRecoveryMessage: PartialFunction[Any, Unit] = {
    case msg: ModelAndFeaturesMessage =>
      log.info(s"recovering state from ModelAndFeaturesMessage")
      state = Option(modelAndFeaturesFromMessage(msg))
  }

  override def processSnapshotMessage: PartialFunction[Any, Unit] = {
    case msg: ModelAndFeaturesMessage =>
      state = Option(modelAndFeaturesFromMessage(msg))
  }

  override def stateToMessage: GeneratedMessage = {
    val modelAndFeatures = state.getOrElse(throw new Exception("No state to convert to serialise to message"))
    modelAndFeaturesToMessage(modelAndFeatures, now().millisSinceEpoch)
  }

  override def receiveCommand: Receive = {
    case maf: ModelAndFeatures =>
      log.info(s"Received model and features for $uniqueId")
      state = Option(maf)
      val replyToAndAck = Option(sender(), Ack)
      persistAndMaybeSnapshot(modelAndFeaturesToMessage(maf, now().millisSinceEpoch), replyToAndAck)
  }
}
