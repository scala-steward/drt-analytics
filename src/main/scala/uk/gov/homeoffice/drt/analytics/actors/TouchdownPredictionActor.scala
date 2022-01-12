package uk.gov.homeoffice.drt.analytics.actors

import org.apache.spark.ml.regression.LinearRegressionModel
import org.slf4j.{Logger, LoggerFactory}
import scalapb.GeneratedMessage
import server.protobuf.messages.ModelAndFeatures.ModelAndFeaturesMessage
import uk.gov.homeoffice.drt.analytics.actors.TouchdownPredictionActor.ModelAndFeatures
import uk.gov.homeoffice.drt.analytics.prediction.Features
import uk.gov.homeoffice.drt.analytics.time.SDate

object TouchdownPredictionActor {
  case class RegressionModel(coefficients: Iterable[Double], intercept: Double)

  object RegressionModel {
    def apply(lrModel: LinearRegressionModel): RegressionModel = RegressionModel(lrModel.coefficients.toArray, lrModel.intercept)
  }

  trait ModelAndFeatures {
    val model: RegressionModel
    val features: Features
    val examplesTrainedOn: Int
    val improvementPct: Int
  }

  object ModelAndFeatures {
    def apply(model: RegressionModel, features: Features, targetName: String, examplesTrainedOn: Int, improvement: Int): ModelAndFeatures = targetName match {
      case TouchdownModelAndFeatures.targetName => TouchdownModelAndFeatures(model, features, examplesTrainedOn, improvement)
    }
  }

  object TouchdownModelAndFeatures {
    val targetName: String = "touchdown"
  }

  case class TouchdownModelAndFeatures(model: RegressionModel, features: Features, examplesTrainedOn: Int, improvementPct: Int) extends ModelAndFeatures {
    //    def maybePrediction(arrival: Arrival): Option[Long] = {
    //      val dow = s"dow_${SDate(arrival.scheduled).getDayOfWeek()}"
    //      val hhmm = s"hhmm_${SDate(arrival.scheduled).getHours() / 12}"
    //      val dowIdx = features.oneToManyValues.indexOf(dow)
    //      val hhmmIdx = features.oneToManyValues.indexOf(hhmm)
    //      for {
    //        dowCo <- model.coefficients.toIndexedSeq.lift(dowIdx)
    //        hhmmCo <- model.coefficients.toIndexedSeq.lift(hhmmIdx)
    //      } yield {
    //        val offScheduled = (model.intercept + dowCo + hhmmCo).toInt
    //        arrival.Scheduled + (offScheduled * MilliTimes.oneMinuteMillis)
    //      }
    //    }
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
