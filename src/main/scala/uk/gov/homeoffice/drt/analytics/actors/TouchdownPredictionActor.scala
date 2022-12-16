package uk.gov.homeoffice.drt.analytics.actors

import akka.NotUsed
import akka.actor.{ActorSystem, PoisonPill, Props}
import akka.pattern.ask
import akka.stream.scaladsl.Source
import akka.util.Timeout
import org.apache.spark.ml.regression.LinearRegressionModel
import org.slf4j.{Logger, LoggerFactory}
import scalapb.GeneratedMessage
import uk.gov.homeoffice.drt.analytics.AnalyticsApp.system
import uk.gov.homeoffice.drt.analytics.actors.TerminalDateActor.FlightRoute
import uk.gov.homeoffice.drt.analytics.prediction.{FlightsMessageValueExtractor, ValuesExtractorForFlightRoutes}
import uk.gov.homeoffice.drt.analytics.time.SDate
import uk.gov.homeoffice.drt.ports.Terminals.Terminal
import uk.gov.homeoffice.drt.prediction.{FeaturesWithOneToManyValues, ModelAndFeatures, RegressionModel, TouchdownModelAndFeatures}
import uk.gov.homeoffice.drt.protobuf.messages.ModelAndFeatures.{ModelAndFeaturesMessage, RemoveModelMessage}

import scala.concurrent.{ExecutionContext, Future}

case class ModelUpdate(rm: RegressionModel, fts: FeaturesWithOneToManyValues, examples: Int, impPct: Double)

case object RemoveModel

trait ExamplesAndUpdates[MI] {
  val modelIdWithExamples: (Terminal, SDate, Int) => Source[(MI, Map[Long, Double]), NotUsed]
  val updateModel: (MI, Option[ModelUpdate]) => Future[_]
}

case class TouchdownExamplesAndUpdates()(
  implicit ec: ExecutionContext, to: Timeout, system: ActorSystem
) extends ExamplesAndUpdates[FlightRoute] {
  val modelIdWithExamples: (Terminal, SDate, Int) => Source[(FlightRoute, Map[Long, Double]), NotUsed] =
    (terminal, start, daysOfData) => ValuesExtractorForFlightRoutes(classOf[FlightValueExtractionActor], FlightsMessageValueExtractor.offScheduledMinutes)
      .extractedValueByFlightRoute(terminal, start, daysOfData)
  val updateModel: (FlightRoute, Option[ModelUpdate]) => Future[_] =
    (mi, maybeModelUpdate) => {
      val actor = system.actorOf(Props(new TouchdownPredictionActor(() => SDate.now(), mi)))
      val msg = maybeModelUpdate match {
        case Some(modelUpdate) => modelUpdate
        case None => RemoveModel
      }
      actor.ask(msg).map(_ => actor ! PoisonPill)
    }
}

object TouchdownPredictionActor {
  object RegressionModelFromSpark {
    def apply(lrModel: LinearRegressionModel): RegressionModel = RegressionModel(lrModel.coefficients.toArray, lrModel.intercept)
  }
}

class TouchdownPredictionActor(val now: () => SDate, flightRoute: FlightRoute) extends RecoveryActorLike {

  import ModelAndFeaturesConversion._

  val terminal: String = flightRoute.terminal
  val number: Int = flightRoute.number
  val origin: String = flightRoute.origin

  override val log: Logger = LoggerFactory.getLogger(getClass)

  override val recoveryStartMillis: Long = now().millisSinceEpoch

  var state: Option[ModelAndFeatures] = None

  private val uniqueId = s"$terminal-$number-$origin"

  override def persistenceId: String = s"touchdown-prediction-$uniqueId".toLowerCase

  override def processRecoveryMessage: PartialFunction[Any, Unit] = {
    case msg: ModelAndFeaturesMessage =>
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
    case ModelUpdate(rm, fts, examples, impPct) =>
      val maf = TouchdownModelAndFeatures(rm, fts, examples, impPct)
      val isUpdated = state match {
        case Some(existingMaf) => maf != existingMaf
        case None => true
      }
      if (isUpdated) {
        state = Option(maf)
        val replyToAndAck = Option(sender(), Ack)
        persistAndMaybeSnapshot(modelAndFeaturesToMessage(maf, now().millisSinceEpoch), replyToAndAck)
      }

    case RemoveModel =>
      state = None
      val replyToAndAck = Option(sender(), Ack)
      persistAndMaybeSnapshot(RemoveModelMessage(Option(now().millisSinceEpoch)), replyToAndAck)
  }
}
