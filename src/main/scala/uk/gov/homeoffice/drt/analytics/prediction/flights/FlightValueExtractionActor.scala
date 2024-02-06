package uk.gov.homeoffice.drt.analytics.prediction.flights

import akka.actor.ActorRef
import akka.persistence.{PersistentActor, RecoveryCompleted, SnapshotOffer}
import org.slf4j.{Logger, LoggerFactory}
import uk.gov.homeoffice.drt.actor.PredictionModelActor.WithId
import uk.gov.homeoffice.drt.actor.TerminalDateActor
import uk.gov.homeoffice.drt.actor.TerminalDateActor.ArrivalKey
import uk.gov.homeoffice.drt.actor.commands.Commands.GetState
import uk.gov.homeoffice.drt.analytics.prediction.flights.FlightMessageConversions.arrivalKeyFromMessage
import uk.gov.homeoffice.drt.analytics.prediction.flights.FlightValueExtractionActor.{PreProcessingFinished, noCtaOrDomestic}
import uk.gov.homeoffice.drt.arrivals.Arrival
import uk.gov.homeoffice.drt.ports.PortCode
import uk.gov.homeoffice.drt.ports.Ports.isDomesticOrCta
import uk.gov.homeoffice.drt.ports.Terminals.Terminal
import uk.gov.homeoffice.drt.protobuf.messages.CrunchState.{FlightWithSplitsMessage, FlightsWithSplitsDiffMessage, FlightsWithSplitsMessage}
import uk.gov.homeoffice.drt.protobuf.messages.FlightsMessage.UniqueArrivalMessage
import uk.gov.homeoffice.drt.protobuf.serialisation.FlightMessageConversion.flightWithSplitsFromMessage
import uk.gov.homeoffice.drt.time.{SDate, UtcDate}

import scala.collection.immutable
import scala.concurrent.{ExecutionContextExecutor, Future}


object FlightValueExtractionActor {
  case class PreProcessingFinished(byArrivalKey: Map[ArrivalKey, Arrival])

  val noCtaOrDomestic: FlightWithSplitsMessage => Boolean = msg => !isDomesticOrCta(PortCode(msg.getFlight.getOrigin))
}

object FlightMessageConversions {
  def arrivalKeyFromMessage(r: UniqueArrivalMessage): Option[ArrivalKey] =
    for {
      scheduled <- r.scheduled
      terminal <- r.terminalName
      flightNumber <- r.number
    } yield {
      ArrivalKey(scheduled, terminal, flightNumber)
    }

}

trait FlightValueExtractionActorLike {
  private val log = LoggerFactory.getLogger(getClass)

  var byArrivalKey: Map[ArrivalKey, Arrival] = Map()

  val extractValues: Arrival => Option[(Double, Seq[String], Seq[Double])]
  val extractKey: Arrival => Option[WithId]

  def processSnapshot(ss: Any): Unit = ss match {
    case msg: FlightsWithSplitsMessage => msg.flightWithSplits.filter(noCtaOrDomestic).foreach(processFlightsWithSplitsMessage)
    case unexpected => log.warn(s"Got unexpected snapshot offer message: ${unexpected.getClass}")
  }

  def processFlightsWithSplitsMessage(u: FlightWithSplitsMessage): Unit = {
    val arrival = flightWithSplitsFromMessage(u).apiFlight
    val key = ArrivalKey(arrival)
    byArrivalKey = byArrivalKey.updated(key, arrival)
  }

  def processRemovalMessage(r: UniqueArrivalMessage): Unit =
    arrivalKeyFromMessage(r).foreach(r => byArrivalKey = byArrivalKey - r)

  def processFlightsWithSplitsDiffMessage(msg: FlightsWithSplitsDiffMessage): Unit = {
    msg.updates.filter(noCtaOrDomestic).foreach(processFlightsWithSplitsMessage)
    msg.removals.foreach(processRemovalMessage)
  }

  def extractions(byArrivalKeyProcessed: Map[ArrivalKey, Arrival]): Map[WithId, immutable.Iterable[(Double, Seq[String], Seq[Double])]] =
    byArrivalKeyProcessed
      .groupBy {
        case (_, msg) => extractKey(msg)
      }
      .collect {
        case (Some(key), arrivals) =>
          val examples = arrivals
            .map { case (_, arrival) => extractValues(arrival) }
            .collect { case Some(value) => value }
          (key, examples)
      }
}

class FlightValueExtractionActor(val terminal: Terminal,
                                 val date: UtcDate,
                                 val extractValues: Arrival => Option[(Double, Seq[String], Seq[Double])],
                                 val extractKey: Arrival => Option[WithId],
                                 val preProcessing: (UtcDate, Map[ArrivalKey, Arrival]) => Future[Map[ArrivalKey, Arrival]],
                                ) extends TerminalDateActor[Arrival] with PersistentActor with FlightValueExtractionActorLike {
  private val log: Logger = LoggerFactory.getLogger(getClass)

  override def persistenceId: String = f"terminal-flights-${terminal.toString.toLowerCase}-${date.year}-${date.month}%02d-${date.day}%02d"

  implicit val ec: ExecutionContextExecutor = context.dispatcher

  var valuesWithFeaturesByExtractedKey: Map[WithId, Iterable[(Double, Seq[String], Seq[Double])]] = Map()
  var preProcessingFinished = false
  var stateRequests: List[ActorRef] = List()

  override def receiveRecover: Receive = {
    case SnapshotOffer(_, ss) => processSnapshot(ss)

    case RecoveryCompleted =>
      preProcessing(date, byArrivalKey)
        .map(self ! PreProcessingFinished(_))
        .recover(e => log.error(s"Failed to pre-process flights for $persistenceId", e))

    case msg: FlightsWithSplitsDiffMessage =>
      processFlightsWithSplitsDiffMessage(msg)
  }

  override def receiveCommand: Receive = {
    case PreProcessingFinished(byArrivalKeyProcessed) =>
      valuesWithFeaturesByExtractedKey = extractions(byArrivalKeyProcessed)
      if (stateRequests.nonEmpty) {
        stateRequests.foreach(_ ! valuesWithFeaturesByExtractedKey)
        stateRequests = List()
      }
      preProcessingFinished = true

    case GetState =>
      if (preProcessingFinished) {
        sender() ! valuesWithFeaturesByExtractedKey
      } else {
        stateRequests = sender() :: stateRequests
      }
  }
}
