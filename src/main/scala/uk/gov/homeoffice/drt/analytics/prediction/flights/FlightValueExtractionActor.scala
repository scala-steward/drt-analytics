package uk.gov.homeoffice.drt.analytics.prediction.flights

import akka.actor.ActorRef
import akka.persistence.{PersistentActor, RecoveryCompleted, SnapshotOffer}
import org.slf4j.{Logger, LoggerFactory}
import uk.gov.homeoffice.drt.actor.PredictionModelActor.WithId
import uk.gov.homeoffice.drt.actor.commands.Commands.GetState
import uk.gov.homeoffice.drt.analytics.actors.TerminalDateActor
import uk.gov.homeoffice.drt.analytics.actors.TerminalDateActor.ArrivalKey
import uk.gov.homeoffice.drt.analytics.prediction.flights.FlightMessageConversions.arrivalKeyFromMessage
import uk.gov.homeoffice.drt.analytics.prediction.flights.FlightValueExtractionActor.{PreProcessingFinished, noCtaOrDomestic}
import uk.gov.homeoffice.drt.arrivals.Arrival
import uk.gov.homeoffice.drt.ports.PortCode
import uk.gov.homeoffice.drt.ports.Ports.isDomesticOrCta
import uk.gov.homeoffice.drt.ports.Terminals.Terminal
import uk.gov.homeoffice.drt.protobuf.messages.CrunchState.{FlightWithSplitsMessage, FlightsWithSplitsDiffMessage, FlightsWithSplitsMessage}
import uk.gov.homeoffice.drt.protobuf.messages.FlightsMessage.UniqueArrivalMessage
import uk.gov.homeoffice.drt.protobuf.serialisation.FlightMessageConversion.flightWithSplitsFromMessage
import uk.gov.homeoffice.drt.time.UtcDate

import scala.concurrent.{ExecutionContextExecutor, Future}

object FlightValueExtractionActor {
  case class PreProcessingFinished(arrivals: Iterable[Arrival])

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

  var byArrivalKey: Map[ArrivalKey, Arrival] = Map.empty[ArrivalKey, Arrival]

  val extractValues: Arrival => Option[(Double, Seq[String], Seq[Double], String)]
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

  def extractions(byArrivalKeyProcessed: Map[ArrivalKey, Arrival]): Map[WithId, Iterable[(Double, Seq[String], Seq[Double], String)]] =
    byArrivalKeyProcessed
      .groupBy {
        case (_, arrival) => extractKey(arrival)
      }
      .collect {
        case (Some(key), arrivals) =>
          val examples = arrivals.values
            .map(a => (a.unique, a))
            .map { case (_, arrival) => extractValues(arrival) }
            .collect { case Some(value) => value }
          (key, examples)
      }
}

class FlightValueExtractionActor(val terminal: Terminal,
                                 val date: UtcDate,
                                 val extractValues: Arrival => Option[(Double, Seq[String], Seq[Double], String)],
                                 val extractKey: Arrival => Option[WithId],
                                 val preProcessing: (UtcDate, Iterable[Arrival]) => Future[Iterable[Arrival]],
                                ) extends TerminalDateActor[Arrival] with PersistentActor with FlightValueExtractionActorLike {
  private val log: Logger = LoggerFactory.getLogger(getClass)

  override def persistenceId: String = f"terminal-flights-${terminal.toString.toLowerCase}-${date.year}-${date.month}%02d-${date.day}%02d"

  implicit val ec: ExecutionContextExecutor = context.dispatcher

  private var valuesWithFeaturesByExtractedKey: Map[WithId, Iterable[(Double, Seq[String], Seq[Double], String)]] = Map()
  private var preProcessingFinished = false
  private var stateRequests: List[ActorRef] = List()

  override def receiveRecover: Receive = {
    case SnapshotOffer(_, ss) => processSnapshot(ss)

    case RecoveryCompleted =>
      preProcessing(date, byArrivalKey.values)
        .map(self ! PreProcessingFinished(_))
        .recover(e => log.error(s"Failed to pre-process flights for $persistenceId", e))

    case msg: FlightsWithSplitsDiffMessage =>
      processFlightsWithSplitsDiffMessage(msg)
  }

  override def receiveCommand: Receive = {
    case PreProcessingFinished(arrivals) =>
      valuesWithFeaturesByExtractedKey = extractions(arrivals.map(a => (ArrivalKey(a), a)).toMap)
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
