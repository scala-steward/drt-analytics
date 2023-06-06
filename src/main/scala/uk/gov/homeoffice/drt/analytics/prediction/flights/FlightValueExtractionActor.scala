package uk.gov.homeoffice.drt.analytics.prediction.flights

import akka.actor.ActorRef
import akka.persistence.{PersistentActor, RecoveryCompleted, SnapshotOffer}
import org.slf4j.LoggerFactory
import uk.gov.homeoffice.drt.actor.PredictionModelActor.WithId
import uk.gov.homeoffice.drt.actor.TerminalDateActor
import uk.gov.homeoffice.drt.actor.TerminalDateActor.{ArrivalKey, GetState}
import uk.gov.homeoffice.drt.analytics.prediction.flights.FlightValueExtractionActor.PreProcessingFinished
import uk.gov.homeoffice.drt.arrivals.Arrival
import uk.gov.homeoffice.drt.ports.PortCode
import uk.gov.homeoffice.drt.ports.Ports.isDomesticOrCta
import uk.gov.homeoffice.drt.ports.Terminals.Terminal
import uk.gov.homeoffice.drt.protobuf.messages.CrunchState.{FlightWithSplitsMessage, FlightsWithSplitsDiffMessage, FlightsWithSplitsMessage}
import uk.gov.homeoffice.drt.protobuf.messages.FlightsMessage.UniqueArrivalMessage
import uk.gov.homeoffice.drt.protobuf.serialisation.FlightMessageConversion.flightWithSplitsFromMessage
import uk.gov.homeoffice.drt.time.UtcDate

import scala.collection.immutable
import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.util.Try


object FlightValueExtractionActor {
  case class PreProcessingFinished(byArrivalKey: Map[ArrivalKey, Arrival])
}

class FlightValueExtractionActor(val terminal: Terminal,
                                 val date: UtcDate,
                                 val extractValues: Arrival => Option[(Double, Seq[String], Seq[Double])],
                                 val extractKey: Arrival => Option[WithId],
                                 val preProcessing: (UtcDate, Map[ArrivalKey, Arrival]) => Future[Map[ArrivalKey, Arrival]],
                                ) extends TerminalDateActor[Arrival] with PersistentActor {
  private val log = LoggerFactory.getLogger(getClass)

  override def persistenceId: String = f"terminal-flights-${terminal.toString.toLowerCase}-${date.year}-${date.month}%02d-${date.day}%02d"

  implicit val ec: ExecutionContextExecutor = context.dispatcher

  var byArrivalKey: Map[ArrivalKey, Arrival] = Map()
  var valuesWithFeaturesByExtractedKey: Map[WithId, Iterable[(Double, Seq[String], Seq[Double])]] = Map()
  var preProcessingFinished = false
  var stateRequests: List[ActorRef] = List()

  private def parseFlightNumber(code: String): Option[Int] = {
    code match {
      case Arrival.flightCodeRegex(_, flightNumber, _) => Try(flightNumber.toInt).toOption
      case _ => None
    }
  }

  val noCtaOrDomestic: FlightWithSplitsMessage => Boolean = msg => !isDomesticOrCta(PortCode(msg.getFlight.origin.get))

  override def receiveRecover: Receive = {
    case SnapshotOffer(_, ss) =>
      ss match {
        case msg: FlightsWithSplitsMessage => msg.flightWithSplits.filter(noCtaOrDomestic).foreach(processFlightsWithSplitsMessage)
        case unexpected => log.warn(s"Got unexpected snapshot offer message: ${unexpected.getClass}")
      }

    case RecoveryCompleted =>
      preProcessing(date, byArrivalKey).map(self ! PreProcessingFinished(_))

    case FlightsWithSplitsDiffMessage(_, removals, updates) =>
      updates.filter(noCtaOrDomestic).foreach(processFlightsWithSplitsMessage)
      removals.foreach(processRemovalMessage)
  }

  private def extractions(byArrivalKeyProcessed: Map[ArrivalKey, Arrival]): Map[WithId, immutable.Iterable[(Double, Seq[String], Seq[Double])]] = {
    byArrivalKeyProcessed
      .groupBy {
        case (_, msg) => extractKey(msg)
      }
      .collect {
        case (Some(key), flightMessages) =>
          val examples = flightMessages
            .map { case (_, msg) => extractValues(msg) }
            .collect { case Some(value) => value }
          (key, examples)
      }
  }

  private def processRemovalMessage(r: UniqueArrivalMessage): Unit =
    for {
      scheduled <- r.scheduled
      terminal <- r.terminalName
      flightNumber <- r.number
    } yield {
      byArrivalKey = byArrivalKey - ArrivalKey(scheduled, terminal, flightNumber)
    }

  private def processFlightsWithSplitsMessage(u: FlightWithSplitsMessage): Unit =
    for {
      flightCode <- u.getFlight.iATA
      flightNumber <- parseFlightNumber(flightCode)
      terminal <- u.getFlight.terminal
      scheduled <- u.getFlight.scheduled
    } yield {
      byArrivalKey = byArrivalKey.updated(ArrivalKey(scheduled, terminal, flightNumber), flightWithSplitsFromMessage(u).apiFlight)
    }

  override def receiveCommand: Receive = {
    case PreProcessingFinished(byArrivalKeyProcessed) =>
      valuesWithFeaturesByExtractedKey = extractions(byArrivalKeyProcessed)
      if (stateRequests.nonEmpty) {
        stateRequests.foreach(_ ! valuesWithFeaturesByExtractedKey)
        stateRequests = List()
      }

    case GetState =>
      if (preProcessingFinished) {
        sender() ! valuesWithFeaturesByExtractedKey
      } else {
        stateRequests = sender() :: stateRequests
      }
  }
}
