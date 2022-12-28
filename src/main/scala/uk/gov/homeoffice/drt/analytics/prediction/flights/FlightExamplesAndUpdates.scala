package uk.gov.homeoffice.drt.analytics.prediction.flights

import akka.NotUsed
import akka.actor.{ActorSystem, PoisonPill, Props}
import akka.pattern.ask
import akka.stream.scaladsl.Source
import akka.util.Timeout
import uk.gov.homeoffice.drt.actor.PredictionModelActor
import uk.gov.homeoffice.drt.actor.PredictionModelActor.{ModelUpdate, RemoveModel}
import uk.gov.homeoffice.drt.actor.TerminalDateActor.FlightRoute
import uk.gov.homeoffice.drt.ports.Terminals.Terminal
import uk.gov.homeoffice.drt.prediction.ExamplesAndUpdates
import uk.gov.homeoffice.drt.protobuf.messages.CrunchState.FlightWithSplitsMessage
import uk.gov.homeoffice.drt.time.{SDate, SDateLike}

import scala.concurrent.{ExecutionContext, Future}

case class FlightExamplesAndUpdates(extractValues: FlightWithSplitsMessage => Option[(Double, Seq[String])], targetName: String)(
  implicit ec: ExecutionContext, to: Timeout, system: ActorSystem
) extends ExamplesAndUpdates[FlightRoute] {
  override val persistenceType: String = "flight"
  override val modelIdWithExamples: (Terminal, SDateLike, Int) => Source[(FlightRoute, Iterable[(Double, Seq[String])]), NotUsed] =
    (terminal, start, daysOfData) => {
      println(s"Getting flight examples for $terminal, $start, $daysOfData")
      FlightRoutesValuesExtractor(classOf[FlightValueExtractionActor], extractValues)
        .extractedValueByFlightRoute(terminal, start, daysOfData)
    }
  override val updateModel: (FlightRoute, Option[ModelUpdate]) => Future[_] =
    (identifier, maybeModelUpdate) => {
      val actor = system.actorOf(Props(new PredictionModelActor(() => SDate.now(), persistenceType, identifier)))
      val msg = maybeModelUpdate match {
        case Some(modelUpdate) => modelUpdate
        case None => RemoveModel(targetName)
      }
      actor.ask(msg).map(_ => actor ! PoisonPill)
    }
}
