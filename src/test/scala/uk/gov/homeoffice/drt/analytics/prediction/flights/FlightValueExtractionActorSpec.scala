package uk.gov.homeoffice.drt.analytics.prediction.flights

import akka.actor.{ActorSystem, Props}
import akka.pattern.ask
import akka.testkit.TestKit
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import org.specs2.mutable.SpecificationLike
import org.specs2.specification.AfterAll
import uk.gov.homeoffice.drt.actor.TerminalDateActor.GetState
import uk.gov.homeoffice.drt.actor.WalkTimeProvider
import uk.gov.homeoffice.drt.analytics.prediction.FlightsMessageValueExtractor.walkTimeMinutes
import uk.gov.homeoffice.drt.ports.Terminals.T1
import uk.gov.homeoffice.drt.time.UtcDate

import scala.concurrent.Await
import scala.concurrent.duration.DurationInt

class FlightValueExtractionActorSpec extends TestKit(ActorSystem("passengers-actor")) with SpecificationLike with AfterAll {
  sequential

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  implicit val timeout: Timeout = new Timeout(60.second)

  val config = ConfigFactory.load()
  val provider = WalkTimeProvider(config.getString("options.walk-time-file-path"))
  println(s"Loaded ${provider.walkTimes.size} walk times from ${config.getString("options.walk-time-file-path")}")

  "I should be able to get data from a flight actor" >> {
    val actor = system.actorOf(Props(new FlightValueExtractionActor(T1, UtcDate(2022, 10, 2), walkTimeMinutes(provider))))
    Await.ready(actor.ask(GetState), 60.second)

    success
  }
}
