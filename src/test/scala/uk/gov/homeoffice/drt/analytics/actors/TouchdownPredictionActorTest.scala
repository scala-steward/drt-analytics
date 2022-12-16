package uk.gov.homeoffice.drt.analytics.actors

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.testkit.{TestKit, TestProbe}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AnyWordSpecLike
import scalapb.GeneratedMessage
import uk.gov.homeoffice.drt.analytics.actors.TerminalDateActor.FlightRoute
import uk.gov.homeoffice.drt.protobuf.messages.ModelAndFeatures.ModelAndFeaturesMessage
import uk.gov.homeoffice.drt.analytics.time.SDate
import uk.gov.homeoffice.drt.prediction.Feature.{OneToMany, Single}
import uk.gov.homeoffice.drt.prediction.{FeaturesWithOneToManyValues, RegressionModel, TouchdownModelAndFeatures}

import scala.concurrent.duration.DurationInt

class MockTouchdownPredictionActor(probeRef: ActorRef) extends TouchdownPredictionActor(() => SDate.now(), FlightRoute("T1", 100, "JFK")) {
  override def persistAndMaybeSnapshot(messageToPersist: GeneratedMessage, maybeAck: Option[(ActorRef, Any)]): Unit = {
    probeRef ! messageToPersist
  }
}

class TouchdownPredictionActorTest extends TestKit(ActorSystem("TouchdownPredictions"))
  with AnyWordSpecLike
  with BeforeAndAfterAll {

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  "A touchdown actor" should {
    val modelAndFeatures = TouchdownModelAndFeatures(RegressionModel(Seq(1, 2, 3), 1.4), FeaturesWithOneToManyValues(List(Single("col_a"), OneToMany(List("col_b", "col_c"), "x")), IndexedSeq("t", "h", "u")), 20, 25)

    "Persist an incoming model" in {
      val probe = TestProbe()
      val actor = system.actorOf(Props(new MockTouchdownPredictionActor(probe.ref)))
      actor ! modelAndFeatures
      probe.expectMsgClass(classOf[ModelAndFeaturesMessage])
    }

    "Not persist an incoming model if it's the same as the last one received" in {
      val probe = TestProbe()
      val actor = system.actorOf(Props(new MockTouchdownPredictionActor(probe.ref)))
      actor ! modelAndFeatures
      probe.expectMsgClass(classOf[ModelAndFeaturesMessage])
      actor ! modelAndFeatures
      probe.expectNoMessage(250.milliseconds)
    }
  }
}
