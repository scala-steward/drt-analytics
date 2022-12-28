package uk.gov.homeoffice.drt.analytics.actors

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.testkit.{TestKit, TestProbe}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AnyWordSpecLike
import scalapb.GeneratedMessage
import uk.gov.homeoffice.drt.actor.PredictionModelActor
import uk.gov.homeoffice.drt.actor.PredictionModelActor.ModelUpdate
import uk.gov.homeoffice.drt.actor.TerminalDateActor.FlightRoute
import uk.gov.homeoffice.drt.prediction.Feature.{OneToMany, Single}
import uk.gov.homeoffice.drt.prediction.{FeaturesWithOneToManyValues, RegressionModel, TouchdownModelAndFeatures}
import uk.gov.homeoffice.drt.protobuf.messages.ModelAndFeatures.ModelAndFeaturesMessage
import uk.gov.homeoffice.drt.time.SDate

import scala.concurrent.duration.DurationInt

class MockTouchdownPredictionActor(probeRef: ActorRef) extends PredictionModelActor(() => SDate.now(), "flight", FlightRoute("T1", 100, "JFK")) {
  override def persistAndMaybeSnapshotWithAck(messageToPersist: GeneratedMessage, maybeAck:List[(ActorRef, Any)]): Unit = {
    probeRef ! messageToPersist
  }
}

class PredictionModelActorTest extends TestKit(ActorSystem("TouchdownPredictions"))
  with AnyWordSpecLike
  with BeforeAndAfterAll {

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  "A touchdown actor" should {
    val features = FeaturesWithOneToManyValues(List(Single("col_a"), OneToMany(List("col_b", "col_c"), "x")), IndexedSeq("t", "h", "u"))
    val modelUpdate = ModelUpdate(RegressionModel(Seq(1, 2), 1.4), features, 10, 10.1, TouchdownModelAndFeatures.targetName)

    "Persist an incoming model" in {
      val probe = TestProbe()
      val actor = system.actorOf(Props(new MockTouchdownPredictionActor(probe.ref)))
      actor ! modelUpdate
      probe.expectMsgClass(classOf[ModelAndFeaturesMessage])
    }

    "Not persist an incoming model if it's the same as the last one received" in {
      val probe = TestProbe()
      val actor = system.actorOf(Props(new MockTouchdownPredictionActor(probe.ref)))
      actor ! modelUpdate
      probe.expectMsgClass(classOf[ModelAndFeaturesMessage])
      actor ! modelUpdate
      probe.expectNoMessage(250.milliseconds)
    }
  }
}
