package uk.gov.homeoffice.drt.analytics.prediction

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.stream.scaladsl.Source
import akka.testkit.{TestKit, TestProbe}
import akka.util.Timeout
import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AnyWordSpecLike
import uk.gov.homeoffice.drt.actor.PredictionModelActor._
import uk.gov.homeoffice.drt.actor.commands.Commands.GetState
import uk.gov.homeoffice.drt.analytics.actors.Ack
import uk.gov.homeoffice.drt.analytics.prediction.dump.NoOpDump
import uk.gov.homeoffice.drt.ports.Terminals.{T2, Terminal}
import uk.gov.homeoffice.drt.prediction.arrival.features.FeatureColumnsV1.{DayOfWeek, PartOfDay}
import uk.gov.homeoffice.drt.prediction.category.FlightCategory
import uk.gov.homeoffice.drt.prediction.{ActorModelPersistence, ModelCategory}
import uk.gov.homeoffice.drt.time.{SDate, SDateLike}

import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}

case class MockPersistenceActor(probe: ActorRef) extends Actor {
  override def receive: Receive = {
    case rm: RemoveModel =>
      probe ! rm
      sender() ! Ack
    case _: ModelUpdate =>
      probe ! "model update"
      sender() ! Ack
    case GetState => sender() ! Models(Map())
  }
}

case class MockPersistence(probe: ActorRef)
                          (implicit
                           val system: ActorSystem, val ec: ExecutionContext, val timeout: Timeout
                          ) extends ActorModelPersistence {
  override val modelCategory: ModelCategory = FlightCategory
  override val actorProvider: (ModelCategory, WithId) => ActorRef =
    (_, _) => system.actorOf(Props(MockPersistenceActor(probe)), s"test-actor")
}

class FlightRouteValuesTrainerSpec
  extends TestKit(ActorSystem("FlightRouteValuesTrainer"))
    with AnyWordSpecLike with BeforeAndAfterAll {

  implicit val ec: ExecutionContextExecutor = system.dispatcher
  implicit val timeout: Timeout = new Timeout(1.second)

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  "FlightRouteValuesTrainer" should {
    val example = (1.0, Seq("car_1", "pod_1"), Seq(), "")

    def examples(n: Int): Iterable[(Double, Seq[String], Seq[Double], String)] = Iterable.fill(n)(example)

    "Send a RemoveModel when there are too few training examples" in {
      val probe = TestProbe("test-probe")

      val trainer1 = getTrainer(examples(1), probe.ref)
      trainer1.trainTerminals("LHR", List(T2))
      probe.expectMsg(10.seconds, RemoveModel("some-model"))
      trainer1.session.stop()
    }

    "Send a ModelUpdate when there are enough training examples" in {
      val probe = TestProbe("test-probe")

      val trainer2 = getTrainer(examples(10), probe.ref)
      trainer2.trainTerminals("LHR", List(T2))
      probe.expectMsg(60.seconds, "model update")
      trainer2.session.stop()
    }
  }

  private def getTrainer(examples: Iterable[(Double, Seq[String], Seq[Double], String)], probe: ActorRef): FlightRouteValuesTrainer = {
    implicit val sdateProvider: Long => SDateLike = (ts: Long) => SDate(ts)
    FlightRouteValuesTrainer(
      modelName = "some-model",
      featuresVersion = 1,
      features = List(DayOfWeek(), PartOfDay()),
      examplesProvider = (_: Terminal, _: SDateLike, _: Int) => {
        Source(List((TerminalFlightNumberOrigin("T2", 1, "JFK"), examples)))
      },
      baselineValue = _ => 1000d,
      daysOfTrainingData = 10,
      lowerQuantile = 0.1,
      upperQuantile = 0.9,
      persistence = MockPersistence(probe),
      dumper = NoOpDump
    )
  }
}
