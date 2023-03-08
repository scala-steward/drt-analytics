package uk.gov.homeoffice.drt.analytics.prediction

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.pattern.StatusReply.Ack
import akka.stream.scaladsl.Source
import akka.testkit.{TestKit, TestProbe}
import akka.util.Timeout
import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AnyWordSpecLike
import uk.gov.homeoffice.drt.actor.PredictionModelActor.{ModelUpdate, Models, RemoveModel, TerminalFlightNumberOrigin, WithId}
import uk.gov.homeoffice.drt.actor.TerminalDateActor.GetState
import uk.gov.homeoffice.drt.ports.Terminals.T2
import uk.gov.homeoffice.drt.prediction.category.FlightCategory
import uk.gov.homeoffice.drt.prediction.{ModelCategory, Persistence}
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
                          ) extends Persistence {
  override val modelCategory: ModelCategory = FlightCategory
  override val actorProvider: (ModelCategory, WithId) => ActorRef =
    (_, _) => system.actorOf(Props(MockPersistenceActor(probe)), s"test-actor")
  override val now: () => SDateLike = () => SDate("2023-01-01T00:00")
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
    val example = (1.0, Seq("dow_1", "pod_1"))

    def examples(n: Int): Iterable[(Double, Seq[String])] = Iterable.fill(n)(example)

    "Send a RemoveModel when there are too few training examples" in {
      val probe = TestProbe("test-probe")
      getTrainer(examples(1), probe.ref).trainTerminals(List(T2))
      probe.expectMsg(5.seconds, RemoveModel("some-model"))
    }

    "Send a ModelUpdate when there are enough training examples" in {
      val probe = TestProbe("test-probe")
      getTrainer(examples(10), probe.ref).trainTerminals(List(T2))
      probe.expectMsg(20.seconds, "model update")
    }
  }

  private def getTrainer(examples: Iterable[(Double, Seq[String])], probe: ActorRef) = {
    FlightRouteValuesTrainer(
      "some-model",
      (_, _, _) => {
        Source(List((TerminalFlightNumberOrigin("T2", 1, "JFK"), examples)))
      },
      MockPersistence(probe),
      _ => 1000d,
      10
    )
  }
}
