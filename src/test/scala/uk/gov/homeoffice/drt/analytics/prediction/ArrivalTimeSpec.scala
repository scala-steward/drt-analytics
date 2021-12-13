package uk.gov.homeoffice.drt.analytics.prediction

import akka.actor.{ActorSystem, PoisonPill, Props}
import akka.pattern.ask
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source
import akka.util.Timeout
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import uk.gov.homeoffice.drt.analytics.actors.MinutesOffScheduledActor
import uk.gov.homeoffice.drt.analytics.actors.MinutesOffScheduledActor.{ArrivalKey, GetState}
import uk.gov.homeoffice.drt.analytics.time.SDate
import uk.gov.homeoffice.drt.ports.Terminals.T2

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContextExecutor}


class ArrivalTimeSpec extends AnyWordSpec with Matchers {
  implicit val timeout: Timeout = new Timeout(1.second)
  "A MinutesOffScheduledActor" should {
    "recover some state" in {
      val system = ActorSystem("test")
      implicit val ec: ExecutionContextExecutor = system.dispatcher
      val actor = system.actorOf(Props(new MinutesOffScheduledActor(T2, 2020, 10, 1)))
      val result = Await.result(actor.ask(GetState).mapTo[Map[ArrivalKey, Int]], 1.second)

      system.terminate()

      result.size should not be (0)
    }
  }

  "A MinutesOffScheduledActor" should {
    "provide a stream of arrivals across a day range" in {
      val system = ActorSystem("test")
      implicit val ec: ExecutionContextExecutor = system.dispatcher
      implicit val mat: ActorMaterializer = ActorMaterializer.create(system)

      val start = SDate(2020, 10, 1, 0, 0)
      val days = 10

      val arrivals = Source((0 until days).toList).mapAsync(1) { day =>
        val currentDay = start.addDays(day)
        val actor = system.actorOf(Props(new MinutesOffScheduledActor(T2, currentDay.fullYear, currentDay.month, currentDay.date)))
        actor
          .ask(GetState).mapTo[Map[ArrivalKey, Int]]
          .map { arrivals =>
            actor ! PoisonPill
            arrivals
          }
      }

      val result = Await.result(arrivals.runFold(Map[ArrivalKey, Int]())(_ ++ _), 5.seconds)

      val byDay = result.keys.groupBy {
        case ArrivalKey(scheduled, _, _) => SDate(scheduled).date
      }

      byDay.forall(_._2.nonEmpty) should be (true)
    }
  }
}
