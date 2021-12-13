package uk.gov.homeoffice.drt.analytics.prediction

import akka.NotUsed
import akka.actor.{ActorSystem, Props, Terminated}
import akka.pattern.ask
import akka.stream.scaladsl.Source
import akka.stream.{ActorMaterializer, Materializer}
import akka.util.Timeout
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import uk.gov.homeoffice.drt.analytics.actors.MinutesOffScheduledActor
import uk.gov.homeoffice.drt.analytics.actors.MinutesOffScheduledActor.{ArrivalKey, GetState}
import uk.gov.homeoffice.drt.analytics.time.SDate
import uk.gov.homeoffice.drt.ports.Terminals.T2

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor, Future}


class ArrivalTimeSpec extends AnyWordSpec with Matchers {
  implicit val timeout: Timeout = new Timeout(5.second)

  val context: (ActorSystem => ExecutionContext => Materializer => Any) => Future[Terminated] = (test: ActorSystem => ExecutionContext => Materializer => Any) => {
    implicit val system: ActorSystem = ActorSystem("test")
    implicit val ec: ExecutionContextExecutor = system.dispatcher
    implicit val mat: ActorMaterializer = ActorMaterializer.create(system)

    test(system)(ec)(mat)

    system.terminate()
  }

  "A MinutesOffScheduledActor" ignore {
    "recover some state" in context {
      implicit system =>
        implicit ec =>
          implicit mat =>
            val actor = system.actorOf(Props(new MinutesOffScheduledActor(T2, 2020, 10, 1)))
            val result = Await.result(actor.ask(GetState).mapTo[Map[ArrivalKey, Int]], 1.second)

            result.size should not be (0)
    }
    "provide a stream of arrivals across a day range" in context {
      implicit system =>
        implicit ec =>
          implicit mat =>
            val start = SDate(2020, 10, 1, 0, 0)
            val days = 10

            val arrivals = MinutesOffScheduledActor.byDaySource(start, days)

            val result = Await.result(arrivals.runFold(Map[ArrivalKey, Int]())(_ ++ _), 5.seconds)

            val byDay = result.keys.groupBy {
              case ArrivalKey(scheduled, _, _) => SDate(scheduled).date
            }

            byDay.forall(_._2.nonEmpty) should be(true)
    }
  }

  "A MinutesOffScheduledActor" should {
    "provide a map of (terminal, flight-number) -> scheduled -> off-scheduled" in context {
      implicit system =>
        implicit ec =>
          implicit mat =>
            val start = SDate(2020, 10, 1, 0, 0)
            val days = 10

            val byTerminalFlightCode: Source[Map[(String, Int), Map[Long, Int]], NotUsed] = MinutesOffScheduledActor.byDaySource(start, days).map { byArrivalKey =>
              byArrivalKey
                .groupBy { case (key, _) =>
                  (key.terminal, key.number)
                }
                .map {
                  case (((terminal, number), byArrivalKey)) => ((terminal, number), byArrivalKey.map { case (key, offScheduled) => (key.scheduled, offScheduled) })
                }
            }

            val result = Await.result(byTerminalFlightCode.runFold(Map[(String, Int), Map[Long, Int]]()) {
              case (acc, incoming) => incoming.foldLeft(acc) {
                case (acc, (key, arrivals)) => acc.updated(key, acc.getOrElse(key, Map()) ++ arrivals)
              }
            }, 5.seconds)

            println(s"result: ${result.mkString("\n")} ")


    }
  }
}
