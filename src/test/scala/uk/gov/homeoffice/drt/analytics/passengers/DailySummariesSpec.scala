package uk.gov.homeoffice.drt.analytics.passengers

import akka.actor.{Actor, ActorSystem, Props}
import akka.stream.ActorMaterializer
import akka.testkit.TestKit
import org.specs2.mutable.SpecificationLike
import uk.gov.homeoffice.drt.analytics.actors.{FeedPersistenceIds, GetArrivals}
import uk.gov.homeoffice.drt.analytics.time.SDate
import uk.gov.homeoffice.drt.analytics.{Arrival, Arrivals, DailyPaxCountsOnDay}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor, Future}

object MockArrivalsActor {
  def props: Arrivals => (String, SDate) => Props = (arrivals: Arrivals) => (_: String, _: SDate) => Props(new MockArrivalsActor(arrivals))
}

class MockArrivalsActor(arrivals: Arrivals) extends Actor {
  override def receive: Receive = {
    case GetArrivals(_, _) => sender() ! arrivals
  }
}

class DailySummariesSpec extends TestKit(ActorSystem("passengers-actor")) with SpecificationLike {
  implicit val ec: ExecutionContextExecutor = ExecutionContext.global
  implicit val mat: ActorMaterializer = ActorMaterializer()

  val livePid: String = FeedPersistenceIds.live
  val forecastPid: String = FeedPersistenceIds.forecastBase
  val date: SDate = SDate("2020-01-01")
  val forecastArrival: Arrival = Arrival("BA", 1, date.millisSinceEpoch, "T1", "JFK", "sched", 100, 0)
  val liveArrival: Arrival = Arrival("BA", 1, date.millisSinceEpoch, "T1", "JFK", "sched", 50, 0)

  "Given a sourcePersistenceId, and props for a mock actor with an arrival" >> {
    val props = MockArrivalsActor.props

    "When I ask for arrivals from sources" >> {

      val eventualsArrivals = DailySummaries.arrivalsForSources(List(forecastPid), date, date, props(genArrivals(Seq(forecastArrival))))
      "I should get the arrival from the mock actor" >> {
        val result: Seq[(String, Arrivals)] = eventualsArrivals.map { eventualArrivals =>
          Await.result(eventualArrivals, 1 second)
        }
        result === Seq((forecastPid, genArrivals(Seq(forecastArrival))))
      }
    }
  }

  "Given a live and a forecast version of the same arrival" >> {
    "When I ask to merge live and forecast arrivals" >> {
      val futureForecastArrivals = Future((forecastPid, genArrivals(Seq(forecastArrival))))
      val futureLiveArrivals = Future((livePid, genArrivals(Seq(liveArrival))))

      val mergedArrivals = Await.result(DailySummaries.mergeArrivals(Seq(futureForecastArrivals, futureLiveArrivals)), 1 second)

      mergedArrivals === Map(liveArrival.uniqueArrival -> liveArrival)
    }
  }

  "Given one arrival" >> {
    "When I ask for the summary csv line for the two days starting from scheduled date of the arrival for" >> {
      val numberOfDays = 2
      "I should get a csv line showing the date, terminal, origin and pax count for that date and a '-' for the day after" >> {
        val eventualCountsByOrigin = DailySummaries.dailyPaxCountsForDayByOrigin(date, date, numberOfDays, "T1", Future(genArrivals(Seq(liveArrival)).arrivals))
        val eventualCsv = DailySummaries.dailyOriginCountsToCsv(date, date, numberOfDays, "T1", eventualCountsByOrigin)
        val summaries = Await.result(eventualCsv, 1 second)

        summaries === "2020-01-01,T1,JFK,50,-"
      }
    }
  }

  "Given one arrival" >> {
    "When I ask for the daily summary for that 1 day when the arrival is scheduled for" >> {
      "I should get a map of the arrival's origin to a DailyPaxCountOnDay containing 1 day with the pax from the 1 arrival" >> {
        val summaries = Await.result(DailySummaries.dailyPaxCountsForDayByOrigin(date, date, 1, "T1", Future(genArrivals(Seq(liveArrival)).arrivals)), 1 second)

        summaries === Map(liveArrival.origin -> DailyPaxCountsOnDay(date.millisSinceEpoch, Map(date.millisSinceEpoch -> liveArrival.actPax)))
      }
    }

    "When I ask for the daily summary for 2 days starting on the 1 day when the arrival is scheduled for" >> {
      "I should get a map of the arrival's origin to a DailyPaxCountOnDay containing just one day as the next doesn't have any pax" >> {
        val summaries = Await.result(DailySummaries.dailyPaxCountsForDayByOrigin(date, date, 2, "T1", Future(genArrivals(Seq(liveArrival)).arrivals)), 1 second)

        summaries === Map(liveArrival.origin -> DailyPaxCountsOnDay(date.millisSinceEpoch, Map(date.millisSinceEpoch -> liveArrival.actPax)))
      }
    }
  }

  "Given 2 arrivals scheduled for the same day" >> {
    "When I ask for the daily summary for the day when the arrival is scheduled for" >> {
      "I should get a map of the arrival's origin to a DailyPaxCountOnDay containing 1 day with the total pax from the 2 arrivals" >> {
        val arrival2 = liveArrival.copy(number = 2)
        val summaries = Await.result(DailySummaries.dailyPaxCountsForDayByOrigin(date, date, 1, "T1", Future(genArrivals(Seq(liveArrival, arrival2)).arrivals)), 1 second)

        summaries === Map(liveArrival.origin -> DailyPaxCountsOnDay(date.millisSinceEpoch, Map(date.millisSinceEpoch -> (liveArrival.actPax + arrival2.actPax))))
      }
    }
  }

  "Given 2 arrivals scheduled for the same day, each with 100 pax and 10 transit pax" >> {
    "When I ask for the daily summary for the day when the arrival is scheduled for" >> {
      "I should get a map of the arrival's origin to a DailyPaxCountOnDay containing 1 day with the total pax from the 2 arrivals minus the transit pax" >> {
        val arrival = liveArrival.copy(actPax = 100, transPax = 10)
        val arrival2 = liveArrival.copy(number = 2, actPax = 100, transPax = 10)
        val summaries = Await.result(DailySummaries.dailyPaxCountsForDayByOrigin(date, date, 1, "T1", Future(genArrivals(Seq(arrival, arrival2)).arrivals)), 1 second)

        summaries === Map(liveArrival.origin -> DailyPaxCountsOnDay(date.millisSinceEpoch, Map(date.millisSinceEpoch -> (100 * 2 - 10 * 2))))
      }
    }
  }

  private def genArrivals(arrivals: Seq[Arrival]): Arrivals = Arrivals(arrivals.map(a => (a.uniqueArrival, a)).toMap)
}
