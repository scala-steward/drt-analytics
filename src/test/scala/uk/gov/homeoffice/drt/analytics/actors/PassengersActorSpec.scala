package uk.gov.homeoffice.drt.analytics.actors

import akka.actor.{ActorSystem, Props}
import akka.pattern.AskableActorRef
import akka.persistence.inmemory.extension.{InMemoryJournalStorage, InMemorySnapshotStorage, StorageExtension}
import akka.testkit.{TestKit, TestProbe}
import akka.util.Timeout
import org.specs2.mutable.SpecificationLike
import org.specs2.specification.{AfterAll, AfterEach, BeforeEach}
import uk.gov.homeoffice.drt.analytics.time.SDate
import uk.gov.homeoffice.drt.analytics.{DailyPaxCountsOnDay, OriginTerminalDailyPaxCountsOnDay}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}


class PassengersActorSpec extends TestKit(ActorSystem("passengers-actor")) with SpecificationLike with AfterAll with BeforeEach {
  sequential

  override def before(): Unit = {
    val tp = TestProbe()
    tp.send(StorageExtension(system).journalStorage, InMemoryJournalStorage.ClearJournal)
    tp.expectMsg(akka.actor.Status.Success(""))
    tp.send(StorageExtension(system).snapshotStorage, InMemorySnapshotStorage.ClearSnapshots)
    tp.expectMsg(akka.actor.Status.Success(""))
  }

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  implicit val timeout: Timeout = new Timeout(5 second)

  val origin = "JFK"
  val terminal = "T1"
  val date20200301: SDate = SDate("2020-03-01")

  "Given a PassengersActor" >> {
    val dailyPax = DailyPaxCountsOnDay(date20200301.millisSinceEpoch, Map(date20200301.millisSinceEpoch -> 100))
    val otDailyPax = OriginTerminalDailyPaxCountsOnDay(origin, terminal, dailyPax)

    "When I send it some counts for an origin and terminal and then ask for the counts" >> {
      "Then I should get back the counts I sent it" >> {
        val actor: AskableActorRef = system.actorOf(Props(new PassengersActor(() => date20200301, 30)))
        val eventualCounts = actor.ask(otDailyPax).flatMap { _ =>
          actor.ask(OriginAndTerminal(origin, terminal)).asInstanceOf[Future[Option[Map[(Long, Long), Int]]]]
        }

        val result = Await.result(eventualCounts, 5 second)
        result === Option(Map((date20200301.millisSinceEpoch, date20200301.millisSinceEpoch) -> 100))
      }
    }

    "When I send it a counts for an origin and terminal, for 2 points in time separately, and then ask for the counts" >> {
      "Then I should get back the combined counts I sent it" >> {
        val actor: AskableActorRef = system.actorOf(Props(new PassengersActor(() => date20200301, 30)))
        val dailyPax2 = DailyPaxCountsOnDay(date20200301.addDays(1).millisSinceEpoch, Map(date20200301.millisSinceEpoch -> 100))
        val otDailyPax2 = OriginTerminalDailyPaxCountsOnDay(origin, terminal, dailyPax2)
        val eventualCounts = actor.ask(otDailyPax).flatMap { _ =>
          actor.ask(otDailyPax2).flatMap { _ =>
            actor.ask(OriginAndTerminal(origin, terminal)).asInstanceOf[Future[Option[Map[(Long, Long), Int]]]]
          }
        }

        val result = Await.result(eventualCounts, 5 second)
        result === Option(Map(
          (date20200301.millisSinceEpoch, date20200301.millisSinceEpoch) -> 100,
          (date20200301.addDays(1).millisSinceEpoch, date20200301.millisSinceEpoch) -> 100))
      }
    }

    "When I send it a counts for one origin and terminal, followed by a different origin & terminal, and then ask for the counts for the first" >> {
      "Then I should get back the counts I sent for the first origin and terminal" >> {
        val actor: AskableActorRef = system.actorOf(Props(new PassengersActor(() => date20200301, 30)))
        val dailyPax2 = DailyPaxCountsOnDay(date20200301.addDays(1).millisSinceEpoch, Map(date20200301.millisSinceEpoch -> 100))
        val origin2 = "BHX"
        val otDailyPax2 = OriginTerminalDailyPaxCountsOnDay(origin2, terminal, dailyPax2)
        val eventualCounts = actor.ask(otDailyPax).flatMap { _ =>
          actor.ask(otDailyPax2).flatMap { _ =>
            actor.ask(OriginAndTerminal(origin, terminal)).asInstanceOf[Future[Option[Map[(Long, Long), Int]]]]
          }
        }

        val result = Await.result(eventualCounts, 5 second)
        result === Option(Map((date20200301.millisSinceEpoch, date20200301.millisSinceEpoch) -> 100))
      }
    }
  }
}
