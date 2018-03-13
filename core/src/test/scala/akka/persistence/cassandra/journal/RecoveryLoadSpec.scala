/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.cassandra.journal

import scala.concurrent.duration._

import akka.actor._
import akka.persistence.PersistentActor
import akka.persistence.RecoveryCompleted
import akka.persistence.SnapshotOffer
import akka.persistence.cassandra.CassandraLifecycle
import akka.testkit._
import com.typesafe.config.ConfigFactory
import org.scalatest._

object RecoveryLoadSpec {
  val config = ConfigFactory.parseString(
    s"""
      akka.loglevel = DEBUG
      cassandra-journal.keyspace=RecoveryLoadSpec
      cassandra-journal.events-by-tag.enabled = off //FIXME try with tags also
      cassandra-journal.replay-filter.mode = off
      cassandra-journal.log-queries = on
      cassandra-snapshot-store.keyspace=RecoveryLoadSpecSnapshot
      cassandra-snapshot-store.log-queries = on
    """
  ).withFallback(CassandraLifecycle.config)

  final case class Init(numberOfEvents: Int)
  case object InitDone
  private final case class Next(remaining: Int)
  case object GetMetrics
  final case class Metrics(snapshotDuration: FiniteDuration, replayDuration1: FiniteDuration,
                           replayDuration2: FiniteDuration, replayedEvents: Int,
                           totalDuration: FiniteDuration)

  def props(persistenceId: String, snapshotEvery: Int): Props =
    Props(new ProcessorA(persistenceId, snapshotEvery))

  class ProcessorA(val persistenceId: String, snapshotEvery: Int) extends PersistentActor {
    val startTime = System.nanoTime()
    var snapshotEndTime = startTime
    var replayStartTime = startTime
    var replayEndTime = startTime
    var replayedEvents = 0

    def receiveRecover: Receive = {
      case s: SnapshotOffer =>
        println(s"# got snap ${s.metadata.sequenceNr}") // FIXME
        snapshotEndTime = System.nanoTime()
        replayStartTime = snapshotEndTime
      case _: String =>
        replayedEvents += 1

        if (replayStartTime == snapshotEndTime)
          replayStartTime = System.nanoTime() // first event
      case RecoveryCompleted =>
        replayEndTime = System.nanoTime()
    }

    def receiveCommand: Receive = {
      case Init(numberOfEvents) =>
        self ! Next(numberOfEvents)
        context.become(init(sender()))
      case GetMetrics =>
        sender() ! Metrics(
          snapshotDuration = (snapshotEndTime - startTime).nanos,
          replayDuration1 = (replayStartTime - snapshotEndTime).nanos,
          replayDuration2 = (replayEndTime - replayStartTime).nanos,
          replayedEvents,
          totalDuration = (replayEndTime - startTime).nanos
        )
    }

    def init(replyTo: ActorRef): Receive = {
      case Next(remaining) =>
        if (remaining == 0)
          replyTo ! InitDone
        else {
          persist(s"event-$lastSequenceNr") { _ =>
            if (lastSequenceNr % snapshotEvery == 0) {
              println(s"# snap-$lastSequenceNr") // FIXME
              saveSnapshot(s"snap-$lastSequenceNr")
            }
            self ! Next(remaining - 1)
          }
        }
    }

  }

}

class RecoveryLoadSpec extends TestKit(ActorSystem("RecoveryLoadSpec", RecoveryLoadSpec.config))
  with ImplicitSender with WordSpecLike with Matchers with CassandraLifecycle {

  import RecoveryLoadSpec._

  override def systemName: String = "RecoveryLoadSpec"

  private def printMetrics(metrics: Metrics): Unit = {
    println(s"  snapshot recovery took ${metrics.snapshotDuration.toMillis} ms")
    println(s"  replay init took ${metrics.replayDuration1.toMillis} ms")
    println(s"  replay of ${metrics.replayedEvents} events took ${metrics.replayDuration2.toMillis} ms")
    println(s"  total recovery took ${metrics.totalDuration.toMillis} ms")
  }

  "Recovery" should {

    "have some reasonable performance" in {
      val p1 = system.actorOf(props(persistenceId = "a1", snapshotEvery = 1000))
      p1 ! Init(numberOfEvents = 99999)
      expectMsg(60.seconds, InitDone)
      system.stop(p1)

      (1 to 10).foreach { n =>
        val p2 = system.actorOf(props(persistenceId = "a1", snapshotEvery = 100))
        p2 ! GetMetrics
        val metrics = expectMsgType[Metrics](60.seconds)
        println(s"iteration #$n")
        printMetrics(metrics)
        system.stop(p2)
      }
    }
  }

}
