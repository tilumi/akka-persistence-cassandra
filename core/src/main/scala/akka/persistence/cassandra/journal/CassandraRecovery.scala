/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.cassandra.journal

import java.lang.{ Long => JLong }
import java.util.concurrent.atomic.AtomicBoolean

import akka.Done
import akka.actor.ActorRef
import akka.persistence.PersistentRepr
import akka.persistence.cassandra._
import akka.persistence.cassandra.journal.CassandraJournal.Tag
import akka.persistence.cassandra.journal.TagWriter.{ TagProgress, TagWrite }
import akka.persistence.cassandra.query.EventsByPersistenceIdStage.{ Extractors, TaggedPersistentRepr }
import akka.stream.scaladsl.{ Sink, Source }
import scala.concurrent._

trait CassandraRecovery extends CassandraTagRecovery with TaggedPreparedStatements {
  this: CassandraJournal =>

  import config._
  import context.dispatcher

  /**
   * It is assumed that this is only called during a replay and if fromSequenceNr == highest
   * then asyncReplayMessages won't be called. In that case the tag progress is updated
   * in here rather than during replay messages.
   */
  override def asyncReadHighestSequenceNr(persistenceId: String, fromSequenceNr: Long): Future[Long] = {
    val startTime = System.nanoTime()
    log.debug("asyncReadHighestSequenceNr {} {}", persistenceId, fromSequenceNr)
    val highestSequenceNr = asyncHighestDeletedSequenceNumber(persistenceId).flatMap { h =>
      println(s"# asyncHighestDeletedSequenceNumber $h, took ${(System.nanoTime() - startTime) / 1000} us") // FIXME
      asyncFindHighestSequenceNr(persistenceId, math.max(fromSequenceNr, h))
    }

    if (config.eventsByTagEnabled) {
      // map to send tag write progress so actor doesn't finish recovery until it is done
      highestSequenceNr.flatMap { seqNr =>

        println(s"# asyncReadHighestSequenceNr $seqNr, took ${(System.nanoTime() - startTime) / 1000} us") // FIXME

        if (seqNr == fromSequenceNr && seqNr != 0) {
          log.debug("Snapshot is current so replay won't be required. Calculating tag progress now.")
          for {
            tp <- lookupTagProgress(persistenceId)
            _ <- sendTagProgress(persistenceId, tp, tagWrites.get)
            startingSequenceNr = calculateStartingSequenceNr(tp)
            _ <- sendPreSnapshotTagWrites(startingSequenceNr, fromSequenceNr, persistenceId, Long.MaxValue, tp)
          } yield seqNr
        } else {
          Future.successful(seqNr)
        }
      }
    } else {
      highestSequenceNr
    }
  }

  // TODO this serialises and re-serialises the messages for fixing tag_views
  // Could have an events by persistenceId stage that has the raw payload
  override def asyncReplayMessages(
    persistenceId:  String,
    fromSequenceNr: Long,
    toSequenceNr:   Long,
    max:            Long
  )(replayCallback: (PersistentRepr) => Unit): Future[Unit] = {
    val startTime = System.nanoTime()
    log.debug("Recovering pid {} from {} to {}", persistenceId, fromSequenceNr, toSequenceNr)

    val gotFirstEvent = new AtomicBoolean() // FIXME

    if (config.eventsByTagEnabled) {
      val recoveryPrep: Future[Map[String, TagProgress]] = for {
        tp <- lookupTagProgress(persistenceId)
        tag1Time = System.nanoTime()
        _ <- sendTagProgress(persistenceId, tp, tagWrites.get)
        tag2Time = System.nanoTime()
        startingSequenceNr = calculateStartingSequenceNr(tp)
        _ <- sendPreSnapshotTagWrites(startingSequenceNr, fromSequenceNr, persistenceId, max, tp)
        tag3Time = System.nanoTime()
      } yield {
        println(s"# asyncReplayMessages tagging1 took ${(tag1Time - startTime) / 1000} us") // FIXME
        println(s"# asyncReplayMessages tagging2 took ${(tag2Time - tag1Time) / 1000} us") // FIXME
        println(s"# asyncReplayMessages tagging3 took ${(tag3Time - tag2Time) / 1000} us") // FIXME
        tp
      }

      Source.fromFutureSource(
        recoveryPrep.map((tp: Map[Tag, TagProgress]) => {
          println(s"# asyncReplayMessages total tagging took ${(System.nanoTime() - startTime) / 1000} us") // FIXME
          log.debug("Starting recovery with tag progress: {}. From {} to {}", tp, fromSequenceNr, toSequenceNr)
          queries
            .eventsByPersistenceId(
              persistenceId,
              fromSequenceNr,
              toSequenceNr,
              max,
              replayMaxResultSize,
              None,
              "asyncReplayMessages",
              someReadConsistency,
              someReadRetryPolicy,
              extractor = Extractors.taggedPersistentRepr
            ).map(sendMissingTagWrite(tp, tagWrites.get))
        })
      ).map(te => queries.mapEvent(te.pr))
        .runForeach {
          if (!gotFirstEvent.get()) {
            gotFirstEvent.set(true)
            println(s"# asyncReplayMessages first event took ${(System.nanoTime() - startTime) / 1000} us") // FIXME
          }
          replayCallback
        }
        .map { _ =>
          ()
          println(s"# asyncReplayMessages took ${(System.nanoTime() - startTime) / 1000} us") // FIXME
        }

    } else {
      queries
        .eventsByPersistenceId(
          persistenceId,
          fromSequenceNr,
          toSequenceNr,
          max,
          replayMaxResultSize,
          None,
          "asyncReplayMessages",
          someReadConsistency,
          someReadRetryPolicy,
          extractor = Extractors.taggedPersistentRepr
        ).map(te => queries.mapEvent(te.pr))
        .runForeach {
          if (!gotFirstEvent.get()) {
            gotFirstEvent.set(true)
            println(s"# asyncReplayMessages first event took ${(System.nanoTime() - startTime) / 1000} us") // FIXME
          }
          replayCallback
        }
        .map { _ =>
          ()
          println(s"# asyncReplayMessages took ${(System.nanoTime() - startTime) / 1000} us") // FIXME
        }
    }
  }

  private def sendPreSnapshotTagWrites(
    minProgressNr:  Long,
    fromSequenceNr: Long,
    pid:            String,
    max:            Long,
    tp:             Map[Tag, TagProgress]
  ): Future[Done] = if (minProgressNr < fromSequenceNr) {
    val scanTo = fromSequenceNr - 1
    log.debug("Scanning events before snapshot to recover tag_views: From: {} to: {}", minProgressNr, scanTo)
    queries.eventsByPersistenceId(
      pid,
      minProgressNr,
      scanTo,
      max,
      replayMaxResultSize,
      None,
      "asyncReplayMessagesPreSnapshot",
      someReadConsistency,
      someReadRetryPolicy,
      Extractors.taggedPersistentRepr
    ).map(sendMissingTagWrite(tp, tagWrites.get)).runWith(Sink.ignore)
  } else {
    log.debug("Recovery is starting before the latest tag writes tag progress. Min progress for pid {}. " +
      "From sequence nr of recovery: {}", minProgressNr, fromSequenceNr)
    Future.successful(Done)
  }

  // TODO migrate this to using raw, maybe after offering a way to migrate old events in message?
  private def sendMissingTagWrite(tp: Map[Tag, TagProgress], to: ActorRef)(tpr: TaggedPersistentRepr): TaggedPersistentRepr = {
    tpr.tags.foreach(tag => {
      tp.get(tag) match {
        case None =>
          log.debug("Tag write not in progress. Sending to TagWriter. Tag {} Sequence Nr {}.", tag, tpr.sequenceNr)
          to ! TagWrite(tag, Vector(serializeEvent(tpr.pr, tpr.tags, tpr.offset, bucketSize, serialization, transportInformation)))
        case Some(progress) =>
          if (tpr.sequenceNr > progress.sequenceNr) {
            log.debug("Sequence nr > than write progress. Sending to TagWriter. Tag {} Sequence Nr {}. ", tag, tpr.sequenceNr)
            to ! TagWrite(tag, Vector(serializeEvent(tpr.pr, tpr.tags, tpr.offset, bucketSize, serialization, transportInformation)))
          }
      }
    })
    tpr
  }

  private def asyncFindHighestSequenceNr(persistenceId: String, fromSequenceNr: Long): Future[Long] = {

    def find(currentPnr: Long, currentSnr: Long): Future[Long] = {
      val startTime = System.nanoTime()
      // if every message has been deleted and thus no sequence_nr the driver gives us back 0 for "null" :(
      val boundSelectHighestSequenceNr = preparedSelectHighestSequenceNr.map(_.bind(persistenceId, currentPnr: JLong))
      boundSelectHighestSequenceNr.flatMap(session.selectResultSet)
        .map { rs =>
          Option(rs.one()).map { row =>
            println(s"# findHighest seqNr $currentSnr, partition $currentPnr, took ${(System.nanoTime() - startTime) / 1000} us") // FIXME
            (row.getBool("used"), row.getLong("sequence_nr"))
          }
        }
        .flatMap {
          // never been to this partition
          case None                   => Future.successful(currentSnr)
          // don't currently explicitly set false
          case Some((false, _))       => Future.successful(currentSnr)
          // everything deleted in this partition, move to the next
          case Some((true, 0))        => find(currentPnr + 1, currentSnr)
          case Some((_, nextHighest)) => find(currentPnr + 1, nextHighest)
        }
    }

    find(partitionNr(fromSequenceNr), fromSequenceNr)
  }

  def asyncHighestDeletedSequenceNumber(persistenceId: String): Future[Long] = {
    val boundSelectDeletedTo = preparedSelectDeletedTo.map(_.bind(persistenceId))
    boundSelectDeletedTo.flatMap(session.selectResultSet)
      .map(r => Option(r.one()).map(_.getLong("deleted_to")).getOrElse(0))
  }
}
