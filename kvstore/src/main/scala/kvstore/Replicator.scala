package kvstore

import akka.actor.Props
import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.Cancellable
import scala.concurrent.duration._
import scala.language.postfixOps

object Replicator {
  case class Replicate(key: String, valueOption: Option[String], id: Long)
  case class Replicated(key: String, id: Long)
  
  case class Snapshot(key: String, valueOption: Option[String], seq: Long)
  case class SnapshotAck(key: String, seq: Long)

  case object TimeForBatchSend

  def props(replica: ActorRef): Props = Props(new Replicator(replica))
}

class Replicator(val replica: ActorRef) extends Actor {
  import Replicator._
  import Replica._
  import context.dispatcher
 
  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */

  // map from sequence number to pair of sender and request; we are waiting for these
  var acks = Map.empty[Long, (ActorRef, Replicate)]
  // a sequence of not-yet-sent snapshots (you can disregard this if not implementing batching)
  var pending = Vector.empty[Snapshot]
  
  var _seqCounter = 0L
  def nextSeq = {
    val ret = _seqCounter
    _seqCounter += 1
    ret
  }

  // Schedule recurring message to self, to send batch of snapshots every 100ms.
  private var batchScheduler: Cancellable = _

  override def preStart(): Unit = {
    batchScheduler = context.system.scheduler.schedule(
      0 milliseconds, 100 milliseconds, self, TimeForBatchSend
    )
  }
  override def postStop(): Unit = {
    batchScheduler.cancel()
  }
  
  def receive: Receive = {
    case rep @ Replicate(k,vOpt,id) =>
      myLog(s"~~> ($id) REPLICATE $k=$vOpt from $sender")
      stageSnapshot(rep, sender)
    case SnapshotAck(k,seq) =>
      myLog(s"~~> [$seq] SNAPSHOTACK $k")
      val (ar: ActorRef, rep: Replicate) = acks(seq)
      acks -= seq
      removeFromPending(seq)
      ar ! Replicated(rep.key,rep.id)
    case TimeForBatchSend =>
      sendBatchOfSnapshots
    case _ =>
  }

  // Adds snapshot to pending queue and registers its expected ack.
  // Not really batched in the sense of consolidating snapshots for shared key--senders
  // of the the Replicate messages generating the snapshots might be different (like,
  // if the primary changes)
  def stageSnapshot(rep: Replicate, sender: ActorRef): Unit = rep match {
    case Replicate(k,vOpt,id) =>
      val seq = nextSeq
      acks += seq -> (sender, rep)
      pending :+= Snapshot(rep.key,rep.valueOption,seq)  // oldest in front
  }

  // (Re-)sends all snapshots that are currently pending.
  def sendBatchOfSnapshots: Unit = {
    pending.foreach { replica ! _ } // oldest in front
  }

  // Upon receipt of ack, remove corresponding snapshot (or snapshots, if we ever
  // do the real batching) from pending queue.
  def removeFromPending(seq: Long): Unit = {
    // Bcs seq should only appear once, could speed this up.
    pending = pending.filterNot(_.seq == seq)
  }


}
