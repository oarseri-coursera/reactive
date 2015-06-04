package kvstore

import akka.actor.{ Props, ActorRef, Actor }
import kvstore.Arbiter._
import scala.collection.immutable.Queue //
//import akka.actor.SupervisorStrategy.Restart  // FIXTHIS clean up all these comments.
import scala.annotation.tailrec //
import akka.pattern.{ ask, pipe } // used
import scala.concurrent.Future
import akka.actor.Terminated  //
import scala.concurrent.duration._
import scala.language.postfixOps
import akka.actor.PoisonPill // used
import akka.actor.OneForOneStrategy
import akka.actor.SupervisorStrategy
import akka.actor.SupervisorStrategy.{ Resume, Escalate }

object Replica {
  sealed trait Operation {
    def key: String
    def id: Long
  }
  case class Insert(key: String, value: String, id: Long) extends Operation
  case class Remove(key: String, id: Long) extends Operation
  case class Get(key: String, id: Long) extends Operation

  sealed trait OperationReply
  case class OperationAck(id: Long) extends OperationReply
  case class OperationFailed(id: Long) extends OperationReply
  case class GetResult(key: String, valueOption: Option[String], id: Long) extends OperationReply
  case class NoMoreRetries(msg: Any)

  def props(arbiter: ActorRef, persistenceProps: Props): Props = Props(new Replica(arbiter, persistenceProps))
}

class Replica(val arbiter: ActorRef, persistenceProps: Props) extends Actor {
  import Replica._
  import Replicator._
  import Propagator._
  import Persistence._
  import context.dispatcher
  import context.system

  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */
  
  var kv = Map.empty[String, String]
  // a map from secondary replicas to replicators
  var secondaries = Map.empty[ActorRef, ActorRef]
  // the current set of replicators
  var expectedSeq = 0l

  // Start up your own persistence actor, and shut it down on completion.
  val persister = context.actorOf(persistenceProps, name = "persister")

  // Supervision strategy (for persistence faillure) just resumes, with 10 retries per second.
  // Otherwise, follow default strategy.
  override val supervisorStrategy =
    OneForOneStrategy(maxNrOfRetries = 100, withinTimeRange = 1 second) {
      case _: PersistenceException => { myLog("RESUMING"); Resume }
      case t => super.supervisorStrategy.decider.applyOrElse(t, (_: Any) => Escalate)
    }

  // Tell arbiter that you are ready to join the cluster.
  arbiter ! Join

  def receive = {
    case JoinedPrimary   => context.become(leader)
    case JoinedSecondary => context.become(replica)
  }

  /* Behavior for  the leader role. */
  val leader: Receive = {
    case Insert(k,v,id) =>
      myLog(s"==> ($id) INSERT $k=$v")
      kv += k -> v
      processUpdate(k,Some(v),id,sender)
    case Remove(k,id) =>
      myLog(s"==> ($id) REMOVE $k")
      kv -= k
      processUpdate(k,None,id,sender)
    case Get(k,id) =>
      myLog(s"==> ($id) GET $k")
      val vOpt = kv.get(k)
      sender ! GetResult(k, vOpt, id)
    case Replicas(reps) =>
      myLog(s"++> REPLICAS: $reps")
      // Remove self from set, only want to update secondaries.
      updateCluster(reps - self)
    case _ =>
  }

  /* Behavior for the replica role. */
  val replica: Receive = {
    case Get(k,id) =>
      myLog(s"==> ($id) GET $k")
      val vOpt = kv.get(k)
      sender ! GetResult(k, vOpt, id)
    case s: Snapshot =>
      processSnapshot(s, sender)
    case _ =>
  }

  // Used by primary to process changes in cluster membership.
  def updateCluster(reps: Set[ActorRef]): Unit = {
    val currentReplicas = secondaries.keys.toSet
    val joinedReplicas = reps -- currentReplicas
    val departedReplicas = currentReplicas -- reps

    // Joined replicas: Create replicators, forward all k=v pairs, publish to propagators.
    // This should be done before departed replicas are handled, to be strict on replication.
    val joinedReplicators: Set[ActorRef] = joinedReplicas.map { r =>
      val replicator = context.actorOf(Replicator.props(r))
      secondaries += r -> replicator
      replicator
    }
    // FIXTHIS forward all k=v pairs
    system.eventStream.publish(ReplicatorsJoined(joinedReplicators))

    // Departed replicas: publish to propagators and then terminate.
    val departedReplicators: Set[ActorRef] = departedReplicas.map { r =>
      val replicator = secondaries(r)
      secondaries -= r
      replicator
    }
    system.eventStream.publish(ReplicatorsDeparted(departedReplicators))
    departedReplicators.map { _ ! PoisonPill }
  }

  // Used by primary to process updates (inserts and removes).  Ultimate result, if any,
  // is to send back an OperationAck.
  def processUpdate(k: String, vOpt: Option[String], id: Long, sender: ActorRef): Unit = {
    val requestor = sender
    val ackId = id

    // Future for persisting the update.
    val persistF: Future[Persisted] = Asker.askWithRetries(
      persister, Persist(k,vOpt,ackId), 100 milliseconds, 10).mapTo[Persisted]

    // Future for replicating the update.
    // (Internal actor needs to be updated by subscription whenever replicas come/go.)
    val propagateF: Future[Boolean]
    = Propagator.propagate(k, vOpt, id, secondaries.values.toSet)

    // If persistence and propagator future both succeed in time (they both have 1 second
    // timeouts, implemented internally), then respond to requestor with OperationAck(id);
    // otherwise respond with OperationFailed(id).
    val processedF = for {
      persisted <- persistF map { case p: Persisted => true }
      propagated <- propagateF
    } yield (persisted && propagated)

    processedF.onSuccess { case x => requestor ! OperationAck(ackId) }
    processedF.onFailure { case x => requestor ! OperationFailed(ackId) }
  }

  // Used by secondary to process replication.  Ultimate result, if any, is to send back
  // a SnapshotAck.
  def processSnapshot(snap: Snapshot, sender: ActorRef): Unit = {
    val (k, vOpt, seq) = (snap.key, snap.valueOption, snap.seq)
    val pre = s"--> [$seq] SNAPSHOT $k=$vOpt; "

    if (seq > expectedSeq) {
      myLog(pre + s"$seq exceeds expected $expectedSeq: IGNORING")
    } else if (seq < expectedSeq) {
      myLog(pre + s"$seq behind expected $expectedSeq: AUTO-ACKING")
      sender ! SnapshotAck(k,seq)
    } else {
      myLog(pre + s"$seq is expected value: PERSISTING")
      if (vOpt.isDefined)
        kv += k -> vOpt.get
      else
        kv -= k
      expectedSeq += 1l
      // Persist with retries.  Don't send SnapshotAck until it succeeds--possibly never.
      val requestor = sender
      val ackId = seq
      val persistF: Future[Persisted] = Asker.askWithRetries(
        persister, Persist(k,vOpt,ackId), 100 milliseconds, 10).mapTo[Persisted]
      persistF.onSuccess{ case(Persisted(k,ackId)) => requestor ! SnapshotAck(k,ackId) }
    }
  }



}





