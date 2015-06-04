package kvstore

import akka.actor.{ Props, ActorRef, Actor}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.language.postfixOps

object Propagator {
  case class Start(initReplicators: Set[ActorRef])
  case class ReplicatorsJoined(replicators: Set[ActorRef])
  case class ReplicatorsDeparted(replicators: Set[ActorRef])
  case class ReplicatorFinished(replicator: ActorRef)

  def props(k: String, vOpt: Option[String], id: Long): Props =
    Props(new Propagator(k,vOpt,id))
}

// Sends given replication message to all the replicators in provided initial
// set.  Receives updates about additions or subtractions to the set of
// replicators and reacts according so as to respond to initiating actor with
// "true" message representing success.
//
// (Wish you could just count responses instead of individually associating them
// them with individual replicators...but count can get messed up if a removal
// occurs while waiting for responses; can't be sure if we are counting the old
// replicator or not when we think we have enough responses to cover the new set.
// Could be a good tradeoff to just kill all active propagators whenever the
// cluster grows.)
class Propagator(k: String, vOpt: Option[String], id: Long) extends Actor {
  import Propagator._
  import Replicator._

  var requestor: ActorRef = _
  var pendingReplicators: Set[ActorRef] = Set.empty

  def receive = {
    case Start(initReps) =>
      requestor = sender
      processJoined(initReps)
      checkIfDone  // only relevant if initReps is empty.
      context.become(processing)
  }

  def processing: Receive = {
    case ReplicatorsJoined(reps) =>
      processJoined(reps)
    case ReplicatorsDeparted(reps) =>
      processDeparted(reps)
      checkIfDone
    case ReplicatorFinished(rep) =>
      markReplicatorFinished(rep)
      checkIfDone
  }

  def processJoined(reps: Set[ActorRef]) = {
    // Create asks, update pending replicators set.
    reps.map { rep =>
      val ask: Future[Replicated] =
        Asker.askWithRetries(rep, Replicate(k,vOpt,id), 100 milliseconds, 10).mapTo[Replicated]
      ask.onSuccess { case _ => self ! ReplicatorFinished(rep) }
    }
    pendingReplicators ++= reps
  }

  def processDeparted(reps: Set[ActorRef]) = {
    // Update pending replicators set.  (Not bothering to cancel their futures.)
    pendingReplicators --= reps
  }

  def markReplicatorFinished(rep: ActorRef) = {
    // Update pending replicators set.  (Equivalent to no-op if replicator already departed.)
    pendingReplicators -= rep
  }

  def checkIfDone = {
    if (pendingReplicators.isEmpty) {
      requestor ! true
      // Should I kill my askers?  They should stop on their own once I stop myself...
      context.stop(self)
    }
  }
}

