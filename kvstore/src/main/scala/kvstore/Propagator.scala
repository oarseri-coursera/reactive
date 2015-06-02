package kvstore

import akka.actor.{ Props, ActorRef, Actor, ActorContext, Cancellable, Status }
import scala.annotation.tailrec //
import akka.pattern.{ ask, pipe } // used
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.language.postfixOps
import akka.util.Timeout // used

object Propagator {
  // These two are the same, just enforcing semantics.
  case class Start(initReplicators: Set[ActorRef])
  case class ClusterUpdate(newReplicators: Set[ActorRef])
  def props(k: String, vOpt: Option[String], id: Long): Props = Propagator(new Asker(k,vOpt,id))
}

// Given a message representing the equivalent of 'recipient ? msg', uses
// retries to perform the ask up to <maxAttempts> times, at timeout
// intervals of length <interval>.
//
// (Wish you could just count responses instead of individually associating them
// them with individual secondaries...but count can get messed up if a removal
// occurs while waiting for responses; can't be sure if we are counting the old
// secondary or not when we think we have enough responses to cover the new set.
// Could be a good tradeoff to just kill all active persisters whenever the
// cluster grows.)
class Propagator(k: String, vOpt: Option[String], id: Long) extends Actor {

  import Propagator._

  var requestor: ActorRef = _
  var replicators: Set[ActorRef] = Set.empty

  def receive = {
    case Start(initReplicators) =>
      requestor = sender
      processClusterUpdate(initReplicators)
      context.become(processing)
  }

  def processing: Receive = {
    case ClusterUpdate(replicators) =>
      processClusterUpdate(replicators)
    case r: Replicated =>
      markReplicatorDone(sender)
      checkIfDone
  }

  // STARTHERE: Actually, will receive updates as specific "added" and "removed" messages
  // since primary already has to break things down to this granularity anyway.  It should
  // send "added" messages before "removed" messages to be strict.


  def processNewReplicators(newReplicators) = {
    // For newly added, create askers 
    // For newly removed, 
    // For rest, do nothing.

    // Update replicators set.

    checkIfDone
  }

  def checkIfDone = {
    // If we are waiting for zero replications, send true back to requestor.
  }


  def retrying[T](sub: Cancellable, s: ActorRef, r: ActorRef, m: T): Receive = {
    case Retry =>
      if (attempts < maxAttempts) {
        attempts += 1
        r ! m
      } else {
        s ! Status.Failure(new Exception(
             s"Reached retry limit after $attempts attempts"))
        sub.cancel
        context.stop(self)
      }
    case result =>
      s ! result
      sub.cancel
      context.stop(self)
  }
}

