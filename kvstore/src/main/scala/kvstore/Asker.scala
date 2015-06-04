package kvstore

import akka.actor.{ Props, ActorRef, Actor, ActorContext, Cancellable, Status }
import akka.pattern.ask
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.language.postfixOps
import akka.util.Timeout

object Asker {
  def askWithRetries[T](recipient: ActorRef, msg: T, interval: FiniteDuration, maxAttempts: Int)(implicit context: ActorContext): Future[Any] = {
    implicit val timeout = Timeout.durationToTimeout(interval * maxAttempts)
    context.actorOf(props(interval, maxAttempts)) ? Ask(recipient, msg)
  }

  case object Retry
  case class Ask[T](recipient: ActorRef, msg: T)
  case class AskerAttempt[T](recipient: ActorRef, msg: T, sender: ActorRef, attempt: Int)
  def props(interval: FiniteDuration, maxAttempts: Int): Props =
    Props(new Asker(interval, maxAttempts))
}

// Given a message representing the equivalent of 'recipient ? msg', uses
// retries to perform the ask up to <maxAttempts> times, at timeout
// intervals of length <interval>.
//
class Asker(interval: FiniteDuration, maxAttempts: Int) extends Actor {
  import Asker._
  import context.dispatcher

  var attempts: Int = 0

  def receive = {
    case Ask(r,m) =>
      val sub = context.system.scheduler.schedule(0 seconds, interval, self, Retry)
      context.become(retrying(sub,sender,r,m))
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

