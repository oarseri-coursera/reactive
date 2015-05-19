/**
 * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */
package actorbintree

import akka.actor._
import scala.collection.immutable.Queue

object BinaryTreeSet {

  trait Operation {
    def requester: ActorRef
    def id: Int
    def elem: Int
  }

  trait OperationReply {
    def id: Int
  }

  /** Request with identifier `id` to insert an element `elem` into the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Insert(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to check whether an element `elem` is present
    * in the tree. The actor at reference `requester` should be notified when
    * this operation is completed.
    */
  case class Contains(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to remove the element `elem` from the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Remove(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request to perform garbage collection*/
  case object GC

  /** Holds the answer to the Contains request with identifier `id`.
    * `result` is true if and only if the element is present in the tree.
    */
  case class ContainsResult(id: Int, result: Boolean) extends OperationReply
  
  /** Message to signal successful completion of an insert or remove operation. */
  case class OperationFinished(id: Int) extends OperationReply

}


class BinaryTreeSet extends Actor {
  import BinaryTreeSet._
  import BinaryTreeNode._

  def createRoot: ActorRef = context.actorOf(BinaryTreeNode.props(0, initiallyRemoved = true))

  var root = createRoot

  // optional
  var pendingQueue = Queue.empty[Operation]

  // optional
  def receive = normal

  // optional
  /** Accepts `Operation` and `GC` messages. */
  val normal: Receive = {
    case o: Operation =>  root ! o
    case GC => {
      val newRoot: ActorRef = createRoot
      root ! CopyTo(newRoot)
      context.become(garbageCollecting(newRoot))
    }
    case x => println("BinaryTreeSet received unintelligible message, ignoring: " + x)
  }

  // optional
  /** Handles messages while garbage collection is performed.
    * `newRoot` is the root of the new binary tree where we want to copy
    * all non-removed elements into.
    */
  def garbageCollecting(newRoot: ActorRef): Receive = {
    case o: Operation => pendingQueue = pendingQueue.enqueue(o)
    case GC => {}
    case CopyFinished => {
      root ! PoisonPill              // Stop all actors in the old tree.
      root = newRoot                 // Switch to new tree.

      // Take care of accumulated messages in queue.
      context.become(normal)
      pendingQueue.foreach( root ! _ )
      pendingQueue = Queue.empty[Operation]
    }
    case x => println("BinaryTreeSet received unintelligible message during gc, ignoring: " + x)
  }

}

object BinaryTreeNode {
  trait Position

  case object Left extends Position
  case object Right extends Position
  case object Self extends Position

  case class CopyTo(treeNode: ActorRef)
  case object CopyFinished

  def props(elem: Int, initiallyRemoved: Boolean) = Props(classOf[BinaryTreeNode], elem, initiallyRemoved)
}

class BinaryTreeNode(val elem: Int, initiallyRemoved: Boolean) extends Actor {
  import BinaryTreeNode._
  import BinaryTreeSet._

  /** Evaluates to position (current node, left child, or right child) for handling given element. */
  def relevantPos(el: Int): Position = 
    if (el < elem)
      Left
    else if (el == elem)
      Self
    else
      Right

  /** Forwards msg to given position if there is a child there; otherwise, excutes provided code block. */
  def forwardOrElse(pos: Position, msg: Any)(f: => Unit): Unit =
    if (subtrees.contains(pos))
      subtrees(pos) ! msg
    else
      f

  var subtrees = Map[Position, ActorRef]()
  var removed = initiallyRemoved

  // optional
  def receive = normal

  // optional
  /** Handles `Operation` messages and `CopyTo` requests. */
  val normal: Receive = {
    case op @ Insert(rq,id,el) => relevantPos(el) match {
      case Self => removed = false; rq ! OperationFinished(id)
      case pos @ (Left | Right) => forwardOrElse(pos, op) {
        val newChild: ActorRef = context.actorOf(props(el,false))
        subtrees += pos -> newChild
        rq ! OperationFinished(id)
      }
    }
    case op @ Contains(rq,id,el) => relevantPos(el) match {
      case Self => rq ! ContainsResult(id, !removed)
      case pos @ (Left | Right) => forwardOrElse(pos, op)(rq ! ContainsResult(id, false))
    }
    case op @ Remove(rq,id,el) => relevantPos(el) match {
      case Self => removed = true; rq ! OperationFinished(id)
      case pos @ (Left | Right) => forwardOrElse(pos,op)(rq ! OperationFinished(id))
    }
    case msg @ CopyTo(newRoot) => {
      val children: Set[ActorRef] = subtrees.values.toSet
      children.map { _ ! msg }
      val canSkip = removed  // Nodes that are marked as removed should not be copied over.
      if (!canSkip) newRoot ! Insert(self,elem,elem)  // Use elem as id.
      context.become(copying(children, canSkip))
    }
    case x => println(s"BinaryTreeNode containing ${elem} received unintelligible message, ignoring: ${x}")
  }

  // optional
  /** `expected` is the set of ActorRefs whose replies we are waiting for,
    * `insertConfirmed` tracks whether the copy of this node to the new tree has been confirmed.
    */
  def copying(expected: Set[ActorRef], insertConfirmed: Boolean): Receive = {
    if (expected.isEmpty && insertConfirmed) {
      context.parent ! CopyFinished
      normal
    } else {
      case OperationFinished(id) => if (id == elem) context.become(copying(expected, true))
      case CopyFinished => context.become(copying(expected - sender, insertConfirmed))
    }
  }


}
