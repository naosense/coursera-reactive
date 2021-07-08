/**
  * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
  */
package actorbintree

import akka.actor.*
import scala.collection.immutable.Queue

object BinaryTreeSet:

  trait Operation:
    def requester: ActorRef

    def id: Int

    def elem: Int

  trait OperationReply:
    def id: Int

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

  /** Request to perform garbage collection */
  case object GC

  /** Holds the answer to the Contains request with identifier `id`.
    * `result` is true if and only if the element is present in the tree.
    */
  case class ContainsResult(id: Int, result: Boolean) extends OperationReply

  /** Message to signal successful completion of an insert or remove operation. */
  case class OperationFinished(id: Int) extends OperationReply

class BinaryTreeSet extends Actor :

  import BinaryTreeSet.*
  import BinaryTreeNode.*

  def createRoot: ActorRef = context.actorOf(BinaryTreeNode.props(0, initiallyRemoved = true))

  var root = createRoot

  // optional (used to stash incoming operations during garbage collection)
  var pendingQueue = Queue.empty[Operation]

  // optional
  def receive = normal

  // optional
  /** Accepts `Operation` and `GC` messages. */
  val normal: Receive = {
    case operation: Operation => root ! operation
    case GC =>
      val newRoot = createRoot
      context.become(garbageCollecting(newRoot))
      root ! CopyTo(newRoot)
  }

  // optional

  /** Handles messages while garbage collection is performed.
    * `newRoot` is the root of the new binary tree where we want to copy
    * all non-removed elements into.
    */
  def garbageCollecting(newRoot: ActorRef): Receive = {
    case msg: Operation => pendingQueue = pendingQueue.enqueue(msg)
    case GC =>
    case CopyFinished =>
      root ! PoisonPill
      root = newRoot

      pendingQueue.foreach(root ! _)
      pendingQueue = Queue.empty

      context.become(normal)
  }

object BinaryTreeNode:
  trait Position

  case object Left extends Position
  case object Right extends Position

  case class CopyTo(treeNode: ActorRef)
  /**
    * Acknowledges that a copy has been completed. This message should be sent
    * from a node to its parent, when this node and all its children nodes have
    * finished being copied.
    */
  case object CopyFinished

  def props(elem: Int, initiallyRemoved: Boolean) = Props(classOf[BinaryTreeNode], elem, initiallyRemoved)

class BinaryTreeNode(val elem: Int, initiallyRemoved: Boolean) extends Actor :

  import BinaryTreeNode.*
  import BinaryTreeSet.*

  var subtrees = Map[Position, ActorRef]()
  var removed = initiallyRemoved

  // optional
  def receive = normal

  // optional
  /** Handles `Operation` messages and `CopyTo` requests. */
  val normal: Receive = {
    case msg@Insert(requester, id, elem) =>
      if (msg.elem > this.elem) {
        subtrees.get(Right) match {
          case Some(right) => right ! msg
          case None =>
            val node = context.actorOf(BinaryTreeNode.props(elem, initiallyRemoved = false))
            subtrees += (Right -> node)
            requester ! OperationFinished(id)
        }
      } else if (msg.elem < this.elem) {
        subtrees.get(Left) match {
          case Some(left) => left ! msg
          case None =>
            val node = context.actorOf(BinaryTreeNode.props(elem, initiallyRemoved = false))
            subtrees += (Left -> node)
            requester ! OperationFinished(id)
        }
      } else {
        // do nothing
        if (removed) removed = false
        requester ! OperationFinished(id)
      }

    case msg@Contains(requester, id, elem) =>
      if (msg.elem > this.elem) {
        subtrees.get(Right) match {
          case Some(right) => right ! msg
          case None => requester ! ContainsResult(id, result = false)
        }
      } else if (msg.elem < this.elem) {
        subtrees.get(Left) match {
          case Some(left) => left ! msg
          case None => requester ! ContainsResult(id, result = false)
        }
      } else {
        requester ! ContainsResult(id, result = !removed)
      }

    case msg@Remove(requester, id, elem) =>
      if (msg.elem > this.elem) {
        subtrees.get(Right) match {
          case Some(right) => right ! msg
          case None => requester ! OperationFinished(id)
        }
      } else if (msg.elem < this.elem) {
        subtrees.get(Left) match {
          case Some(left) => left ! msg
          case None => requester ! OperationFinished(id)
        }
      } else {
        removed = true
        requester ! OperationFinished(id)
      }

    case CopyTo(newRoot) =>
      if (elem == 0 && removed) {
        self ! OperationFinished(-1)
      }
      if (!removed) {
        newRoot ! Insert(self, 0, elem)
      }

      subtrees.values foreach (_ ! CopyTo(newRoot))

      if (removed && subtrees.isEmpty) {
        sender() ! CopyFinished
      } else {
        context.become(copying(subtrees.values.toSet, insertConfirmed = removed))
      }
  }

  // optional

  /** `expected` is the set of ActorRefs whose replies we are waiting for,
    * `insertConfirmed` tracks whether the copy of this node to the new tree has been confirmed.
    */
  def copying(expected: Set[ActorRef], insertConfirmed: Boolean): Receive = {
    case OperationFinished(_) =>
      if (expected.isEmpty) {
        context.parent ! CopyFinished
        self ! PoisonPill
      } else {
        context.become(copying(expected, insertConfirmed = true))
      }
    case CopyFinished =>
      val newExpected = expected - sender()
      if (newExpected.isEmpty && insertConfirmed) {
        context.parent ! CopyFinished
        self ! PoisonPill
      } else {
        context.become(copying(newExpected, insertConfirmed))
      }
  }


