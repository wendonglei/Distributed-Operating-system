package Chordbonus

import akka.actor.{Actor,ActorRef,ActorSystem,Props,actorRef2Scala,ActorContext,Cancellable}
import scala.collection.mutable.ArrayBuffer
import scala.math._
import akka.routing.RoundRobinRouter
import scala.util.Random
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext
import ExecutionContext.Implicits.global
import scala.concurrent.Future
import akka.pattern.ask
import scala.collection.mutable.Set
import scala.concurrent.duration.Duration
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.Random
//import scala.util.control._

case class getsuccessor(nid:Long)
case class setsuccessor(nid:Long)
case class getpredecessor(nid:Long)
case class setpredecessor(nid:Long)

case class FindKeyLocation(key:Long)
case class find_predecessor(nodeid:Long, key:Long, hop:Int, initflag:Boolean, updateflag: Boolean, itr:Int)
case class GetLocation(hop: Int)
case class FindKey(successor: Long)
case class FindSearchKey(succesor: Long, key: Long)
case class StartSearch(numRequests:Int)
case class FinishWork(nodeid: Long)

case class init_finger_table(finger_list:ArrayBuffer[FingerTable])
case class UpdateFingerTable(nid:Long, i:Int)
case class Getsuccessorlist(nid:Long, numlist:Int, curnum:Int)
case class Setsuccessorlist(nid:Long, numlist:Int)
case class FinishInit(nid:Long)
case object StartSetsuccessor
case object StartGetsuccessor
case object FinishSuccessor

case object StartWork
case object KillNodes
case class NodeFail(node_list:ArrayBuffer[Long])
case class ChangeSuccessor(nid:Long, successor:Long, predecessor: Long)

object Chordbonus extends App{
  override def main(args: Array[String]): Unit={
    var numNodes: Int = args(0).toInt
    var numRequests: Int = args(1).toInt
    var dim: Int = args(2).toInt
    var numError: Int =args(3).toInt
    val system = ActorSystem("gossip")
    val master = system.actorOf(Props(new master(numNodes,numRequests,dim,numError)),name="master")
  }
}
  

