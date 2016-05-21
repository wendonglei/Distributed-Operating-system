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
import scala.util.control._

/**
 * @author Asks
 */
class master(numNodes: Int, numRequests: Int, dim: Int, numError: Int) extends Actor{
  var numWorkers:Int=0
  val system = ActorSystem("Node")
  var node_list:ArrayBuffer[Long]=ArrayBuffer[Long]() 
  var dimension=dim
  var hop: Int = 0
  var startime: Long = 0
  var numerror = numError
  
  var numsetsuc: Int = 0
  
  var finger_list:ArrayBuffer[FingerTable]=ArrayBuffer[FingerTable]() 
  var nodeid: Int=Random.nextInt(Math.pow(2, dimension).toInt)  
  //nodeid = 2
  println("Add node id: "+nodeid)
  context.actorOf(Props(new Nodes(nodeid, -1, numNodes, numRequests,dimension,numerror)),nodeid.toString())    

  
  def receive ={
    case FinishInit(nodeid) =>{
      node_list += nodeid
      numWorkers += 1
      if(numWorkers < numNodes){
        var nid:Int= Random.nextInt(Math.pow(2, dimension).toInt)
        while(node_list.contains(nid))
          nid = Random.nextInt(Math.pow(2, dimension).toInt)
        var rand : Int = Random.nextInt(node_list.length)
        var arbinode: Long = node_list(rand)
        println("Add node id: "+nid)
        //nid = 3
        //println("Get arbinode "+arbinode)
        context.actorOf(Props(new Nodes(nid, arbinode, numNodes, numRequests, dimension,numerror)),nid.toString())
      }
      if(numWorkers == numNodes){
        self ! StartSetsuccessor
        /*startime = System.currentTimeMillis
        for(i<-0 to node_list.length-1){
          context.actorSelection(node_list(i).toString()) ! StartSearch(numRequests)
        }*/
      }
    }
    
    case GetLocation(hop) => {
      this.hop += hop
    }
    
    case FinishWork(nodeid) =>{
      node_list -= nodeid
      if(node_list.length == 0){
        var Hop:Float=hop
        var avghops:Float=Hop/(numRequests * numNodes)
        println("All nodes finished work")
        println("Average number of hops for one Requst is: "+avghops)
        println("Time taken "+(System.currentTimeMillis-startime))   
        context.stop(self)
        context.system.shutdown
        System.exit(0)   
      }
    }
    
    case StartSetsuccessor =>{
      println("Start set successor list")
      for(i<-0 to numNodes-1)
        context.actorSelection(node_list(i).toString()) ! StartGetsuccessor
    }
    
    case FinishSuccessor =>{
      numsetsuc += 1
      if(numsetsuc == numNodes){
        println("\nAll nodes finished set successor list")
        self ! StartWork
      }    
    }
    
    case StartWork =>{
      println("\nAll nodes finished init")
      startime=System.currentTimeMillis
      for(i<-0 to numNodes-1){
        context.actorSelection(node_list(i).toString()) ! StartSearch(numRequests)
      }
      self ! KillNodes
    }
    
    case KillNodes =>{
      if(numerror>0){
        var randkill = Random.nextInt(node_list.length)
        println("Kill node "+node_list(randkill))
        context.actorSelection(node_list(randkill).toString()) ! NodeFail(node_list) 
        node_list -= node_list(randkill)
        numerror-=1
        context.system.scheduler.scheduleOnce(1000 milliseconds, self, KillNodes)
      }
      
    }
  }
}