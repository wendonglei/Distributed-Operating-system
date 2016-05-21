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

class Nodes(nodeid: Long, arbinode: Long, numNodes: Int, numRequests: Int, dim: Int, numError: Int) extends Actor{
  var finger_list: ArrayBuffer[FingerTable]=new ArrayBuffer[FingerTable]()
  var predecessor: Long = -1
  var successor: Long = -1
  var startnode: Long = 0
  var dimension = dim
  var fingerpoint: Int = 0
  var initflag = false
  var requests: Int = 0
  var sendrequests: Int = 0
  var getmessage: Int = 0
  
  var numerror = numError
  var successor_list: Array[Long] = new Array[Long](numerror+1)
  
  for (i <- 0 to dimension-1){
    var finger: FingerTable = new FingerTable
    finger_list += finger
    startnode = (nodeid+Math.pow(2, i).toLong) % Math.pow(2, dimension).toLong
    finger_list(i).setstart(startnode)
  }
  if(arbinode == -1){
    for(i<-0 to dimension-1){
      finger_list(i).setsuccessor(nodeid)
    }
    predecessor = nodeid
    successor = nodeid
    context.parent ! FinishInit(nodeid)
    
  }
  else{
    initflag = true
    context.actorSelection("../"+arbinode.toString()) ! find_predecessor(nodeid,finger_list(0).getstart(),0,true,false, 0)
  }
  
  def receive = {
    
    case  getsuccessor(nid) =>{
      //context.actorSelection("../"+nid.toString()) ! setpredecessor(successor)
    }
   
    case setsuccessor(nid) =>{
      successor = nid
    } 
    
    case getpredecessor(nid) =>{
      context.actorSelection("../"+nid.toString()) ! setpredecessor(predecessor)
      
    }
   
    case setpredecessor(nid) =>{
      predecessor = nid
      self ! FindKey(nid)
      context.actorSelection("../"+predecessor.toString()) ! setsuccessor(nodeid)
    }
    
    case Getsuccessorlist(nid, numlist, curnum) =>{
      var cur=curnum
      if(numlist>cur){
        cur += 1
        context.actorSelection("../"+successor.toString()) ! Getsuccessorlist(nid,numlist,cur)
      }
      else{
        context.actorSelection("../"+nid.toString()) ! Setsuccessorlist(nodeid,numlist)
      }
    }
    
    case Setsuccessorlist(nid, numlist) =>{
      successor_list(numlist) = nid  
      if(numlist == numerror){
        for(i<-0 to successor_list.length-1)
          println("Nodeid "+nodeid+" has "+i+" successorlist "+successor_list(i))
        context.parent ! FinishSuccessor
      }
        
    }
   
    case FindKeyLocation(kid) => {
      var hop:Int=0
      /*if(kid == nodeid){
        self ! FindKey(nodeid)
        context.parent ! GetLocation(hop)
      }
      else*/
      self ! find_predecessor(nodeid,kid,hop,false,false,0)
      //context.system.scheduler.scheduleOnce(900 milliseconds, self, find_predecessor(nodeid,kid,hop,false,false,0))
    }
   
   case find_predecessor(nid,kid,hop,initflag,updateflag,itr) => {
      val loop = new Breaks
      if((nodeid<successor&&kid>nodeid && kid<=successor) || (nodeid>successor && (kid>nodeid || kid<=successor)) || (nodeid==successor)){
        if(updateflag == false && initflag == false){
          context.actorSelection("../"+nid.toString()) ! FindSearchKey(successor,kid)
          context.parent ! GetLocation(hop)
        }
        else{
          if(initflag==true)
            context.actorSelection("../"+nid.toString()) ! FindKey(successor)
          if(updateflag == true){
            if( successor == kid)
              context.actorSelection("../"+successor.toString()) ! UpdateFingerTable(nid, itr)
            else
              self ! UpdateFingerTable(nid, itr)
          }
            
        }
      }
      else{
        loop.breakable{
          for(i<-finger_list.size-1 to 0 by -1){
            if((finger_list(i).getsuccessor()>nodeid && (finger_list(i).getsuccessor()<kid || kid<=nodeid)) 
                || (finger_list(i).getsuccessor()<nodeid && kid<=nodeid && kid>finger_list(i).getsuccessor())){  
              context.actorSelection("../"+finger_list(i).getsuccessor().toString()) ! find_predecessor(nid,kid,hop+1,initflag,updateflag,itr)
              loop.break;
            }
          }
        }
      }
    }

   
    case FindKey(nid) => {
      if(initflag == true){
        if(predecessor>=0 && fingerpoint==0){}
        else{
          finger_list(fingerpoint).setsuccessor(nid)
        }
          
        if(fingerpoint == 0 && predecessor<0){
          successor = nid
          context.actorSelection("../"+successor.toString()) ! getpredecessor(nodeid)
          context.actorSelection("../"+successor.toString()) ! setpredecessor(nodeid)
        }
        if(fingerpoint<dimension-1 && -1<predecessor){
          fingerpoint+=1 
          
          if((finger_list(fingerpoint-1).getsuccessor()>nodeid && finger_list(fingerpoint).getstart()>nodeid && finger_list(fingerpoint).getstart()<=finger_list(fingerpoint-1).getsuccessor()) ||
              (finger_list(fingerpoint-1).getsuccessor()<nodeid && (finger_list(fingerpoint).getstart()>nodeid || finger_list(fingerpoint).getstart()<=finger_list(fingerpoint-1).getsuccessor()))){
            self ! FindKey(finger_list(fingerpoint-1).getsuccessor())
          }
          else{
            context.actorSelection("../"+arbinode.toString()) ! find_predecessor(nodeid,finger_list(fingerpoint).getstart(),0,true,false,0)
          }
        }
        else if(fingerpoint == dimension-1){
          for(itr <-0 to dimension-1){
            var updatekid =(nodeid-Math.pow(2, itr).toInt) % Math.pow(2, dimension).toInt
            if(updatekid<0)
              updatekid+=Math.pow(2, dimension).toInt
            self ! find_predecessor(nodeid, updatekid,0,false,true,itr)
          }
          initflag = false
        }
      }
    }
    
    case UpdateFingerTable(nid,i)=>{
      if((nid>nodeid && (nid<finger_list(i).getsuccessor() || finger_list(i).getsuccessor()<=nodeid)) ||
          (nid<nodeid && finger_list(i).getsuccessor()<=nodeid && finger_list(i).getsuccessor()>nid)){
        finger_list(i).setsuccessor(nid)
        context.actorSelection("../"+predecessor.toString()) ! UpdateFingerTable(nid,i)
      }
      else{
        if(i==dimension-1){
          context.parent ! FinishInit(nid)
        }
      }
    }
    
    case StartGetsuccessor =>{
      for(i<-0 to numerror){
        context.actorSelection("../"+successor.toString()) ! Getsuccessorlist(nodeid, i, 0)
      }
      
    }
    
    case StartSearch(numRequests) =>{
      requests = numRequests
      sendrequests+=1
      if(sendrequests<= numRequests){   
        var rand = Random.nextInt(Math.pow(2, dimension).toInt)
        //self ! FindKeyLocation(rand)
        println(nodeid+" has to search "+rand)
        context.system.scheduler.scheduleOnce(800 milliseconds, self, FindKeyLocation(rand))
        context.system.scheduler.scheduleOnce(800 milliseconds, self, StartSearch(numRequests))
        //self ! StartSearch(numRequests)
      }
    }
    
    case FindSearchKey(nid,kid)=>{
      getmessage+=1
      println("Nodeid = "+nodeid+" found aim key = "+kid+" on destination nodeid = "+nid)
      if(getmessage == requests){
        context.parent ! FinishWork(nodeid)
      }
    }
    
    case NodeFail(node_list) => {
      for(i<-0 to node_list.length-1){
        if(node_list(i) != nodeid)
          context.actorSelection("../"+node_list(i).toString()) ! ChangeSuccessor(nodeid, successor, predecessor)
      }
      context.stop(self)
    }
    
    case ChangeSuccessor(nid, suc, pre) => {
      if(successor == nid){
        println(nodeid+" set new successor "+suc)
        successor = suc
      }
      if(predecessor == nid){
        println(nodeid+" set new predecessor "+pre)
        predecessor = pre
      }
      for(i<-0 to successor_list.length-1){
        if(successor_list(i) == nid)
          successor_list(i) = -1
      }
      for(i<-0 to finger_list.length-1){
        if(finger_list(i).getsuccessor() == nid){
          finger_list(i).setsuccessor(suc)
          println(nodeid+" change finger table "+finger_list(i).getstart()+" with "+finger_list(i).getsuccessor())
        }   
      }
    }
  }
}