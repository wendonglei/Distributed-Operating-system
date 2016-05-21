package Chord
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

class Nodes(nodeid: Long, arbinode: Long, numRequests: Int, dim: Int) extends Actor{
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
  
  for (i <- 0 to dimension-1){
    var finger: FingerTable = new FingerTable
    finger_list += finger
    startnode = (nodeid+Math.pow(2, i).toLong) % Math.pow(2, dimension).toLong
    finger_list(i).setstart(startnode)
  }
  if(arbinode == -1){
    //println("First node is: "+nodeid)
    for(i<-0 to dimension-1){
      finger_list(i).setsuccessor(nodeid)
    }
    predecessor = nodeid
    successor = nodeid
    context.parent ! FinishInit(nodeid)
    
  }
  else{
    initflag = true
    //println("Generate nodeid " + nodeid)
    //println("Arbinode is "+arbinode)
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
      //println(nodeid+" precessor is: "+predecessor)
      //if(nodeid==successor)
        //successor = nid
    }
   
    case FindKeyLocation(kid) => {
      var hop:Int=0
      /*if(kid == nodeid){
        self ! FindKey(nodeid)
        context.parent ! GetLocation(hop)
      }
      else*/
      self ! find_predecessor(nodeid,kid,hop,false,false,0)
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
         //var tempfinger = finger_list(i)
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
          
        //println("Fingerpoint set: "+ nodeid +" "+finger_list(fingerpoint).getstart()+ " setsuccessor "+ finger_list(fingerpoint).getsuccessor())
        if(fingerpoint == 0 && predecessor<0){
          println("Nodeid "+ nodeid +" set finger "+finger_list(fingerpoint).getstart()+ " setsuccessor "+ finger_list(fingerpoint).getsuccessor())
          successor = nid
          context.actorSelection("../"+successor.toString()) ! getpredecessor(nodeid)
          context.actorSelection("../"+successor.toString()) ! setpredecessor(nodeid)
          //context.actorSelection("../"+predecessor.toString()) ! setsuccessor(nodeid)
        }
        if(fingerpoint<dimension-1 && -1<predecessor){
          println("Nodeid "+ nodeid +" set finger "+finger_list(fingerpoint).getstart()+ " setsuccessor "+ finger_list(fingerpoint).getsuccessor())
		  fingerpoint+=1 
		   
          //if(finger_list(fingerpoint).getstart()>nodeid && finger_list(fingerpoint).getstart()<=finger_list(fingerpoint-1).getsuccessor()){
          if((finger_list(fingerpoint-1).getsuccessor()>nodeid && finger_list(fingerpoint).getstart()>nodeid && finger_list(fingerpoint).getstart()<=finger_list(fingerpoint-1).getsuccessor()) ||
              (finger_list(fingerpoint-1).getsuccessor()<nodeid && (finger_list(fingerpoint).getstart()>nodeid || finger_list(fingerpoint).getstart()<=finger_list(fingerpoint-1).getsuccessor()))){
            self ! FindKey(finger_list(fingerpoint-1).getsuccessor())
          }
          else{
            //println("arbinode is: "+arbinode)
            context.actorSelection("../"+arbinode.toString()) ! find_predecessor(nodeid,finger_list(fingerpoint).getstart(),0,true,false,0)
          }
        }
        else if(fingerpoint == dimension-1){
          //context.actorSelection("../"+predecessor.toString()) ! setsuccessor(nodeid)
          for(itr <-0 to dimension -1){
            var updatekid =(nodeid-Math.pow(2, itr).toLong) % Math.pow(2, dimension).toLong
            if(updatekid<0)
              updatekid+=Math.pow(2, dimension).toLong
            self ! find_predecessor(nodeid, updatekid,0,false,true,itr)
            //Thread.sleep(1000)
          }
          initflag = false
          //context.parent ! FinishInit(nodeid)
        }
        //println("Predecessor is "+predecessor)     
        //context.parent ! FinishInit(nodeid)
      }
      /*else{
        if(searchflag == false){
          getmessage+=1
          //println(nodeid+" has found "+nid+" "+getmessage)
          if(getmessage == requests){
            //println(nodeid+" has found all requests")
            context.parent ! FinishWork(nodeid)
          }
        }  
      }*/
    }
    
    case UpdateFingerTable(nid,i)=>{
      //println("Update node "+nodeid+" fingertable "+i+" with "+nid)
      if((nid>nodeid && (nid<finger_list(i).getsuccessor() || finger_list(i).getsuccessor()<=nodeid)) ||
          (nid<nodeid && finger_list(i).getsuccessor()<=nodeid && finger_list(i).getsuccessor()>nid)){
      //if((nid>=finger_list(i).getstart() && (nid<finger_list(i).getsuccessor() || finger_list(i).getsuccessor()<=nodeid)) ||
        //(nid<=finger_list(i).getstart() && finger_list(i).getsuccessor()<=nodeid && finger_list(i).getsuccessor()>nid)){
        finger_list(i).setsuccessor(nid)
        println("Update nodeid "+nodeid+" finger "+finger_list(i).getstart()+" successor is "+finger_list(i).getsuccessor())
        context.actorSelection("../"+predecessor.toString()) ! UpdateFingerTable(nid,i)
      }
      else{
        if(i==dimension-1){
          for(i<-0 to dimension-1){
            //println("nodeid: "+nodeid+" "+finger_list(i).getstart()+" has finger node: "+finger_list(i).getsuccessor())
          }
         context.parent ! FinishInit(nid)
        }
      }
    }
    
    case StartSearch(numRequests) =>{
      requests = numRequests
      sendrequests+=1
      if(sendrequests<= numRequests){  
        var rand = Random.nextInt(Math.pow(2, dimension).toInt)
        self ! FindKeyLocation(rand)
        //println(nodeid+" has to search "+rand)
        context.system.scheduler.scheduleOnce(100 milliseconds, self, StartSearch(numRequests))
        //self ! FindKeyLocation(rand)
      }
    }
    
    case FindSearchKey(nid,kid)=>{
      getmessage+=1
      println("Nodeid = "+nodeid+" found aim key = "+kid+" on destination nodeid = "+nid)
      if(getmessage == requests){
        //println(nodeid+" has found all requests")
        context.parent ! FinishWork(nodeid)
      }
    }
  }
}