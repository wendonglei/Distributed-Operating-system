package Chord

import scala.math._
import scala.collection.mutable.ArrayBuffer
/**
 * @author Asks
 */

class FingerTable() {
   var startnode:Long = 0 
   //var intevalstart: Int = 0 
   //var intevalend: Int = 0
   var successor: Long = 0
   
   def setstart(startnode: Long){
     this.startnode = startnode
   }
   
   def getstart(): Long = {
     return startnode
   }
   
   /*def setintevalstart(intevalstart: Int){
     this.intevalstart = intevalstart
   }
   
   def getintevalstart(): Int = {
     return intevalstart
   }
   
   def setintevalend(intevalend: Int){
     this.intevalend = intevalend
   }
   
   def getintevalend(): Int = {
     return intevalend
   }*/
   
   def setsuccessor(successor: Long){
     this.successor = successor
   }
   
   def getsuccessor(): Long = {
     return successor
   }
   /*def InitFingerTable(nodeid: Int, dimention: Int, nodelist: ArrayBuffer[Node]){
     var i: Int = 0
     for(i <- 1 to dimention){
       start_list(i) = (nodeid+Math.pow(2, i).toInt) % Math.pow(2, dimention).toInt
       var j: Int= start_list(i)
       while(nodelist(j).getkey().length == 0)
         j+=1
       node_list(i) = nodelist(j).getnodeid()
     }     
   }*/
}