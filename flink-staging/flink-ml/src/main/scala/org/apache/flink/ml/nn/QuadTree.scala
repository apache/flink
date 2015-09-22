/// QuadTree

//package util

package org.apache.flink.ml.nn.util

import scala.collection.mutable.ListBuffer
import collection.mutable

class QuadTree(minVec:ListBuffer[Double], maxVec:ListBuffer[Double]){
  val maxPerBox = 3
  var size = 0
  val dim = minVec.length

  class Node(c:ListBuffer[Double],L:ListBuffer[Double], var children:ListBuffer[Node]){

    var objects = new ListBuffer[ListBuffer[Double]]

    def overlap(obj:ListBuffer[Double],radius:Double):Boolean = {
      var count = 0
      for (i <- obj.indices){
        if(obj(i) - radius < c(i) + L(i)/2 && obj(i)+radius > c(i) - L(i)/2){
          count += 1
        }
      }

      if (count == obj.length){
        true
      } else{
        false
      }
    }

    def whichChild(obj:ListBuffer[Double]):Int = {
      var count = 0
      for (i <- obj.indices){
        if (obj(i) > c(i)) {
          count += Math.pow(2,i).toInt
        }
      }
      count
    }

    def makeChildren() {
      var cBuff = new ListBuffer[ListBuffer[Double]]
      cBuff += c
      var Childrennodes = new ListBuffer[Node]
      val cPart = partitionBox(cBuff,L,L.length)
      for (i <- cPart.indices){
        Childrennodes = Childrennodes :+ new Node(cPart(i), L.map(x => x/2.0), null)

      }
      children = Childrennodes.clone()
    }

    def partitionBox(cPart:ListBuffer[ListBuffer[Double]],L:ListBuffer[Double], dim:Int):ListBuffer[ListBuffer[Double]]=
    {
      if (L.length == 1){

        var cPart2 = cPart.clone()
        //// need to map all centers and shift
        val cPartMap = cPart.map{v => v.patch(dim-1, Seq(v(dim - 1) + L(dim-1)/4), 1)}
        cPart2 = cPart2.map{v => v.patch(dim-1, Seq(v(dim - 1) - L(dim-1)/4), 1)}

        return cPartMap ++ cPart2
      }

      var cPart2 = cPart.clone()
      //// need to map all centers and shift (UGLY: PROBABLY DON'T NEED A MAP!!!!)
      val cPartMap = cPart.map{v => v.patch(dim-1, Seq(v(dim - 1) + L(dim-1)/4), 1)}
      cPart2 = cPart2.map{v => v.patch(dim-1, Seq(v(dim - 1) - L(dim-1)/4), 1)}

      cPart -= cPart.head
      cPart ++= partitionBox(cPartMap,L.take(dim-1),dim-1)
      cPart ++= partitionBox(cPart2,L.take(dim-1),dim-1)
    }
  }

  val root = new Node( (minVec, maxVec).zipped.map(_ + _).map(x=>0.5*x),(minVec, maxVec).zipped.map(_ - _),null)

  def printTree(){
    printTreeRecur(root)
  }

  def printTreeRecur(n:Node){
    if(n.children !=null) {
      for (c <- n.children){
        printTreeRecur(c)
      }
    }else{
      println("printing tree:  " + n.objects)
    }
  }

  def insert(ob:ListBuffer[Double]){
    insertRecur(ob,root)
  }

  private def insertRecur(ob:ListBuffer[Double],n:Node) {
    if(n.children==null) {
      if(n.objects.length < maxPerBox )
      {
        n.objects += ob
      }

      else{
        n.makeChildren()
        for (o <- n.objects){
          insertRecur(o, n.children(n.whichChild(o)))
        }
        n.objects.clear()
        insertRecur(ob, n.children(n.whichChild(ob)))
      }
    } else{ /// move down to children
      insertRecur(ob, n.children(n.whichChild(ob)))
    }
  }

  def searchNeighbors(obj:ListBuffer[Double],radius:Double):mutable.Buffer[ListBuffer[Double]] = {
    val ret = mutable.Buffer[ListBuffer[Double]]()
    searchRecur(obj,radius,root,ret)
    ret
  }

  private def searchRecur(obj:ListBuffer[Double],radius:Double,n:Node,ret:mutable.Buffer[ListBuffer[Double]]) {
    if(n.children==null) {
      ret ++= n.objects.filter(o => distance(o,obj) < radius)
    } else {
      for(child <- n.children; if !child.overlap(obj,radius))
        searchRecur(obj,radius,child,ret)
    }
  }

  private def distance(a:ListBuffer[Double],b:ListBuffer[Double]):Double = {
    val diffSQ = (a, b).zipped.map(_ - _).map(x=>math.pow(x,2))
    math.sqrt(diffSQ.sum)
  }

}
