/// QuadTree

//package util

package org.apache.flink.ml.nn.util

import org.apache.flink.ml.math.DenseVector
import org.apache.flink.ml.metrics.distances.SquaredEuclideanDistanceMetric

import scala.collection.mutable.ListBuffer
import collection.mutable


class QuadTree(minVec:ListBuffer[Double], maxVec:ListBuffer[Double]){
  val maxPerBox = 3
  var size = 0
  val dim = minVec.length

  class Node(c:ListBuffer[Double],L:ListBuffer[Double], var children:ListBuffer[Node]){

    //var objects = new ListBuffer[ListBuffer[Double]]
    var objects = new ListBuffer[DenseVector]

    def overlap(obj:DenseVector,radius:Double):Boolean = {
      var count = 0
      for (i <- 0 to obj.size - 1){
        if(obj(i) - radius < c(i) + L(i)/2 && obj(i)+radius > c(i) - L(i)/2){
          count += 1
        }
      }

      if (count == obj.size){
        true
      } else{
        false
      }
    }

    def whichChild(obj:DenseVector):Int = {

      var count = 0
      for (i <- 0 to obj.size - 1){
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

  def insert(ob:DenseVector){
    insertRecur(ob,root)
  }

  private def insertRecur(ob:DenseVector,n:Node) {
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

  def searchNeighbors(obj:DenseVector,radius:Double):mutable.Buffer[DenseVector] = {
    val ret = mutable.Buffer[DenseVector]()
    searchRecur(obj,radius,root,ret)
    ret
  }

  private def searchRecur(obj:DenseVector,radius:Double,n:Node,ret:mutable.Buffer[DenseVector]) {
    if(n.children==null) {
      ret ++= n.objects.filter(o => distanceSq(o,obj) < radius)
    } else {
      for(child <- n.children; if !child.overlap(obj,radius))
        searchRecur(obj,radius,child,ret)
    }
  }

  private def distanceSq(a:DenseVector,b:DenseVector):Double = {
    SquaredEuclideanDistanceMetric().distance(a,b)

    //val diffSQ = (a, b).zipped.map(_ - _).map(x=>math.pow(x,2))
   // math.sqrt(diffSQ.sum)
  }

}
