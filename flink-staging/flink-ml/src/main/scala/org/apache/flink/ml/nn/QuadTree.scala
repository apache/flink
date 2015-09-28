
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.ml.nn.util

import org.apache.flink.ml.math.DenseVector
import org.apache.flink.ml.metrics.distances.SquaredEuclideanDistanceMetric

import scala.collection.mutable.ListBuffer

/**
 * n-dimensional QuadTree data structure; partitions spatial data for faster queries (e.g. KNN query)
 * The skeleton of the data structure was initially based off of the 2D Quadtree found here:
 * http://www.cs.trinity.edu/~mlewis/CSCI1321-F11/Code/src/util/Quadtree.scala
 *
 * Additional methods were added to the class both for efficient KNN queries and generalizing to n-dim.
 *
 * @param minVec
 * @param maxVec
 */

//////////// CHANGE OR ADD CONSTRUCTOR OF THE CLASS TO ALLOW WHOLE SET TO BE INPUT AND CREATE BOUNDING
/////////// BOX AUTOMATICALLY.............................

class QuadTree(minVec:ListBuffer[Double], maxVec:ListBuffer[Double]){
  var maxPerBox = 20

  class Node(c:ListBuffer[Double],L:ListBuffer[Double], var children:ListBuffer[Node]) {

    var objects = new ListBuffer[DenseVector]

    /** for testing purposes only
      *
      * @return
      */
    def getCenterLength(): (ListBuffer[Double], ListBuffer[Double]) = {
      (c, L)
    }

    def contains(obj: DenseVector): Boolean = {
      overlap(obj, 0.0)
    }

    /** Tests if obj is within a radius of the node
      *
      * @param obj
      * @param radius
      * @return
      */

    def overlap(obj: DenseVector, radius: Double): Boolean = {
      var count = 0
      for (i <- 0 to obj.size - 1) {
        if (obj(i) - radius < c(i) + L(i) / 2 && obj(i) + radius > c(i) - L(i) / 2) {
          count += 1
        }
      }

      if (count == obj.size) {
        return true
      } else {
        return false
      }
    }

    /** Tests if obj is near a node:  minDist is defined so that every point in the box
      * has distance to obj greater than minDist
      * (minDist adopted from "Nearest Neighbors Queries" by N. Roussopoulos et al.)
      *
      * @param obj
      * @param radius
      * @return
      */
    def isNear(obj: DenseVector, radius: Double): Boolean = {
      if (minDist(obj) < radius) {
        true
      } else {
        false
      }
    }

    def minDist(obj: DenseVector): Double = {
      var minDist = 0.0
      for (i <- 0 to obj.size - 1) {
        if (obj(i) < c(i) - L(i) / 2) {
          minDist += math.pow(math.abs(obj(i) - c(i) + L(i) / 2), 2)
        } else if (obj(i) > c(i) + L(i) / 2) {
          minDist += math.pow(math.abs(obj(i) - c(i) - L(i) / 2), 2)
        }
      }
      math.sqrt(minDist)
    }


    def getClosestChild(obj:DenseVector): Node = {
      var bestMinDist = this.children.head.minDist(obj)
      var index = 0
      var count = 0
      for (child <- this.children){
          if(child.minDist(obj) < bestMinDist){
            bestMinDist = child.minDist(obj)
            index = count
            count += 1
          }
      }
      this.children(index)
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

    /**  Partitioning of box into equal area/volume sub-boxes
      * TO-DO:  refactor to remove use of .map, probably not needed.
     *
     * @param cPart
     * @param L
     * @param dim
     * @return
     */

    def partitionBox(cPart:ListBuffer[ListBuffer[Double]],L:ListBuffer[Double], dim:Int):ListBuffer[ListBuffer[Double]]=
    {
      if (L.length == 1){

        var cPartDown = cPart.clone()
        //// need to map all centers and shift
        val cPartUp = cPart.map{v => v.patch(dim-1, Seq(v(dim - 1) + L(dim-1)/4), 1)}
        cPartDown = cPartDown.map{v => v.patch(dim-1, Seq(v(dim - 1) - L(dim-1)/4), 1)}

        return cPartDown ++ cPartUp
      }

      var cPartDown = cPart.clone()
      //// need to map all centers and shift
      //val cPartUp = ListBuffer(cPart.head.patch(dim-1, Seq(cPart.head(dim - 1) + L(dim-1)/4), 1)) // more verbose..
      val cPartUp = cPart.map{v => v.patch(dim-1, Seq(v(dim - 1) + L(dim-1)/4), 1)}
      cPartDown = cPartDown.map{v => v.patch(dim-1, Seq(v(dim - 1) - L(dim-1)/4), 1)}

      cPart -= cPart.head
      cPart ++= partitionBox(cPartDown,L.take(dim-1),dim-1)
      cPart ++= partitionBox(cPartUp,L.take(dim-1),dim-1)
    }
  }

  val root = new Node( (minVec, maxVec).zipped.map(_ + _).map(x=>0.5*x),(maxVec, minVec).zipped.map(_ - _),null)

    /**
     *   primitive printing of tree for testing/debugging
     */

  def printTree(){
    printTreeRecur(root)
  }

  def printTreeRecur(n:Node){
    if(n.children != null) {
      for (c <- n.children){
        printTreeRecur(c)
      }
    }else{
      println("printing tree: n.objects " + n.objects)
    }
  }

  /**
   * Recursively adds an object to the tree
   * @param obj
   */

  def insert(obj:DenseVector){
    insertRecur(obj,root)
  }

  private def insertRecur(obj:DenseVector,n:Node) {
    if(n.children==null) {
      if(n.objects.length < maxPerBox )
      {
        n.objects += obj
      }

      else{
        n.makeChildren()
        for (o <- n.objects){
          insertRecur(o, n.children(n.whichChild(o)))
        }
        n.objects.clear()
        insertRecur(obj, n.children(n.whichChild(obj)))
      }
    } else{ /// move down to children
      insertRecur(obj, n.children(n.whichChild(obj)))
    }
  }

  /** REWORDFollowing 3 routines are used to find all objects in minimal bounding box and
    * also all objects in the siblings' minimal bounding box.
    *
    * This capability is used in the KNN query to find k "near" neighbors n_1,...,n_k, from which one computes the
    * max distance D_s to obj.  D_s is then used during the kNN query to find all points
    * within a radius D_s of obj using searchNeighbors
   *
   * @param obj
   * @return
   */


  private def subOne(tuple: (Double,Node)) = tuple._1

  def searchNeighborsSiblingQueue(obj:DenseVector):ListBuffer[DenseVector] = {
    val nodeBuff = new ListBuffer[Node]
    var ret = new ListBuffer[DenseVector]
    searchRecurSiblingQueue(obj, root, nodeBuff)
    var NodeQueue = new scala.collection.mutable.PriorityQueue[(Double, Node)]()(Ordering.by(subOne))
    for (n <- nodeBuff){
      NodeQueue += ( (-n.minDist(obj),n) )  /// Queues are max, so take negative minDist
    }
    var count = 0
    while(count < maxPerBox){
      val dq = NodeQueue.dequeue()
      if (dq._2.objects.nonEmpty){
        ret ++= dq._2.objects
        count += dq._2.objects.length
      }
    }
    ret
}

  private def searchRecurSiblingQueue(obj:DenseVector,n:Node, nodeBuff:ListBuffer[Node]) {
    if(n.children != null) {
      for(child <- n.children; if child.contains(obj)) {
        if (child.children == null) {
          for (c <- n.children) {
            ////// Go down to minimal bounding box
            MinNodes(c,nodeBuff)
          }
        }
        else {
          for(child <- n.children) {
            searchRecurSiblingQueue(obj, child, nodeBuff)
          }
        }
      }
    }
  }

  private def MinNodes(n:Node, nodeBuff:ListBuffer[Node]) {
    if (n.children == null){
      nodeBuff += n
    } else{
      for (c <- n.children) {

          MinNodes(c, nodeBuff)
        }
    }
  }


  /** Finds all objects within a neigiborhood of obj of a specified radius
    * scope is modified from original 2D version in:
    * http://www.cs.trinity.edu/~mlewis/CSCI1321-F11/Code/src/util/Quadtree.scala
    *
    * original version only looks in minimal box; for the KNN Query, we look at
    * all nearby boxes.  Moreover, there is no filtering by radius
   *
   * @param obj
   * @param radius
   * @return
   */

  def searchNeighbors(obj:DenseVector,radius:Double):ListBuffer[DenseVector] = {
    var ret = new ListBuffer[DenseVector]
    searchRecur(obj,radius,root,ret)
    ret
  }

  private def searchRecur(obj:DenseVector,radius:Double,n:Node,ret:ListBuffer[DenseVector]) {
    if(n.children==null) {
      ret ++= n.objects
    } else {
      for(child <- n.children; if child.isNear(obj,radius)) {
        searchRecur(obj, radius, child, ret)
      }
    }
  }

   def distance(a:DenseVector,b:DenseVector):Double = {
    val diffSQ = SquaredEuclideanDistanceMetric().distance(a,b)
    math.sqrt(diffSQ)
  }
}
