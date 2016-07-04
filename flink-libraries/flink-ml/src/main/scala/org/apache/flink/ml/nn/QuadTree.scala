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

package org.apache.flink.ml.nn

import org.apache.flink.ml.math.Breeze._
import org.apache.flink.ml.math.Vector
import org.apache.flink.ml.metrics.distances._

import scala.annotation.tailrec
import scala.collection.mutable

/** n-dimensional QuadTree data structure; partitions
  * spatial data for faster queries (e.g. KNN query)
  * The skeleton of the data structure was initially
  * based off of the 2D Quadtree found here:
  * http://www.cs.trinity.edu/~mlewis/CSCI1321-F11/Code/src/util/Quadtree.scala
  *
  * Many additional methods were added to the class both for
  * efficient KNN queries and generalizing to n-dim.
  *
  * @param minVec     vector of the corner of the bounding box with smallest coordinates
  * @param maxVec     vector of the corner of the bounding box with smallest coordinates
  * @param distMetric metric, must be Euclidean or squareEuclidean
  * @param maxPerBox  threshold for number of points in each box before slitting a box
  */
class QuadTree(
  minVec: Vector,
  maxVec: Vector,
  distMetric: DistanceMetric,
  maxPerBox: Int) {

  class Node(
    center: Vector,
    width: Vector,
    var children: Seq[Node]) {

    val nodeElements = new mutable.ListBuffer[Vector]

    /** for testing purposes only; used in QuadTreeSuite.scala
      *
      * @return center and width of the box
      */
    def getCenterWidth(): (Vector, Vector) = (center, width)

    /** Tests whether the queryPoint is in the node, or a child of that node
      *
      * @param queryPoint a point to test
      * @return whether the given point is in the node, or a child of this node
      */
    def contains(queryPoint: Vector): Boolean = overlap(queryPoint, 0.0)

    /** Tests if queryPoint is within a radius of the node
      *
      * @param queryPoint a point to test
      * @param radius     radius of test area
      * @return whether the given point is in the area
      */
    def overlap(queryPoint: Vector, radius: Double): Boolean = {
      (0 until queryPoint.size).forall { i =>
        (queryPoint(i) - radius < center(i) + width(i) / 2) &&
          (queryPoint(i) + radius > center(i) - width(i) / 2)
      }
    }

    /** Tests if queryPoint is near a node
      *
      * @param queryPoint a point to test
      * @param radius     radius of covered area
      */
    def isNear(queryPoint: Vector, radius: Double): Boolean = minDist(queryPoint) < radius

    /** minDist is defined so that every point in the box has distance to queryPoint greater
      * than minDist (minDist adopted from "Nearest Neighbors Queries" by N. Roussopoulos et al.)
      *
      * @param queryPoint
      */
    def minDist(queryPoint: Vector): Double = {
      val minDist = (0 until queryPoint.size).map { i =>
        if (queryPoint(i) < center(i) - width(i) / 2) {
          math.pow(queryPoint(i) - center(i) + width(i) / 2, 2)
        } else if (queryPoint(i) > center(i) + width(i) / 2) {
          math.pow(queryPoint(i) - center(i) - width(i) / 2, 2)
        } else {
          0
        }
      }.sum

      distMetric match {
        case _: EuclideanDistanceMetric => math.sqrt(minDist)
        case _: SquaredEuclideanDistanceMetric => minDist
        case _ => throw new IllegalArgumentException(s" Error: metric must be" +
          s" Euclidean or SquaredEuclidean!")
      }
    }

    /** Finds which child queryPoint lies in. node.children is a Seq[Node], and
      * [[whichChild]] finds the appropriate index of that Seq.
      *
      * @param queryPoint
      * @return
      */
    def whichChild(queryPoint: Vector): Int = {
      (0 until queryPoint.size).map { i =>
        if (queryPoint(i) > center(i)) {
          scala.math.pow(2, queryPoint.size - 1 - i).toInt
        } else {
          0
        }
      }.sum
    }

    /** Makes children nodes by partitioning the box into equal sub-boxes
      * and adding a node for each sub-box
      */
    def makeChildren() {
      val centerClone = center.copy
      val cPart = partitionBox(centerClone, width)
      val mappedWidth = 0.5 * width.asBreeze
      children = cPart.map(p => new Node(p, mappedWidth.fromBreeze, null))
    }

    /** Recursive function that partitions a n-dim box by taking the (n-1) dimensional
      * plane through the center of the box keeping the n-th coordinate fixed,
      * then shifting it in the n-th direction up and down
      * and recursively applying partitionBox to the two shifted (n-1) dimensional planes.
      *
      * @param center the center of the box
      * @param width  a vector of lengths of each dimension of the box
      * @return
      */
    def partitionBox(center: Vector, width: Vector): Seq[Vector] = {
      @tailrec
      def partitionHelper(box: Seq[Vector], dim: Int): Seq[Vector] = {
        if (dim >= width.size) {
          box
        } else {
          val newBox = box.flatMap { vector =>
            val (up, down) = (vector.copy, vector)
            up.update(dim, up(dim) - width(dim) / 4)
            down.update(dim, down(dim) + width(dim) / 4)

            Seq(up, down)
          }
          partitionHelper(newBox, dim + 1)
        }
      }
      partitionHelper(Seq(center), 0)
    }
  }


  val root = new Node(((minVec.asBreeze + maxVec.asBreeze) * 0.5).fromBreeze,
    (maxVec.asBreeze - minVec.asBreeze).fromBreeze, null)

  /** Prints tree for testing/debugging */
  def printTree(): Unit = {
    def printTreeRecur(node: Node) {
      if (node.children != null) {
        for (c <- node.children) {
          printTreeRecur(c)
        }
      } else {
        println("printing tree: n.nodeElements " + node.nodeElements)
      }
    }

    printTreeRecur(root)
  }

  /** Recursively adds an object to the tree
    *
    * @param queryPoint an object which is added
    */
  def insert(queryPoint: Vector) = {
    def insertRecur(queryPoint: Vector, node: Node): Unit = {
      if (node.children == null) {
        if (node.nodeElements.length < maxPerBox) {
          node.nodeElements += queryPoint
        } else {
          node.makeChildren()
          for (o <- node.nodeElements) {
            insertRecur(o, node.children(node.whichChild(o)))
          }
          node.nodeElements.clear()
          insertRecur(queryPoint, node.children(node.whichChild(queryPoint)))
        }
      } else {
        insertRecur(queryPoint, node.children(node.whichChild(queryPoint)))
      }
    }

    insertRecur(queryPoint, root)
  }

  /** Used to zoom in on a region near a test point for a fast KNN query.
    * This capability is used in the KNN query to find k "near" neighbors n_1,...,n_k, from
    * which one computes the max distance D_s to queryPoint.  D_s is then used during the
    * kNN query to find all points within a radius D_s of queryPoint using searchNeighbors.
    * To find the "near" neighbors, a min-heap is defined on the leaf nodes of the leaf
    * nodes of the minimal bounding box of the queryPoint. The priority of a leaf node
    * is an appropriate notion of the distance between the test point and the node,
    * which is defined by minDist(queryPoint),
    *
    * @param queryPoint a test point for which the method finds the minimal bounding
    *                   box that queryPoint lies in and returns elements in that boxes
    *                   siblings' leaf nodes
    */
  def searchNeighborsSiblingQueue(queryPoint: Vector): mutable.ListBuffer[Vector] = {
    val ret = new mutable.ListBuffer[Vector]
    // edge case when the main box has not been partitioned at all
    if (root.children == null) {
      root.nodeElements.clone()
    } else {
      val nodeQueue = new mutable.PriorityQueue[(Double, Node)]()(Ordering.by(x => x._1))
      searchRecurSiblingQueue(queryPoint, root, nodeQueue)

      var count = 0
      while (count < maxPerBox) {
        val dq = nodeQueue.dequeue()
        if (dq._2.nodeElements.nonEmpty) {
          ret ++= dq._2.nodeElements
          count += dq._2.nodeElements.length
        }
      }
      ret
    }
  }

  /**
    *
    * @param queryPoint point under consideration
    * @param node       node that queryPoint lies in
    * @param nodeQueue  defined in searchSiblingQueue, this stores nodes based on their
    *                   distance to node as defined by minDist
    */
  private def searchRecurSiblingQueue(
    queryPoint: Vector,
    node: Node,
    nodeQueue: mutable.PriorityQueue[(Double, Node)]
  ): Unit = {
    if (node.children != null) {
      for (child <- node.children; if child.contains(queryPoint)) {
        if (child.children == null) {
          for (c <- node.children) {
            minNodes(queryPoint, c, nodeQueue)
          }
        } else {
          searchRecurSiblingQueue(queryPoint, child, nodeQueue)
        }
      }
    }
  }

  /** Goes down to minimal bounding box of queryPoint, and add elements to nodeQueue
    *
    * @param queryPoint point under consideration
    * @param node       node that queryPoint lies in
    * @param nodeQueue  [[mutable.PriorityQueue]] that stores all points in minimal bounding box
    *                   of queryPoint
    */
  private def minNodes(
    queryPoint: Vector,
    node: Node,
    nodeQueue: mutable.PriorityQueue[(Double, Node)]
  ): Unit = {
    if (node.children == null) {
      nodeQueue += ((-node.minDist(queryPoint), node))
    } else {
      for (c <- node.children) {
        minNodes(queryPoint, c, nodeQueue)
      }
    }
  }

  /** Finds all objects within a neighborhood of queryPoint of a specified radius
    * scope is modified from original 2D version in:
    * http://www.cs.trinity.edu/~mlewis/CSCI1321-F11/Code/src/util/Quadtree.scala
    *
    * original version only looks in minimal box; for the KNN Query, we look at
    * all nearby boxes. The radius is determined from searchNeighborsSiblingQueue
    * by defining a min-heap on the leaf nodes
    *
    * @param queryPoint a point which is center
    * @param radius     radius of scope
    * @return all points within queryPoint with given radius
    */
  def searchNeighbors(queryPoint: Vector, radius: Double): mutable.ListBuffer[Vector] = {
    def searchRecur(
      queryPoint: Vector,
      radius: Double,
      node: Node,
      ret: mutable.ListBuffer[Vector]
    ): Unit = {
      if (node.children == null) {
        ret ++= node.nodeElements
      } else {
        for (child <- node.children; if child.isNear(queryPoint, radius)) {
          searchRecur(queryPoint, radius, child, ret)
        }
      }
    }

    val ret = new mutable.ListBuffer[Vector]
    searchRecur(queryPoint, radius, root, ret)
    ret
  }
}
