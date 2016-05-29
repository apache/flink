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

import org.apache.flink.ml.math.{Vector => FlinkVector, DenseVector, Breeze}
import Breeze._

import org.apache.flink.ml.metrics.distances.{SquaredEuclideanDistanceMetric,
EuclideanDistanceMetric, DistanceMetric}
import org.apache.flink.util.Collector

import scala.collection.immutable.Vector
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.PriorityQueue


class QuadtreeKNN() {

  def knnQueryWithQuadTree[T <: FlinkVector](
                                              training: Vector[T],
                                              testing: Vector[(Long, T)],
                                              k: Int, metric: DistanceMetric,
                                              out: Collector[(FlinkVector,
                                                FlinkVector, Long, Double)]) {
    /// find a bounding box
    val MinArr = Array.tabulate(training.head.size)(x => x)
    val MaxArr = Array.tabulate(training.head.size)(x => x)

    val minVecTrain = MinArr.map(i => training.map(x => x(i)).min - 0.01)
    val minVecTest = MinArr.map(i => testing.map(x => x._2(i)).min - 0.01)
    val maxVecTrain = MaxArr.map(i => training.map(x => x(i)).max + 0.01)
    val maxVecTest = MaxArr.map(i => testing.map(x => x._2(i)).max + 0.01)

    val Min = DenseVector(MinArr.map(i => Array(minVecTrain(i), minVecTest(i)).min))
    val Max = DenseVector(MinArr.map(i => Array(maxVecTrain(i), maxVecTest(i)).max))

    //default value of max elements/box is set to max(20,k)
    val maxPerBox = Array(k, 20).max
    val trainingQuadTree = new QuadTree(Min, Max, metric, maxPerBox)

    val queue = mutable.PriorityQueue[(FlinkVector, FlinkVector, Long, Double)]()(
      Ordering.by(_._4))

    for (v <- training) {
      trainingQuadTree.insert(v)
    }

    for ((id, vector) <- testing) {
      //  Find siblings' objects and do local kNN there
      val siblingObjects =
        trainingQuadTree.searchNeighborsSiblingQueue(vector)

      // do KNN query on siblingObjects and get max distance of kNN
      // then rad is good choice for a neighborhood to do a refined
      // local kNN search
      val knnSiblings = siblingObjects.map(v => metric.distance(vector, v)
      ).sortWith(_ < _).take(k)

      val rad = knnSiblings.last
      val trainingFiltered = trainingQuadTree.searchNeighbors(vector, rad)

      for (b <- trainingFiltered) {
        // (training vector, input vector, input key, distance)
        queue.enqueue((b, vector, id, metric.distance(b, vector)))
        if (queue.size > k) {
          queue.dequeue()
        }
      }
      for (v <- queue) {
        out.collect(v)
      }
    }
  }

  /**
    * n-dimensional QuadTree data structure; partitions
    * spatial data for faster queries (e.g. KNN query)
    * The skeleton of the data structure was initially
    * based off of the 2D Quadtree found here:
    * http://www.cs.trinity.edu/~mlewis/CSCI1321-F11/Code/src/util/Quadtree.scala
    *
    * Many additional methods were added to the class both for
    * efficient KNN queries and generalizing to n-dim.
    *
    * @param minVec vector of the corner of the bounding box with smallest coordinates
    * @param maxVec vector of the corner of the bounding box with smallest coordinates
    * @param distMetric metric, must be Euclidean or squareEuclidean
    * @param maxPerBox threshold for number of points in each box before slitting a box
    */

  class QuadTree(
                  minVec: FlinkVector,
                  maxVec: FlinkVector,
                  distMetric: DistanceMetric,
                  maxPerBox: Int) {

    class Node(
                center: FlinkVector,
                width: FlinkVector,
                var children: Seq[Node]) {

      val nodeElements = new ListBuffer[FlinkVector]

      /** for testing purposes only; used in QuadTreeSuite.scala
        *
        * @return center and width of the box
        */
      def getCenterWidth(): (FlinkVector, FlinkVector) = {
        (center, width)
      }

      def contains(queryPoint: FlinkVector): Boolean = {
        overlap(queryPoint, 0.0)
      }

      /** Tests if queryPoint is within a radius of the node
        *
        * @param queryPoint
        * @param radius
        * @return
        */
      def overlap(
                   queryPoint: FlinkVector,
                   radius: Double): Boolean = {
        val count = (0 until queryPoint.size).filter { i =>
          (queryPoint(i) - radius < center(i) + width(i) / 2) &&
            (queryPoint(i) + radius > center(i) - width(i) / 2)
        }.size

        count == queryPoint.size
      }

      /** Tests if queryPoint is near a node
        *
        * @param queryPoint
        * @param radius
        * @return
        */
      def isNear(
                  queryPoint: FlinkVector,
                  radius: Double): Boolean = {
        minDist(queryPoint) < radius
      }

      /**
        * minDist is defined so that every point in the box
        * has distance to queryPoint greater than minDist
        * (minDist adopted from "Nearest Neighbors Queries" by N. Roussopoulos et al.)
        *
        * @param queryPoint
        * @return
        */
      def minDist(queryPoint: FlinkVector): Double = {
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
          case _: SquaredEuclideanDistanceMetric => minDist
          case _: EuclideanDistanceMetric => math.sqrt(minDist)
          case _ => throw new IllegalArgumentException(s" Error: metric must be" +
            s" Euclidean or SquaredEuclidean!")
        }
      }

      /**
        * Finds which child queryPoint lies in.  node.children is a Seq[Node], and
        * whichChild finds the appropriate index of that Seq.
        * @param queryPoint
        * @return
        */
      def whichChild(queryPoint: FlinkVector): Int = {
        (0 until queryPoint.size).map { i =>
          if (queryPoint(i) > center(i)) {
            Math.pow(2, queryPoint.size - 1 - i).toInt
          } else {
            0
          }
        }.sum
      }

      def makeChildren() {
        val centerClone = center.copy
        val cPart = partitionBox(centerClone, width)
        val mappedWidth = 0.5 * width.asBreeze
        children = cPart.map(p => new Node(p, mappedWidth.fromBreeze, null))
      }

      /**
        * Recursive function that partitions a n-dim box by taking the (n-1) dimensional
        * plane through the center of the box keeping the n-th coordinate fixed,
        * then shifting it in the n-th direction up and down
        * and recursively applying partitionBox to the two shifted (n-1) dimensional planes.
        *
        * @param center the center of the box
        * @param width a vector of lengths of each dimension of the box
        * @return
        */
      def partitionBox(
                        center: FlinkVector,
                        width: FlinkVector): Seq[FlinkVector] = {
        def partitionHelper(
                             box: Seq[FlinkVector],
                             dim: Int): Seq[FlinkVector] = {
          if (dim >= width.size) {
            box
          } else {
            val newBox = box.flatMap {
              vector =>
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

    /**
      * simple printing of tree for testing/debugging
      */
    def printTree(): Unit = {
      printTreeRecur(root)
    }

    def printTreeRecur(node: Node) {
      if (node.children != null) {
        for (c <- node.children) {
          printTreeRecur(c)
        }
      } else {
        println("printing tree: n.nodeElements " + node.nodeElements)
      }
    }

    /**
      * Recursively adds an object to the tree
      * @param queryPoint
      */
    def insert(queryPoint: FlinkVector) {
      insertRecur(queryPoint, root)
    }

    private def insertRecur(
                             queryPoint: FlinkVector,
                             node: Node) {
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

    /**
      * Used to zoom in on a region near a test point for a fast KNN query.
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
      * @return
      */
    def searchNeighborsSiblingQueue(queryPoint: FlinkVector): ListBuffer[FlinkVector] = {
      val ret = new ListBuffer[FlinkVector]
      // edge case when the main box has not been partitioned at all
      if (root.children == null) {
        root.nodeElements.clone()
      } else {
        val nodeQueue = new PriorityQueue[(Double, Node)]()(Ordering.by(x => x._1))
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
      * @param node node that queryPoint lies in
      * @param nodeQueue defined in searchSiblingQueue, this stores nodes based on their
      *                  distance to node as defined by minDist
      */
    private def searchRecurSiblingQueue(
                                         queryPoint: FlinkVector,
                                         node: Node,
                                         nodeQueue: PriorityQueue[(Double, Node)]) {
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

    /**
      * Goes down to minimal bounding box of queryPoint, and add elements to nodeQueue
      *
      * @param queryPoint point under consideration
      * @param node node that queryPoint lies in
      * @param nodeQueue PriorityQueue that stores all points in minimal bounding box of queryPoint
      */
    private def minNodes(
                          queryPoint: FlinkVector,
                          node: Node,
                          nodeQueue: PriorityQueue[(Double, Node)]) {
      if (node.children == null) {
        nodeQueue += ((-node.minDist(queryPoint), node))
      } else {
        for (c <- node.children) {
          minNodes(queryPoint, c, nodeQueue)
        }
      }
    }

    /** Finds all objects within a neigiborhood of queryPoint of a specified radius
      * scope is modified from original 2D version in:
      * http://www.cs.trinity.edu/~mlewis/CSCI1321-F11/Code/src/util/Quadtree.scala
      *
      * original version only looks in minimal box; for the KNN Query, we look at
      * all nearby boxes. The radius is determined from searchNeighborsSiblingQueue
      * by defining a min-heap on the leaf nodes
      *
      * @param queryPoint
      * @param radius
      * @return all points within queryPoint with given radius
      */
    def searchNeighbors(
                         queryPoint: FlinkVector,
                         radius: Double): ListBuffer[FlinkVector] = {
      val ret = new ListBuffer[FlinkVector]
      searchRecur(queryPoint, radius, root, ret)
      ret
    }

    private def searchRecur(
                             queryPoint: FlinkVector,
                             radius: Double,
                             node: Node,
                             ret: ListBuffer[FlinkVector]) {
      if (node.children == null) {
        ret ++= node.nodeElements
      } else {
        for (child <- node.children; if child.isNear(queryPoint, radius)) {
          searchRecur(queryPoint, radius, child, ret)
        }
      }
    }

  }

}
