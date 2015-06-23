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

package org.apache.flink.ml.tree

import java.util

import org.apache.flink.ml.math.Vector
import org.apache.flink.ml.statistics.FieldType._
import org.apache.flink.ml.statistics.{ContinuousHistogram, DiscreteHistogram, OnlineHistogram}

import scala.Double.MaxValue
import scala.collection.mutable

/**
 * Defines a Classification tree.
 *
 * @param treeID ID of the tree
 * @param config Tree configuration
 */

class Tree(val treeID: Int, val config: TreeConfiguration) extends Serializable {

  private val nodes = new mutable.HashMap[Int, Node]()
  nodes.put(1, new Node(1, treeID))

  private val depth = config.depth
  private val maxBins = config.maxBins
  private val splitStrategy = config.splitStrategy
  private val minInstancePerNode = config.minInstancePerNode
  private val fieldStats = config.getDataStats.fields
  private val labels = config.getLabels

  /**
   * Determines the node where the vector belongs in the tree. This is accomplished by a series
   * of left-right movements based on the Split Values
   *
   * @param vector Input vector
   * @return Node to which vector belongs
   */
  def filter(vector: Vector): Node = {
    var node: Node = nodes.get(1).get
    while (node.predict.isEmpty && node.split.nonEmpty) {
      if (node.split.get.getSplitDirection(vector)) {
        node = nodes.get(2 * node.id).get
      }
      else {
        node = nodes.get(2 * node.id + 1).get
      }
    }
    node
  }

  /**
   * Splits the tree based on the histograms of values associated with every node of the tree
   *
   * @param histograms Histograms for every node, dimension and label
   * @return Whether the tree was further split
   */
  def split(histograms: Seq[(Int, Int, Double, OnlineHistogram)]): Boolean = {
    val histogramMap = new mutable.HashMap[(Int, Int, Double), OnlineHistogram]()
    // nodes which have received any elements in the tree must have sent a histogram
    val nodesWithElements = new mutable.HashSet[Int]()
    histograms.iterator.foreach(h => {
      histogramMap.put((h._1, h._2, h._3), h._4)
      nodesWithElements.add(h._1)
    })

    // check that every unlabeled leaf node sends something. Otherwise its sibling will be stuck
    // forever
    // So, we store whether to split a node later on
    val nodesToSplit = new mutable.HashMap[Int, Boolean]()
    nodesWithElements.iterator.foreach(
      x => {
        // if it is root node, no issue.
        if (x == 1) {
          nodesToSplit.put(x, true)
        } else {
          // for every other node, its sibling must exist too otherwise we can't split further.
          // We'll be stuck in an infinite chain, splitting as our parent did.
          val siblingID = 2 * (x / 2) + 1 - x % 2
          if (nodesWithElements.contains(siblingID)) {
            nodesToSplit.put(x, true)
          } else {
            nodesToSplit.put(x, false)
          }
        }
      }
    )

    growTree(nodesToSplit, histogramMap)
  }

  /**
   * Grow the tree further based on the list of nodes to split and associated histograms
   *
   * @param nodesToSplit Node ids along with whether they should be split or not
   * @param histogramMap Maps of histograms keyed by (node id, index, label)
   * @return Whether any node was split or not
   */
  private def growTree(
      nodesToSplit: mutable.HashMap[Int, Boolean],
      histogramMap: mutable.HashMap[(Int, Int, Double), OnlineHistogram])
    : Boolean = {
    var ret = false
    nodesToSplit.keysIterator.foreach(
      nodeID => {
        val relevantHistograms = new mutable.HashMap[(Int, Double), OnlineHistogram]()
        histogramMap.keysIterator.foreach(
          key => if (key._1 == nodeID) {
            relevantHistograms.put((key._2, key._3), histogramMap.get(key).get)
          }
        )

        val classCounts = new mutable.HashMap[Double, Int]()
        // count instances for each label. This will be the same for all indexes. So pick 0.
        labels.iterator.foreach(
          label => relevantHistograms.get((0, label)) match {
            case None => classCounts.put(label, 0)
            case Some(value) => classCounts.put(label, value.total)
          }
        )

        val totalPoints = classCounts.valuesIterator.sum
        val maxClass = classCounts.iterator.max(new Ordering[(Double, Int)] {
          override def compare(x: (Double, Int), y: (Double, Int)): Int = {
            if (x._2 > y._2) 1 else if (x._2 == y._2 && x._1 > y._1) 1 else -1
          }
        })

        if (maxClass._2 == totalPoints || Node.getDepth(nodeID) == depth) {
          // this becomes a leaf node if all elements belong to one class
          // or if we're already at the maximum depth
          nodes.get(nodeID).get.setPredict(maxClass._1)
        } else if (!nodesToSplit.get(nodeID).get) {
          // if the sibling didn't receive any elements, we make sure the parent never splits again
          nodes.get(nodeID / 2).get.setPredict(maxClass._1)
          nodes.remove(nodeID)
        } else {
          // otherwise we should split this node
          val gain = evaluateGain(classCounts)
          findBestSplit(relevantHistograms, gain, totalPoints, nodeID) match {
            case None => nodes.get(nodeID).get.setPredict(maxClass._1)
            case Some(split) =>
              nodes.get(nodeID).get.setSplit(split)
              // create two untrained child nodes
              nodes.put(nodeID * 2, new Node(nodeID * 2, this.treeID))
              nodes.put(nodeID * 2 + 1, new Node(nodeID * 2 + 1, this.treeID))
              ret = true
          }
        }
      }
    )
    ret
  }

  /**
   * Evaluates the Gain function for a certain distribution of instances across different labels
   *
   * @param classCounts Counts of classes for all labels
   * @return Entropy or Gini impurity based on the configuration of tree
   */
  private def evaluateGain(classCounts: mutable.HashMap[Double, Int]) = {
    val strategy = splitStrategy == "Gini"
    var result = if (strategy) 1.0 else 0.0

    val total = classCounts.reduce((x, y) => (x._1, x._2 + y._2))._2.toDouble
    classCounts.keysIterator.foreach(
      x => {
        if (strategy) {
          result -= Math.pow(classCounts.get(x).get, 2) / Math.pow(total, 2)
        } else {
          val p = classCounts.get(x).get
          if (p != 0) {
            result -= (p / total) * Math.log(p / total)
          }
        }
      }
    )
    result
  }

  /**
   * Evaluate the best split for a particular node based on the corresponding histograms
   *
   * @param relevantHistograms Histograms corresponding to a node
   * @param parentGain Gain of the node itself
   * @param totalPoints Number of instances at this node
   * @param nodeID ID of the node
   * @return Best split value, if any, or None
   */
  private def findBestSplit(
      relevantHistograms: mutable.HashMap[(Int, Double), OnlineHistogram],
      parentGain: Double,
      totalPoints: Int,
      nodeID: Int)
    : Option[SplitValue] = {
    // first merge histograms across all labels for a dimension
    val mergedHistograms = new mutable.HashMap[Int, OnlineHistogram]()
    relevantHistograms.keysIterator.foreach(
      key => {
        mergedHistograms.get(key._1) match {
          case None => mergedHistograms.put(key._1, relevantHistograms.get(key).get)
          case Some(value) => value.merge(
            relevantHistograms.get(key).get,
            if (fieldStats.apply(key._1).fieldType == CONTINUOUS) 2 * maxBins else 0
          )
        }
      }
    )

    var bestSplit: Option[SplitValue] = None
    var bestGain: Double = -MaxValue
    mergedHistograms.keysIterator.foreach(
      index => {
        val splits = findSplits(index, mergedHistograms.get(index).get)
        splits.iterator.foreach(
          split => {
            val leftClassCounts = new mutable.HashMap[Double, Int]()
            val rightClassCounts = new mutable.HashMap[Double, Int]()
            labels.iterator.foreach(
              label => {
                val (left, right) = fieldStats.apply(index).fieldType match {
                  case CONTINUOUS =>
                    val h = relevantHistograms.get((index, label)).get
                      .asInstanceOf[ContinuousHistogram]
                    val left = h.count(split.asInstanceOf[ContinuousSplit].splitValue)
                    (left, h.total - left)
                  case DISCRETE =>
                    val h = relevantHistograms.get((index, label)).get
                      .asInstanceOf[DiscreteHistogram]
                    val left = split.asInstanceOf[DiscreteSplit]
                      .splitList.map(x => h.count(x)).iterator.sum
                    (left, h.total - left)
                }
                leftClassCounts.put(label, left)
                rightClassCounts.put(label, right)

                val totalLeft = leftClassCounts.valuesIterator.sum
                val totalRight = rightClassCounts.valuesIterator.sum

                if (totalLeft >= minInstancePerNode && totalRight >= minInstancePerNode) {
                  val scaling = Math.pow(0.1, Node.getDepth(nodeID) + 1)
                  val balancing = Math.abs(totalLeft - totalRight) / totalPoints
                  val childGain = (1 - scaling) * (
                    parentGain -
                      totalLeft * evaluateGain(leftClassCounts) / totalPoints -
                      totalRight * evaluateGain(rightClassCounts) / totalPoints) +
                    scaling * balancing
                  if (childGain > bestGain) {
                    bestGain = childGain
                    bestSplit = Some(split)
                  }
                }
              }
            )
          }
        )
      }
    )
    bestSplit
  }

  /**
   * Find all possible splits to be considered for a histogram
   *
   * @param index Index of which this histogram is of
   * @param histogram Histogram which provides the distribution of elements
   * @return List of all splits
   */
  private def findSplits(index: Int, histogram: OnlineHistogram): List[SplitValue] = {
    var splitArray: Array[SplitValue] = Array.ofDim(0)
    histogram match {
      case h: ContinuousHistogram => {
        val actualBins = Math.min(maxBins, h.bins)
        splitArray = new Array[SplitValue](actualBins)
        for (i <- 1 to actualBins) {
          splitArray(i - 1) = new ContinuousSplit(index, h.quantile(i / (actualBins + 1.0)))
        }
      }
      case h: DiscreteHistogram => {
        val numClasses = fieldStats.apply(index).categoryCounts.size
        splitArray = Array.ofDim[SplitValue](Math.pow(2, numClasses).toInt)
        for (i <- 0 to Math.pow(2, numClasses).toInt - 1) {
          val binaryString = String.format("%" + numClasses + "s", Integer.toBinaryString(i))
          val thisSplitCategoryList = new util.ArrayList[Double]()
          for (j <- 0 to numClasses - 1) {
            if (binaryString.charAt(j) == '1') thisSplitCategoryList.add(labels.apply(j))
          }
          val thisSplitCategory = Array.ofDim[Double](thisSplitCategoryList.size())
          for (j <- 0 to thisSplitCategoryList.size() - 1) {
            thisSplitCategory(j) = thisSplitCategoryList.get(j)
          }
          splitArray(i) = new DiscreteSplit(index, thisSplitCategory)
        }
      }
    }
    splitArray.toList
  }

  override def toString: String = {
    var ret = ""
    nodes.valuesIterator.foreach(node => ret += node.toString + "\n")
    ret
  }
}
