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

import org.apache.flink.ml.math.{ContinuousHistogram, OnlineHistogram, Vector}

import scala.collection.mutable

/** Tree structure. This is kind of maintained in an unconventional way.
  * We provide direct access to all nodes
  * The obvious assumption is that child of i are 2*i and 2*i+1, while parent of i is i/2
  *
  */
class Tree(
            val treeID: Int,
            val nodes: mutable.HashMap[Int, Node],
            val config: TreeConfiguration
            ) extends Serializable {

  override def toString: String = {
    var ret = s"Tree ID=$treeID\nConfiguration:\n$config \nTree Structure:\n"
    for (i <- 1 to Math.pow(2, 20).toInt) {
      if (nodes.get(i).nonEmpty) {
        ret = ret + nodes.get(i).get.toString + "\n"
      }
    }
    ret
  }

  /** Determines which node of the tree this vector will go to
    * If predict at any node is -1 and it has a split, we'll go down recursively
    *
    */
  def filter(vector: Vector): (Int, Double) = {
    var node: Node = nodes.get(1).get
    while (node.predict.round.toInt == -1 && node.split.nonEmpty) {
      if (node.split.get.getSplitDirection(vector)) {
        node = nodes.get(2 * node.id).get
      }
      else {
        node = nodes.get(2 * node.id + 1).get
      }
    }
    (node.id, node.predict)
  }

  /** Splits the tree based on the histograms provided.
    * Returns an array of node ids which were split in the process
    *
    */
  def split(histogram: util.List[(Int, Int, Double, OnlineHistogram)]): Array[Int] = {
    val h = new mutable.HashMap[(Int, Int, Double), OnlineHistogram]()
    for (i <- 0 to histogram.size() - 1) {
      val ele = histogram.get(i)
      h.put((ele._1, ele._2, ele._3), ele._4)
    }
    val (splits, activeNodes) = calculateSplits(h)


    // first, every unlabeled leaf node must have sent something.
    // If not, its sibling will be stuck forever
    val finalNodes = new mutable.HashMap[Int,Int]()
    activeNodes.keysIterator.foreach(
      x=>{
        // since this node received instances, it's sibling also must have done so
        // for the root node, which happens only at level zero, there is no sibling. So just add it
        if(x!=1){
          val y = 2*(x/2) + 1 - x%2
          if(activeNodes.get(y).isEmpty){
            finalNodes.put(x,1)
          } else{
            finalNodes.put(x,-1)
          }
        } else{
          finalNodes.put(x,-1)
        }
      }
    )
    evaluateNodes(finalNodes, splits, h)
  }

  private def evaluateNodes(
                             activeNodes: mutable.HashMap[Int, Int],
                             splits: mutable.HashMap[(Int, Int), Array[SplitValue]],
                             histogram: mutable.HashMap[(Int, Int, Double), OnlineHistogram])
  : Array[Int] = {
    val depth = config.Depth
    val labelAddition = config.labelAddition
    val dimension = config.dimension
    val numClasses = config.numClasses
    val minInstancePerNode = config.MinInstancePerNode
    val labels = config.labels
    var splitNodes = 0
    activeNodes.keysIterator.foreach(
      node_id => {

        val classCounts = findCountsAtThisNode(node_id, histogram, labels)
        val totalNumPointsHere = classCounts.reduce((x, y) => (x._1, x._2 + y._2))._2
        var maxClassCount = -1
        var maxClassCountLabel = -1.0
        classCounts.keysIterator.foreach(
          x => {
            val thisClassCount = classCounts.get(x).get
            if (thisClassCount > maxClassCount) {
              maxClassCount = thisClassCount
              maxClassCountLabel = x
            }
          }
        )
        // now see if this node has become pure or if it's depth is at a maximum
        if (totalNumPointsHere == maxClassCount || Node.getDepth(node_id) == depth) {
          nodes.get(node_id).get.predict = maxClassCountLabel + labelAddition
          // sanity check. If we're declaring something a leaf, it better not have any children
          require(nodes.get(node_id).get.split.isEmpty, "An unexpected error occurred")
        } else if (activeNodes.get(node_id).get == 1) {
          // this node is meaningless. The parent didn't do anything at all.
          // just remove this node from the tree and set the label of parent,
          // which would be same as the label of this
          nodes.get(node_id / 2).get.predict = maxClassCountLabel + labelAddition
          nodes.get(node_id / 2).get.split = None
          nodes.remove(node_id)
        } else {
          val gainParent = findGain(classCounts)
          var bestGainChild = -java.lang.Double.MAX_VALUE
          var bestSplitValue: SplitValue = new SplitValue(-1, true, 0)
          // consider all splits across all dimensions and pick the best one
          for (j <- 0 to dimension - 1) {
            val splitsArray = splits.get((node_id, j)).get
            val actualSplits = splitsArray.length
            for (k <- 0 to actualSplits - 1) {
              // maintain left and right class counts
              val leftClassCounts = new mutable.HashMap[Double, Int]()
              val rightClassCounts = new mutable.HashMap[Double, Int]()
              for (l <- 0 to numClasses - 1) {
                val h = histogram.get((node_id, j, labels.apply(l)))
                if (h.nonEmpty) {
                  val (left, right) = evaluateSplit(splitsArray.apply(k), h.get)
                  leftClassCounts.put(labels.apply(l), left)
                  rightClassCounts.put(labels.apply(l), right)
                } else {
                  leftClassCounts.put(labels.apply(l), 0)
                  rightClassCounts.put(labels.apply(l), 0)
                }
              }
              // count total entries in the left and right
              val total_left = leftClassCounts.reduce((x, y) => (x._1, x._2 + y._2))._2
              val total_right = rightClassCounts.reduce((x, y) => (x._1, x._2 + y._2))._2
              // ensure that the split is allowed by user. We need at least this many instances
              if (total_left >= minInstancePerNode && total_right >= minInstancePerNode) {
                // use a balancing term to ensure the tree is balanced more on the top
                // the exponential term in scaling makes sure that as we go deeper,
                // Gini becomes more important in splits
                // this makes sense. We want balanced tree on top and fine-grained splitting at
                // deeper levels
                val scaling = Math.pow(0.1, Node.getDepth(node_id) + 1)
                val balancing = Math.abs(total_left - total_right) / totalNumPointsHere
                val childGain = (1 - scaling) * (gainParent - total_left * findGain
                  (leftClassCounts) / totalNumPointsHere - total_right * findGain
                  (rightClassCounts)
                  / totalNumPointsHere) + scaling * balancing
                if (childGain > bestGainChild) {
                  bestGainChild = childGain
                  bestSplitValue = splits.get((node_id, j)).get.apply(k)
                }
              }
            }
          }
          if (bestSplitValue.attribute != -1) {
            nodes.get(node_id).get.split = Some(bestSplitValue)
            nodes.put(node_id * 2, new Node(node_id * 2, this.treeID, None))
            nodes.put(node_id * 2 + 1, new Node(node_id * 2 + 1, this.treeID, None))
            splitNodes = splitNodes + 1
          } else {
            nodes.get(node_id).get.predict = maxClassCountLabel + labelAddition
          }
        }
      }
    )
    Array.ofDim(splitNodes)
  }

  private def findGain(classCounts: mutable.HashMap[Double, Int]) = {
    var result = 0.0
    val strategy = config.splitStrategy == "Gini"
    if (strategy) {
      result = 1.0
    }
    val total = classCounts.reduce((x, y) => (x._1, x._2 + y._2))._2.toDouble
    classCounts.keysIterator.foreach(
      x => {
        if (strategy) {
          result -= Math.pow(classCounts.get(x).get, 2) / Math.pow(total, 2)
        } else {
          val p = classCounts.get(x).get
          if(p!=0){
            result -= (p/total) * Math.log(p/total)
          }
        }
      }
    )
    result
  }

  def evaluateSplit(splitVal: SplitValue, histogram: OnlineHistogram): (Int, Int) = {
    var left, right = 0
    if (splitVal.splitType) {
      left = histogram.count(splitVal.splitValueDouble)
      right = histogram.total - left
    } else {
      for (i <- 0 to splitVal.splitValueList.length - 1) {
        left += histogram.count(splitVal.splitValueList.apply(i))
      }
      right = histogram.total - left
    }
    (left, right)
  }

  private def findCountsAtThisNode(
                                    nodeID: Int,
                                    histogram: mutable.HashMap[(Int, Int, Double), OnlineHistogram],
                                    labels: Array[Double])
  : mutable.HashMap[Double, Int] = {
    val ret = new mutable.HashMap[Double, Int]()
    // since the count of classes across any dimension is same, pick 0
    labels.iterator.foreach(
      x => {
        val h = histogram.get((nodeID, 0, x))
        if (h.nonEmpty) {
          val countOfClass = h.get.total
          ret.put(x, countOfClass)
        } else ret.put(x, 0)
      }
    )
    ret
  }

  /** Returns an Array of possible splits for each node at each dimension
    * For every node, we merge histogram at each dimension for all labels to figure out the
    * distribution of values on that dimension
    * Also returned is a set of nodes for which we need to consider splitting, basically all the
    * nodes for which we have received histograms
    *
    */
  private def calculateSplits(
                               histogram: mutable.HashMap[(Int, Int, Double), OnlineHistogram]
                               )
  : (mutable.HashMap[(Int, Int), Array[SplitValue]], mutable.HashMap[Int, Int]) = {
    val fieldStats = config.fieldStats
    val mergedHistograms = new mutable.HashMap[(Int, Int), OnlineHistogram]()
    val splits = new mutable.HashMap[(Int, Int), Array[SplitValue]]()
    val activeNodes = new mutable.HashMap[Int, Int]()
    histogram.keysIterator.foreach(
      x => {
        // we need to consider this node for splitting in the calling function
        activeNodes.put(x._1, -1)
        // if this (node,dimension) hasn't been considered, start afresh
        if (mergedHistograms.get((x._1, x._2)).isEmpty) {
          mergedHistograms.put((x._1, x._2), histogram.get(x).get)
        } else {
          // otherwise merge with the previous histogram at this (mode,dimension)
          var maxBins = config.MaxBins
          if (!fieldStats.apply(x._2).fieldType) {
            // for categorical fields, we don't care about the maximum bins allowed
            maxBins = fieldStats.apply(x._2).fieldCategories.length
          }
          val tempHist = histogram.get(x).get.merge(
            mergedHistograms.get((x._1, x._2)).get, maxBins + 1)
          mergedHistograms.put((x._1, x._2), tempHist)
        }
      }
    )
    // for each merged histogram at (node,dimension), find possible splits
    mergedHistograms.keysIterator.foreach(
      x => {
        val h = mergedHistograms.get(x).get
        // for continuous fields, use quantiles for find maxBins values
        if (h.isInstanceOf[ContinuousHistogram]) {
          val actualBins = Math.min(config.MaxBins, h.bins)
          val quantileArray = new Array[SplitValue](actualBins)
          for (i <- 1 to actualBins) {
            quantileArray(i - 1) = new SplitValue(
              x._2, true, h.quantile((i + 0.0) / (actualBins + 1)))
          }
          splits.put((x._1, x._2), quantileArray)
        } else {
          // for categorical fields, we can potentially have 2^numClass splits
          // Let's spit them all out for now
          // TODO figure out some heuristic measure to only pick some of these splits
          val numClasses = fieldStats.apply(x._2).fieldCategories.length
          val splitArray = Array.ofDim[SplitValue](Math.pow(2, numClasses).toInt)
          for (i <- 0 to Math.pow(2, numClasses).toInt - 1) {
            val binaryString = String.format("%0" + numClasses + "d", i.toInt.toBinaryString)
            val thisSplitCategoryList = new util.ArrayList[Double]()
            for (j <- 0 to numClasses - 1) {
              if (binaryString.charAt(j) == '1') thisSplitCategoryList.add(config.labels.apply(j))
            }
            val thisSplitCategory = Array.ofDim[Double](thisSplitCategoryList.size())
            for (j <- 0 to thisSplitCategoryList.size() - 1) {
              thisSplitCategory(j) = thisSplitCategoryList.get(j)
            }
            splitArray(i) = new SplitValue(x._2, false, splitValueList = thisSplitCategory)
          }
          splits.put((x._1, x._2), splitArray)
        }
      }
    )
    (splits, activeNodes)
  }
}
