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

    nodes.valuesIterator.foreach(
      this_node => {
        if (this_node.predict == -1 && this_node.split.isEmpty
          && activeNodes.get(this_node.id).isEmpty) {
          // we're in trouble
          var sibling_id = 0
          if (this_node.id % 2 == 0) {
            sibling_id = this_node.id + 1
          }
          else {
            sibling_id = this_node.id - 1
          }
          // this node is pointless. Remove it from the tree
          nodes.remove(this_node.id)
          // we're not going to split the sibling anymore.
          activeNodes.put(sibling_id, 1) // we'll check for this '1' later
        }
      }
    )
    evaluateNodes(activeNodes, splits, h)
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

        var node: Node = nodes.get(node_id).get
        // find the gini index of this node
        val (sumClassSquare, totalNumPointsHere, maxClassCountLabel, _) =
          findGini(node_id, histogram, labels)
        // println(node_id  + " " + totalNumPointsHere+ " "+ sumClassSquare+ " "+maxClassCountLabel)

        // now see if this node has become pure or if it's depth is at a maximum
        if (totalNumPointsHere * totalNumPointsHere == sumClassSquare || node.getDepth == depth) {
          node.predict = maxClassCountLabel + labelAddition
          // sanity check. If we're declaring something a leaf, it better not have any children
          require(node.split.isEmpty, "An unexpected error occurred")
        } else if (activeNodes.get(node_id).get == 1) {
          // this node is meaningless. The parent didn't do anything at all.
          // just remove this node from the tree and set the label of parent,
          // which would be same as the label of this
          node = nodes.get(node_id / 2).get
          node.predict = maxClassCountLabel + labelAddition
          node.split = None
          nodes.remove(node_id)
        } else {
          val giniParent = 1 - sumClassSquare / Math.pow(totalNumPointsHere, 2)
          var best_gini = -java.lang.Double.MAX_VALUE
          var best_split_value: SplitValue = new SplitValue(-1, true, 0)
          var best_left_total, best_right_total = 0.0
          // consider all splits across all dimensions and pick the best one
          for (j <- 0 to dimension - 1) {
            val splitsArray = splits.get((node_id, j)).get
            val actualSplits = splitsArray.length
            for (k <- 0 to actualSplits - 1) {
              // maintain how many instances go left and right for Gini
              var total_left, total_right, left_sum_sqr, right_sum_sqr = 0.0
              for (l <- 0 to numClasses - 1) {
                val h = histogram.get((node_id, j, labels.apply(l)))
                if (h.nonEmpty) {
                  val left = h.get.count(splitsArray.apply(k).splitValueDouble)
                  val right = h.get.total - left
                  total_left = total_left + left
                  total_right = total_right + right
                  left_sum_sqr = left_sum_sqr + left * left
                  right_sum_sqr = right_sum_sqr + right * right
                }
              }
              // ensure that the split is allowed by user. We need at least this many instances
              if (total_left >= minInstancePerNode && total_right >= minInstancePerNode) {
                // use a balancing term to ensure the tree is balanced more on the top
                // the exponential term in scaling makes sure that as we go deeper,
                // Gini becomes more important in splits
                // this makes sense. We want balanced tree on top and fine-grained splitting at
                // deeper levels
                val scaling = Math.pow(0.1, node.getDepth + 1)
                val balancing = Math.abs(total_left - total_right) / totalNumPointsHere
                val this_gini = (1 - scaling) * (giniParent -
                  total_left * (1 - left_sum_sqr / Math.pow(total_left, 2)) / totalNumPointsHere -
                  total_right * (1 - right_sum_sqr / Math.pow(total_right, 2)) / totalNumPointsHere
                  ) + scaling * balancing
                if (this_gini > best_gini) {
                  best_gini = this_gini
                  best_split_value = splits.get((node_id, j)).get.apply(k)
                  best_left_total = total_left
                  best_right_total = total_right
                }
              }
            }
          }
          if (best_split_value.attribute != -1) {
            node.split = Some(best_split_value)
            nodes.put(node_id * 2, new Node(node_id * 2, this.treeID, None))
            nodes.put(node_id * 2 + 1, new Node(node_id * 2 + 1, this.treeID, None))
            splitNodes = splitNodes + 1
          } else {
            node.predict = maxClassCountLabel + labelAddition
          }
        }
      }
    )
    Array.ofDim(splitNodes)
  }

  private def findGini(
                        nodeId: Int,
                        histogram: mutable.HashMap[(Int, Int, Double), OnlineHistogram],
                        labels: Array[Double]):
  (Double, Double, Double, Double) = {
    var sumClassSquare, totalNumPointsHere, maxClassCountLabel, maxClassCount = 0.0
    // since the count of classes across any dimension is same, pick 0
    // for calculating Gini index, we need count(c)^2 and \sum count(c)^2
    // also maintain which class occurred most frequently in case we need to mark this as a leaf
    // node
    labels.iterator.foreach(
      x => {
        val h = histogram.get((nodeId, 0, x))
        if (h.nonEmpty) {
          val countOfClass = h.get.total
          totalNumPointsHere = totalNumPointsHere + countOfClass
          sumClassSquare = sumClassSquare + countOfClass * countOfClass
          if (countOfClass > maxClassCount) {
            maxClassCount = countOfClass
            maxClassCountLabel = x
          }
        }
      }
    )
    (sumClassSquare, totalNumPointsHere, maxClassCountLabel, maxClassCount)
  }

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
        activeNodes.put(x._1, -1)
        if (mergedHistograms.get((x._1, x._2)).isEmpty) {
          mergedHistograms.put((x._1, x._2), histogram.get(x).get)
        } else {
          var maxBins = config.MaxBins
          if (!fieldStats.apply(x._2).fieldType) {
            maxBins = fieldStats.apply(x._2).fieldCategories.length
          }
          val tempHist = histogram.get(x).get.merge(
            mergedHistograms.get((x._1, x._2)).get, maxBins + 1)
          mergedHistograms.put((x._1, x._2), tempHist)
        }
      }
    )
    mergedHistograms.keysIterator.foreach(
      x => {
        val h = mergedHistograms.get(x).get
        if (h.isInstanceOf[ContinuousHistogram]) {
          val actualBins = Math.min(config.MaxBins, h.bins - 1)
          val quantileArray = new Array[SplitValue](actualBins)
          for (i <- 1 to actualBins) {
            quantileArray(i - 1) = new SplitValue(
              x._2, true, h.quantile((i + 0.0) / (actualBins + 1)))
          }
          splits.put((x._1, x._2), quantileArray)
        } else {
          // TODO handle categorical splits
        }
      }
    )
    (splits, activeNodes)
  }
}