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

package org.apache.flink.ml.classification

import java.util

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.ml.common.FlinkTools.ModuloKeyPartitioner
import org.apache.flink.ml.common._
import org.apache.flink.ml.math.{Histogram, OnlineHistogram, Vector}
import org.apache.flink.ml.tree._

import scala.collection.mutable

/** Companion object of Decision Tree.
  * Contains convenience functions and the parameter type definitions of the algorithm.
  *
  */
object DecisionTree {
  val DECISION_TREE = "decision_tree"
  val DECISION_TREE_CONFIG = "decision_tree_configuration"

  def apply(): DecisionTree = {
    new DecisionTree()
  }

  case object Depth extends Parameter[Int] {
    val defaultValue: Option[Int] = Some(30)
  }

  case object SplitStrategy extends Parameter[String] {
    val defaultValue: Option[String] = Some("Gini")
  }

  case object MinInstancesPerNode extends Parameter[Int] {
    val defaultValue: Option[Int] = Some(1)
  }

  case object Pruning extends Parameter[Boolean] {
    val defaultValue: Option[Boolean] = Some(false)
  }

  case object MaxBins extends Parameter[Int] {
    val defaultValue: Option[Int] = Some(100)
  }

  case object Dimension extends Parameter[Int] {
    val defaultValue: Option[Int] = Some(2)
  }

  case object Category extends Parameter[Array[Int]] {
    val defaultValue: Option[Array[Int]] = Some(Array.ofDim(0))
  }

  case object Classes extends Parameter[Int] {
    val defaultValue: Option[Int] = Some(2)
  }

}

class DecisionTree extends Learner[LabeledVector, DecisionTreeModel] with Serializable {

  import DecisionTree._

  /** Sets the maximum allowed depth of the tree.
    * Currently only allowed values up to 30
    *
    * *@param depth
    * @return itself
    */
  def setDepth(depth: Int): DecisionTree = {
    require(depth <= 30, "Maximum depth allowed: 30")
    parameters.add(Depth, depth)
    this
  }

  /** Sets minimum number of instances that must be present at a node for its parent to split
    *
    * *@param minInstancesPerNode
    * @return itself
    */
  def setMinInstancePerNode(minInstancesPerNode: Int): DecisionTree = {
    require(minInstancesPerNode >= 1,
      "Every node must have at least one instance associated with it")
    parameters.add(MinInstancesPerNode, minInstancesPerNode)
    this
  }

  /** Sets whether or not to prune the tree after building
    *
    * *@param prune
    * @return itself
    */
  def setPruning(prune: Boolean): DecisionTree = {
    parameters.add(Pruning, prune)
    this
  }

  /** Sets maximum number of bins to be used for calculating splits.
    *
    * *@param maxBins
    * @return itself
    */
  def setMaxBins(maxBins: Int): DecisionTree = {
    require(maxBins >= 1, "Maximum bins used must be at least one")
    parameters.add(MaxBins, maxBins)
    this
  }

  /** Sets the splitting strategy. Gini and Entropy supported.
    *
    * *@param splitStrategy
    * @return itself
    */
  def setSplitStrategy(splitStrategy: String): DecisionTree = {
    require(splitStrategy == "Gini" || splitStrategy == "Entropy",
      "Algorithm " + splitStrategy + " not supported")
    parameters.add(SplitStrategy, splitStrategy)
    this
  }

  /** Sets the dimension of data. Will be cross checked with the data later
    *
    * *@param dimension
    * @return itself
    */
  def setDimension(dimension: Int): DecisionTree = {
    require(dimension >= 1, "Dimension cannot be less than one")
    parameters.add(Dimension, dimension)
    this
  }

  /** Sets which fields are to be considered categorical. Array of field indices
    *
    * *@param category
    * @return itself
    */
  def setCategory(category: Array[Int]): DecisionTree = {
    parameters.add(Category, category)
    this
  }

  /** Sets how many classes there are in the data [will be cross checked with the data later]
    *
    * *@param numClasses
    * @return itself
    */
  def setClasses(numClasses: Int): DecisionTree = {
    require(numClasses > 1, "There must be at least two classes in the data")
    parameters.add(Classes, numClasses)
    this
  }

  /** Trains a Decision Tree
    *
    * @param input Training data set
    * @param fitParameters Parameter values
    * @return Trained Decision Tree Model
    */
  override def fit(input: DataSet[LabeledVector], fitParameters: ParameterMap):
  DecisionTreeModel = {
    val resultingParameters = this.parameters ++ fitParameters
    val depth = resultingParameters(Depth)
    val minInstancePerNode = resultingParameters(MinInstancesPerNode)
    val pruneTree = resultingParameters(Pruning)
    val maxBins = resultingParameters(MaxBins)
    val splitStrategy = resultingParameters(SplitStrategy)
    val dimension = resultingParameters(Dimension)
    val category = resultingParameters(Category)
    val numClasses = resultingParameters(Classes)

    require(category.length == 0, "Only continuous fields supported right now")
    require(splitStrategy == "Gini", "Only Gini algorithm implemented right now")
    var tree = createInitialTree(input, new TreeConfiguration(maxBins, minInstancePerNode, depth,
      pruneTree, splitStrategy, numClasses, dimension, category))

    var treeCopy = tree.collect().toArray.apply(0)

    val numberVectors = tree.getExecutionEnvironment.fromElements(treeCopy.config.numTrainVector)

    tree = input.getExecutionEnvironment.fromElements(treeCopy) // remake the tree as a dataset

    // Group the input data into blocks in round robin fashion
    val blockedInputNumberElements = FlinkTools.block(
      input, input.getParallelism, Some(ModuloKeyPartitioner)).
      cross(numberVectors).
      map { x => x }

    var any_unlabeled_left = true
    while (any_unlabeled_left) {

      // next iteration will only happen if we happen to split an unlabeled leaf in this iteration
      any_unlabeled_left = false

      // histograms from each node with key (tree_node, dimension, label)
      val localHists = localHistUpdate(
        tree,
        blockedInputNumberElements
      )

      // merge histograms over key
      val combinedHists = localHists.reduce(
        (a, b) => {
          b.iterator.foreach(
            x => {
              a.get(x._1) match {
                case Some(hist) => a.put(x._1, a.get(x._1).get.merge(x._2, maxBins + 1))
                case None => a.put(x._1, x._2)
              }
            }
          )
          a
        }
      )
      // now collect the tree. We're gonna need direct access to it.
      treeCopy = tree.collect().toArray.apply(0)

      val finalHists = combinedHists.collect().toArray.apply(0)

      any_unlabeled_left = evaluateSplits(treeCopy, finalHists)
      tree = tree.getExecutionEnvironment.fromElements(treeCopy)
    }

    DecisionTreeModel(tree)
  }

  private def evaluateSplits(
                              tree: Tree,
                              finalHists: mutable.HashMap[(Int, Int, Double), Histogram])
  : Boolean = {
    val fieldStats = tree.config.fieldStats
    val labels = tree.config.labels
    val maxBins = tree.config.MaxBins

    var any_split_done = false

    val (nodeDimensionSplits, nodes) = calculateSplits(finalHists, fieldStats, maxBins, labels)

    // first, every unlabeled leaf node must have sent something.
    // If not, its sibling will be stuck forever

    tree.nodes.valuesIterator.foreach(
      this_node => {
        if (this_node.predict == -1 && this_node.split.isEmpty &&
          nodes.get(this_node.id).isEmpty) {
          // we're in trouble
          var sibling_id = 0
          if (this_node.id % 2 == 0)
            sibling_id = this_node.id + 1
          else
            sibling_id = this_node.id - 1
          // this node is pointless. Remove it from the tree
          tree.nodes.remove(this_node.id)
          // we're not going to split the sibling anymore.
          nodes.put(sibling_id, 1) // we'll check for this '1' later
        }
      }
    )
    // now, for each node, for each dimension, evaluate splits based on the above histograms
    any_split_done = evaluateNodes(nodes, tree, nodeDimensionSplits, finalHists)
    return any_split_done
  }

  /** Merge all histograms (node_id, dim, _) in finalHists and return an array of splits
    * that need to be considered at node node_id on dimension dim.
    * Also, return a hashmap containing entries for whichever nodes have some instances allocated
    * to them
    *
    */
  private def calculateSplits(
                               finalHists: mutable.HashMap[(Int, Int, Double), Histogram],
                               fieldStats: Array[FieldStats],
                               maxBins: Int,
                               labels: Array[Double]):
  (mutable.HashMap[(Int, Int), Array[Double]], mutable.HashMap[Int, Int]) = {

    // keep a list of all the nodes we'll have to find splits for later
    // we only get stuff for leafs which are unlabeled, so nodes only has those which are
    // unlabeled leafs

    val nodeDimensionSplits = new mutable.HashMap[(Int, Int), Array[Double]]
    val nodes = new mutable.HashMap[Int, Int]

    finalHists.keysIterator.foreach(
      x => {
        nodes.put(x._1, -1)
        if (nodeDimensionSplits.get((x._1, x._2)).isEmpty) {
          val min_value = fieldStats.apply(x._2).fieldMinValue
          val max_value = fieldStats.apply(x._2).fieldMaxValue
          var tmp_hist: Histogram =
            new OnlineHistogram(maxBins, 2 * min_value - max_value, 2 * max_value - min_value)
          labels.iterator.foreach(
            c => {
              if (finalHists.get((x._1, x._2, c)).nonEmpty) {
                tmp_hist = tmp_hist.merge(finalHists.get((x._1, x._2, c)).get, maxBins)
              }
            }
          )
          // find maxBins quantiles, or if the size of histogram is less than that, just those many
          val actualBins = Math.min(maxBins, tmp_hist.bins - 1)
          val quantileArray = new Array[Double](actualBins)
          for (i <- 1 to actualBins) {
            quantileArray(i - 1) = tmp_hist.quantile((i + 0.0) / (actualBins + 1))
          }
          nodeDimensionSplits.put((x._1, x._2), quantileArray)
        }
      }
    )
    (nodeDimensionSplits, nodes)
  }

  private def findGini(
                        node_id: Int,
                        finalHists: mutable.HashMap[(Int, Int, Double), Histogram],
                        labels: Array[Double]):
  (Double, Double, Double, Double) = {
    var sumClassSquare, totalNumPointsHere, maxClassCountLabel, maxClassCount = 0.0
    // since the count of classes across any dimension is same, pick 0
    // for calculating Gini index, we need count(c)^2 and \sum count(c)^2
    // also maintain which class occurred most frequently in case we need to mark this as a leaf
    // node
    labels.iterator.foreach(
      x => {
        val h = finalHists.get((node_id, 0, x))
        if (h.nonEmpty) {
          val countOfClass = h.get.sum(h.get.upper)
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

  private def evaluateNodes(
                             nodes: mutable.HashMap[Int, Int],
                             tree: Tree,
                             nodeDimensionSplits: mutable.HashMap[(Int, Int), Array[Double]],
                             finalHists: mutable.HashMap[(Int, Int, Double), Histogram]):
  Boolean = {
    val depth = tree.config.Depth
    val labelAddition = tree.config.labelAddition
    val dimension = tree.config.dimension
    val numClasses = tree.config.numClasses
    val minInstancePerNode = tree.config.MinInstancePerNode
    val labels = tree.config.labels
    var any_split_done = false

    nodes.keysIterator.foreach(
      node_id => {

        var node: Node = tree.nodes.get(node_id).get

        // find the gini index of this node
        val (sumClassSquare, totalNumPointsHere, maxClassCountLabel, maxClassCount) =
          findGini(node_id, finalHists, labels)

        // now see if this node has become pure or if it's depth is at a maximum
        if (totalNumPointsHere * totalNumPointsHere == sumClassSquare || node.getDepth == depth) {
          node.predict = maxClassCountLabel + labelAddition
          // sanity check. If we're declaring something a leaf, it better not have any children
          require(node.split.isEmpty, "An unexpected error occurred")
        }
        else if (nodes.get(node_id).get == 1) {
          // this node is meaningless. The parent didn't do anything at all.
          // just remove this node from the tree and set the label of parent,
          // which would be same as the label of this
          node = tree.nodes.get(node_id / 2).get
          node.predict = maxClassCountLabel + labelAddition
          node.split = None
          tree.nodes.remove(node_id)
        }
        else {
          val giniParent = 1 - sumClassSquare / Math.pow(totalNumPointsHere, 2)
          var best_gini = -java.lang.Double.MAX_VALUE
          var best_dimension = -1
          var best_split_value, best_left_total, best_right_total = 0.0
          // consider all splits across all dimensions and pick the best one
          for (j <- 0 to dimension - 1) {
            val splitsArray = nodeDimensionSplits.get((node_id, j)).get
            val actualSplits = splitsArray.length
            for (k <- 0 to actualSplits - 1) {
              // maintain how many instances go left and right for Gini
              var total_left, total_right, left_sum_sqr, right_sum_sqr = 0.0
              for (l <- 0 to numClasses - 1) {
                val h = finalHists.get((node_id, j, labels.apply(l)))
                if (h.nonEmpty) {
                  val left = h.get.sum(splitsArray.apply(k))
                  val right = h.get.sum(h.get.upper) - left
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
                  best_dimension = j
                  best_split_value = nodeDimensionSplits.get((node_id, j)).get.apply(k)
                  best_left_total = total_left
                  best_right_total = total_right
                }
              }
            }
          }
          if (best_dimension != -1) {
            node.split = Some(new SplitValue(best_dimension, true, best_split_value))
            tree.nodes.put(node_id * 2, new Node(node_id * 2, tree.treeID, None))
            tree.nodes.put(node_id * 2 + 1, new Node(node_id * 2 + 1, tree.treeID, None))
            any_split_done = true
          } else {
            node.predict = maxClassCountLabel + labelAddition
          }
        }
      }
    )
    return any_split_done
  }

  private def localHistUpdate(
                               tree: DataSet[Tree],
                               blockedInputNumberElements: DataSet[(Block[LabeledVector], Int)])
  : DataSet[mutable.HashMap[(Int, Int, Double), Histogram]] = {

    /** Rich mapper calculating histograms for each data block. We use a RichMapFunction here,
      * because we broadcast the current value of the tree to all mappers.
      *
      */
    val localUpdate = new RichMapFunction[(Block[LabeledVector], Int),
      mutable.HashMap[(Int, Int, Double), Histogram]] {

      var tree: Tree = _
      var config: TreeConfiguration = _
      var histograms: mutable.HashMap[(Int, Int, Double), Histogram] =
        new mutable.HashMap[(Int, Int, Double), Histogram]()

      override def open(parameters: Configuration): Unit = {
        // get the tree
        tree = getRuntimeContext.getBroadcastVariable(DECISION_TREE).get(0)
        config = tree.config
      }

      override def map(blockNumberElements: (Block[LabeledVector], Int))
      : mutable.HashMap[(Int, Int, Double), Histogram] = {
        // for all instances in the block
        val (block, _) = blockNumberElements
        val numLocalDataPoints = block.values.length
        for (i <- 0 to numLocalDataPoints - 1) {
          val LabeledVector(label, vector) = block.values(i)
          // find where this instance goes
          val (node, predict) = tree.filter(vector)
          if (predict.round.toInt == -1) {
            // we can be sure that this is an unlabeled leaf and not some internal node.
            // That's how filter works
            for (j <- 0 to tree.config.dimension - 1) {
              val min_value = config.fieldStats.apply(j).fieldMinValue
              val max_value = config.fieldStats.apply(j).fieldMaxValue
              histograms.get((node, j, label)) match {
                // if this histogram already exists, add a new entry to it
                case Some(hist) => hist.add(vector.apply(j))
                case None =>
                  // otherwise, create the histogram and put the entry in it
                  histograms.put((node, j, label), new OnlineHistogram(
                    config.MaxBins + 1, 2 * min_value - max_value, 2 * max_value - min_value))
                  histograms.get((node, j, label)).get.add(vector.apply(j))
              }
            }
          }
        }
        histograms
      }
    }
    // map using the RichMapFunction with the tree broadcasted
    blockedInputNumberElements.map(localUpdate).withBroadcastSet(tree, DECISION_TREE)
  }

  /** Creates the initial root
    *
    * @return initial decision tree, after making sanity checks on data and evaluating statistics
    */
  private def createInitialTree(input: DataSet[LabeledVector], config: TreeConfiguration):
  DataSet[Tree] = {

    val initTree = new RichMapFunction[LabeledVector, (Array[FieldStats], Int)] {

      var config: TreeConfiguration = _

      override def open(parameters: Configuration): Unit = {
        config = getRuntimeContext.getBroadcastVariable(DECISION_TREE_CONFIG).get(0)
      }

      override def map(labeledVector: LabeledVector): (Array[FieldStats], Int) = {
        require(labeledVector.vector.size == config.dimension,
          "Specify the dimension of data correctly")
        // we also include the label field for now
        val ret = new Array[FieldStats](config.dimension + 1)
        for (i <- 0 to config.dimension - 1) {
          if (config.category.indexOf(i) >= 0) {
            // if this is a categorical field
            val h = new mutable.HashMap[Double, Int]()
            h.put(labeledVector.vector.apply(i), 1)
            ret(i) = new FieldStats(false, fieldCategories = h)
          } else {
            // if continuous field
            ret(i) = new FieldStats(
              true, labeledVector.vector.apply(i), labeledVector.vector.apply(i))
          }
        }
        val h = new mutable.HashMap[Double, Int]()
        h.put(labeledVector.label, 1)
        // the label field is always categorical
        ret(config.dimension) = new FieldStats(false, fieldCategories = h)
        (ret, 1)
      }

    }

    // now reduce to merge all fieldStats and to get the total number of points in the data set
    val combinedStats_tuple = input.map(initTree).withBroadcastSet(
      input.getExecutionEnvironment.fromElements(config), DECISION_TREE_CONFIG
    ).reduce(
        (x, y) => {
          val a = x._1
          val b = y._1
          val c = new Array[FieldStats](a.length) // to hold the merged fieldStats
          for (i <- 0 to a.length - 1) {
            if (a(i).fieldType) {
              val min = Math.min(a(i).fieldMinValue, b(i).fieldMinValue)
              val max = Math.max(a(i).fieldMaxValue, b(i).fieldMaxValue)
              c(i) = new FieldStats(true, min, max)
            } else {
              // merge both hashmaps. We don't care about the values, just the keys
              b(i).fieldCategories.keysIterator.foreach(
                cat => a(i).fieldCategories.put(cat, 1)
              )
              c(i) = new FieldStats(false, fieldCategories = a(i).fieldCategories)
            }
          }
          (c, x._2 + y._2) // counting number of instances
        }
      ).collect().toArray.apply(0)

    val combinedStats = combinedStats_tuple._1
    config.setNumTrainVector(combinedStats_tuple._2)

    // cross check user-specified number of classes
    require(combinedStats.apply(config.dimension).fieldCategories.size == config.numClasses,
      "Specify number of classes correctly")

    // now copy the label list to an array of double
    // Find the minimum value, negative of which will be set to labelAddition
    var min_label = java.lang.Double.MAX_VALUE
    val labels_list = new util.ArrayList[Double](
      combinedStats.apply(config.dimension).fieldCategories.size)

    combinedStats.apply(config.dimension).fieldCategories.keysIterator.foreach(
      x => {
        if (x < min_label) min_label = x
        labels_list.add(x)
      }
    )
    val labels = new Array[Double](labels_list.size)
    for (i <- 0 to labels.length - 1) {
      labels(i) = labels_list.get(i)
    }

    // the root node of the tree. Tree ID set to 1 by default
    val h = new collection.mutable.HashMap[Int, Node]
    h.put(1, new Node(1, 1, None, -1))

    config.setFieldStats(combinedStats.slice(0, config.dimension))
    config.setLabelAddition(-min_label)
    config.setLabels(labels)

    input.getExecutionEnvironment.fromElements(new Tree(1, h, config))
  }
}

/** Resulting Tree model calculated by the Decision Tree algorithm.
  *
  * @param tree the final tree generated by fit which is to be used for predictions
  */
case class DecisionTreeModel(tree: DataSet[Tree])
  extends Transformer[Vector, LabeledVector]
  with Serializable {

  import DecisionTree.DECISION_TREE

  /** Calculates the label for the input set using the tree
    *
    * @param input [[DataSet]] containing the vector for which to calculate the predictions
    * @param parameters Parameter values for the algorithm
    * @return [[DataSet]] containing the labeled vectors
    */
  override def transform(input: DataSet[Vector], parameters: ParameterMap):
  DataSet[LabeledVector] = {
    input.map(new DecisionTreePredictionMapper).withBroadcastSet(tree, DECISION_TREE)
  }

  /** Calculates the accuracy of classification on te test data set input
    *
    * @param input [[DataSet]] containing the vector for which to calculate the predictions
    * @return Percentage accuracy, using a 0-1 loss function
    */

  def testAccuracy(input: DataSet[LabeledVector]): Double = {

    val accuracyMapper = new RichMapFunction[LabeledVector, (Int, Int)] {
      var tree: Tree = _

      override def open(parameters: Configuration): Unit = {
        tree = getRuntimeContext.getBroadcastVariable(DECISION_TREE).get(0)
      }

      override def map(labeledVector: LabeledVector): (Int, Int) = {
        val label = tree.filter(labeledVector.vector)._2 - tree.config.labelAddition
        if (label == labeledVector.label) (1, 1)
        else (0, 1)
      }
    }
    val result = input.map(accuracyMapper).withBroadcastSet(tree, DECISION_TREE).
      reduce((a, b) => (a._1 + b._1, a._2 + b._2)).
      collect().toArray.apply(0)
    100 * (result._1 + 0.0) / result._2
  }
}

/** Mapper to calculate the value of the prediction function. This is a RichMapFunction, because
  * we broadcast the tree to all mappers.
  *
  */
class DecisionTreePredictionMapper
  extends RichMapFunction[Vector, LabeledVector] {

  import DecisionTree.DECISION_TREE

  var tree: Tree = _
  var labelAddition: Double = 0

  @throws(classOf[Exception])
  override def open(configuration: Configuration): Unit = {
    // get the Tree
    tree = getRuntimeContext.getBroadcastVariable[Tree](DECISION_TREE).get(0)
    labelAddition = tree.config.labelAddition
  }

  override def map(vector: Vector): LabeledVector = {
    // calculate the predicted label
    val label = tree.filter(vector)._2
    LabeledVector(label - labelAddition, vector)
  }
}
