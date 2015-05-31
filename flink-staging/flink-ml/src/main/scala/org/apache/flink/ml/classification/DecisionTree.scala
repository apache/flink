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

import java.lang.Double.MAX_VALUE
import java.util

import org.apache.flink.api.common.functions.{RichFlatMapFunction, RichMapFunction}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.ml.common.FlinkMLTools.ModuloKeyPartitioner
import org.apache.flink.ml.common._
import org.apache.flink.ml.math.{CategoricalHistogram, ContinuousHistogram, OnlineHistogram, Vector}
import org.apache.flink.ml.pipeline.{FitOperation, PredictOperation, Predictor}
import org.apache.flink.ml.tree._
import org.apache.flink.util.Collector

import scala.collection.mutable

class DecisionTree extends Predictor[DecisionTree] {

  import DecisionTree._

  // store the decision tree learned after the fit operation
  var treeOption: Option[DataSet[Tree]] = None

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

  def testAccuracy(input: DataSet[LabeledVector], tree: DataSet[Tree]): Double = {

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

/** Companion object of Decision Tree.
  * Contains convenience functions and the parameter type definitions of the algorithm.
  *
  */
object DecisionTree {
  val DECISION_TREE = "decision_tree"
  val DECISION_TREE_CONFIG = "decision_tree_configuration"
  val DECISION_TREE_HISTS = "decision_tree_histograms"

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


  // ========================================== Operations =========================================

  /** [[org.apache.flink.ml.pipeline.PredictOperation]] for vector types. The result type is a
    * [[LabeledVector]]
    *
    * @tparam T Subtype of [[Vector]]
    * @return
    */
  implicit def predictValues[T <: Vector] = {
    new PredictOperation[DecisionTree, T, LabeledVector] {
      override def predict(
                            instance: DecisionTree,
                            predictParameters: ParameterMap,
                            input: DataSet[T])
      : DataSet[LabeledVector] = {

        instance.treeOption match {
          case Some(tree) => {
            input.map(new PredictionMapper[T]).withBroadcastSet(tree, DECISION_TREE)
          }
          case None => {
            throw new RuntimeException("The Decision Tree model has not been trained. Call first" +
              " fit before calling the predict operation.")
          }
        }
      }
    }
  }

  /** Mapper to calculate the value of the prediction function. This is a RichMapFunction, because
    * we broadcast the tree to all mappers.
    */
  class PredictionMapper[T <: Vector] extends RichMapFunction[T, LabeledVector] {

    var tree: Tree = _

    @throws(classOf[Exception])
    override def open(configuration: Configuration): Unit = {
      // get current tree
      tree = getRuntimeContext.
        getBroadcastVariable[Tree](DECISION_TREE).get(0)
    }

    override def map(vector: T): LabeledVector = {
      // calculate the prediction value
      val label = tree.filter(vector)._2
      LabeledVector(label - tree.config.labelAddition, vector)
    }
  }

  /** [[FitOperation]] which trains a decision tree
    *
    */
  implicit val fitTree = {
    new FitOperation[DecisionTree, LabeledVector] {

      override def fit(
                        instance: DecisionTree,
                        fitParameters: ParameterMap,
                        input: DataSet[LabeledVector])
      : Unit = {
        val resultingParameters = instance.parameters ++ fitParameters
        val depth = resultingParameters(Depth)
        val minInstancePerNode = resultingParameters(MinInstancesPerNode)
        val pruneTree = resultingParameters(Pruning)
        val maxBins = resultingParameters(MaxBins)
        val splitStrategy = resultingParameters(SplitStrategy)
        val dimension = resultingParameters(Dimension)
        val category = resultingParameters(Category)
        val numClasses = resultingParameters(Classes)

        require(category.length == 0, "Only continuous fields supported right now")
        val (initialTree, numberVectors) = createInitialTree(input, new TreeConfiguration(maxBins,
          minInstancePerNode, depth, pruneTree, splitStrategy, numClasses, dimension, category))

        // Group the input data into blocks in round robin fashion
        val blockedInputNumberElements = FlinkMLTools.block(
          input, input.getParallelism, Some(ModuloKeyPartitioner))
          .cross(numberVectors)
          .map { x => x }

        instance.treeOption = Some(initialTree.iterateWithTermination(depth) {
          tree => {
            val currentHistograms = localHistograms(tree, blockedInputNumberElements)
              .groupBy(0, 1, 2)
              .reduce(
                (a, b) => {
                  if (a._4.isInstanceOf[ContinuousHistogram]) {
                    (a._1, a._2, a._3, a._4.merge(b._4, maxBins + 1))
                  } else {
                    (a._1, a._2, a._3, a._4.merge(b._4, a._4.bins))
                  }
                }
              )
            val updates = tree.map(new RichMapFunction[Tree, (Tree, Array[Int])] {
              var histograms: util.List[(Int, Int, Double, OnlineHistogram)] = _

              override def open(parameters: Configuration): Unit = {
                histograms = getRuntimeContext.getBroadcastVariable(DECISION_TREE_HISTS)
              }

              override def map(tree: Tree): (Tree, Array[Int]) = {
                //println(tree.toString)
                val changedNodes = tree.split(histograms)
                (tree, changedNodes)
              }
            }).withBroadcastSet(currentHistograms, DECISION_TREE_HISTS)
            (updates.map(x => x._1), updates.filter(x => x._2.length > 0))
          }
        })
      }
    }
  }

  /** Returns histograms from each input block
    * Output format follows the form of (Node ID, Attribute Index, Label, Histogram)
    *
    */
  private def localHistograms(
                               tree: DataSet[Tree],
                               blockedInputNumberElements: DataSet[(Block[LabeledVector], Int)])
  : DataSet[(Int, Int, Double, OnlineHistogram)] = {

    /** Rich flat mapper calculating histograms for each data block. We use a RichFlatMapFunction
      * here because we broadcast the current value of the tree to all mappers.
      *
      */
    val localUpdate = new RichFlatMapFunction[
      (Block[LabeledVector], Int),
      (Int, Int, Double, OnlineHistogram)] {

      var tree: Tree = _
      var config: TreeConfiguration = _
      var histograms: mutable.HashMap[(Int, Int, Double), OnlineHistogram] = _

      override def open(parameters: Configuration): Unit = {
        // get the tree
        tree = getRuntimeContext.getBroadcastVariable(DECISION_TREE).get(0)
        config = tree.config
        histograms = new mutable.HashMap[(Int, Int, Double), OnlineHistogram]()
      }

      override def flatMap(
                            blockNumberElements: (Block[LabeledVector], Int),
                            out: Collector[(Int, Int, Double, OnlineHistogram)]
                            )
      : Unit = {
        // for all instances in the block
        val (block, _) = blockNumberElements
        val numLocalDataPoints = block.values.length

        for (i <- 0 to numLocalDataPoints - 1) {
          val LabeledVector(label, vector) = block.values(i)
          // find where this instance goes
          val (node, predict) = tree.filter(vector)
          //println(node)
          if (predict.round.toInt == -1) {
            // we can be sure that this is an unlabeled leaf and not some internal node.
            // That's how filter works
            for (j <- 0 to tree.config.dimension - 1) {
              histograms.get((node, j, label)) match {
                // if this histogram already exists, add a new entry to it
                case Some(hist) => hist.add(vector.apply(j))
                case None =>
                  // otherwise, create the histogram and put the entry in it
                  if (config.fieldStats.apply(j).fieldType) {
                    val min_value = config.fieldStats.apply(j).fieldMinValue
                    val max_value = config.fieldStats.apply(j).fieldMaxValue
                    histograms.put((node, j, label), new ContinuousHistogram(
                      config.MaxBins + 1, 2 * min_value - max_value, 2 * max_value - min_value))
                  } else {
                    val categoryList = config.fieldStats.apply(j).fieldCategories
                    histograms.put((node, j, label), new CategoricalHistogram(categoryList))
                  }
                  histograms.get((node, j, label)).get.add(vector.apply(j))
              }
            }
          }
        }
        histograms.keysIterator.foreach(
          x => out.collect((x._1, x._2, x._3, histograms.get(x).get))
        )
      }
    }
    // map using the RichFlatMapFunction with the tree broadcasted
    blockedInputNumberElements.flatMap(localUpdate).withBroadcastSet(tree, DECISION_TREE)
  }

  /** Creates the initial root
    *
    * @return initial decision tree, after making sanity checks on data and evaluating statistics
    */
  private def createInitialTree(input: DataSet[LabeledVector], config: TreeConfiguration):
  (DataSet[Tree], DataSet[Int]) = {

    val initTree = new RichMapFunction[LabeledVector, (Array[FieldStats], Int)] {

      var config: TreeConfiguration = _

      override def open(parameters: Configuration): Unit = {
        config = getRuntimeContext.getBroadcastVariable(DECISION_TREE_CONFIG).get(0)
      }

      override def map(labeledVector: LabeledVector):
      (Array[FieldStats], Int) = {
        require(labeledVector.vector.size == config.dimension,
          "Specify the dimension of data correctly")
        // we also include the label field for now
        val ret = Array.ofDim[FieldStats](config.dimension + 1)
        for (i <- 0 to config.dimension - 1) {
          val fieldValue = labeledVector.vector.apply(i)
          if (config.category.indexOf(i) >= 0) {
            // if this is a categorical field
            ret(i) = new FieldStats(false, fieldCategories = Array(fieldValue))
          } else {
            // if continuous field
            ret(i) = new FieldStats(true, fieldValue, fieldValue)
          }
        }
        // the label field is always categorical
        ret(config.dimension) = new FieldStats(false, fieldCategories = Array(labeledVector.label))
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
              // merge both arrays using a hashmap and convert to an array again
              val hash = new mutable.HashMap[Double, Int]()
              a(i).fieldCategories.iterator.foreach(
                x => hash.put(x, 1)
              )
              b(i).fieldCategories.iterator.foreach(
                x => hash.put(x, 1)
              )
              c(i) = new FieldStats(false, fieldCategories = hash.keySet.toVector.toArray)
            }
          }
          (c, x._2 + y._2) // counting number of instances
        }
      ).collect().toArray.apply(0)

    val combinedStats = combinedStats_tuple._1
    config.setNumTrainVector(combinedStats_tuple._2)

    // cross check user-specified number of classes
    require(combinedStats.apply(config.dimension).fieldCategories.length == config.numClasses,
      "Specify number of classes correctly")

    // now copy the label list to an array of double
    // Find the minimum value, negative of which will be set to labelAddition
    var min_label = MAX_VALUE
    val labels_list = new util.ArrayList[Double](
      combinedStats.apply(config.dimension).fieldCategories.length)

    combinedStats.apply(config.dimension).fieldCategories.iterator.foreach(
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

    (input.getExecutionEnvironment.fromElements(new Tree(1, h, config)),
      input.getExecutionEnvironment.fromElements(config.numTrainVector))
  }

}
