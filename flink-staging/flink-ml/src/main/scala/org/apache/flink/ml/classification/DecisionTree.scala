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

import org.apache.flink.api.scala._
import org.apache.flink.ml._
import org.apache.flink.ml.common.FlinkMLTools.ModuloKeyPartitioner
import org.apache.flink.ml.common._
import org.apache.flink.ml.math.{DenseVector, Vector}
import org.apache.flink.ml.pipeline.{PredictOperation, FitOperation, Predictor}
import org.apache.flink.ml.statistics.{OnlineHistogram, DataStats, DiscreteHistogram, ContinuousHistogram}
import org.apache.flink.ml.statistics.FieldType._
import org.apache.flink.ml.tree._

import scala.collection.mutable

/**
 * Decision Tree Classifier for classification problems.
 *
 * A decision tree is a binary tree structure where every [[tree.Node]] holds a [[SplitValue]] which
 * determines whether a particular instance goes to the left tree or the right tree.
 * A leaf node of the tree holds the predicted label for any instance which reaches there.
 *
 * Training phase:
 * During training, we start with a root node, which is in untrained state. The tree goes
 * through a series of growth steps where each untrained node is trained using the instances
 * which reach to this node.
 * Based on all training instances which reach a particular node, a histogram is calculated to
 * evaluate the distribution of values across every attribute of the examples, which is used to
 * find the best possible split of the node.
 *
 * Testing phase:
 * During testing, every instance is sent down to a leaf node following a series of left-right
 * steps determined by the [[SplitValue]] held by the nodes of the tree. Upon reaching a leaf
 * node, we have a prediction for this instance.
 *
 * Details of the algorithm can be found
 * {{{@link http://www.jmlr.org/papers/volume11/ben-haim10a/ben-haim10a.pdf}}}
 * @example
 * {{{
 *             val trainingDS: DataSet[LabeledVector] = ...
 *             val tree = DecisionTree()
 *             .setDepth(20).setMinInstancePerNode(2)
 *             .setSplitStrategy("Gini")
 *             .setMaxBins(100)
 *             .setDimension(4)
 *             .setCategory(Array(0,2))
 *             .setClasses(2)
 *
 *             tree.fit(trainingDS)
 *
 *             val testingDS: DataSet[Vector] = ...
 *             val predictions: DataSet[LabeledVector] = tree.predict(testingDS)
 *
 * }}}
 *
 * =Parameters=
 *
 * - [[org.apache.flink.ml.classification.DecisionTree.Depth]]:
 * Sets the maximum allowed depth of the tree.
 * (Default value: '''30''')
 *
 * - [[org.apache.flink.ml.classification.DecisionTree.Category]]:
 * Sets which attributes are to be considered as categorical fields. For example, if the 0th ,2nd
 * and 5th attributes are categorical, this input would be [0,2,5]
 * (Default value: '''Array()''')
 *
 * - [[org.apache.flink.ml.classification.DecisionTree.Classes]]:
 * Sets the number of classes in the data
 * (Default value: '''2''')
 *
 * - [[org.apache.flink.ml.classification.DecisionTree.MaxBins]]:
 * Sets the maximum number of splits allowed for every attribute. This number should be neither
 * too high nor too low. Increasing this would lead to an increase in accuracy, but would
 * require more time for training.
 * (Default value: '''100''')
 *
 * - [[org.apache.flink.ml.classification.DecisionTree.MinInstancesPerNode]]:
 * Sets the minimum number of instances that must be present at a node
 * (Default value: '''1''')
 *
 * - [[org.apache.flink.ml.classification.DecisionTree.Pruning]]:
 * Sets whether to prune the tree after training or not. As of now, this option is obsolete.
 * (Default value: '''false''')
 *
 * - [[org.apache.flink.ml.classification.DecisionTree.SplitStrategy]]:
 * Sets the split algorithm which should be used. The two most popular methods of "Gini" gain and
 * "Entropy" are supported.
 * (Default value: '''Gini''')
 */

class DecisionTree extends Predictor[DecisionTree] {

  import DecisionTree._

  /** store the decision tree learned after the fit operation */
  var treeOption: Option[DataSet[Tree]] = None

  /**
   * Sets the maximum allowed depth of the tree.
   * Currently only allowed values up to 30
   *
   * @param depth Maximum depth of the trained tree
   * @return itself
   */
  def setDepth(depth: Int): DecisionTree = {
    require(depth <= 30, "Maximum depth allowed: 30")
    parameters.add(Depth, depth)
    this
  }

  /**
   * Sets the splitting strategy. Gini and Entropy supported.
   *
   * @param splitStrategy Splitting strategy to be used
   * @return itself
   */
  def setSplitStrategy(splitStrategy: String): DecisionTree = {
    require(splitStrategy == "Gini" || splitStrategy == "Entropy",
      "Strategy " + splitStrategy + " not supported.")
    parameters.add(SplitStrategy, splitStrategy)
    this
  }

  /**
   * Sets minimum number of instances that must be present at a node for its parent to split
   *
   * @param minInstancesPerNode Minimum instances at every leaf node
   * @return itself
   */
  def setMinInstancePerNode(minInstancesPerNode: Int): DecisionTree = {
    require(minInstancesPerNode >= 1,
      "Every node must have at least one instance associated with it")
    parameters.add(MinInstancesPerNode, minInstancesPerNode)
    this
  }

  /**
   * Sets whether or not to prune the tree after building
   *
   * @param prune Whether to prune the tree or not
   * @return itself
   */
  def setPruning(prune: Boolean): DecisionTree = {
    parameters.add(Pruning, prune)
    this
  }

  /**
   * Sets maximum number of bins to be used for calculating splits.
   *
   * @param maxBins Maximum number of splits to be evaluated at a node
   * @return itself
   */
  def setMaxBins(maxBins: Int): DecisionTree = {
    require(maxBins >= 1, "Maximum bins used must be at least one")
    parameters.add(MaxBins, maxBins)
    this
  }

  /**
   * Sets which fields are to be considered categorical. Array of field indices
   *
   * @param category Indexes of attributes which are categorical
   * @return itself
   */
  def setCategory(category: Array[Int]): DecisionTree = {
    parameters.add(Category, category)
    this
  }

  /**
   * Sets how many classes there are in the data [will be cross checked with the data later]
   *
   * @param numClasses Number of classes in the data
   * @return itself
   */
  def setClasses(numClasses: Int): DecisionTree = {
    require(numClasses > 1, "There must be at least two classes in the data")
    parameters.add(Classes, numClasses)
    this
  }

  /**
   * Sets how many classes there are in the data [will be cross checked with the data later]
   *
   * @param dimension Dimensionality of the data
   * @return itself
   */
  def setDimension(dimension: Int): DecisionTree = {
    require(dimension > 0, "Dimension must at least be one.")
    parameters.add(Dimension, dimension)
    this
  }

  def testAccuracy(input: DataSet[LabeledVector], tree: DataSet[Tree]): Double = {
    val result = input.mapWithBcVariable(tree) {
      (vector, tree) =>
        val label = tree.filter(vector.vector).predict.get
        if (label == vector.label) {
          (1, 1)
        }
        else {
          (0, 1)
        }
    }.reduce((a, b) => (a._1 + b._1, a._2 + b._2)).collect().toArray.apply(0)
    100 * (result._1 + 0.0) / result._2
  }
}

/** Companion object of Decision Tree.
  * Contains convenience functions and the parameter type definitions of the algorithm.
  *
  */
object DecisionTree {

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

  case object Category extends Parameter[Array[Int]] {
    val defaultValue = Some(Array.ofDim[Int](0))
  }

  case object Classes extends Parameter[Int] {
    val defaultValue = None
  }

  case object Dimension extends Parameter[Int] {
    val defaultValue = None
  }


  // ========================================== Operations =========================================

  /** Provides the operation that makes the predictions for individual examples.
    *
    * @tparam T
    * @return A PredictOperation, through which it is possible to predict a value, given a
    *         feature vector
    */
  implicit def predictVectors[T <: Vector] = {
    new PredictOperation[DecisionTree, Tree, T, Double]() {

      override def getModel(self: DecisionTree, predictParameters: ParameterMap): DataSet[Tree] = {
        self.treeOption match {
          case Some(model) => model
          case None => {
            throw new RuntimeException("The Decision Tree model has not been trained. Call first " +
              "fit before calling the predict operation.")
          }
        }
      }

      override def predict(value: T, model: Tree): Double = {
        model.filter(value).predict.get
      }
    }
  }

  /**
   * [[FitOperation]] which trains a decision tree
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
        val category = resultingParameters(Category)
        val numClasses = resultingParameters(Classes)
        val dimension = resultingParameters(Dimension)

        val (initialTree, numberVectors) = createInitialTree(
          input,
          new TreeConfiguration(
            maxBins, minInstancePerNode, depth, pruneTree, splitStrategy, numClasses
          ),
          category, dimension)

        // Group the input data into blocks in round robin fashion
        val blockedInputNumberElements = FlinkMLTools.block(
          input,
          input.getParallelism,
          Some(ModuloKeyPartitioner)
        ).cross(numberVectors)
          .map { x => x }

        instance.treeOption = Some(initialTree.iterateWithTermination(depth) {
          tree => {
            val currentHistograms = localHistograms(tree, blockedInputNumberElements)
              .groupBy(0, 1, 2)
              .reduce(
                (a, b) => {
                  // a & b are tuple of (node id, attribute index, label, histogram)
                  // we have them grouped by (node id, attribute index, histogram)
                  val node_id = a._1
                  val attribute_index = a._2
                  val label_val = a._3
                  val firstHistogram = a._4
                  val secondHistogram = b._4
                  firstHistogram match {
                    case temp: ContinuousHistogram =>
                      (node_id,
                        attribute_index,
                        label_val,
                        firstHistogram.merge(secondHistogram, maxBins + 1))
                    case temp: DiscreteHistogram =>
                      (node_id,
                        attribute_index,
                        label_val,
                        firstHistogram.merge(secondHistogram, firstHistogram.bins))
                    case default =>
                      throw new RuntimeException("An unexpected error occurred")
                  }
                }
              )
            val updates = tree.mapWithBcSet(currentHistograms) {
              (tree, histograms) =>
                val changedNodes = tree.split(histograms)
                (tree, changedNodes)
            }
            (updates.map(x => x._1), updates.filter(x => x._2))
          }
        })
      }
    }
  }

  /**
   * Returns histograms from each input block
   * Output format follows the form of (Node ID, Attribute Index, Label, Histogram)
   *
   */
  private def localHistograms(
      tree: DataSet[Tree],
      blockedInputNumberElements: DataSet[(Block[LabeledVector], Long)])
    : DataSet[(Int, Int, Double, OnlineHistogram)] = {

    blockedInputNumberElements.flatMapWithBcVariable(tree) {
      (blockedElements, tree) => {
        val histograms = new mutable.HashMap[(Int, Int, Double), OnlineHistogram]()
        val dimension = tree.config.getDataStats.dimension
        val fieldStats = tree.config.getDataStats.fields
        val maxBins = tree.config.maxBins
        val (block, _) = blockedElements
        val numPoints = block.values.length
        for (i <- 0 to numPoints - 1) {
          val LabeledVector(label, vector) = block.values.apply(i)
          val node = tree.filter(vector)
          if (node.predict.isEmpty) {
            for (j <- 0 to dimension - 1) {
              histograms.get((node.id, j, label)) match {
                // if this histogram already exists, add a new entry to it
                case Some(hist) => hist.add(vector.apply(j))
                case None =>
                  // otherwise, create the histogram and put the entry in it
                  if (fieldStats.apply(j).fieldType == CONTINUOUS) {
                    val min_value = fieldStats.apply(j).min
                    val max_value = fieldStats.apply(j).max
                    histograms.put((node.id, j, label), new ContinuousHistogram(
                      maxBins + 1, 2 * min_value - max_value, 2 * max_value - min_value))
                  } else {
                    val categoryList = fieldStats.apply(j).categoryCounts
                    histograms.put((node.id, j, label), new DiscreteHistogram(categoryList.size))
                  }
                  histograms.get((node.id, j, label)).get.add(vector.apply(j))
              }
            }
          }
        }
        histograms.map(x => (x._1._1, x._1._2, x._1._3, x._2)).iterator.toList
      }
    }
  }

  /** Creates the initial root
    *
    * @return initial decision tree, after making sanity checks on data and evaluating statistics
    */
  private def createInitialTree(
      input: DataSet[LabeledVector],
      config: TreeConfiguration,
      category: Array[Int],
      dimension: Int)
    : (DataSet[Tree], DataSet[Long]) = {

    // treat the label as another class in itself.
    val categoryBuffer = category.toBuffer
    categoryBuffer += dimension
    val newCategory = categoryBuffer.toArray
    val stats = DataStats.dataStats(input.map(labeledVector => {
      val vector = labeledVector.vector
      val newVector = Array.ofDim[Double](vector.size + 1)
      for (i <- 0 to vector.size - 1) {
        newVector(i) = vector.apply(i)
      }
      newVector(vector.size) = labeledVector.label
      DenseVector(newVector)
    }), newCategory).collect().head

    require(stats.dimension == dimension + 1, "Invalid data.")
    config.setDataStats(
      new DataStats(stats.total, stats.dimension - 1, stats.fields.slice(0, dimension))
    )
    config.setLabels(stats.fields.apply(dimension).categoryCounts.keysIterator.toArray)

    (input.getExecutionEnvironment.fromElements(new Tree(1, config)),
      input.getExecutionEnvironment.fromElements(stats.total))
  }
}
