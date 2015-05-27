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

/** Holds the configuration of the tree. User specified and automatically detected from data
  * All the following fields are user specified unless otherwise mentioned
  * @param MaxBins maximum splits to be considered while training
  * @param MinInstancePerNode Minimum instances that should be present at every leaf node
  * @param Depth Maximum allowed depth of the tree. (Maximum 30)
  * @param Pruning Whether to prune the tree after training or not
  * @param splitStrategy Which algorithm to use for splitting. Gini or Entropy
  * @param numClasses Number of classes in data (cross checked with data)
  * @param dimension Dimensionality of data(cross checked with data)
  * @param category Which fields are to be considered as categorical. Array of field indexes
  * @param fieldStats Field maximum and minimum values, list of categories [Automatically]
  * @param labels Array of labels slash classes in data [Automatically]
  * @param labelAddition Addition term to make all labels >=0 [Automatically]
  *                      [Only for internal use. Not visible to user]
  * @param numTrainVector Number of training instances [Automatically]
  */
class TreeConfiguration(
                         val MaxBins: Int,
                         val MinInstancePerNode: Int,
                         val Depth: Int,
                         val Pruning: Boolean,
                         val splitStrategy: String,
                         val numClasses: Int,
                         val dimension: Int,
                         val category: Array[Int],
                         var fieldStats: Array[FieldStats] = Array.ofDim(0),
                         var labels: Array[Double] = Array.ofDim(0),
                         var labelAddition: Double = 0,
                         var numTrainVector: Int = 0
                         ) {

  override def toString: String = {
    var ret = s"Maximum Binning: $MaxBins, Minimum Instance per leaf node: $MinInstancePerNode, " +
      s"Maximum Depth: $Depth, Pruning:$Pruning\nSplit strategy: $splitStrategy, Number of " +
      s"classes: $numClasses, Dimension of data: $dimension, Number of training vectors:" +
      s"$numTrainVector\n Categorical fields: " + java.util.Arrays.toString(category) +
      "\nLabels in data: " + java.util.Arrays.toString(labels) + "\nField stats:"

    fieldStats.iterator.foreach(x => ret = ret + x.toString)
    ret + s"\nLabel Addition: $labelAddition"
  }

  def setNumTrainVector(count: Int): Unit = {
    numTrainVector = count
  }

  def setFieldStats(stats: Array[FieldStats]): Unit = {
    fieldStats = stats
  }

  def setLabels(labels: Array[Double]): Unit = {
    this.labels = labels
  }

  def setLabelAddition(label_add: Double): Unit = {
    labelAddition = label_add
  }
}
