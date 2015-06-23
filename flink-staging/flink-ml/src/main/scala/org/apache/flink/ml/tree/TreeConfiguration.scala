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

import org.apache.flink.ml.statistics.DataStats

/**
 * Holds the configuration of the tree. User specified configuration entries and evaluated from
 * the training data
 *
 * @param maxBins Maximum splits to be considered while training
 * @param minInstancePerNode Minimum number of instances that should be present at every leaf node
 * @param depth Maximum allowed depth of the tree. (Maximum 30)
 * @param pruning Whether to prune the tree after training or not
 * @param splitStrategy Which algorithm to use for splitting. Gini or Entropy
 * @param numClasses Number of labels for the data
 *
 */
class TreeConfiguration(
    val maxBins: Int,
    val minInstancePerNode: Int,
    val depth: Int,
    val pruning: Boolean,
    val splitStrategy: String,
    val numClasses: Int)
  extends Serializable {

  private var dataStats: Option[DataStats] = None
  private var labels: Option[Array[Double]] = None

  override def toString: String = {
    s"Maximum Binning: $maxBins\n" +
      s"Minimum Instance per leaf node: $minInstancePerNode\n" +
      s"Maximum Depth: $depth\n" +
      s"Pruning:$pruning\n" +
      s"Split strategy: $splitStrategy\n" +
      s"Number of classes: $numClasses\n"
  }

  /**
   * Sets the statistics evaluated from the data
   *
   * @param dataStats Data Statistics
   */
  def setDataStats(dataStats: DataStats): Unit = {
    this.dataStats = Some(dataStats)
  }

  /**
   * Sets the labels in the training data
   *
   * @param labels Labels
   */
  def setLabels(labels: Array[Double]): Unit = {
    this.labels = Some(labels)
  }

  /**
   * Gets the data statistics
   *
   * @return Data Statistics
   */
  def getDataStats: DataStats = {
    dataStats match {
      case None => throw new RuntimeException("Data Statistics haven't been set.")
      case Some(stats) => stats
    }
  }

  /**
   * Gets the label set
   *
   * @return Labels
   */
  def getLabels: Array[Double] = {
    labels match {
      case None => throw new RuntimeException("Labels haven't been set.")
      case Some(labelArray) => labelArray
    }
  }
}
