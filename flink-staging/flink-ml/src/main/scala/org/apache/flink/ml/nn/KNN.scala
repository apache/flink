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

import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.DataSetUtils._
import org.apache.flink.api.scala._
import org.apache.flink.ml.common._
import org.apache.flink.ml.math.Vector
import org.apache.flink.ml.metrics.distances.{DistanceMetric, EuclideanDistanceMetric}
import org.apache.flink.ml.pipeline.{FitOperation, PredictDataSetOperation, Predictor}
import org.apache.flink.util.Collector

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

/** Implements a k-nearest neighbor join.
  *
  * Calculates the `k` nearest neighbor points in the training set for each point in the test set.
  *
  * @example
  * {{{
  *     val trainingDS: DataSet[Vector] = ...
  *     val testingDS: DataSet[Vector] = ...
  *
  *     val knn = KNN()
  *       .setK(10)
  *       .setBlocks(5)
  *       .setDistanceMetric(EuclideanDistanceMetric())
  *
  *     knn.fit(trainingDS)
  *
  *     val predictionDS: DataSet[(Vector, Array[Vector])] = knn.predict(testingDS)
  * }}}
  *
  * =Parameters=
  *
  * - [[org.apache.flink.ml.nn.KNN.K]]
  * Sets the K which is the number of selected points as neighbors. (Default value: '''5''')
  *
  * - [[org.apache.flink.ml.nn.KNN.Blocks]]
  * Sets the number of blocks into which the input data will be split. This number should be set
  * at least to the degree of parallelism. If no value is specified, then the parallelism of the
  * input [[DataSet]] is used as the number of blocks. (Default value: '''None''')
  *
  * - [[org.apache.flink.ml.nn.KNN.DistanceMetric]]
  * Sets the distance metric we use to calculate the distance between two points. If no metric is
  * specified, then [[org.apache.flink.ml.metrics.distances.EuclideanDistanceMetric]] is used.
  * (Default value: '''EuclideanDistanceMetric()''')
  *
  */
class KNN extends Predictor[KNN] {

  import KNN._

  var trainingSet: Option[DataSet[Block[Vector]]] = None

  /** Sets K
    * @param k the number of selected points as neighbors
    */
  def setK(k: Int): KNN = {
    require(k > 0, "K must be positive.")
    parameters.add(K, k)
    this
  }

  /** Sets the distance metric
    * @param metric the distance metric to calculate distance between two points
    */
  def setDistanceMetric(metric: DistanceMetric): KNN = {
    parameters.add(DistanceMetric, metric)
    this
  }

  /** Sets the number of data blocks/partitions
    * @param n the number of data blocks
    */
  def setBlocks(n: Int): KNN = {
    require(n > 0, "Number of blocks must be positive.")
    parameters.add(Blocks, n)
    this
  }
}

object KNN {

  case object K extends Parameter[Int] {
    val defaultValue: Option[Int] = Some(5)
  }

  case object DistanceMetric extends Parameter[DistanceMetric] {
    val defaultValue: Option[DistanceMetric] = Some(EuclideanDistanceMetric())
  }

  case object Blocks extends Parameter[Int] {
    val defaultValue: Option[Int] = None
  }

  def apply(): KNN = {
    new KNN()
  }

  /** [[FitOperation]] which trains a KNN based on the given training data set.
    * @tparam T Subtype of [[org.apache.flink.ml.math.Vector]]
    */
  implicit def fitKNN[T <: Vector : TypeInformation] = new FitOperation[KNN, T] {
    override def fit(
        instance: KNN,
        fitParameters: ParameterMap,
        input: DataSet[T]): Unit = {
      val resultParameters = instance.parameters ++ fitParameters

      require(resultParameters.get(K).isDefined, "K is needed for calculation")

      val blocks = resultParameters.get(Blocks).getOrElse(input.getParallelism)
      val partitioner = FlinkMLTools.ModuloKeyPartitioner
      val inputAsVector = input.asInstanceOf[DataSet[Vector]]

      instance.trainingSet = Some(FlinkMLTools.block(inputAsVector, blocks, Some(partitioner)))
    }
  }

  /** [[PredictDataSetOperation]] which calculates k-nearest neighbors of the given testing data
    * set.
    * @tparam T Subtype of [[Vector]]
    * @return The given testing data set with k-nearest neighbors
    */
  implicit def predictValues[T <: Vector : ClassTag : TypeInformation] = {
    new PredictDataSetOperation[KNN, T, (Vector, Array[Vector])] {
      override def predictDataSet(
          instance: KNN,
          predictParameters: ParameterMap,
          input: DataSet[T]): DataSet[(Vector, Array[Vector])] = {
        val resultParameters = instance.parameters ++ predictParameters

        instance.trainingSet match {
          case Some(trainingSet) =>
            val k = resultParameters.get(K).get
            val blocks = resultParameters.get(Blocks).getOrElse(input.getParallelism)
            val metric = resultParameters.get(DistanceMetric).get
            val partitioner = FlinkMLTools.ModuloKeyPartitioner

            // attach unique id for each data
            val inputWithId: DataSet[(Long, T)] = input.zipWithUniqueId

            // split data into multiple blocks
            val inputSplit = FlinkMLTools.block(inputWithId, blocks, Some(partitioner))

            // join input and training set
            val crossed = trainingSet.cross(inputSplit).mapPartition {
              (iter, out: Collector[(Vector, Vector, Long, Double)]) => {
                for ((training, testing) <- iter) {
                  val queue = mutable.PriorityQueue[(Vector, Vector, Long, Double)]()(
                    Ordering.by(_._4))

                  for (a <- testing.values; b <- training.values) {
                    // (training vector, input vector, input key, distance)
                    queue.enqueue((b, a._2, a._1, metric.distance(b, a._2)))

                    if (queue.size > k) {
                      queue.dequeue()
                    }
                  }

                  for (v <- queue) {
                    out.collect(v)
                  }
                }
              }
            }

            // group by input vector id and pick k nearest neighbor for each group
            val result = crossed.groupBy(2).sortGroup(3, Order.ASCENDING).reduceGroup {
              (iter, out: Collector[(Vector, Array[Vector])]) => {
                if (iter.hasNext) {
                  val head = iter.next()
                  val key = head._2
                  val neighbors: ArrayBuffer[Vector] = ArrayBuffer(head._1)

                  for ((vector, _, _, _) <- iter.take(k - 1)) { // we already took a first element
                    neighbors += vector
                  }

                  out.collect(key, neighbors.toArray)
                }
              }
            }

            result
          case None => throw new RuntimeException("The KNN model has not been trained." +
              "Call first fit before calling the predict operation.")
        }
      }
    }
  }
}
