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
import org.apache.flink.api.scala.utils._
import org.apache.flink.api.scala._
import org.apache.flink.ml.common._
import org.apache.flink.ml.math.{Vector => FlinkVector}
import org.apache.flink.ml.metrics.distances.{SquaredEuclideanDistanceMetric,
DistanceMetric, EuclideanDistanceMetric}
import org.apache.flink.ml.pipeline.{FitOperation, PredictDataSetOperation, Predictor}
import org.apache.flink.util.Collector
import org.apache.flink.api.common.operators.base.CrossOperatorBase.CrossHint

import scala.collection.immutable.Vector
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

/** Implements a k-nearest neighbor join.
  *
  * Calculates the `k` nearest neighbor points in the training set for each point in the test set.
  *
  * @example
  * {{{
  *            val trainingDS: DataSet[Vector] = ...
  *            val testingDS: DataSet[Vector] = ...
  *
  *            val knn = KNN()
  *              .setK(10)
  *              .setBlocks(5)
  *              .setDistanceMetric(EuclideanDistanceMetric())
  *
  *            knn.fit(trainingDS)
  *
  *            val predictionDS: DataSet[(Vector, Array[Vector])] = knn.predict(testingDS)
  * }}}
  *
  * =Parameters=
  *
  * - [[org.apache.flink.ml.nn.KNN.K]]
  * Sets the K which is the number of selected points as neighbors. (Default value: '''5''')
  *
  * - [[org.apache.flink.ml.nn.KNN.DistanceMetric]]
  * Sets the distance metric we use to calculate the distance between two points. If no metric is
  * specified, then [[org.apache.flink.ml.metrics.distances.EuclideanDistanceMetric]] is used.
  * (Default value: '''EuclideanDistanceMetric()''')
  *
  * - [[org.apache.flink.ml.nn.KNN.Blocks]]
  * Sets the number of blocks into which the input data will be split. This number should be set
  * at least to the degree of parallelism. If no value is specified, then the parallelism of the
  * input [[DataSet]] is used as the number of blocks. (Default value: '''None''')
  *
  * - [[org.apache.flink.ml.nn.KNN.computeExactKNNParam]]
  * A boolean variable that whether or not to do an exact or approximate knn query
  * (Default value:  ```false```)
  *
  * - [[org.apache.flink.ml.nn.KNN.UseQuadTreeParam]]
  * A boolean variable that whether or not to use a Quadtree to partition the training set
  * to potentially simplify the KNN search.  If no value is specified, the code will
  * automatically decide whether or not to use a Quadtree.  Use of a Quadtree scales well
  * with the number of training and testing points, though poorly with the dimension.
  * (Default value:  ```None```)
  *
  * - [[org.apache.flink.ml.nn.KNN.UseLSHParam]]
  * A boolean variable that whether or not to use a LSH based hashing
  * (Default value:  ```false```)
  *
  * * - [[org.apache.flink.ml.nn.KNN.UseZValuesParam]]
  * A boolean variable that whether or not to use a z-value based hashing
  * (Default value:  ```None```)
  *
  * - [[org.apache.flink.ml.nn.KNN.SizeHint]]
  * Specifies whether the training set or test set is small to optimize the cross
  * product operation needed for the KNN search.  If the training set is small
  * this should be `CrossHint.FIRST_IS_SMALL` and set to `CrossHint.SECOND_IS_SMALL`
  * if the test set is small.
  * (Default value:  ```None```)
  *
  */

class KNN extends Predictor[KNN] {

  import KNN._

  var trainingSet: Option[DataSet[Block[FlinkVector]]] = None

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

  /** Sets whether to use exact or non-exact query
    * @param computeExactKNN Sets whether to do exact or approximate KNN search
    */
  def setExact(computeExactKNN: Boolean): KNN = {
    if (computeExactKNN && parameters.get(UseLSHParam).isDefined) {
      require(!parameters.get(UseLSHParam).get, "parameters setExact and" +
        " setUseLSH cannot both be true!!\"")
    }
    if (computeExactKNN && parameters.get(UseZValuesParam).isDefined) {
      require(!parameters.get(UseZValuesParam).get, "parameters setExact and" +
        " setUseZValues cannot both be true!!\"")
    }

    if (!computeExactKNN && parameters.get(UseQuadTreeParam).isDefined){
      require(!parameters.get(UseQuadTreeParam).get, "parameter setExact cannot" +
        "be false while useQuadTree is true")
    }
    parameters.add(computeExactKNNParam, computeExactKNN)
    this
  }

  /** Sets whether to use Quadtree
    * @param useQuadTree Boolean variable that decides whether to use the QuadTree
    */
  def setUseQuadTree(useQuadTree: Boolean): KNN = {
    if (useQuadTree) {
      require(parameters(DistanceMetric).isInstanceOf[SquaredEuclideanDistanceMetric] ||
        parameters(DistanceMetric).isInstanceOf[EuclideanDistanceMetric],
        "when using the quadtree, metric must be Euclidean!")
      if (parameters.get(computeExactKNNParam).isDefined) {
        require(parameters.get(computeExactKNNParam).get, "parameters setUseZValues and" +
          "when using quadtree, setExact cannot be false!")
      } else {
        setExact(true)
      }
    }
    parameters.add(UseQuadTreeParam, useQuadTree)
    this
  }

  /** Sets whether to use z-value based approximate KNN
    * @param UseZValues Boolean variable that decides whether to LSH based approximate KNN
    */
  def setUseZValues(UseZValues: Boolean): KNN = {
    if (UseZValues) {
      if (parameters.get(computeExactKNNParam).isDefined) {
        require(!parameters.get(computeExactKNNParam).get, "parameters setUseZValues and" +
          " setExact cannot both be true!!\"")
      } else {
        setExact(false)
      }

      if (parameters.get(UseLSHParam).isDefined) {
        require(!parameters.get(UseLSHParam).get, "parameters setUseZvalues and" +
          " setUseLSH cannot both be true!!\"")
      }
    }
    parameters.add(UseZValuesParam, UseZValues)
    this
  }

  /** Sets whether to use LSH based approximate KNN
    * @param UseLSH Boolean variable that decides whether to LSH based approximate KNN
    */
  def setUseLSH(UseLSH: Boolean): KNN = {
    if (UseLSH) {
      if (parameters.get(computeExactKNNParam).isDefined) {
        require(!parameters.get(computeExactKNNParam).get, "parameters setUseLSH and" +
          " setExact cannot both be true!!\"")
      } else {
        setExact(false)
      }
      if (parameters.get(UseZValuesParam).isDefined) {
        require(!parameters.get(UseZValuesParam).get, "parameters setUseLSH and" +
          " setUseZvalues cannot both be true!!\"")
      }
    }

    parameters.add(UseLSHParam, UseLSH)
    this
  }

  /** Sets hint for when one of the training or test set is small
    * Parameter a user can specify if one of the training or test sets are small
    * @param sizeHint use FIRST_IS_SMALL or SECOND_IS_SMALL
    *                 if the training and test sets are respectively small
    */
  def setSizeHint(sizeHint: CrossHint): KNN = {
    parameters.add(SizeHint, sizeHint)
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

  case object computeExactKNNParam extends Parameter[Boolean] {
    val defaultValue: Option[Boolean] = None
  }

  case object UseQuadTreeParam extends Parameter[Boolean] {
    val defaultValue: Option[Boolean] = None
  }

  case object UseZValuesParam extends Parameter[Boolean] {
    val defaultValue: Option[Boolean] = None
  }

  case object UseLSHParam extends Parameter[Boolean] {
    val defaultValue: Option[Boolean] = Some(false)
  }

  case object SizeHint extends Parameter[CrossHint] {
    val defaultValue: Option[CrossHint] = None
  }

  def apply(): KNN = {
    new KNN()
  }

  /** [[FitOperation]] which trains a KNN based on the given training data set.
    * @tparam T Subtype of [[org.apache.flink.ml.math.Vector]]
    */
  implicit def fitKNN[T <: FlinkVector : TypeInformation] = new FitOperation[KNN, T] {
    override def fit(
                      instance: KNN,
                      fitParameters: ParameterMap,
                      input: DataSet[T]): Unit = {
      val resultParameters = instance.parameters ++ fitParameters

      require(resultParameters.get(K).isDefined, "K is needed for calculation")

      // if using z-values, make sure dimension <= 30
      if (resultParameters.get(UseZValuesParam).isDefined) {
        if (resultParameters.get(UseZValuesParam).get) {
          require(input.first(1).collect().head.size <= 30, "dimension of points" +
            "should be <= 30 when using z-values!")
        }
      }

      val blocks = resultParameters.get(Blocks).getOrElse(input.getParallelism)
      val partitioner = FlinkMLTools.ModuloKeyPartitioner
      val inputAsVector = input.asInstanceOf[DataSet[FlinkVector]]

      instance.trainingSet = Some(FlinkMLTools.block(inputAsVector, blocks, Some(partitioner)))
    }
  }

  /** [[PredictDataSetOperation]] which calculates k-nearest neighbors of the given testing data
    * set.
    * @tparam T Subtype of [[Vector]]
    * @return The given testing data set with k-nearest neighbors
    */
  implicit def predictValues[T <: FlinkVector : ClassTag : TypeInformation] = {
    new PredictDataSetOperation[KNN, T, (FlinkVector, Array[FlinkVector])] {
      override def predictDataSet(
                                   instance: KNN,
                                   predictParameters: ParameterMap,
                                   input: DataSet[T]): DataSet[(FlinkVector,
        Array[FlinkVector])] = {
        val resultParameters = instance.parameters ++ predictParameters

        instance.trainingSet match {
          case Some(trainingSet) =>
            val k = resultParameters.get(K).get
            val blocks = resultParameters.get(Blocks).getOrElse(input.getParallelism)
            val metric = resultParameters.get(DistanceMetric).get
            val partitioner = FlinkMLTools.ModuloKeyPartitioner
            val exact = resultParameters.get(computeExactKNNParam).get
            val lsh = resultParameters.get(UseLSHParam).get

            // attach unique id for each data
            val inputWithId: DataSet[(Long, T)] = input.zipWithUniqueId

            // split data into multiple blocks
            val inputSplit = FlinkMLTools.block(inputWithId, blocks, Some(partitioner))

            val sizeHint = resultParameters.get(SizeHint)
            val crossTuned = sizeHint match {
              case Some(hint) if hint == CrossHint.FIRST_IS_SMALL =>
                trainingSet.crossWithHuge(inputSplit)
              case Some(hint) if hint == CrossHint.SECOND_IS_SMALL =>
                trainingSet.crossWithTiny(inputSplit)
              case _ => trainingSet.cross(inputSplit)
            }

            // join input and training set
            val crossed = crossTuned.mapPartition {
              (iter, out: Collector[(FlinkVector, FlinkVector, Long, Double)]) => {
                for ((training, testing) <- iter) {

                  // use a quadtree if (4^dim)Ntest*log(Ntrain)
                  // < Ntest*Ntrain, and distance is Euclidean
                  val useQuadTree = resultParameters.get(UseQuadTreeParam).getOrElse(
                    training.values.head.size * math.log(4.0) +
                      math.log(math.log(training.values.length))
                      < math.log(training.values.length) &&
                      (metric.isInstanceOf[EuclideanDistanceMetric] ||
                        metric.isInstanceOf[SquaredEuclideanDistanceMetric]))

                  if (useQuadTree) {
                    if (metric.isInstanceOf[EuclideanDistanceMetric] ||
                      metric.isInstanceOf[SquaredEuclideanDistanceMetric]) {
                      val quadTreeKNN = new QuadtreeKNN()
                      quadTreeKNN.knnQueryWithQuadTree(training.values, testing.values,
                        k, metric, out)
                    } else {
                      throw new IllegalArgumentException(s" Error: metric must be" +
                        s" Euclidean or SquaredEuclidean!")
                    }
                  } else if (exact) {
                    val basicKNNClass = new basicKNN()
                    basicKNNClass.knnQueryBasic(training.values, testing.values, k, metric, out)
                  } else if (!exact && training.values.head.size < 30) {
                    val zKNNClass = new zKNN()
                    zKNNClass.zknnQuery(training.values, testing.values, k, metric, out)
                  } else if (lsh) {
                    val lshKNNClass = new lshKNN()
                    lshKNNClass.lshknnQuery(training.values, testing.values, k, metric, out)
                  } else {
                    // when not exact and dim > 30, use LSH
                    val lshKNNClass = new lshKNN()
                    lshKNNClass.lshknnQuery(training.values, testing.values, k, metric, out)
                  }
                }
              }
            }

            // group by input vector id and pick k nearest neighbor for each group
            val result = crossed.groupBy(2).sortGroup(3, Order.ASCENDING).reduceGroup {
              (iter, out: Collector[(FlinkVector, Array[FlinkVector])]) => {
                if (iter.hasNext) {
                  val head = iter.next()
                  val key = head._2
                  val neighbors: ArrayBuffer[FlinkVector] = ArrayBuffer(head._1)

                  for ((vector, _, _, _) <- iter.take(k - 1)) {
                    // we already took a first element
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
