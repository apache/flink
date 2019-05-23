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
import org.apache.flink.api.common.operators.base.CrossOperatorBase.CrossHint
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.utils._
import org.apache.flink.ml.common._
import org.apache.flink.ml.math.{DenseVector, Vector => FlinkVector}
import org.apache.flink.ml.metrics.distances._
import org.apache.flink.ml.pipeline.{FitOperation, PredictDataSetOperation, Predictor}
import org.apache.flink.util.Collector

import scala.collection.immutable.Vector
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

/** Implements a `k`-nearest neighbor join.
  *
  * Calculates the `k`-nearest neighbor points in the training set for each point in the test set.
  *
  * @example
  * {{{
  * val trainingDS: DataSet[Vector] = ...
  * val testingDS: DataSet[Vector] = ...
  *
  * val knn = KNN()
  *   .setK(10)
  *   .setBlocks(5)
  *   .setDistanceMetric(EuclideanDistanceMetric())
  *
  *   knn.fit(trainingDS)
  *
  * val predictionDS: DataSet[(Vector, Array[Vector])] = knn.predict(testingDS)
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
  * - [[org.apache.flink.ml.nn.KNN.UseQuadTree]]
  * A boolean variable that whether or not to use a quadtree to partition the training set
  * to potentially simplify the KNN search.  If no value is specified, the code will
  * automatically decide whether or not to use a quadtree.  Use of a quadtree scales well
  * with the number of training and testing points, though poorly with the dimension.
  * (Default value: '''None''')
  *
  * - [[org.apache.flink.ml.nn.KNN.SizeHint]]
  * Specifies whether the training set or test set is small to optimize the cross
  * product operation needed for the KNN search.  If the training set is small
  * this should be `CrossHint.FIRST_IS_SMALL` and set to `CrossHint.SECOND_IS_SMALL`
  * if the test set is small.
  * (Default value: '''None''')
  *
  */

class KNN extends Predictor[KNN] {

  import KNN._

  var trainingSet: Option[DataSet[Block[FlinkVector]]] = None

  /** Sets K
    *
    * @param k the number of selected points as neighbors
    */
  def setK(k: Int): KNN = {
    require(k > 0, "K must be positive.")
    parameters.add(K, k)
    this
  }

  /** Sets the distance metric
    *
    * @param metric the distance metric to calculate distance between two points
    */
  def setDistanceMetric(metric: DistanceMetric): KNN = {
    parameters.add(DistanceMetric, metric)
    this
  }

  /** Sets the number of data blocks/partitions
    *
    * @param n the number of data blocks
    */
  def setBlocks(n: Int): KNN = {
    require(n > 0, "Number of blocks must be positive.")
    parameters.add(Blocks, n)
    this
  }

  /** Sets the Boolean variable that decides whether to use the QuadTree or not */
  def setUseQuadTree(useQuadTree: Boolean): KNN = {
    if (useQuadTree) {
      require(parameters(DistanceMetric).isInstanceOf[SquaredEuclideanDistanceMetric] ||
        parameters(DistanceMetric).isInstanceOf[EuclideanDistanceMetric])
    }
    parameters.add(UseQuadTree, useQuadTree)
    this
  }

  /** Parameter a user can specify if one of the training or test sets are small
    *
    * @param sizeHint cross hint tells the system which sizes to expect from the data sets
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

  case object UseQuadTree extends Parameter[Boolean] {
    val defaultValue: Option[Boolean] = None
  }

  case object SizeHint extends Parameter[CrossHint] {
    val defaultValue: Option[CrossHint] = None
  }

  def apply(): KNN = {
    new KNN()
  }

  /** [[FitOperation]] which trains a KNN based on the given training data set.
    *
    * @tparam T Subtype of [[org.apache.flink.ml.math.Vector]]
    */
  implicit def fitKNN[T <: FlinkVector : TypeInformation] = new FitOperation[KNN, T] {
    override def fit(
      instance: KNN,
      fitParameters: ParameterMap,
      input: DataSet[T]
    ): Unit = {
      val resultParameters = instance.parameters ++ fitParameters

      require(resultParameters.get(K).isDefined, "K is needed for calculation")

      val blocks = resultParameters.get(Blocks).getOrElse(input.getParallelism)
      val partitioner = FlinkMLTools.ModuloKeyPartitioner
      val inputAsVector = input.asInstanceOf[DataSet[FlinkVector]]

      instance.trainingSet = Some(FlinkMLTools.block(inputAsVector, blocks, Some(partitioner)))
    }
  }

  /** [[PredictDataSetOperation]] which calculates k-nearest neighbors of the given testing data
    * set.
    *
    * @tparam T Subtype of [[Vector]]
    * @return The given testing data set with k-nearest neighbors
    */
  implicit def predictValues[T <: FlinkVector : ClassTag : TypeInformation] = {
    new PredictDataSetOperation[KNN, T, (FlinkVector, Array[FlinkVector])] {
      override def predictDataSet(
        instance: KNN,
        predictParameters: ParameterMap,
        input: DataSet[T]
      ): DataSet[(FlinkVector, Array[FlinkVector])] = {
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
              (iter: Iterator[(Block[FlinkVector], Block[(Long, T)])],
               out: Collector[(FlinkVector, FlinkVector, Long, Double)]) => {
                for ((training, testing) <- iter) {
                  // use a quadtree if (4 ^ dim) * Ntest * log(Ntrain)
                  // < Ntest * Ntrain, and distance is Euclidean
                  val checkSize = math.log(4.0) * training.values.head.size +
                    math.log(math.log(training.values.length)) < math.log(training.values.length)
                  val checkMetric = metric match {
                    case _: EuclideanDistanceMetric => true
                    case _: SquaredEuclideanDistanceMetric => true
                    case _ => false
                  }
                  val useQuadTree = resultParameters.get(UseQuadTree)
                    .getOrElse(checkSize && checkMetric)

                  if (useQuadTree) {
                    knnQueryWithQuadTree(training.values, testing.values, k, metric, out)
                  } else {
                    knnQueryBasic(training.values, testing.values, k, metric, out)
                  }
                }
                }
            }

            // group by input vector id and pick k nearest neighbor for each group
            val result = crossed.groupBy(2).sortGroup(3, Order.ASCENDING).reduceGroup {
              (iter: Iterator[(FlinkVector, FlinkVector, Long, Double)],
               out: Collector[(FlinkVector, Array[FlinkVector])]) => {
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

  private def knnQueryWithQuadTree[T <: FlinkVector](
    training: Vector[T],
    testing: Vector[(Long, T)],
    k: Int,
    metric: DistanceMetric,
    out: Collector[(FlinkVector, FlinkVector, Long, Double)]
  ): Unit = {
    // find a bounding box
    val MinArr = Array.tabulate(training.head.size)(x => x)
    val MaxArr = Array.tabulate(training.head.size)(x => x)

    val minVecTrain = MinArr.map(i => training.map(x => x(i)).min - 0.01)
    val minVecTest = MinArr.map(i => testing.map(x => x._2(i)).min - 0.01)
    val maxVecTrain = MaxArr.map(i => training.map(x => x(i)).max + 0.01)
    val maxVecTest = MaxArr.map(i => testing.map(x => x._2(i)).max + 0.01)

    val MinVec = DenseVector(MinArr.map(i => math.min(minVecTrain(i), minVecTest(i))))
    val MaxVec = DenseVector(MinArr.map(i => math.max(maxVecTrain(i), maxVecTest(i))))

    // default value of max elements/box is set to max(20,k)
    val maxPerBox = math.max(k, 20)
    val trainingQuadTree = new QuadTree(MinVec, MaxVec, metric, maxPerBox)

    val queue = mutable.PriorityQueue[(FlinkVector, FlinkVector, Long, Double)]()(
      Ordering.by(_._4))

    for (v <- training) {
      trainingQuadTree.insert(v)
    }

    for ((id, vector) <- testing) {
      // Find siblings' objects and do local kNN there
      val siblingObjects = trainingQuadTree.searchNeighborsSiblingQueue(vector)

      // do KNN query on siblingObjects and get max distance of kNN then rad is good choice
      // for a neighborhood to do a refined local kNN search
      val knnSiblings = siblingObjects.map(v => metric.distance(vector, v)).sortWith(_ < _).take(k)

      val rad = knnSiblings.last
      val trainingFiltered = trainingQuadTree.searchNeighbors(vector, rad)

      for (b <- trainingFiltered) {
        // (training vector, input vector, input key, distance)
        queue.enqueue((b, vector, id, metric.distance(b, vector)))
        if (queue.size > k) {
          queue.dequeue()
        }
      }

      for (v <- queue) {
        out.collect(v)
      }
    }
  }

  private def knnQueryBasic[T <: FlinkVector](
    training: Vector[T],
    testing: Vector[(Long, T)],
    k: Int,
    metric: DistanceMetric,
    out: Collector[(FlinkVector, FlinkVector, Long, Double)]
  ): Unit = {
    val queue = mutable.PriorityQueue[(FlinkVector, FlinkVector, Long, Double)]()(
      Ordering.by(_._4))
    
    for ((id, vector) <- testing) {
      for (b <- training) {
        // (training vector, input vector, input key, distance)
        queue.enqueue((b, vector, id, metric.distance(b, vector)))
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
