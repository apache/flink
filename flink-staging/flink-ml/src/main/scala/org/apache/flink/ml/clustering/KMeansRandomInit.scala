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

package org.apache.flink.ml.clustering

import java.util

import org.apache.flink.api.common.functions.{RichFilterFunction, RichFlatMapFunction, RichMapFunction}
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFields
import org.apache.flink.api.scala.{DataSet, _}
import org.apache.flink.configuration.Configuration
import org.apache.flink.ml.common.{LabeledVector, _}
import org.apache.flink.ml.math.Vector
import org.apache.flink.ml.metrics.distances.EuclideanDistanceMetric
import org.apache.flink.ml.pipeline._
import org.apache.flink.util.Collector

import scala.collection.JavaConverters._
import scala.util.Random

/**
 * Implements the Random initialization algorithm which calculates cluster centroids based on
 * set of training data points.
 *
 * [[KMeansRandomInit]] is a [[Predictor]] which needs to be trained on a set of data points and can
 * then be used to assign new points to the learned cluster centroids. Further, the trained
 * centroids can also be used as initial centroids for the KMeans algorithm
 *
 * The KMeansRandomInit algorithm picks centers uniformly at random from the input data set
 *
 * @example
 * {{{
 *            val trainingDS: DataSet[Vector] = env.fromCollection(Clustering.trainingData)
 *            val KMeansRandom = KMeansRandomInit().setNumClusters(10)
 *            KMeansRandom.fit(trainingDS)
 *
 *            val testDS: DataSet[Vector] = env.fromCollection(Clustering.testData)
 *
 *            val clusters: DataSet[LabeledVector] = model.transform(testDS)
 *
 *
 *            val initialCentroids: DataSet[LabeledVector] = KMeansRandom.centroids.get
 *
 * }}}
 * =Parameters=
 *
 * - [[org.apache.flink.ml.clustering.KMeansRandomInit.NumClusters]]:
 * Defines the number of clusters that need to be initialized. This should be the same as the
 * number of clusters desired for the KMeans algorithm in case the latter is run using the
 * clusters initialized with this algorithm
 * (Default value: '''0''')
 *
 */

class KMeansRandomInit extends Predictor[KMeansRandomInit] {

  import KMeansRandomInit._

  /** Stores the initialized clusters after the fit operation */
  var centroids: Option[DataSet[LabeledVector]] = None


  /**
   * Sets the number of clusters to initialize.
   *
   * @param numCluster number of clusters
   * @return itself
   */
  def setNumClusters(numCluster: Int): KMeansRandomInit = {
    parameters.add(NumClusters, numCluster)
    this
  }

}

/**
 * Companion object of KMeansRandomInit. Contains convenience functions, the parameter type
 * definitions of the algorithm and the [[FitOperation]] & [[PredictOperation]].
 *
 */
object KMeansRandomInit {
  val CENTROIDS = "KMeansRandomInitCentroids"
  val CLUSTER_NUM = "KMeansRandomInitClusterNum"
  val VECTOR_NUM = "KMeansRandomInitVectorNum"
  val OVER_SAMPLE = "KMeansRandomInitOverSample"

  case object NumClusters extends Parameter[Int] {
    val defaultValue = Some(0)
  }

  // ========================================== Factory methods ====================================

  def apply(): KMeansRandomInit = {
    new KMeansRandomInit()
  }

  // ========================================== Operations =========================================

  /**
   * [[PredictOperation]] for vector types. The result type is a [[LabeledVector]].
   */
  implicit def predictValues = {
    new PredictOperation[KMeansRandomInit, Vector, LabeledVector] {
      override def predict(
                            instance: KMeansRandomInit,
                            predictParameters: ParameterMap,
                            input: DataSet[Vector])
      : DataSet[LabeledVector] = {

        instance.centroids match {
          case Some(centroids) => {
            input.map(new SelectNearestCenterMapper).withBroadcastSet(centroids, CENTROIDS)
          }

          case None => {
            throw new RuntimeException("The KMeans model has not been trained. Call first fit" +
              "before calling the predict operation.")
          }
        }
      }
    }
  }

  /**
   * [[FitOperation]] which initializes the initial centers using the kmeans++ algorithm
   */
  implicit def fitKMeans = {
    new FitOperation[KMeansRandomInit, Vector] {

      override def fit(
                        instance: KMeansRandomInit,
                        fitParameters: ParameterMap,
                        input: DataSet[Vector])
      : Unit = {
        val resultingParameters = instance.parameters ++ fitParameters
        val k: Int = resultingParameters.get(NumClusters).get

        val numberVectors = input map { x => 1 } reduce {
          _ + _
        }

        val oversampledSet = input.filter(overSampler)
          .withBroadcastSet(numberVectors, VECTOR_NUM)
          .withBroadcastSet(input.getExecutionEnvironment.fromElements(k), CLUSTER_NUM)

        instance.centroids = Some(numberVectors
          .flatMap(pickTrueSample)
          .withBroadcastSet(oversampledSet, OVER_SAMPLE)
          .withBroadcastSet(input.getExecutionEnvironment.fromElements(k), CLUSTER_NUM)
        )
      }
    }
  }

  /** Samples every point in the data set with a probability 10*k/N
    * We get an expected number of 10*k samples.
    *
    */
  private def overSampler = new RichFilterFunction[Vector] {

    var ratio: Double = 0
    var r: Random = _

    override def open(parameter: Configuration): Unit = {
      val numCluster = getRuntimeContext.getBroadcastVariable[Int](CLUSTER_NUM).get(0)
      val numVector = getRuntimeContext.getBroadcastVariable[Int](VECTOR_NUM).get(0)
      ratio = 10 * (numCluster + 0.0) / numVector
      r = new Random()
    }

    override def filter(value: Vector): Boolean = {
      r.nextDouble() < ratio
    }
  }

  /** Select the first k samples from the oversampled set and also assign labels to them
    *
    */
  private def pickTrueSample = new RichFlatMapFunction[Int, LabeledVector] {

    var overSampleSet: util.List[Vector] = _
    var k: Int = 0

    override def open(parameter: Configuration): Unit = {
      overSampleSet = getRuntimeContext.getBroadcastVariable(OVER_SAMPLE)
      k = getRuntimeContext.getBroadcastVariable(CLUSTER_NUM).get(0)
    }

    override def flatMap(count: Int, out: Collector[LabeledVector]): Unit = {
      // this shouldn't normally happen. We're oversampling too much for this to happen
      require(overSampleSet.size() > k, "An unexpected error occurred")
      for (i <- 0 to k - 1) {
        out.collect(LabeledVector(i + 1, overSampleSet.get(i)))
      }
    }
  }

  /** Finds the nearest cluster center from the centroid set to the given vector
    * Returns the distance to the nearest center and the label of the nearest center
    *
    */
  private def MinClusterDistance(
                                  vec: Vector,
                                  centroids: Traversable[LabeledVector])
  : (Double, Double) = {
    var minDistance: Double = Double.MaxValue
    var closestCentroidLabel: Double = -1
    centroids.foreach { c =>
      val distance = EuclideanDistanceMetric().distance(vec, c.vector)
      if (distance < minDistance) {
        minDistance = distance
        closestCentroidLabel = c.label
      }
    }
    (minDistance, closestCentroidLabel)
  }


  /**
   * Converts a given vector into a labeled vector where the label denotes the label of the closest
   * centroid.
   */
  @ForwardedFields(Array("*->vector"))
  final class SelectNearestCenterMapper extends RichMapFunction[Vector, LabeledVector] {

    private var centroids: Traversable[LabeledVector] = null

    /** Reads the centroid values from a broadcast variable into a collection. */
    override def open(parameters: Configuration) {
      centroids = getRuntimeContext.getBroadcastVariable[LabeledVector](CENTROIDS).asScala
    }

    def map(v: Vector): LabeledVector = {
      LabeledVector(MinClusterDistance(v, centroids)._2, v)
    }
  }

}
