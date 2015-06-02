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

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFields
import org.apache.flink.api.scala.{DataSet, _}
import org.apache.flink.configuration.Configuration
import org.apache.flink.ml.common.FlinkMLTools.ModuloKeyPartitioner
import org.apache.flink.ml.common.{LabeledVector, _}
import org.apache.flink.ml.math.{ProbabilityDistribution, Vector}
import org.apache.flink.ml.metrics.distances.EuclideanDistanceMetric
import org.apache.flink.ml.pipeline._

import scala.collection.JavaConverters._
import scala.util.Random

/**
 * Implements the KMeans++ initialization algorithm which calculates cluster centroids based on
 * set of training data points.
 *
 * [[KMeansPPInit]] is a [[Predictor]] which needs to be trained on a set of data points and can
 * then be used to assign new points to the learned cluster centroids. Further, the trained
 * centroids can also be used as initial centroids for the KMeans algorithm
 *
 * The KMeans++ algorithm works as described by the original authors
 * (http://ilpubs.stanford.edu:8090/778/1/2006-13.pdf):
 *
 * Given a data set X with |X| points, the KMeans++ algorithm proceeds as follows:
 *
 * 1. Initialize C \leftarrow \{\}
 * 2. Let p be a point sampled uniformly at random from X. C \leftarrow C \cup \{p\}
 * 3. for i \leftarrow 1 to k-1
 * Choose an x in X with probability \frac{d(x,C)}{sigma_nolimits{p \in X }d(p,C)} where
 * d(p,C) denotes the distance of p from its nearest center in C
 * 4. Output C as the initial seed points. These can now be used in a KMeans algorithm as initial
 * centers

 * @example
 * {{{
 *           val trainingDS: DataSet[Vector] = env.fromCollection(Clustering.trainingData)
 *           val KMeansPP = KMeansPPInit().setNumClusters(10)
 *           KMeansPP.fit(trainingDS)
 *
 *           val testDS: DataSet[Vector] = env.fromCollection(Clustering.testData)
 *
 *           val clusters: DataSet[LabeledVector] = model.transform(testDS)
 *
 *           val initialCentroids: DataSet[LabeledVector] = KMeansPP.centroids.get
 *
 * }}}
 * =Parameters=
 *
 * - [[org.apache.flink.ml.clustering.KMeansPPInit.NumClusters]]:
 * Defines the number of clusters that need to be initialized. This should be the same as the
 * number of clusters desired for the KMeans algorithm in case the latter is run using the
 * clusters initialized with this algorithm
 * (Default value: '''0''')
 *
 */
class KMeansPPInit extends Predictor[KMeansPPInit] {

  import KMeansPPInit._

  /** Stores the initialized clusters after the fit operation */
  var centroids: Option[DataSet[LabeledVector]] = None


  /**
   * Sets the number of clusters to initialize.
   *
   * @param numCluster number of clusters
   * @return itself
   */
  def setNumClusters(numCluster: Int): KMeansPPInit = {
    parameters.add(NumClusters, numCluster)
    this
  }

}

/**
 * Companion object of KMeansPPInit. Contains convenience functions, the parameter type
 * definitions of the algorithm and the [[FitOperation]] & [[PredictOperation]].
 *
 */
object KMeansPPInit {
  val CENTROIDS = "KMeansPPInitCentroids"
  val LOCAL_SAMPLES = "KMeansPPLocalSamples"

  case object NumClusters extends Parameter[Int] {
    val defaultValue = Some(0)
  }

  // ========================================== Factory methods ====================================

  def apply(): KMeansPPInit = {
    new KMeansPPInit()
  }

  // ========================================== Operations =========================================

  /**
   * [[PredictOperation]] for vector types. The result type is a [[LabeledVector]].
   */
  implicit def predictValues = {
    new PredictOperation[KMeansPPInit, Vector, LabeledVector] {
      override def predict(
                            instance: KMeansPPInit,
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
    new FitOperation[KMeansPPInit, Vector] {

      override def fit(
                        instance: KMeansPPInit,
                        fitParameters: ParameterMap,
                        input: DataSet[Vector])
      : Unit = {
        val resultingParameters = instance.parameters ++ fitParameters
        val k: Int = resultingParameters.get(NumClusters).get

        val numberVectors = input map { x => 1 } reduce {
          _ + _
        }

        // Group the input data into blocks in round robin fashion
        val blockedInputNumberElements = FlinkMLTools.block(
          input,
          input.getParallelism,
          Some(ModuloKeyPartitioner)).
          cross(numberVectors).
          map { x => x }

        val randomInit = new KMeansRandomInit().setNumClusters(1)
        randomInit.fit(input)
        val initialCentroids = randomInit.centroids.get

        instance.centroids = Some(initialCentroids.iterate(k - 1) {
          currentCentroid =>
            val localSamples = localSampling(blockedInputNumberElements, currentCentroid)
            val globalSample = globalSampling(localSamples, numberVectors)
            currentCentroid.union(globalSample)
        })
      }
    }
  }

  /** Returns local samples from each block of data
    * A point p is sampled from a block B with probability d(p,C)/cost(B,C) where d(p,C) is the
    * square of the distance of p to the nearest center in C, and cost(B,C) is the cost of the
    * k-means problem on B using C as centers
    *
    */
  private def localSampling(
                             blockedInputNumberElements: DataSet[(Block[Vector], Int)],
                             currentCentroids: DataSet[LabeledVector])
  : DataSet[(Vector, Double)] = {
    val localSampler = new RichMapFunction[(Block[Vector], Int), (Vector, Double)] {

      var centroids: Traversable[LabeledVector] = _

      override def open(parameter: Configuration): Unit = {
        centroids = getRuntimeContext.getBroadcastVariable(CENTROIDS).asScala
      }

      override def map(blockedInputNumberElements: (Block[Vector], Int)): (Vector, Double) = {
        val (block, _) = blockedInputNumberElements
        val numElements = block.values.length
        var cost = 0.0
        val distances = Array.ofDim[Double](numElements)
        for (i <- 0 to numElements - 1) {
          distances(i) = Math.pow(MinClusterDistance(block.values.apply(i), centroids)._1, 2)
          cost += distances(i)
        }
        val r = new Random()
        val distribution = new ProbabilityDistribution(distances)
        (block.values.apply(distribution.sample(r.nextDouble())), cost)
      }
    }
    blockedInputNumberElements.map(localSampler).withBroadcastSet(currentCentroids, CENTROIDS)
  }

  /** Picks one point from the local samples based on their costs.
    * A local sample from block B will be sampled with probability
    * cost(B,C)/sum_nolimits{i} cost(B_i,C)
    * This multiplied with the earlier probability of a point being sampled with d(p,C)/cost(B,C)
    * gives exactly the probability we need.
    *
    */
  private def globalSampling(
                              localSamples: DataSet[(Vector, Double)],
                              count: DataSet[Int])
  : DataSet[LabeledVector] = {
    val globalSamples = new RichMapFunction[Int, LabeledVector] {

      var localSamples: util.List[(Vector, Double)] = _

      override def open(parameter: Configuration): Unit = {
        localSamples = getRuntimeContext.getBroadcastVariable(LOCAL_SAMPLES)
      }

      override def map(count: Int): LabeledVector = {
        val costArray = Array.ofDim[Double](localSamples.size())
        for (i <- 0 to localSamples.size() - 1) {
          costArray(i) = localSamples.get(i)._2
        }
        val r = new Random()
        LabeledVector(
          getIterationRuntimeContext.getSuperstepNumber + 1,
          localSamples.get(new ProbabilityDistribution(costArray).sample(r.nextDouble()))._1
        )
      }
    }
    count.map(globalSamples).withBroadcastSet(localSamples, LOCAL_SAMPLES)
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
