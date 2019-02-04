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

package org.apache.flink.ml.outlier

/** An implementation of the Stochastic Outlier Selection algorithm by Jeroen Jansen
  *
  * For more information about SOS, see https://github.com/jeroenjanssens/sos
  * J.H.M. Janssens, F. Huszar, E.O. Postma, and H.J. van den Herik. Stochastic
  * Outlier Selection. Technical Report TiCC TR 2012-001, Tilburg University,
  * Tilburg, the Netherlands, 2012.
  *
  * @example
  * {{{
  *   val data = env.fromCollection(List(
  *     LabeledVector(0.0, DenseVector(1.0, 1.0)),
  *     LabeledVector(1.0, DenseVector(2.0, 1.0)),
  *     LabeledVector(2.0, DenseVector(1.0, 2.0)),
  *     LabeledVector(3.0, DenseVector(2.0, 2.0)),
  *     LabeledVector(4.0, DenseVector(5.0, 8.0)) // The outlier!
  *   ))
  *
  *   val sos = new StochasticOutlierSelection().setPerplexity(3)
  *
  *   val outputVector = sos
  *     .transform(data)
  *     .collect()
  *
  *   val expectedOutputVector = Map(
  *     0 -> 0.2790094479202896,
  *     1 -> 0.25775014551682535,
  *     2 -> 0.22136130977995766,
  *     3 -> 0.12707053787018444,
  *     4 -> 0.9922779902453757 // The outlier!
  *   )
  *
  *   outputVector.foreach(output => expectedOutputVector(output._1) should be(output._2))
  * }}}
  *
  * =Parameters=
  *
  *  - [[org.apache.flink.ml.outlier.StochasticOutlierSelection.Perplexity]]:
  *  Perplexity can be interpreted as the k in k-nearest neighbor algorithms. The difference is
  *  in SOS being a neighbor is not a binary property, but a probabilistic one, and therefore it
  *  a real number. Must be between 1 and n-1, where n is the number of points.
  *  (Default value: '''30''')
  *
  *  - [[org.apache.flink.ml.outlier.StochasticOutlierSelection.ErrorTolerance]]:
  *  The accepted error tolerance when computing the perplexity. When increasing this number, it
  *  will sacrifice accuracy in return for reduced computational time.
  *  (Default value: '''1e-20''')
  *
  *  - [[org.apache.flink.ml.outlier.StochasticOutlierSelection.MaxIterations]]:
  *  The maximum number of iterations to perform to constrain the computational time.
  *  (Default value: '''5000''')
  */

import breeze.linalg.functions.euclideanDistance
import breeze.linalg.{sum, DenseVector => BreezeDenseVector, Vector => BreezeVector}
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.utils._
import org.apache.flink.ml.common.{LabeledVector, Parameter, ParameterMap, WithParameters}
import org.apache.flink.ml.math.Breeze._
import org.apache.flink.ml.math.{BreezeVectorConverter, Vector}
import org.apache.flink.ml.pipeline.{TransformDataSetOperation, Transformer}

import scala.language.implicitConversions
import scala.reflect.ClassTag

class StochasticOutlierSelection extends Transformer[StochasticOutlierSelection] {

  import StochasticOutlierSelection._


  /** Sets the perplexity of the outlier selection algorithm, can be seen as the k of kNN
    * For more information, please read the Stochastic Outlier Selection algorithm technical paper.
    *
    * @param perplexity the perplexity of the affinity fit
    * @return
    */
  def setPerplexity(perplexity: Double): StochasticOutlierSelection = {
    require(perplexity >= 1, "Perplexity must be at least one.")
    parameters.add(Perplexity, perplexity)
    this
  }

  /** The accepted error tolerance to reduce computational time when approximating the affinity.
    *
    * @param errorTolerance the accepted error tolerance with respect to the affinity
    * @return
    */
  def setErrorTolerance(errorTolerance: Double): StochasticOutlierSelection = {
    require(errorTolerance >= 0, "Error tolerance cannot be negative.")
    parameters.add(ErrorTolerance, errorTolerance)
    this
  }

  /** The maximum number of iterations to approximate the affinity of the algorithm.
    *
    * @param maxIterations the maximum number of iterations.
    * @return
    */
  def setMaxIterations(maxIterations: Int): StochasticOutlierSelection = {
    require(maxIterations > 0, "Maximum iterations must be positive.")
    parameters.add(MaxIterations, maxIterations)
    this
  }

}

object StochasticOutlierSelection extends WithParameters {

  // ========================================= Parameters ==========================================
  case object Perplexity extends Parameter[Double] {
    val defaultValue: Option[Double] = Some(30)
  }

  case object ErrorTolerance extends Parameter[Double] {
    val defaultValue: Option[Double] = Some(1e-20)
  }

  case object MaxIterations extends Parameter[Int] {
    val defaultValue: Option[Int] = Some(5000)
  }

  // ==================================== Factory methods ==========================================

  def apply(): StochasticOutlierSelection = {
    new StochasticOutlierSelection()
  }

  // ===================================== Operations ==============================================
  case class BreezeLabeledVector(idx: Int, data: BreezeVector[Double])

  implicit val transformLabeledVectors = {

    new TransformDataSetOperation[StochasticOutlierSelection, LabeledVector, (Int, Double)] {


      /** Overrides the method of the parent class and applies the stochastic outlier selection
        * algorithm.
        *
        * @param instance Instance of the class
        * @param transformParameters The user defined parameters of the algorithm
        * @param input A data set which consists of all the LabeledVectors, which should have an
        *              index or unique integer label as vector.
        * @return The outlierness of the vectors compared to each other
        */
      override def transformDataSet(instance: StochasticOutlierSelection,
                                    transformParameters: ParameterMap,
                                    input: DataSet[LabeledVector]): DataSet[(Int, Double)] = {

        val resultingParameters = instance.parameters ++ transformParameters

        val vectorsWithIndex = input.map(labeledVector => {
          BreezeLabeledVector(labeledVector.label.toInt, labeledVector.vector.asBreeze)
        })

        // Don't map back to a labeled-vector since the output of the algorithm is
        // a single double instead of vector
        outlierSelection(vectorsWithIndex, resultingParameters)
      }
    }
  }

  /** [[TransformDataSetOperation]] applies the stochastic outlier selection algorithm on a
    * [[Vector]] which will transform the high-dimensional input to a single Double output.
    *
    * @tparam T Type of the input and output data which has to be a subtype of [[Vector]]
    * @return [[TransformDataSetOperation]] a single double which represents the oulierness of
    *         the input vectors, where the output is in [0, 1]
    */
  implicit def transformVectors[T <: Vector : BreezeVectorConverter : TypeInformation : ClassTag]
  = {
    new TransformDataSetOperation[StochasticOutlierSelection, T, Double] {
      override def transformDataSet(instance: StochasticOutlierSelection,
                                    transformParameters: ParameterMap,
                                    input: DataSet[T]): DataSet[Double] = {

        val resultingParameters = instance.parameters ++ transformParameters

        // Map to the right format
        val vectorsWithIndex = input.zipWithUniqueId.map((vector: (Long, T)) => {
          BreezeLabeledVector(vector._1.toInt, vector._2.asBreeze)
        })

        outlierSelection(vectorsWithIndex, resultingParameters).map(_._2)
      }
    }
  }

  /** Internal entry point which will execute the different stages of the algorithm using a single
    * interface
    *
    * @param inputVectors        Input vectors on which the stochastic outlier selection algorithm
    *                            will be applied which should be the index or a unique integer value
    * @param transformParameters The user defined parameters of the algorithm
    * @return The outlierness of the vectors compared to each other
    */
  private def outlierSelection(inputVectors: DataSet[BreezeLabeledVector],
                               transformParameters: ParameterMap): DataSet[(Int, Double)] = {
    val dissimilarityVectors = computeDissimilarityVectors(inputVectors)
    val affinityVectors = computeAffinity(dissimilarityVectors, transformParameters)
    val bindingProbabilityVectors = computeBindingProbabilities(affinityVectors)
    val outlierProbability = computeOutlierProbability(bindingProbabilityVectors)

    outlierProbability
  }

  /** Compute pair-wise distance from each vector, to all other vectors.
    *
    * @param inputVectors The input vectors, will compare the vector to all other vectors based
    *                     on an distance method.
    * @return Returns new set of [[BreezeLabeledVector]] with dissimilarity vector
    */
  def computeDissimilarityVectors(inputVectors: DataSet[BreezeLabeledVector]):
  DataSet[BreezeLabeledVector] =
  inputVectors.cross(inputVectors) {
    (a, b) => (a.idx, b.idx, euclideanDistance(a.data, b.data))
  }.filter(dist => dist._1 != dist._2) // Filter out the diagonal, this contains no information.
    .groupBy(0)
    .sortGroup(1, Order.ASCENDING)
    .reduceGroup {
      distancesIterator => {
        val distances = distancesIterator.toList
        val distanceVector = distances.map(_._3).toArray

        BreezeLabeledVector(distances.head._1, BreezeDenseVector(distanceVector))
      }
    }

  /** Approximate the affinity by fitting a Gaussian-like function
    *
    * @param dissimilarityVectors The dissimilarity vectors which represents the distance to the
    *                             other vectors in the data set.
    * @param resultingParameters  The user defined parameters of the algorithm
    * @return Returns new set of [[BreezeLabeledVector]] with dissimilarity vector
    */
  def computeAffinity(dissimilarityVectors: DataSet[BreezeLabeledVector],
                      resultingParameters: ParameterMap): DataSet[BreezeLabeledVector] = {
    val logPerplexity = Math.log(resultingParameters(Perplexity))
    val maxIterations = resultingParameters(MaxIterations)
    val errorTolerance = resultingParameters(ErrorTolerance)

    dissimilarityVectors.map(vec => {
      val breezeVec = binarySearch(vec.data, logPerplexity, maxIterations, errorTolerance)
      BreezeLabeledVector(vec.idx, breezeVec)
    })
  }

  /** Normalizes the input vectors so each row sums up to one.
    *
    * @param affinityVectors The affinity vectors which is the quantification of the relationship
    *                        between the original vectors.
    * @return Returns new set of [[BreezeLabeledVector]] with represents the binding
    *         probabilities, which is in fact the affinity where each row sums up to one.
    */
  def computeBindingProbabilities(affinityVectors: DataSet[BreezeLabeledVector]):
  DataSet[BreezeLabeledVector] =
  affinityVectors.map(vec => BreezeLabeledVector(vec.idx, vec.data :/ sum(vec.data)))

  /** Compute the final outlier probability by taking the product of the column.
    *
    * @param bindingProbabilityVectors The binding probability vectors where the binding
    *                                  probability is based on the affinity and represents the
    *                                  probability of a vector binding with another vector.
    * @return Returns a single double which represents the final outlierness of the input vector.
    */
  def computeOutlierProbability(bindingProbabilityVectors: DataSet[BreezeLabeledVector]):
  DataSet[(Int, Double)] = bindingProbabilityVectors
    .flatMap(vec => vec.data.toArray.zipWithIndex.map(pair => {

      // The DistanceMatrix removed the diagonal, but we need to compute the product
      // of the column, so we need to correct the offset.
      val columnIndex = if (pair._2 >= vec.idx) {
        1
      } else {
        0
      }

      (columnIndex + pair._2, pair._1)
    })).groupBy(0).reduceGroup {
    probabilities => {
      var rowNumber = -1
      var outlierProbability = 1.0
      for (probability <- probabilities) {
        rowNumber = probability._1
        outlierProbability = outlierProbability * (1.0 - probability._2)
      }

      (rowNumber, outlierProbability)
    }
  }

  /** Performs a binary search to get affinities in such a way that each conditional Gaussian has
    *  the same perplexity.
    *
    * @param dissimilarityVector The input dissimilarity vector which represents the current
    *                            vector distance to the other vectors in the data set
    * @param logPerplexity The log of the perplexity, which represents the probability of having
    *                      affinity with another vector.
    * @param maxIterations The maximum iterations to limit the computational time.
    * @param tolerance The allowed tolerance to sacrifice precision for decreased computational
    *                  time.
    * @param beta: The current beta
    * @param betaMin The lower bound of beta
    * @param betaMax The upper bound of beta
    * @param iteration The current iteration
    * @return Returns the affinity vector of the input vector.
    */
  def binarySearch(
      dissimilarityVector: BreezeVector[Double],
      logPerplexity: Double,
      maxIterations: Int,
      tolerance: Double,
      beta: Double = 1.0,
      betaMin: Double = Double.NegativeInfinity,
      betaMax: Double = Double.PositiveInfinity,
      iteration: Int = 0)
    : BreezeVector[Double] = {

    val newAffinity = dissimilarityVector.map(d => Math.exp(-d * beta))
    val sumA = sum(newAffinity)
    val hCurr = Math.log(sumA) + beta * sum(dissimilarityVector :* newAffinity) / sumA
    val hDiff = hCurr - logPerplexity

    if (iteration < maxIterations && Math.abs(hDiff) > tolerance) {
      // Compute the Gaussian kernel and entropy for the current precision
      val (newBeta, newBetaMin, newBetaMax) = if (hDiff.isNaN) {
        (beta / 10.0, betaMin, betaMax) // Reduce beta to get it in range
      } else {
        if (hDiff > 0) {
          val newBeta =
            if (betaMax == Double.PositiveInfinity || betaMax == Double.NegativeInfinity) {
              beta * 2.0
            } else {
              (beta + betaMax) / 2.0
            }

          (newBeta, beta, betaMax)
        } else {
          val newBeta =
            if (betaMin == Double.PositiveInfinity || betaMin == Double.NegativeInfinity) {
              beta / 2.0
            } else {
              (beta + betaMin) / 2.0
            }

          (newBeta, betaMin, beta)
        }
      }

      binarySearch(dissimilarityVector,
        logPerplexity,
        maxIterations,
        tolerance,
        newBeta,
        newBetaMin,
        newBetaMax,
        iteration + 1)
    }
    else {
      newAffinity
    }
  }
}
