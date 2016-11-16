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

package org.apache.flink.ml.recommendation

import org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint
import org.apache.flink.api.scala._
import org.apache.flink.ml.common._
import org.apache.flink.ml.pipeline.{PredictDataSetOperation, Predictor}
import com.github.fommil.netlib.BLAS.{getInstance => blas}

import scala.util.Random

/** Common trait for matrix factorization algorithm.
  *
  * Given a matrix `R`, a matrix factorization algorithm calculates two matricess `U` and `V`
  * such that `R ~~ U^TV`. The unknown row dimension is given by the number of latent factors.
  * Since matrix factorization
  * is often used in the context of recommendation, we'll call the first matrix the user and the
  * second matrix the item matrix. The `i`th column of the user matrix is `u_i` and the `i`th
  * column of the item matrix is `v_i`. The matrix `R` is called the ratings matrix and
  * `(R)_{i,j} = r_{i,j}`.
  *
  * The matrix `R` is given in its sparse representation as a tuple of `(i, j, r)` where `i` is the
  * row index, `j` is the column index and `r` is the matrix value at position `(i,j)`.
  *
  * =Parameters=
  *
  *  - [[org.apache.flink.ml.recommendation.MatrixFactorization.NumFactors]]:
  *  The number of latent factors. It is the dimension of the calculated user and item vectors.
  *  (Default value: '''10''')
  *
  *  - [[org.apache.flink.ml.recommendation.MatrixFactorization.Lambda]]:
  *  Regularization factor. Tune this value in order to avoid overfitting/generalization.
  *  (Default value: '''1''')
  *
  *  - [[org.apache.flink.ml.regression.MultipleLinearRegression.Iterations]]:
  *  The number of iterations to perform. (Default value: '''10''')
  *
  *  - [[org.apache.flink.ml.recommendation.MatrixFactorization.Blocks]]:
  *  The number of blocks into which the user and item matrix a grouped. The fewer
  *  blocks one uses, the less data is sent redundantly. However, bigger blocks entail bigger
  *  update messages which have to be stored on the Heap. If the algorithm fails because of
  *  an OutOfMemoryException, then try to increase the number of blocks. (Default value: '''None''')
  *
  *  - [[org.apache.flink.ml.recommendation.MatrixFactorization.Seed]]:
  *  Random seed used when randomization is needed in the algorithm
  *  (e.g. when generating the initial user and item matrices).
  *  (Default value: '''0''')
  *
  *  - [[org.apache.flink.ml.recommendation.MatrixFactorization.TemporaryPath]]:
  *  Path to a temporary directory into which intermediate results are stored. If
  *  this value is set, then the algorithm is split into preprocessing steps, the iteration
  *  and post-processing steps. The result of the individual steps are stored in the specified
  *  directory. These steps differ at each iterative matrix factorization algorithm, but persisting
  *  have the same purpose. By splitting the algorithm into multiple smaller steps, Flink does not
  *  have to split the available memory amongst too many operators. This allows the system to
  *  process bigger individual messages and improves the overall performance.
  *  (Default value: '''None''')
  */
trait MatrixFactorization[Self] extends Predictor[Self] {
  that: Self =>

  import MatrixFactorization._

  // Stores the matrix factorization after the fitting phase
  var factorsOption: Option[(DataSet[Factors], DataSet[Factors])] = None

  /** Sets the number of latent factors/row dimension of the latent model
    *
    * @param numFactors
    * @return
    */
  def setNumFactors(numFactors: Int): Self = {
    parameters.add(NumFactors, numFactors)
    this
  }

  /** Sets the regularization coefficient lambda
    *
    * @param lambda
    * @return
    */
  def setLambda(lambda: Double): Self = {
    parameters.add(Lambda, lambda)
    this
  }

  /** Sets the number of iterations of the MatrixFactorization algorithm
    *
    * @param iterations
    * @return
    */
  def setIterations(iterations: Int): Self = {
    parameters.add(Iterations, iterations)
    this
  }

  /** Sets the number of blocks into which the user and item matrix shall be partitioned
    *
    * @param blocks
    * @return
    */
  def setBlocks(blocks: Int): Self = {
    parameters.add(Blocks, blocks)
    this
  }

  /** Sets the random seed for the initial item matrix initialization
    *
    * @param seed
    * @return
    */
  def setSeed(seed: Long): Self = {
    parameters.add(Seed, seed)
    this
  }

  /** Sets the temporary path into which intermediate results are written in order to increase
    * performance.
    *
    * @param temporaryPath
    * @return
    */
  def setTemporaryPath(temporaryPath: String): Self = {
    parameters.add(TemporaryPath, temporaryPath)
    this
  }

  /** Empirical risk of the trained model (matrix factorization).
    *
    * @param labeledData Reference data
    * @param riskParameters Additional parameters for the empirical risk calculation
    * @return
    */
  def empiricalRisk(
                     labeledData: DataSet[(Int, Int, Double)],
                     riskParameters: ParameterMap = ParameterMap.Empty)
  : DataSet[Double] = {
    val resultingParameters = parameters ++ riskParameters

    val lambda = resultingParameters(Lambda)

    val data = labeledData map {
      x => (x._1, x._2)
    }

    factorsOption match {
      case Some((userFactors, itemFactors)) => {
        val predictions = data.join(userFactors, JoinHint.REPARTITION_HASH_SECOND).where(0)
          .equalTo(0).join(itemFactors, JoinHint.REPARTITION_HASH_SECOND).where("_1._2")
          .equalTo(0).map {
          triple => {
            val (((uID, iID), uFactors), iFactors) = triple

            val uFactorsVector = uFactors.factors
            val iFactorsVector = iFactors.factors

            val squaredUNorm2 = blas.ddot(
              uFactorsVector.length,
              uFactorsVector,
              1,
              uFactorsVector,
              1)
            val squaredINorm2 = blas.ddot(
              iFactorsVector.length,
              iFactorsVector,
              1,
              iFactorsVector,
              1)

            val prediction = blas.ddot(uFactorsVector.length, uFactorsVector, 1, iFactorsVector, 1)

            (uID, iID, prediction, squaredUNorm2, squaredINorm2)
          }
        }

        labeledData.join(predictions).where(0,1).equalTo(0,1) {
          (left, right) => {
            val (_, _, expected) = left
            val (_, _, predicted, squaredUNorm2, squaredINorm2) = right

            val residual = expected - predicted

            residual * residual + lambda * (squaredUNorm2 + squaredINorm2)
          }
        } reduce {
          _ + _
        }
      }

      case None => throw new RuntimeException("The ALS model has not been fitted to data. " +
        "Prior to predicting values, it has to be trained on data.")
    }
  }
}

object MatrixFactorization {
  // fixme unused:
  val USER_FACTORS_FILE = "userFactorsFile"
  val ITEM_FACTORS_FILE = "itemFactorsFile"

  // ========================================= Parameters ==========================================

  case object NumFactors extends Parameter[Int] {
    val defaultValue: Option[Int] = Some(10)
  }

  case object Lambda extends Parameter[Double] {
    val defaultValue: Option[Double] = Some(1.0)
  }

  case object Iterations extends Parameter[Int] {
    val defaultValue: Option[Int] = Some(10)
  }

  case object Blocks extends Parameter[Int] {
    val defaultValue: Option[Int] = None
  }

  case object Seed extends Parameter[Long] {
    val defaultValue: Option[Long] = Some(0L)
  }

  case object TemporaryPath extends Parameter[String] {
    val defaultValue: Option[String] = None
  }

  // ============================ MatrixFactorization type definitions =============================

  /** Latent factor model vector
    *
    * @param id
    * @param factors
    */
  case class Factors(id: Int, factors: Array[Double]) {
    override def toString = s"($id, ${factors.mkString(",")})"
  }

  // ===================================== Operations ==============================================

  /** Predict operation which calculates the matrix entry for the given indices  */
  implicit def predictRating[MFInstance <: MatrixFactorization[MFInstance]] =
    new PredictDataSetOperation[
      MFInstance, (Int, Int), (Int ,Int, Double)] {
    override def predictDataSet(
                                 instance: MFInstance,
                                 predictParameters: ParameterMap,
                                 input: DataSet[(Int, Int)])
    : DataSet[(Int, Int, Double)] = {

      instance.factorsOption match {
        case Some((userFactors, itemFactors)) => {
          input.join(userFactors, JoinHint.REPARTITION_HASH_SECOND).where(0).equalTo(0)
            .join(itemFactors, JoinHint.REPARTITION_HASH_SECOND).where("_1._2").equalTo(0).map {
            triple => {
              val (((uID, iID), uFactors), iFactors) = triple

              val uFactorsVector = uFactors.factors
              val iFactorsVector = iFactors.factors

              val prediction = blas.ddot(
                uFactorsVector.length,
                uFactorsVector,
                1,
                iFactorsVector,
                1)

              (uID, iID, prediction)
            }
          }
        }

        case None => throw new RuntimeException("The MatrixFactorization model has not been" +
          " fitted to data. Prior to predicting values, it has to be trained on data.")
      }
    }
  }

  // ================================ Math helper functions ========================================

  def randomFactors(factors: Int, random: Random): Array[Double] = {
    Array.fill(factors)(random.nextDouble())
  }
}
