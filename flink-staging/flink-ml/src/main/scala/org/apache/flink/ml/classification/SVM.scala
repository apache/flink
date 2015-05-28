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

import org.apache.flink.ml.pipeline.{FitOperation, PredictOperation, Predictor}

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.ml.common.FlinkMLTools.ModuloKeyPartitioner
import org.apache.flink.ml.common._
import org.apache.flink.ml.math.Vector
import org.apache.flink.ml.math.Breeze._

import breeze.linalg.{Vector => BreezeVector, DenseVector => BreezeDenseVector}

/** Implements a soft-margin SVM using the communication-efficient distributed dual coordinate
  * ascent algorithm (CoCoA) with hinge-loss function.
  *
  * The algorithm solves the following minimization problem:
  *
  * `min_{w in bbb"R"^d} lambda/2 ||w||^2 + 1/n sum_(i=1)^n l_{i}(w^Tx_i)`
  *
  * with `w` being the weight vector, `lambda` being the regularization constant,
  * `x_{i} in bbb"R"^d` being the data points and `l_{i}` being the convex loss functions, which
  * can also depend on the labels `y_{i} in bbb"R"`.
  * In the current implementation the regularizer is the 2-norm and the loss functions are the
  * hinge-loss functions:
  *
  * `l_{i} = max(0, 1 - y_{i} * w^Tx_i`
  *
  * With these choices, the problem definition is equivalent to a SVM with soft-margin.
  * Thus, the algorithm allows us to train a SVM with soft-margin.
  *
  * The minimization problem is solved by applying stochastic dual coordinate ascent (SDCA).
  * In order to make the algorithm efficient in a distributed setting, the CoCoA algorithm
  * calculates several iterations of SDCA locally on a data block before merging the local
  * updates into a valid global state.
  * This state is redistributed to the different data partitions where the next round of local
  * SDCA iterations is then executed.
  * The number of outer iterations and local SDCA iterations control the overall network costs,
  * because there is only network communication required for each outer iteration.
  * The local SDCA iterations are embarrassingly parallel once the individual data partitions have
  * been distributed across the cluster.
  *
  * Further details of the algorithm can be found [[http://arxiv.org/abs/1409.1458 here]].
  *
  * @example
  *          {{{
  *             val trainingDS: DataSet[LabeledVector] = env.readSVMFile(pathToTrainingFile)
  *
  *             val svm = SVM()
  *               .setBlocks(10)
  *               .setIterations(10)
  *               .setLocalIterations(10)
  *               .setRegularization(0.5)
  *               .setStepsize(0.5)
  *
  *             svm.fit(trainingDS)
  *
  *             val testingDS: DataSet[Vector] = env.readVectorFile(pathToTestingFile)
  *
  *             val predictionDS: DataSet[LabeledVector] = svm.predict(testingDS)
  *          }}}
  *
  * =Parameters=
  *
  *  - [[org.apache.flink.ml.classification.SVM.Blocks]]:
  *  Sets the number of blocks into which the input data will be split. On each block the local
  *  stochastic dual coordinate ascent method is executed. This number should be set at least to
  *  the degree of parallelism. If no value is specified, then the parallelism of the input
  *  [[DataSet]] is used as the number of blocks. (Default value: '''None''')
  *
  *  - [[org.apache.flink.ml.classification.SVM.Iterations]]:
  *  Defines the maximum number of iterations of the outer loop method. In other words, it defines
  *  how often the SDCA method is applied to the blocked data. After each iteration, the locally
  *  computed weight vector updates have to be reduced to update the global weight vector value.
  *  The new weight vector is broadcast to all SDCA tasks at the beginning of each iteration.
  *  (Default value: '''10''')
  *
  *  - [[org.apache.flink.ml.classification.SVM.LocalIterations]]:
  *  Defines the maximum number of SDCA iterations. In other words, it defines how many data points
  *  are drawn from each local data block to calculate the stochastic dual coordinate ascent.
  *  (Default value: '''10''')
  *
  *  - [[org.apache.flink.ml.classification.SVM.Regularization]]:
  *  Defines the regularization constant of the SVM algorithm. The higher the value, the smaller
  *  will the 2-norm of the weight vector be. In case of a SVM with hinge loss this means that the
  *  SVM margin will be wider even though it might contain some false classifications.
  *  (Default value: '''1.0''')
  *
  *  - [[org.apache.flink.ml.classification.SVM.Stepsize]]:
  *  Defines the initial step size for the updates of the weight vector. The larger the step size
  *  is, the larger will be the contribution of the weight vector updates to the next weight vector
  *  value. The effective scaling of the updates is `stepsize/blocks`. This value has to be tuned
  *  in case that the algorithm becomes instable. (Default value: '''1.0''')
  *
  *  - [[org.apache.flink.ml.classification.SVM.Seed]]:
  *  Defines the seed to initialize the random number generator. The seed directly controls which
  *  data points are chosen for the SDCA method. (Default value: '''0''')
  */
class SVM extends Predictor[SVM] {

  import SVM._

  /** Stores the learned weight vector after the fit operation */
  var weightsOption: Option[DataSet[BreezeDenseVector[Double]]] = None

  /** Sets the number of data blocks/partitions
    *
    * @param blocks
    * @return itself
    */
  def setBlocks(blocks: Int): SVM = {
    parameters.add(Blocks, blocks)
    this
  }

  /** Sets the number of outer iterations
    *
    * @param iterations
    * @return itself
    */
  def setIterations(iterations: Int): SVM = {
    parameters.add(Iterations, iterations)
    this
  }

  /** Sets the number of local SDCA iterations
    *
    * @param localIterations
    * @return itselft
    */
  def setLocalIterations(localIterations: Int): SVM =  {
    parameters.add(LocalIterations, localIterations)
    this
  }

  /** Sets the regularization constant
    *
    * @param regularization
    * @return itself
    */
  def setRegularization(regularization: Double): SVM = {
    parameters.add(Regularization, regularization)
    this
  }

  /** Sets the stepsize for the weight vector updates
    *
    * @param stepsize
    * @return itself
    */
  def setStepsize(stepsize: Double): SVM = {
    parameters.add(Stepsize, stepsize)
    this
  }

  /** Sets the seed value for the random number generator
    *
    * @param seed
    * @return itself
    */
  def setSeed(seed: Long): SVM = {
    parameters.add(Seed, seed)
    this
  }
}

/** Companion object of SVM. Contains convenience functions and the parameter type definitions
  * of the algorithm.
  */
object SVM{
  val WEIGHT_VECTOR ="weightVector"

  // ========================================== Parameters =========================================

  case object Blocks extends Parameter[Int] {
    val defaultValue: Option[Int] = None
  }

  case object Iterations extends Parameter[Int] {
    val defaultValue = Some(10)
  }

  case object LocalIterations extends Parameter[Int] {
    val defaultValue = Some(10)
  }

  case object Regularization extends Parameter[Double] {
    val defaultValue = Some(1.0)
  }

  case object Stepsize extends Parameter[Double] {
    val defaultValue = Some(1.0)
  }

  case object Seed extends Parameter[Long] {
    val defaultValue = Some(0L)
  }

  // ========================================== Factory methods ====================================

  def apply(): SVM = {
    new SVM()
  }

  // ========================================== Operations =========================================

  /** [[org.apache.flink.ml.pipeline.PredictOperation]] for vector types. The result type is a
    * [[LabeledVector]]
    *
    * @tparam T Subtype of [[Vector]]
    * @return
    */
  implicit def predictValues[T <: Vector] = {
    new PredictOperation[SVM, T, LabeledVector]{
      override def predict(
          instance: SVM,
          predictParameters: ParameterMap,
          input: DataSet[T])
        : DataSet[LabeledVector] = {

        instance.weightsOption match {
          case Some(weights) => {
            input.map(new PredictionMapper[T]).withBroadcastSet(weights, WEIGHT_VECTOR)
          }

          case None => {
            throw new RuntimeException("The SVM model has not been trained. Call first fit" +
              "before calling the predict operation.")
          }
        }
      }
    }
  }

  /** Mapper to calculate the value of the prediction function. This is a RichMapFunction, because
    * we broadcast the weight vector to all mappers.
    */
  class PredictionMapper[T <: Vector] extends RichMapFunction[T, LabeledVector] {

    var weights: BreezeDenseVector[Double] = _

    @throws(classOf[Exception])
    override def open(configuration: Configuration): Unit = {
      // get current weights
      weights = getRuntimeContext.
        getBroadcastVariable[BreezeDenseVector[Double]](WEIGHT_VECTOR).get(0)
    }

    override def map(vector: T): LabeledVector = {
      // calculate the prediction value (scaled distance from the separating hyperplane)
      val dotProduct = weights dot vector.asBreeze

      LabeledVector(dotProduct, vector)
    }
  }

  /** [[org.apache.flink.ml.pipeline.PredictOperation]] for [[LabeledVector ]]types. The result type
    * is a [[(Double, Double)]] tuple, corresponding to (truth, prediction)
    *
    * @return A DataSet[(Double, Double)] where each tuple is a (truth, prediction) pair.
    */
  implicit def predictLabeledValues = {
    new PredictOperation[SVM, LabeledVector, (Double, Double)]{
      override def predict(
                            instance: SVM,
                            predictParameters: ParameterMap,
                            input: DataSet[LabeledVector])
      : DataSet[(Double, Double)] = {

        instance.weightsOption match {
          case Some(weights) => {
            input.map(new LabeledPredictionMapper).withBroadcastSet(weights, WEIGHT_VECTOR)
          }

          case None => {
            throw new RuntimeException("The SVM model has not been trained. Call first fit" +
              "before calling the predict operation.")
          }
        }
      }
    }
  }

  /** Mapper to calculate the value of the prediction function. This is a RichMapFunction, because
    * we broadcast the weight vector to all mappers.
    */
  class LabeledPredictionMapper extends RichMapFunction[LabeledVector, (Double, Double)] {

    var weights: BreezeDenseVector[Double] = _

    @throws(classOf[Exception])
    override def open(configuration: Configuration): Unit = {
      // get current weights
      weights = getRuntimeContext.
        getBroadcastVariable[BreezeDenseVector[Double]](WEIGHT_VECTOR).get(0)
    }

    override def map(labeledVector: LabeledVector): (Double, Double) = {
      // calculate the prediction value (scaled distance from the separating hyperplane)
      val prediction = weights dot labeledVector.vector.asBreeze
      val truth = labeledVector.label

      (truth, prediction)
    }
  }


  /** [[FitOperation]] which trains a SVM with soft-margin based on the given training data set.
    *
    */
  implicit val fitSVM = {
    new FitOperation[SVM, LabeledVector] {
      override def fit(
          instance: SVM,
          fitParameters: ParameterMap,
          input: DataSet[LabeledVector])
        : Unit = {
        val resultingParameters = instance.parameters ++ fitParameters

        // Check if the number of blocks/partitions has been specified
        val blocks = resultingParameters.get(Blocks) match {
          case Some(value) => value
          case None => input.getParallelism
        }

        val scaling = resultingParameters(Stepsize)/blocks
        val iterations = resultingParameters(Iterations)
        val localIterations = resultingParameters(LocalIterations)
        val regularization = resultingParameters(Regularization)
        val seed = resultingParameters(Seed)

        // Obtain DataSet with the dimension of the data points
        val dimension = input.map{_.vector.size}.reduce{
          (a, b) => {
            require(a == b, "Dimensions of feature vectors have to be equal.")
            a
          }
        }

        val initialWeights = createInitialWeights(dimension)

        // Count the number of vectors, but keep the value in a DataSet to broadcast it later
        // TODO: Once efficient count and intermediate result partitions are implemented, use count
        val numberVectors = input map { x => 1 } reduce { _ + _ }

        // Group the input data into blocks in round robin fashion
        val blockedInputNumberElements = FlinkMLTools.block(
          input,
          blocks,
          Some(ModuloKeyPartitioner)).
          cross(numberVectors).
          map { x => x }

        val resultingWeights = initialWeights.iterate(iterations) {
          weights => {
            // compute the local SDCA to obtain the weight vector updates
            val deltaWs = localDualMethod(
              weights,
              blockedInputNumberElements,
              localIterations,
              regularization,
              scaling,
              seed
            )

            // scale the weight vectors
            val weightedDeltaWs = deltaWs map {
              deltaW => {
                deltaW :*= scaling
              }
            }

            // calculate the new weight vector by adding the weight vector updates to the weight
            // vector value
            weights.union(weightedDeltaWs).reduce { _ + _ }
          }
        }

        // Store the learned weight vector in hte given instance
        instance.weightsOption = Some(resultingWeights)
      }
    }
  }

  /** Creates a zero vector of length dimension
    *
    * @param dimension [[DataSet]] containing the dimension of the initial weight vector
    * @return Zero vector of length dimension
    */
  private def createInitialWeights(dimension: DataSet[Int]): DataSet[BreezeDenseVector[Double]] = {
    dimension.map {
      d => BreezeDenseVector.zeros[Double](d)
    }
  }

  /** Computes the local SDCA on the individual data blocks/partitions
    *
    * @param w Current weight vector
    * @param blockedInputNumberElements Blocked/Partitioned input data
    * @param localIterations Number of local SDCA iterations
    * @param regularization Regularization constant
    * @param scaling Scaling value for new weight vector updates
    * @param seed Random number generator seed
    * @return [[DataSet]] of weight vector updates. The weight vector updates are double arrays
    */
  private def localDualMethod(
    w: DataSet[BreezeDenseVector[Double]],
    blockedInputNumberElements: DataSet[(Block[LabeledVector], Int)],
    localIterations: Int,
    regularization: Double,
    scaling: Double,
    seed: Long)
  : DataSet[BreezeDenseVector[Double]] = {
    /*
    Rich mapper calculating for each data block the local SDCA. We use a RichMapFunction here,
    because we broadcast the current value of the weight vector to all mappers.
     */
    val localSDCA = new RichMapFunction[(Block[LabeledVector], Int), BreezeDenseVector[Double]] {
      var originalW: BreezeDenseVector[Double] = _
      // we keep the alphas across the outer loop iterations
      val alphasArray = ArrayBuffer[BreezeDenseVector[Double]]()
      // there might be several data blocks in one Flink partition, therefore store mapping
      val idMapping = scala.collection.mutable.HashMap[Int, Int]()
      var counter = 0

      var r: Random = _

      override def open(parameters: Configuration): Unit = {
        originalW = getRuntimeContext.getBroadcastVariable(WEIGHT_VECTOR).get(0)

        if(r == null){
          r = new Random(seed ^ getRuntimeContext.getIndexOfThisSubtask)
        }
      }

      override def map(blockNumberElements: (Block[LabeledVector], Int))
      : BreezeDenseVector[Double] = {
        val (block, numberElements) = blockNumberElements

        // check if we already processed a data block with the corresponding block index
        val localIndex = idMapping.get(block.index) match {
          case Some(idx) => idx
          case None =>
            idMapping += (block.index -> counter)
            counter += 1

            alphasArray += BreezeDenseVector.zeros[Double](block.values.length)

            counter - 1
        }

        // create temporary alpha array for the local SDCA iterations
        val tempAlphas = alphasArray(localIndex).copy

        val numLocalDatapoints = tempAlphas.length
        val deltaAlphas = BreezeDenseVector.zeros[Double](numLocalDatapoints)

        val w = originalW.copy

        val deltaW = BreezeDenseVector.zeros[Double](originalW.length)

        for(i <- 1 to localIterations) {
          // pick random data point for SDCA
          val idx = r.nextInt(numLocalDatapoints)

          val LabeledVector(label, vector) = block.values(idx)
          val alpha = tempAlphas(idx)

          // maximize the dual problem and retrieve alpha and weight vector updates
          val (deltaAlpha, deltaWUpdate) = maximize(
            vector.asBreeze,
            label,
            regularization,
            alpha,
            w,
            numberElements)

          // update alpha values
          tempAlphas(idx) += deltaAlpha
          deltaAlphas(idx) += deltaAlpha

          // deltaWUpdate is already scaled with 1/lambda/n
          w += deltaWUpdate
          deltaW += deltaWUpdate
        }

        // update local alpha values
        alphasArray(localIndex) += deltaAlphas * scaling

        deltaW
      }
    }

    blockedInputNumberElements.map(localSDCA).withBroadcastSet(w, WEIGHT_VECTOR)
  }

  /** Maximizes the dual problem using hinge loss functions. It returns the alpha and weight
    * vector updates.
    *
    * @param x Selected data point
    * @param y Label of selected data point
    * @param regularization Regularization constant
    * @param alpha Alpha value of selected data point
    * @param w Current weight vector value
    * @param numberElements Number of elements in the training data set
    * @return Alpha and weight vector updates
    */
  private def maximize(
    x: BreezeVector[Double],
    y: Double, regularization: Double,
    alpha: Double,
    w: BreezeVector[Double],
    numberElements: Int)
  : (Double, BreezeVector[Double]) = {
    // compute hinge loss gradient
    val dotProduct = x dot w
    val grad = (y * dotProduct - 1.0) * (regularization * numberElements)

    // compute projected gradient
    var proj_grad = if(alpha  <= 0.0){
      Math.min(grad, 0)
    } else if(alpha >= 1.0) {
      Math.max(grad, 0)
    } else {
      grad
    }

    if(Math.abs(grad) != 0.0){
      val qii = x dot x
      val newAlpha = if(qii != 0.0){
        Math.min(Math.max((alpha - (grad / qii)), 0.0), 1.0)
      } else {
        1.0
      }

      val deltaW = x * y * (newAlpha - alpha) / (regularization * numberElements)

      (newAlpha - alpha, deltaW)
    } else {
      (0.0 , BreezeVector.zeros(w.length))
    }
  }

}
