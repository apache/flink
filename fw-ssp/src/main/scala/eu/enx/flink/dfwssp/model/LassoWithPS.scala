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

package eu.enx.flink.dfwssp.model

import breeze.linalg._
import breeze.numerics._
import com.github.fommil.netlib.BLAS.{getInstance => blas}
import eu.enx.flink.dfwssp.utils.ResultsLogger
import eu.enx.flink.dfwssp.SparseCoefficientElement
import org.apache.flink.api.common.functions.Partitioner
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.parameterserver.impl.ignite.functions.RichMapFunctionWithParameterServer
import org.apache.flink.parameterserver.model.ParameterElement
import org.slf4j.LoggerFactory

/**
 * Frank-Wolfe Lasso regression methods with use of parameter server.
 *
 * @param beta: regularization parameter
 * @param numIter: number of iterations to perform
 * @param line_search: whether to optimize the step size or not
 * @param epsilon: stop iterations if duality_gap is smaller than epsilon
 * @param opt: optimization strategy
 * @param slack: parameter controlling the staleness bound
 */

class LassoWithPS(
  beta: Double,
  numIter: Int,
  line_search: Boolean = false,
  epsilon: Double = 1e-3,
  opt: String = "CD",
  slack: Int = 3)
  extends Serializable {

  var coef_ : Array[Double] = null
  var idx_ : Array[Int] = null

  override def toString: String = {
    if (this.coef_ == null) "Empty model."
    else {
      assert(this.idx_.length == this.coef_.length)
      this.idx_.zip(this.coef_).map {
        case (idx: Int, coef: Double) => idx + "," + coef
      }.mkString("\n")
    }
  }

  /**
   * Fit regression coefficients.
   * @param X: data matrix as a DataSet of column vectors
   * @param Y: target values
   * @param log: whether to log or not
   * @return
   */
  def fit(
    X: DataSet[ColumnVector],
    Y: DataSet[Array[Double]],
    log: Boolean = false
    ): LassoWithPS = {

    // Initialize coefficients
    val initial = Y map { y => new SparseCoefficientElement }

    val iteration = opt match {
      // Process coordinate-wise
      case "CD" => {
        val splittedData = X.partitionCustom(new ColumnPartitioner, "idx")
          .mapPartition(colums => Some(colums.toArray))

        def stepFunction(alpha: DataSet[SparseCoefficientElement]):
        (DataSet[SparseCoefficientElement], DataSet[_]) = {

          val alphaDuality = splittedData.map {
            new UpdateParameterCD("alpha", beta, line_search, epsilon, numIter, this.slack, log)
          }.withBroadcastSet(Y, "Y").withBroadcastSet(alpha, "alpha")

          val termination = alphaDuality filter {
            tuple => tuple._2 >= epsilon
          }

          val next = alphaDuality map {
            tuple => tuple._1
          }

          (next, termination)
        }

        if (this.slack == 0) {
          initial.iterateWithTermination(numIter) {
            stepFunction
          }
        }
        else {
          initial.iterateWithSSPWithTermination(numIter, this.slack) {
            stepFunction
          }
        }
      }
      // Process by mini-batches over coordinates
      case "GR" => {
        throw new Exception("Not yet implemented !")
      }
    }

    val alpha: SparseCoefficientElement = iteration.collect()(0)
    this.idx_ = alpha.getValue._1
    this.coef_ = alpha.getValue._2
    this
  }

  /**
   * Predit the target value given a data matrix as a DataSet of rows.
   * @param X
   * @return
   */
  def predict(X: DataSet[ColumnVector]): DataSet[(Int, Double)] = {
    assert(this.coef_ != null && this.idx_ != null)
    val Y = X map {
      vector => {
        var res = 0.0
        for ((idx, i) <- this.idx_.zipWithIndex) {
          res += this.coef_(i) * vector.values(idx)
        }
        (vector.idx, res)
      }
    }
    Y
  }
}


/**
 * RichMapFunction that implements the Frank-Wolfe algorithm.
 * @param id
 * @param beta
 * @param line_search
 * @param epsilon
 * @param maxIter
 * @param slack
 * @param logResultsToDisk
 */
@SerialVersionUID(123L)
class UpdateParameterCD(
  id: String,
  beta: Double,
  line_search: Boolean,
  epsilon: Double,
  maxIter: Int,
  slack: Int,
  logResultsToDisk: Boolean
  ) extends RichMapFunctionWithParameterServer[Array[ColumnVector], (SparseCoefficientElement,
  Double)]
with Serializable {

  val LOG = LoggerFactory.getLogger(getClass)
  var Y: Array[Double] = null
  var resultslogger: ResultsLogger = null
  var size: Int = 0

  override def open(config: Configuration): Unit = {
    super.open(config)

    if (Y == null) {
      Y = getRuntimeContext.getBroadcastVariable[Array[Double]]("Y").get(0)
      size = Y.length
    }
    if (logResultsToDisk) {
      resultslogger = new ResultsLogger(
        getRuntimeContext.getIndexOfThisSubtask,
        getIterationRuntimeContext.getSuperstepNumber,
        slack,
        beta
      )
    }
  }

  def map(in: Array[ColumnVector]): (SparseCoefficientElement, Double) = {
    val t0 = System.nanoTime

    var new_residual: DenseVector[Double] = null
    var new_sol: SparseCoefficientElement = null

    var el: SparseCoefficientElement = getParameter(id).asInstanceOf[SparseCoefficientElement]
    // Get the model from the parameter server
    if (el == null) el = new SparseCoefficientElement
    val model = el.getValue

    // Compute the current approximation
    val approx = Array.fill(size)(0.0)
    for (i <- 0 until model._1.size) {
      val atom = getSharedParameter(model._1(i).toString).asInstanceOf[AtomElement]
      if (atom != null) {
        blas.daxpy(size, model._2(i), atom.getValue, 1, approx, 1)
      }
    }

    // Compute the current residual
    val residual = Array.fill(size)(0.0)
    Y.copyToArray(residual)
    if (model._1.size > 0) {
      blas.daxpy(size, -1.0, approx, 1, residual, 1)
    }

    // Select the best atom
    val (atom, index, grad) = in.map {
      x => (x.values, x.idx, -blas.ddot(size, x.values, 1, residual, 1))
    }.reduce {
      (left, right) => if (abs(left._3) > abs(right._3)) left else right
    }

    // Update using the selected atom
    val s_k: DenseVector[Double] = DenseVector(atom) * (signum(-grad) * beta)
    val A_temp = if (model._1.size == 0) s_k else s_k - DenseVector(approx)
    val duality_gap = A_temp.t * DenseVector(residual)

    // Compute step-size
    val gamma = if (line_search) {
      max(0.0, min(1.0, duality_gap / (A_temp.t * A_temp)))
    } else {
      val k = getIterationRuntimeContext.getSuperstepNumber - 1
      2.0 / (k + 2.0)
    }

    val v = gamma * beta * signum(-grad)

    if (model._1.size == 0) {
      new_residual = DenseVector(residual) - s_k * gamma
      new_sol = new SparseCoefficientElement(0, (Array(index), Array(v)))
      updateShared(index.toString, new AtomElement(0, atom))
    }
    else {
      new_residual = DenseVector(residual) + gamma * (DenseVector(approx) - s_k)
      val idx = model._1.indexOf(index)
      val coef: DenseVector[Double] = DenseVector(model._2) * (1.0 - gamma)
      if (idx == -1) {
        val new_idx = (Array(index) ++ model._1).clone()
        val new_coef = Array(v) ++ coef.toArray
        updateShared(index.toString, new AtomElement(0, atom))
        new_sol = new SparseCoefficientElement(0, (new_idx, new_coef))
      } else {
        coef(idx) += v
        new_sol = new SparseCoefficientElement(0, (model._1, coef.toArray))
      }
    }
    val residualNorm = norm(new_residual)

    if (LOG.isInfoEnabled) {
      LOG.info("Worker = " + getRuntimeContext.getIndexOfThisSubtask
        + " Residual norm = " + residualNorm
        + " Duality_gap = " + duality_gap
        + " ##### Actual clock : " + el.getClock)
    }

    // Update parameter server
    updateParameter(id, new_sol, new DualityGapElement(residualNorm))

    val t1 = System.nanoTime

    // Logs
    if (logResultsToDisk) {
      resultslogger.writeToDisk(index, residualNorm, duality_gap, t0, t1)
    }
    (new_sol, duality_gap)
  }
}

/**
 * Class to store the duality-gap in the parameter server.
 * @param value
 */
case class DualityGapElement(value: Double) extends ParameterElement[Double] with Serializable {
  def getClock: Int = 0
  def getValue: Double = value
}

/**
 * Case class to represent a vector of doubles with an associated index.
 * @param idx
 * @param values
 */
@SerialVersionUID(123L)
case class ColumnVector(idx: Int, values: Array[Double]) extends Serializable

/**
 * Class to partition the columns of a matrix across multiple workers.
 */
class ColumnPartitioner extends Partitioner[Int] {
  override def partition(columnIndex: Int, numberOfPartitions: Int): Int = {
    columnIndex % numberOfPartitions
  }
}
