/**
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package eu.stratosphere.pact4s.examples.datamining

import scala.math._
import eu.stratosphere.scala._
import eu.stratosphere.scala.operators._


abstract class BatchGradientDescent(eps: Double, eta: Double, lambda: Double, examplesInput: String, weightsInput: String, weightsOutput: String) extends Serializable {
  def computeGradient(example: Array[Double], weight: Array[Double]): (Double, Array[Double])

  def updateWeight = (prev: (Int, Array[Double], Double), vg: ValueAndGradient) => {
    val (id, wOld, eta) = prev
    val ValueAndGradient(_, lossSum, gradSum) = vg

    val delta = lossSum + lambda * wOld.norm
    val wNew = (wOld + (gradSum * eta)) * (1 - eta * lambda)
    (id, delta, wNew, eta * 0.9)
  }

  class WeightVector(vector: Array[Double]) {
    def +(that: Array[Double]): Array[Double] = (vector zip that) map { case (x1, x2) => x1 + x2 }
    def -(that: Array[Double]): Array[Double] = (vector zip that) map { case (x1, x2) => x1 - x2 }
    def *(x: Double): Array[Double] = vector map { x * _ }
    def norm: Double = sqrt(vector map { x => x * x } reduce { _ + _ })
  }

  implicit def array2WeightVector(vector: Array[Double]): WeightVector = new WeightVector(vector)

  case class ValueAndGradient(id: Int, value: Double, gradient: Array[Double]) {
    def this(id: Int, vg: (Double, Array[Double])) = this(id, vg._1, vg._2)
    def +(that: ValueAndGradient) = ValueAndGradient(id, value + that.value, gradient + that.gradient)
  }

  def readVector = (line: String) => {
    val Seq(id, vector @ _*) = line.split(',').toSeq
    id.toInt -> (vector map { _.toDouble } toArray)
  }

  def formatOutput = (id: Int, vector: Array[Double]) => "%s,%s".format(id, vector.mkString(","))

  def getPlan() = {

    val examples = DataSource(examplesInput, DelimitedInputFormat(readVector))
    val weights = DataSource(weightsInput, DelimitedInputFormat(readVector))

    def gradientDescent = (s: DataSet[(Int, Array[Double])], ws: DataSet[(Int, Array[Double], Double)]) => {

      val lossesAndGradients = ws cross examples map { (w, ex) => new ValueAndGradient(w._1, computeGradient(ex._2, w._2)) }
      val lossAndGradientSums = lossesAndGradients groupBy { _.id } reduce (_ + _)
      val newWeights = ws join lossAndGradientSums where { _._1 } isEqualTo { _.id } map updateWeight

      val s1 = newWeights map { case (wId, _, wNew, _) => (wId, wNew) } // updated solution elements
      val ws1 = newWeights filter { case (_, delta, _, _) => delta > eps } map { case (wId, _, wNew, etaNew) => (wId, wNew, etaNew) } // new workset

      (s1, ws1)
    }

    val newWeights = weights.iterateWithWorkset(weights.map { case (id, w) => (id, w, eta) }, {_._1}, gradientDescent, 10)

    val output = newWeights.write(weightsOutput, DelimitedOutputFormat(formatOutput.tupled))
    new ScalaPlan(Seq(output), "Batch Gradient Descent")
  }
}

