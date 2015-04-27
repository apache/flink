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

package org.apache.flink.ml.feature

import org.apache.flink.api.scala.DataSet
import org.apache.flink.ml.common.{Parameter, ParameterMap, Transformer, LabeledVector}
import org.apache.flink.ml.feature.PolynomialBase.Degree
import org.apache.flink.ml.math.{DenseVector, Vector}

import org.apache.flink.api.scala._

/** Maps a vector into the polynomial feature space.
  *
  * This transformer takes a a vector of values `(x, y, z, ...)` and maps it into the
  * polynomial feature space of degree `d`. That is to say, it calculates the following
  * representation:
  *
  * `(x, y, z, x^2, xy, y^2, yz, z^2, x^3, x^2y, x^2z, xyz, ...)^T`
  *
  * This transformer can be prepended to all [[Transformer]] and
  * [[org.apache.flink.ml.common.Learner]] implementations which expect an input of
  * [[LabeledVector]].
  *
  * @example
  *          {{{
  *             val trainingDS: DataSet[LabeledVector] = ...
  *
  *             val polyBase = PolynomialBase()
  *               .setDegree(3)
  *
  *             val mlr = MultipleLinearRegression()
  *
  *             val chained = polyBase.chain(mlr)
  *
  *             val model = chained.fit(trainingDS)
  *          }}}
  *
  * =Parameters=
  *
  *  - [[PolynomialBase.Degree]]: Maximum polynomial degree
  */
class PolynomialBase extends Transformer[LabeledVector, LabeledVector] with Serializable {

  def setDegree(degree: Int): PolynomialBase = {
    parameters.add(Degree, degree)
    this
  }

  override def transform(input: DataSet[LabeledVector], parameters: ParameterMap):
  DataSet[LabeledVector] = {
    val resultingParameters = this.parameters ++ parameters

    val degree = resultingParameters(Degree)

    input.map {
      labeledVector => {
        val vector = labeledVector.vector
        val label = labeledVector.label

        val transformedVector = calculatePolynomial(degree, vector)

        LabeledVector(label, transformedVector)
      }
    }
  }

  private def calculatePolynomial(degree: Int, vector: Vector): Vector = {
    new DenseVector(calculateCombinedCombinations(degree, vector).toArray)
  }

  /** Calculates for a given vector its representation in the polynomial feature space.
    *
    * @param degree Maximum degree of polynomial
    * @param vector Values of the polynomial variables
    * @return List of polynomial values
    */
  private def calculateCombinedCombinations(degree: Int, vector: Vector): List[Double] = {
    if(degree == 0) {
      List()
    } else {
      val partialResult = calculateCombinedCombinations(degree - 1, vector)

      val combinations = calculateCombinations(vector.size, degree)

      val result = combinations map {
        combination =>
          combination.zipWithIndex.map{
            case (exp, idx) => math.pow(vector(idx), exp)
          }.fold(1.0)(_ * _)
      }

      result ::: partialResult
    }

  }

  /** Calculates all possible combinations of a polynom of degree `value`, whereas the polynom
    * can consist of up to `length` factors. The return value is the list of the exponents of the
    * individual factors
    *
    * @param length maximum number of factors
    * @param value degree of polynomial
    * @return List of lists which contain the exponents of the individual factors
    */
  private def calculateCombinations(length: Int, value: Int): List[List[Int]] = {
    if(length == 0) {
      List()
    } else if (length == 1) {
      List(List(value))
    } else {
      value to 0 by -1 flatMap {
        v =>
          calculateCombinations(length - 1, value - v) map {
            v::_
          }
      } toList
    }
  }
}

object PolynomialBase{

  case object Degree extends Parameter[Int] {
    override val defaultValue: Option[Int] = Some(1)
  }

  // ========================= Factory methods ======================================

  def apply(): PolynomialBase = {
    new PolynomialBase()
  }
}
