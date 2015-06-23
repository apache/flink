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

package org.apache.flink.ml.statistics

import org.apache.flink.ml.common.FieldType._

import scala.Double.{MaxValue, MinValue}
import scala.collection.mutable

/** Class to represent Field statistics.
  *
  * =Parameters=
  * -[[fieldType]]:
  * Type of this field, [[DISCRETE]] or [[CONTINUOUS]]
  *
  * For [[DISCRETE]] fields, [[entropy]], [[gini]] and [[categoryCounts]] are provided.
  * For [[CONTINUOUS]] fields, [[min]], [[max]], [[mean]] and [[variance]] are provided.
  *
  */
class FieldStats(val fieldType: FieldType) extends Serializable {

  private val logConversion = math.log(2)
  private[statistics] var _min: Double = MaxValue
  private[statistics] var _max: Double = MinValue
  private[statistics] var _mean: Double = 0
  private[statistics] var _variance: Double = 0
  private[statistics] var _counts: mutable.HashMap[Double, Int] = null

  if (fieldType == DISCRETE) _counts = new mutable.HashMap[Double, Int]()

  //-------------------- Access methods ----------------------------//

  /**
   * Returns the entropy value for this [[DISCRETE]] field.
   *
   * @return entropy of the field
   */
  def entropy: Double = {
    assume(fieldType == DISCRETE, "Entropy cannot be accessed for continuous fields")
    val total: Double = _counts.valuesIterator.sum
    _counts.iterator.map(x => -(x._2 / total) * Math.log(x._2 / total)).sum / logConversion
  }

  /**
   * Returns the Gini impurity for this [[DISCRETE]] field.
   *
   * @return Gini Impurity of the field
   */
  def gini: Double = {
    assume(fieldType == DISCRETE, "Gini impurity cannot be accessed for continuous fields")
    val total: Double = _counts.valuesIterator.sum
    1 - _counts.iterator.map(x => x._2 * x._2).sum / (total * total)
  }

  override def toString: String = {
    if (fieldType == DISCRETE) {
      s"Discrete: " + categoryCounts.toString()
    } else {
      s"Continuous: Min: $min, Max: $max, Mean: $mean, Variance: $variance"
    }
  }

  /**
   * Returns the minimum value of this [[CONTINUOUS]] field
   *
   * @return minimum value of field
   */
  def min: Double = {
    assume(fieldType == CONTINUOUS, "Calculation of min is not supported on discrete fields")
    _min
  }

  /**
   * Returns the maximum value of this [[CONTINUOUS]] field
   *
   * @return maximum value of the field
   */
  def max: Double = {
    assume(fieldType == CONTINUOUS, "Calculation of max is not supported on discrete fields")
    _max
  }

  /**
   * Returns the average value of this [[CONTINUOUS]] field
   *
   * @return average value of the field
   */
  def mean: Double = {
    assume(fieldType == CONTINUOUS, "Calculation of mean is not supported on discrete " +
      "fields")
    _mean
  }

  /**
   * Returns the variance of this [[CONTINUOUS]] field
   *
   * @return variance of the field
   */
  def variance: Double = {
    assume(fieldType == CONTINUOUS, "Calculation of variance is not supported on discrete " +
      "fields")
    _variance
  }

  //------------------ Setter methods ------------------//

  /**
   * Returns the class-wise counts of this [[DISCRETE]] field
   *
   * @return class-wise counts of the field
   */
  def categoryCounts: mutable.HashMap[Double, Int] = {
    assume(fieldType == DISCRETE, "Category counts cannot be accessed for continuous fields")
    _counts
  }

  /**
   * Set the statistics for a [[CONTINUOUS]] field
   *
   * @param min minimum value of the field
   * @param max maximum value of the field
   * @param mean mean value of the field
   * @param variance variance of the field
   * @return itself
   */
  private[statistics] def setContinuousParameters(
      min: Double,
      max: Double,
      mean: Double,
      variance: Double)
    : FieldStats = {
    assume(fieldType == CONTINUOUS, "Min, max etc. cannot be set for Discrete fields")
    _min = min
    _max = max
    _mean = mean
    _variance = variance
    this
  }

  /**
   * Sets the statistics for a [[DISCRETE]] field
   *
   * @param counts Class-wise counts of the field
   * @return itself
   */
  private[statistics] def setDiscreteParameters(
      counts: mutable.HashMap[Double, Int])
    : FieldStats = {
    assume(fieldType == DISCRETE, "Class counts cannot be set for Continuous fields")
    _counts = counts
    this
  }
}
