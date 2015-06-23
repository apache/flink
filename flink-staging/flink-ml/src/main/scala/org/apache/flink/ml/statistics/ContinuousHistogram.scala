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

import scala.Double.MaxValue
import scala.collection.mutable

/** Implementation of a continuous valued online histogram
  * Adapted from Ben-Haim and Yom-Tov's Streaming Decision Tree Algorithm
  * Refer http://www.jmlr.org/papers/volume11/ben-haim10a/ben-haim10a.pdf
  *
  * =Parameters=
  * -[[capacity]]:
  * Number of bins to be used in the histogram
  *
  * -[[min]]:
  * Lower limit on the elements
  *
  * -[[max]]:
  * Upper limit on the elements
  */
class ContinuousHistogram(capacity: Int, min: Double, max: Double) extends OnlineHistogram {

  private val lower = min
  private val upper = max

  require(capacity > 0, "Capacity should be a positive integer")
  require(lower < upper, "Lower must be less than upper")

  val data = new mutable.ArrayBuffer[(Double, Int)]()

  /** Adds a new item to the histogram
    *
    * @param p value to be added
    */
  override def add(p: Double): Unit = {
    require(p > lower && p < upper, p + " not in (" + lower + "," + upper + ")")
    // search for the index where the value is just higher than p
    val search = find(p)
    // add the new value there, shifting everything to the right
    data.insert(search, (p, 1))
    // If we're over capacity or any two elements are within 1e-9 of each other, merge.
    // This will take care of the case if p was actually equal to some value in the histogram and
    // just increment the value there
    mergeElements()
  }

  /** Merges the histogram with h and returns a histogram with capacity B
    *
    * @param h histogram to be merged
    * @param B capacity of the resultant histogram
    * @return Merged histogram with capacity B
    */
  override def merge(h: OnlineHistogram, B: Int): ContinuousHistogram = {
    h match {
      case temp: ContinuousHistogram => {
        val m: Int = bins
        val n: Int = temp.bins
        var i, j: Int = 0
        val mergeList = new mutable.ArrayBuffer[(Double, Int)]()
        while (i < m || j < n) {
          if (i >= m) {
            mergeList += ((temp.getValue(j), temp.getCounter(j)))
            j = j + 1
          } else if (j >= n || getValue(i) <= temp.getValue(j)) {
            mergeList += data.apply(i)
            i = i + 1
          } else {
            mergeList += ((temp.getValue(j), temp.getCounter(j)))
            j = j + 1
          }
        }
        // the size will be brought to capacity while constructing the new histogram itself
        val finalLower = Math.min(lower, temp.lower)
        val finalUpper = Math.max(upper, temp.upper)
        val ret = new ContinuousHistogram(B, finalLower, finalUpper)
        ret.loadData(mergeList.toArray)
        ret
      }
      case default =>
        throw new RuntimeException("Only a continuous histogram is allowed to be merged with a " +
          "continuous histogram")

    }
  }

  /** Returns the qth quantile of the histogram
    *
    * @param q Quantile value in (0,1)
    * @return Value at quantile q
    */
  def quantile(q: Double): Double = {
    require(bins > 0, "Histogram is empty")
    require(q > 0 && q < 1, "Quantile must be between 0 and 1")
    val wantedSum = (q * total).round.toInt
    var currSum = count(getValue(0))

    if (wantedSum < currSum) {
      require(lower > -MaxValue, "Set a lower bound before proceeding")
      return Math.sqrt(2 * wantedSum * Math.pow(getValue(0) - lower, 2) / getCounter(0)) + lower
    }

    /** Walk the histogram to find sums at every bin value
      * As soon as you hit the interval where you should be
      * Walk along the trapezoidal line
      * This leads to solving a quadratic equation.
      */
    for (i <- 1 to bins - 1) {
      val tmpSum = count(getValue(i))
      if (currSum <= wantedSum && wantedSum < tmpSum) {
        val neededSum = wantedSum - currSum
        val a: Double = getCounter(i) - getCounter(i - 1)
        val b: Double = 2 * getCounter(i - 1)
        val c: Double = -2 * neededSum
        if (a == 0) {
          return getValue(i - 1) + (getValue(i) - getValue(i - 1)) * (-c / b)
        } else {
          return getValue(i - 1) +
            (getValue(i) - getValue(i - 1)) * (-b + Math.sqrt(b * b - 4 * a * c)) / (2 * a)
        }
      } else {
        currSum = tmpSum
      }
    }
    require(upper < MaxValue, "Set an upper bound before proceeding")
    // this means wantedSum > sum(getValue(bins-1))
    // this will likely fail to return a bounded value.
    // Make sure you set some proper limits on min and max.
    getValue(bins - 1) + Math.sqrt(
      Math.pow(upper - getValue(bins - 1), 2) * 2 * (wantedSum - currSum) / getCounter(bins - 1))
  }

  /** Returns the probability (and by extension, the number of points) for the value b
    * Since this is a continuous histogram, return the cumulative probability at b
    *
    * @return Cumulative number of points at b
    */
  def count(b: Double): Int = {
    require(bins > 0, "Histogram is empty")
    if (b < lower) {
      return 0
    }
    if (b >= upper) {
      return total
    }
    /** Suppose x is the index with value just less than or equal to b
      * Then, sum everything up to x-1
      * Add half the value at x
      * Find area of the trapezoid for x and the value b
      */

    val index = find(b) - 1
    var m_b, s: Double = 0
    if (index == -1) {
      m_b = getCounter(index + 1) * (b - lower) / (getValue(index + 1) - lower)
      s = m_b * (b - lower) / (2 * (getValue(index + 1) - lower))
      return s.round.toInt
    } else if (index == bins - 1) {
      m_b = getCounter(index) +
        (-getCounter(index)) * (b - getValue(index)) / (upper - getValue(index))
      s = (getCounter(index) + m_b) *
        (b - getValue(index)) / (2 * (upper - getValue(index)))
    } else {
      m_b = getCounter(index) + (getCounter(index + 1) - getCounter(index)) *
        (b - getValue(index)) / (getValue(index + 1) - getValue(index))
      s = (getCounter(index) + m_b) *
        (b - getValue(index)) / (2 * (getValue(index + 1) - getValue(index)))
    }
    for (i <- 0 to index - 1) {
      s = s + getCounter(i)
    }
    s = s + getCounter(index) / 2
    s.round.toInt
  }

  /** Returns the total number of entries in the histogram
    *
    * @return Total number of points added in the histogram
    */
  override def total: Int = data.iterator.map(_._2).sum

  /** Bin counter access function
    *
    * @param bin bin number to access
    * @return `m_bin` = counter of bin
    */
  private[statistics] def getCounter(bin: Int): Int = {
    require(0 <= bin && bin < bins, bin + " not in [0, " + bins + ")")
    data.apply(bin)._2
  }

  /** Number of bins currently being used
    *
    * @return number of bins
    */
  override def bins: Int = {
    data.size
  }

  /** Returns the string representation of the histogram.
    *
    */
  override def toString: String = {
    s"Size:" + bins + " " + data.toString
  }

  /** Loads values and counters into the histogram.
    * This action can only be performed when there the histogram is empty
    *
    * @param counts Array of tuple of (value,count)
    */
  def loadData(counts: Array[(Double, Int)]): Unit = {
    if (data.nonEmpty) {
      throw new RuntimeException("Data can only be loaded during initialization")
    }
    data ++= counts
    require(checkSanity, "Invalid initialization from data")
  }

  /** Bin value access function
    *
    * @param bin bin number to access
    * @return `v_bin` = value of bin
    */
  private[statistics] def getValue(bin: Int): Double = {
    require(0 <= bin && bin < bins, bin + " not in [0, " + bins + ")")
    data.apply(bin)._1
  }

  /** Checks whether the values loaded into data by loadData are properly sorted or not.
    * All values should be in (lower,upper)
    * All counters loaded into data by loadData should be positive
    *
    */
  private def checkSanity: Boolean = {
    val size = bins
    if (size == 0) return true
    if (lower >= getValue(0)) return false
    if (upper <= getValue(size - 1)) return false
    for (i <- 0 to size - 2) {
      if (getValue(i + 1) < getValue(i)) return false // equality will get merged later on
      if (getCounter(i) <= 0) return false
    }
    if (getCounter(size - 1) <= 0) return false
    // now bring the histogram to capacity and merge all <i>very-close</i> elements
    while (mergeElements()) {}
    true
  }

  /** Searches for the index with value just greater than p
    * If `p >= v_{bins-1}`, return bins
    *
    * @param p value to search for
    * @return the bin with value just greater than p
    */
  private def find(p: Double): Int = {
    val size: Int = bins
    if (size == 0 || p < getValue(0)) {
      return 0
    }
    for (i <- 0 to size - 2) {
      if (p >= getValue(i) && p < getValue(i + 1)) {
        return i + 1
      }
    }
    size
  }

  /** Merges the closest two elements in the histogram
    * If we're over capacity, merge without thought
    * Otherwise, only merge when two elements have almost equal values. We're talking doubles, so
    * no exact equalities
    *
    * @return Whether we succeeded in merging any two elements
    */
  private def mergeElements(): Boolean = {
    val size: Int = bins
    var minDiffIndex: Int = -1
    var minDiff: Double = MaxValue
    for (i <- 0 to size - 2) {
      val currDiff: Double = getValue(i + 1) - getValue(i)
      if (currDiff < minDiff) {
        minDiff = currDiff
        minDiffIndex = i
      }
    }
    if (bins > capacity || minDiff < 1e-9) {
      val counter1 = getCounter(minDiffIndex)
      val counter2 = getCounter(minDiffIndex + 1)
      val weightedValue = getValue(minDiffIndex + 1) * counter2 +
        getValue(minDiffIndex) * counter1
      val counterSum = counter1 + counter2
      data.update(minDiffIndex, (weightedValue / counterSum, counterSum))
      data.remove(minDiffIndex + 1)
      return true
    }
    false
  }
}
