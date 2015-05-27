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

package org.apache.flink.ml.math

import java.lang.Double.MAX_VALUE
import java.util

/** Implementation of a continuous valued online histogram
  * Adapted from Ben-Haim and Yom-Tov's Streaming Decision Tree Algorithm
  * Refer http://www.jmlr.org/papers/volume11/ben-haim10a/ben-haim10a.pdf
  *
  */
class ContinuousHistogram(
                           capacity: Int,
                           min: Double,
                           max: Double,
                           data: util.ArrayList[(Double, Int)] = new util.ArrayList[(Double, Int)]()
                           )
  extends OnlineHistogram
  with Serializable {

  // sanity checks
  require(capacity > 0, "Capacity should be a positive integer")
  require(lower < upper, "Lower must be less than upper")
  require(checkSanity, "Invalid values in data. Make sure it is sorted and all counters are " +
    "positive")

  /** Adds a new instance
    *
    * @param p value to be added
    */
  override def add(p: Double): Unit = {
    require(p > lower && p < upper, p + " not in (" + lower + "," + upper + ")")
    // search for the index where the value is just higher than p
    val search = find(p)
    // add the new value there, shifting everything to the right
    data.add(search, (p, 1))
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
    require(h.isInstanceOf[ContinuousHistogram],
      "Only a continuous histogram is allowed to be merged with a continuous histogram")
    val temp = h.asInstanceOf[ContinuousHistogram]
    val m: Int = bins
    val n: Int = temp.bins
    var i, j: Int = 0
    val mergeList: util.ArrayList[(Double, Int)] = new util.ArrayList[(Double, Int)]()
    while (i < m || j < n) {
      if (i >= m) {
        mergeList.add((temp.getValue(j), temp.getCounter(j)))
        j = j + 1
      } else if (j >= n) {
        mergeList.add(data.get(i))
        i = i + 1
      } else if (getValue(i) <= temp.getValue(j)) {
        mergeList.add(data.get(i))
        i = i + 1
      } else {
        mergeList.add((temp.getValue(j), temp.getCounter(j)))
        j = j + 1
      }
    }
    // the size will be brought to capacity while constructing the new histogram itself
    new ContinuousHistogram(B, Math.min(lower, temp.lower), Math.max(upper, temp.upper), mergeList)
  }

  /** Bin value access function
    *
    * @param bin bin number to access
    * @return `v_bin` = value of bin
    */
  override def getValue(bin: Int): Double = {
    require(0 <= bin && bin < bins, bin + " not in [0, " + bins + ")")
    data.get(bin)._1
  }

  /** Returns the lower limit on values of the histogram
    *
    * @return lower limit on values
    */
  private def lower: Double = {
    min
  }

  /** Returns the upper limit on values of the histogram
    *
    * @return upper limit on values
    */
  private def upper: Double = {
    max
  }

  /** Returns the qth quantile of the histogram
    *
    * @param q Quantile value in (0,1)
    * @return Value at quantile q
    */
  override def quantile(q: Double): Double = {
    require(bins > 0, "Histogram is empty")
    require(q > 0 && q < 1, "Quantile must be between 0 and 1")
    var total = 0
    for (i <- 0 to bins - 1) {
      total = total + getCounter(i)
    }
    val wantedSum = (q * total).round.toInt
    var currSum = count(getValue(0))

    if (wantedSum < currSum) {
      require(lower > -MAX_VALUE, "Set a lower bound before proceeding")
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
        } else return getValue(i - 1) +
          (getValue(i) - getValue(i - 1)) * (-b + Math.sqrt(b * b - 4 * a * c)) / (2 * a)
      } else currSum = tmpSum
    }
    require(upper < MAX_VALUE, "Set an upper bound before proceeding")
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
  override def count(b: Double): Int = {
    require(bins > 0, "Histogram is empty")
    if (b < lower) {
      return 0
    }
    if (b >= upper) {
      var ret = 0
      for (i <- 0 to bins - 1) {
        ret = ret + getCounter(i)
      }
      return ret
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
  override def total: Int = {
    var result = 0
    for (i <- 0 to bins - 1) {
      result = result + getCounter(i)
    }
    result
  }

  /** Bin counter access function
    *
    * @param bin bin number to access
    * @return `m_bin` = counter of bin
    */
  override def getCounter(bin: Int): Int = {
    require(0 <= bin && bin < bins, bin + " not in [0, " + bins + ")")
    data.get(bin)._2
  }

  /** Number of bins currently being used
    *
    * @return number of bins
    */
  override def bins: Int = {
    data.size()
  }

  /** Returns the string representation of the histogram.
    *
    */
  override def toString: String = {
    s"Size:" + bins + " " + data.toString
  }

  /** Checks whether the arraylist provided is properly sorted or not.
    * All values should be in (lower,upper)
    * All counters should be positive
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
    if (size == 0) {
      return 0
    }
    if (p < getValue(0)) {
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
    var minDiff: Double = MAX_VALUE
    for (i <- 0 to size - 2) {
      val currDiff: Double = getValue(i + 1) - getValue(i)
      if (currDiff < minDiff) {
        minDiff = currDiff
        minDiffIndex = i
      }
    }
    if (bins > capacity || minDiff < 1e-9) {
      val weightedValue = getValue(minDiffIndex + 1) * getCounter(minDiffIndex + 1) +
        getValue(minDiffIndex) * getCounter(minDiffIndex)
      val counterSum = getCounter(minDiffIndex + 1) + getCounter(minDiffIndex)
      data.set(minDiffIndex, (weightedValue / counterSum, counterSum))
      data.remove(minDiffIndex + 1)
      return true
    }
    false
  }
}
