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

/** Implementation of an online histogram
  * Adapted from Ben-Haim and Yom-Tov
  * Refer http://www.jmlr.org/papers/volume11/ben-haim10a/ben-haim10a.pdf
  *
  */
package org.apache.flink.ml.math

import org.apache.flink.api.java.tuple.Tuple2

import java.util

class OnlineHistogram(
                       val capacity: Int,
                       val min: Double = -java.lang.Double.MAX_VALUE,
                       val max: Double = java.lang.Double.MAX_VALUE,
                       val data: util.ArrayList[Tuple2[Double, Int]] = new util.ArrayList[Tuple2[Double, Int]]
                       ) extends Histogram with Serializable {
  require(checkSanity, "Invalid data provided")

  /** Number of bins in the histogram
    *
    * @return number of bins
    */
  def bins: Int = {
    data.size()
  }

  /** Returns the lower limit on values of the histogram
    *
    * @return lower limit on values
    */
  def lower: Double = {
    min
  }

  /** Returns the upper limit on values of the histogram
    *
    * @return upper limit on values
    */
  def upper: Double = {
    max
  }

  /** Bin value access function
    *
    * @param bin bin number to access
    * @return `v_bin` = value of bin
    */
  def getValue(bin: Int): Double = {
    require(0 <= bin && bin < bins, bin + " not in [0, " + bins + ")")
    data.get(bin).getField(0).asInstanceOf[Double]
  }

  /** Bin counter access function
    *
    * @param bin bin number to access
    * @return `m_bin` = counter of bin
    */
  def getCounter(bin: Int): Int = {
    require(0 <= bin && bin < bins, bin + " not in [0, " + bins + ")")
    data.get(bin).getField(1).asInstanceOf[Int]
  }

  /** Adds a new instance to this histogram and appropriately updates counters and values
    *
    * @param value value to be added
    */
  def add(value: Double): Unit = {
    require(value > lower && value < upper, value + " not in (" + lower + "," + upper + ")")
    val search = find(value)
    data.add(search, new Tuple2[Double, Int](value, 1))
    mergeElements() // if the value we just added is already there, mergeElements will take care of this
  }

  /** Merges the histogram with h and returns a histogram with B bins
    *
    * @param h histogram to be merged
    * @param B final size of the merged histogram
    */
  def merge(h: Histogram, B: Int): Histogram = {
    val m: Int = bins
    val n: Int = h.bins
    var i, j: Int = 0
    val tmp_list: util.ArrayList[Tuple2[Double, Int]] = new util.ArrayList[Tuple2[Double, Int]]()
    while (i < m || j < n) {
      if (i >= m) {
        tmp_list.add(new Tuple2[Double, Int](h.getValue(j), h.getCounter(j)))
        j = j + 1
      } else if (j >= n) {
        tmp_list.add(data.get(i))
        i = i + 1
      }
      else if (getValue(i) <= h.getValue(j)) {
        tmp_list.add(data.get(i))
        i = i + 1
      }
      else {
        tmp_list.add(new Tuple2[Double, Int](h.getValue(j), h.getCounter(j)))
        j = j + 1
      }
    }
    new OnlineHistogram(B, Math.min(lower, h.lower), Math.max(upper, h.upper), tmp_list)
  }

  /** Returns the qth quantile of the histogram
    *
    */
  def quantile(q: Double): Double = {
    require(bins > 0, "Histogram is currently empty. Can't find a quantile value")
    var total = 0
    for (i <- 0 to bins - 1) total = total + getCounter(i)
    val wantedSum = (q * total).round.toInt
    var currSum = sum(getValue(0))
    if (wantedSum < currSum) {
      require(lower > -java.lang.Double.MAX_VALUE, "Set a lower bound before proceeding")
      return Math.sqrt(2 * wantedSum * Math.pow(getValue(0) - lower, 2) / getCounter(0)) + lower
    }
    for (i <- 1 to bins - 1) {
      val tmpSum = sum(getValue(i))
      if (currSum <= wantedSum && wantedSum < tmpSum) {
        val neededSum = wantedSum - currSum
        val a: Double = getCounter(i) - getCounter(i - 1)
        val b: Double = 2 * getCounter(i - 1)
        val c: Double = -2 * neededSum
        if (a == 0) {
          return getValue(i - 1) + (getValue(i) - getValue(i - 1)) * (-c / b)
        } else return getValue(i - 1) + (getValue(i) - getValue(i - 1)) * (-b + Math.sqrt(b * b - 4 * a * c)) / (2 * a)
      } else currSum = tmpSum
    }
    require(upper < java.lang.Double.MAX_VALUE, "Set an upper bound before proceeding")
    // this means wantedSum > sum(getValue(bins-1))
    // this will likely fail to return a bounded value. Make sure you set some proper limits on min and max.
    getValue(bins - 1) + Math.sqrt(Math.pow(upper - getValue(bins - 1), 2) * 2 * (wantedSum - currSum) / getCounter(bins - 1))
  }

  /** Returns the string representation of the histogram.
    *
    */
  override def toString: String = {
    s"Size:" + bins + " " + data.toString
  }

  /** Returns the estimated number of points in the interval `(-\infty,b]`
    *
    * @return Number of values in the interval `(-\infty,b]`
    */
  def sum(b: Double): Int = {
    require(bins > 0, "Histogram is empty")
    if (b < lower) return 0
    if (b >= upper) {
      var ret = 0
      for (i <- 0 to bins - 1) {
        ret = ret + getCounter(i)
      }
      return ret
    }
    val index = find(b) - 1
    var m_b, s: Double = 0
    if (index == -1) {
      m_b = getCounter(index + 1) * (b - lower) / (getValue(index + 1) - lower)
      s = m_b * (b - lower) / (2 * (getValue(index + 1) - lower))
      return s.round.toInt
    } else if (index == bins - 1) {
      m_b = getCounter(index) + (-getCounter(index)) * (b - getValue(index)) / (upper - getValue(index))
      s = (getCounter(index) + m_b) * (b - getValue(index)) / (2 * (upper - getValue(index)))
    } else {
      m_b = getCounter(index) + (getCounter(index + 1) - getCounter(index)) * (b - getValue(index)) / (getValue(index + 1) - getValue(index))
      s = (getCounter(index) + m_b) * (b - getValue(index)) / (2 * (getValue(index + 1) - getValue(index)))
    }
    for (i <- 0 to index - 1) {
      s = s + getCounter(i)
    }
    s = s + getCounter(index) / 2
    s.round.toInt
  }

  /** Updates the given bin with the provided value and counter. Sets `v_bin`=value and `m_bin`=counter
    *
    * @param bin bin to be updated
    * @param value value to be set at bin
    * @param counter counter to be set at bin
    */
  private def set(bin: Int, value: Double, counter: Int): Unit = {
    require(0 <= bin && bin < bins, bin + " not in [0, " + bins + ")")
    require(value > lower && value < upper, value + " not in (" + lower + "," + upper + ")")
    data.set(bin, new Tuple2[Double, Int](value, counter))
  }

  /** Searches for value in the histogram
    *
    * @param p value to search for
    * @return the bin with value just greater than p. If `p > m_{bins-1}`, return bins
    */
  private def find(p: Double): Int = {
    val size: Int = bins
    for (i <- 0 to size - 1) {
      if (p >= getValue(i) && (i + 1 >= size || p < getValue(i + 1))) {
        return i + 1
      }
    }
    0
  }

  /** Merges the closest two elements in the histogram. Definitely merge if we're over capacity.
    * Otherwise, merge only if two elements are really close
    *
    */
  private def mergeElements(): Boolean = {
    var index: Int = -1
    val size: Int = bins
    var diff: Double = java.lang.Double.MAX_VALUE
    for (i <- 0 to size - 2) {
      val curr_diff: Double = getValue(i + 1) - getValue(i)
      if (curr_diff < diff) {
        diff = curr_diff
        index = i
      }
    }
    if (bins > capacity || diff < 1e-9) {
      val merged_tuple: Tuple2[Double, Int] = mergeBins(index)
      set(index, merged_tuple.getField(0).asInstanceOf[Double] / merged_tuple.getField(1).asInstanceOf[Int], merged_tuple.getField(1))
      data.remove(index + 1)
      true
    } else false
  }

  /** Returns the merging of the bin b and its next bin
    *
    * *@param b the bin to be merging with bin b+1
    * @return the tuple (`v_b.m_b + v_{b+1}.m_{b+1}`,`m_b+m_{b+1}`)
    */
  private def mergeBins(b: Int): Tuple2[Double, Int] = {
    val ret: Tuple2[Double, Int] = new Tuple2[Double, Int]()
    ret.setField(getValue(b + 1) * getCounter(b + 1) + getValue(b) * getCounter(b), 0)
    ret.setField(getCounter(b + 1) + getCounter(b), 1)
    ret
  }

  /** Checks whether the arraylist provided is properly sorted or not.
    *
    */
  private def checkSanity: Boolean = {
    if (lower >= upper) return false
    if (capacity <= 0) return false
    if (data.size() == 0) return true
    if (lower >= getValue(0)) return false
    if (upper <= getValue(bins - 1)) return false
    for (i <- 0 to bins - 2) {
      if (getValue(i + 1) < getValue(i)) return false // equality will get merged later on
      if (getCounter(i) <= 0) return false
    }
    if (getCounter(bins - 1) <= 0) return false
    while (mergeElements()) {}
    true
  }
}