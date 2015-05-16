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

import org.apache.flink.api.java.tuple.Tuple2

import java.util

class OnlineHistogram(
                       capacity: Int,
                       min: Double = java.lang.Double.MIN_VALUE,
                       max: Double = java.lang.Double.MIN_VALUE,
                       data: util.ArrayList[Tuple2[Double, Int]] = new util.ArrayList[Tuple2[Double, Int]]
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
    if (bins > capacity) mergeElements()
  }

  /** Returns the estimated number of points in the interval `(-\infty,b]`
    *
    * @return Number of values in the interval `(-\infty,b]`
    */
  def sum(b: Double): Int = {
    require(bins > 0, "Histogram is empty")
    if (b < lower) return 0
    if (b > upper) return sum(upper)
    val index = find(b) - 1
    var m_b, s: Double = 0
    if (index == -1) {
      m_b = getCounter(index + 1) * (b - lower) / (getValue(index + 1) - lower)
      s = m_b * (b - lower) / (2 * (getValue(index + 1) - lower))
      return s.toInt
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
    s.toInt
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

  /** Returns a list `u_1,u_2,\ldots,u_{B-1}` such that the number of points in
    * `(-\infty,u_1],[u_1,u_2],\ldots,[u_{B-1},\infty)` is `\frac_{1}{B} \sum_{i=0}^{bins-1} m_i`.
    *
    * @param B number of intervals required
    */
  def uniform(B: Int): Array[Double] = {
    require(bins > 0, "Histogram is empty")
    require(B > 1, "Cannot equalize in less than two intervals")
    val ret: Array[Double] = new Array[Double](B - 1)
    val total: Int = sum(upper)
    for (j <- 0 to B - 2) {
      val s: Int = (j + 1) * total / B
      val search: Tuple2[Int, Int] = searchSum(s)
      val i: Int = search.getField(1).asInstanceOf[Int]
      val d: Int = s - search.getField(0).asInstanceOf[Int]
      var a, b, c: Double = 0
      if (i == -1) {
        a = getCounter(i + 1)
        b = 0
        c = -2 * d
        val z: Double = (-b + Math.sqrt(b * b - 4 * a * c)) / (2 * a)
        ret(j) = lower + (getValue(i + 1) - lower) * z
      } else if (i == bins - 1) {
        a = -getCounter(i)
        b = 2 * getCounter(i)
        c = -2 * d
        val z: Double = (-b + Math.sqrt(b * b - 4 * a * c)) / (2 * a)
        ret(j) = getValue(i) + (upper - getValue(i)) * z
      } else {
        a = getCounter(i + 1) - getCounter(i)
        b = 2 * getCounter(i)
        c = -2 * d
        val z: Double = (-b + Math.sqrt(b * b - 4 * a * c)) / (2 * a)
        ret(j) = getValue(i) + (getValue(i + 1) - getValue(i)) * z
      }
    }
    ret
  }

  /** Returns the string representation of the histogram.
    *
    */
  override def toString: String = {
    s"Size:" + bins + " " + data.toString
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

  /** Searches for an index i such that sum(v_i) < s < sum(v_{i+1})
    *
    * *@param s
    * @return a tuple of sum(v_i) and index i
    */
  private def searchSum(s: Double): Tuple2[Int, Int] = {
    val size: Int = bins
    var curr_sum: Int = sum(getValue(0))
    for (i <- 0 to size - 1) {
      var tmp_sum: Int = 0
      if (i + 1 < size) tmp_sum = sum(getValue(i + 1))
      if (s >= curr_sum && (i + 1 >= size || s < tmp_sum)) {
        return new Tuple2[Int, Int](curr_sum, i)
      }
      curr_sum = tmp_sum
    }
    new Tuple2[Int, Int](0, -1)
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

  /** Merges the closest two elements in the histogram
    *
    */
  private def mergeElements(): Unit = {
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
    val merged_tuple: Tuple2[Double, Int] = mergeBins(index)
    set(index, merged_tuple.getField(0).asInstanceOf[Double] / merged_tuple.getField(1).asInstanceOf[Int], merged_tuple.getField(1))
    data.remove(index + 1)
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
      if (getValue(i + 1) <= getValue(i)) return false
      if (getCounter(i) <= 0) return false
    }
    if (getCounter(bins - 1) <= 0) return false
    while (bins > capacity)
      mergeElements()
    true
  }
}
