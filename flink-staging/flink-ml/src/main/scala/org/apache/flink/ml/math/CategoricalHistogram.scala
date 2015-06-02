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

import java.util

/** Implementation of a discrete valued online histogram
  *
  */
class CategoricalHistogram(
                            values: Array[Double],
                            counts: Array[Int] = Array.ofDim[Int](0))
  extends OnlineHistogram
  with Serializable {
  // sanity checks
  require(values.length > 0, "Number of classes should be a positive integer")
  require(counts.length == 0 || counts.length == values.length, "Counts should have the same " +
    "number" + " " + "of " + "entries as values")
  val data = new util.ArrayList[(Double, Int)](values.length)
  require(checkSanity, "All values in counts should be non-negative")

  /** Number of bins currently being used
    *
    * @return number of bins
    */
  override def bins: Int = {
    data.size
  }

  /** Adds a new instance
    *
    * @param p value to be added
    */
  override def add(p: Double): Unit = {
    // search for the index where the value is equal to1 p
    val search = find(p)
    require(search >= 0, p + "is not present in the histogram")
    val currentVal = data.get(search)
    data.set(search, (currentVal._1, currentVal._2 + 1))
  }

  /** Merges the histogram with h and returns a new histogram
    *
    * @param h histogram to be merged
    * @param B capacity of the resultant histogram
    * @return Merged histogram with capacity B
    */
  override def merge(h: OnlineHistogram, B: Int): CategoricalHistogram = {
    require(h.isInstanceOf[CategoricalHistogram],
      "Only a continuous histogram is allowed to be merged with a continuous histogram")
    val h1 = h.asInstanceOf[CategoricalHistogram]
    require(B == bins && B == h.bins, "Size of the histograms and B should be equal")
    val finalValues = Array.ofDim[Double](bins)
    val finalCounts = Array.ofDim[Int](bins)
    for (i <- 0 to bins - 1) {
      require(getValue(i) == h1.getValue(i), "Both histograms must have the same classes, in same" +
        " order")
      finalCounts(i) = getCounter(i) + h1.getCounter(i)
      finalValues(i) = getValue(i)
    }
    new CategoricalHistogram(finalValues, finalCounts)
  }

  /** Returns the qth quantile of the histogram
    *
    * @param q Quantile value in (0,1)
    * @return Value at quantile q
    */
  override def quantile(q: Double): Double = {
    require(0 == 1, "Categorical histograms don't support quantiles")
    0
  }

  /** Returns the probability (and by extension, the number of points) for the value b
    * Since this is a discrete histogram, return the probability mass function at b
    *
    * @return Number of points with value b
    */
  override def count(b: Double): Int = {
    val index = find(b)
    require(index >= 0, b + " is not present in the histogram")
    getCounter(index)
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

  /** Bin value access function
    *
    * @param bin bin number to access
    * @return `v_bin` = value of bin
    */
  override def getValue(bin: Int): Double = {
    require(0 <= bin && bin < bins, bin + " not in [0, " + bins + ")")
    data.get(bin)._1
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

  /** Returns the string representation of the histogram.
    *
    */
  override def toString: String = {
    s"Size:" + bins + " " + data.toString
  }

  /** Checks whether all counters are non-negative or not
    *
    */
  private def checkSanity: Boolean = {
    for (i <- 0 to values.length - 1) {
      var counter = 0
      if (counts.length > 0) {
        counter = counts.apply(i)
      }
      data.add(i, (values.apply(i), counter))
    }
    val size = bins
    for (i <- 0 to size - 1) {
      if (getCounter(i) < 0) return false
    }
    true
  }

  /** Searches for the index with value equal to p
    *
    * @param p value to search for
    * @return the bin with value equal to p
    */
  private def find(p: Double): Int = {
    val size: Int = bins
    for (i <- 0 to size - 1) {
      if (getValue(i) == p) {
        return i
      }
    }
    -1
  }
}
