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

/** Base trait for Histogram
  *
  */
trait Histogram {
  /** Number of bins in the histogram
    *
    * @return number of bins
    */
  def bins: Int

  /** Returns the lower limit on values of the histogram
    *
    * @return lower limit on values
    */
  def lower: Double

  /** Returns the upper limit on values of the histogram
    *
    * @return upper limit on values
    */
  def upper: Double

  /** Bin value access function
    *
    * @param bin bin number to access
    * @return `v_bin` = value of bin
    */
  def getValue(bin: Int): Double

  /** Bin counter access function
    *
    * @param bin bin number to access
    * @return `m_bin` = counter of bin
    */
  def getCounter(bin: Int): Int

  /** Adds a new instance to this histogram and appropriately updates counters and values
    *
    * @param value value to be added
    */
  def add(value: Double): Unit

  /** Returns the estimated number of points in the interval `(-\infty,b]`
    *
    * @return Number of values in the interval `(-\infty,b]`
    */
  def sum(b: Double): Int

  /** Merges the histogram with h and returns a histogram with B bins
    *
    * @param h histogram to be merged
    * @param B final size of the merged histogram
    */
  def merge(h: Histogram, B: Int): Histogram

  /** Returns a list `u_1,u_2,\ldots,u_{B-1}` such that the number of points in
    * `(-\infty,u_1],[u_1,u_2],\ldots,[u_{B-1},\infty)` is `\frac_{1}{B} \sum_{i=0}^{bins-1} m_i`.
    *
    * @param B number of intervals required
    */
  def uniform(B: Int): Array[Double]
}
