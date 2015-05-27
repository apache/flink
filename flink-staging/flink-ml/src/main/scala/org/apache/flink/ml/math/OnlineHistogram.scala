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

/** Base trait for an Online Histogram
  *
  */
trait OnlineHistogram {

  /** Number of bins currently being used
    *
    * @return number of bins
    */
  def bins: Int

  /** Adds a new instance
    *
    * @param p value to be added
    */
  def add(p: Double): Unit

  /** Merges the histogram with h and returns a histogram with capacity B
    *
    * @param h histogram to be merged
    * @param B capacity of the resultant histogram
    * @return Merged histogram with capacity B
    */
  def merge(h: OnlineHistogram, B: Int): OnlineHistogram

  /** Returns the qth quantile of the histogram
    * Should fail for a discrete version
    *
    * @param q Quantile value in (0,1)
    * @return Value at quantile q
    */
  def quantile(q: Double): Double

  /** Returns the probability (and by extension, the number of points) for the value b
    * For a discrete version, it should return the probability mass function value at b
    * For a continuous version, it should return the cumulative probability at b
    *
    * @return Probability of value b
    */
  def count(b: Double): Int

  /** Returns the total number of entries in the histogram
    *
    * @return Total number of points added in the histogram
    */
  def total: Int

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
}
