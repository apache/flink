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

/** Base trait for an Online Histogram
  *
  */
trait OnlineHistogram extends Serializable {

  /** Number of bins currently being utilized
    *
    * @return number of bins
    */
  def bins: Int

  /** Adds a new item to the histogram
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

  /** Returns the total number of entries in the histogram
    *
    * @return Total number of points added in the histogram
    */
  def total: Int

}
