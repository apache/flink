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

import scala.collection.mutable

/** Implementation of a discrete valued online histogram
  *
  * =Parameters=
  * -[[numCategories]]:
  * Number of categories in the histogram
  */
class DiscreteHistogram(numCategories: Int) extends OnlineHistogram {

  require(numCategories > 0, "Capacity must be greater than zero")
  val data = new mutable.HashMap[Double, Int]()

  /** Number of categories in the histogram
    *
    * @return number of categories
    */
  override def bins: Int = {
    numCategories
  }

  /** Increment count of category c
    *
    * @param c category whose count needs to be incremented
    */
  override def add(c: Double): Unit = {
    data.get(c) match {
      case None =>
        require(data.size < numCategories, "Insufficient capacity. Failed to add.")
        data.put(c, 1)
      case Some(value) =>
        data.update(c, value + 1)
    }
  }

  /** Merges the histogram with h and returns a new histogram
    *
    * @param h histogram to be merged
    * @param B number of categories in the resultant histogram.
    *          (Default: ```0```, number of categories will be the size of union of categories in
    *          both histograms)
    * @return Merged histogram with capacity B
    */
  override def merge(h: OnlineHistogram, B: Int = 0): DiscreteHistogram = {
    h match {
      case h1: DiscreteHistogram => {
        val finalMap = new mutable.HashMap[Double, Int]()
        data.iterator.foreach(x => finalMap.put(x._1, x._2))
        h1.data.iterator.foreach(x => {
          finalMap.get(x._1) match {
            case None => finalMap.put(x._1, x._2)
            case Some(value) => finalMap.update(x._1, x._2 + value)
          }
        })
        require(B == 0 || finalMap.size <= B, "Insufficient capacity. Failed to merge")
        val finalSize = if (B > 0) B else finalMap.size
        val ret = new DiscreteHistogram(finalSize)
        ret.loadData(finalMap.toArray)
        ret
      }
      case default =>
        throw new RuntimeException("Only a discrete histogram is allowed to be merged with a " +
          "discrete histogram")
    }
  }

  /** Number of elements in category c
    *
    * @return Number of points in category c
    */
  def count(c: Double): Int = {
    data.get(c) match {
      case None => 0
      case Some(value) => value
    }
  }

  /** Returns the total number of elements in the histogram
    *
    * @return total number of elements added so far
    */
  override def total: Int = data.values.sum

  /** Returns the string representation of the histogram.
    *
    */
  override def toString: String = {
    s"Size:" + bins + " " + data.toString
  }

  /** Loads values and counters into the histogram.
    * This action can only be performed when there the histogram is empty
    *
    * @param counts Array of tuple of (category,count)
    */
  def loadData(counts: Array[(Double, Int)]): Unit = {
    if (data.nonEmpty) {
      throw new RuntimeException("Data can only be loaded during initialization")
    }
    require(counts.size <= bins, "Insufficient capacity. Failed to initialize")
    counts.iterator.foreach(valCount => {
      require(valCount._2 >= 0, "Counters must be non-negative")
      data.put(valCount._1, valCount._2)
    })
  }
}
