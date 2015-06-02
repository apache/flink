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

/** Defines a probability distribution
  * It doesn't have to be a distribution with the sum of all probabilities equal to 1
  * Instead, the sum of all elements acts as a scaling factor.
  * So, for example, the distribution
  * a. [0.1, 0.2, 0.4, 0.3]
  * b. [1, 2, 4, 3]
  * are equivalent
  *
  */
class ProbabilityDistribution(data: Array[Double]) {
  val cumulative: Array[Double] = Array.ofDim[Double](data.length)
  require(checkSanity, "Invalid Probability Distribution")


  /** Maps a random number r to the happening of the event at index i
    * For example, consider the distribution [0.1, 0.3, 0.4, 0.2]
    * Then, occurrence of event corresponding to probabilities at index 0,1,2 and 3 corresponds
    * to generation of a random number between (0,0.1), (0.1,0.4), (0.4,0.8) and (0.8,1.0)
    * respectively.
    * @param r A random number between (0,1)
    * @return Index of the event this r corresponds to
    */
  def sample(r: Double): Int = {
    if (data.length == 1) {
      return 0
    }
    var start = 0
    var end = data.length - 1
    var mid: Int = 0
    while (start <= end) {
      mid = (start + end) / 2
      if (r < cumulative(mid - 1)) {
        end = mid - 1
      } else if (r > cumulative(mid)) {
        start = mid + 1
      } else {
        return mid
      }
    }
    // we can never reach here
    require(1 == 2, "Unexpected error occurred")
    mid
  }

  /** Verifies that the given data provided is correct. It must be non-negative
    *
    */
  private def checkSanity: Boolean = {
    require(data.length >= 1, "There must be at least one element in the distribution")
    for (i <- 0 to data.length - 1) {
      // probability can't be less than zero
      if (data.apply(i) < 0) {
        return false
      }
    }
    // start building the cumulative distribution
    cumulative(0) = data(0)
    for (i <- 1 to data.length - 1) {
      cumulative(i) = cumulative(i - 1) + data(i)
    }
    // now normalize in case the sum of probabilities wasn't one
    for (i <- 0 to data.length - 1) {
      cumulative(i) /= cumulative(data.length - 1)
    }
    true
  }
}
