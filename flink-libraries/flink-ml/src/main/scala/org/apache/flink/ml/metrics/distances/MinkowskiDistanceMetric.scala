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

package org.apache.flink.ml.metrics.distances

import org.apache.flink.ml.math.Vector

/** This class implements a Minkowski distance metric. The metric is a generalization of
  * L(p) distances: Euclidean distance and Manhattan distance. If you need for a special case of
  * p = 1 or p = 2, use [[ManhattanDistanceMetric]], [[EuclideanDistanceMetric]]. This class is
  * useful for high exponents.
  *
  * @param p the norm exponent of space
  *
  * @see http://en.wikipedia.org/wiki/Minkowski_distance
  */
class MinkowskiDistanceMetric(val p: Double) extends DistanceMetric {
  override def distance(a: Vector, b: Vector): Double = {
    checkValidArguments(a, b)
    math.pow((0 until a.size).map(i => math.pow(math.abs(a(i) - b(i)), p)).sum, 1 / p)
  }
}

object MinkowskiDistanceMetric {
  def apply(p: Double) = new MinkowskiDistanceMetric(p)
}
