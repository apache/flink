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

/** This class implements a Euclidean distance metric. The metric calculates the distance between
  * the given two vectors by summing the square root of the squared differences between
  * each coordinate.
  *
  * http://en.wikipedia.org/wiki/Euclidean_distance
  *
  * If you don't care about the true distance and only need for comparison,
  * [[SquaredEuclideanDistanceMetric]] will be faster because it doesn't calculate the actual
  * square root of the distances.
  *
  * @see http://en.wikipedia.org/wiki/Euclidean_distance
  */
class EuclideanDistanceMetric extends SquaredEuclideanDistanceMetric {
  override def distance(a: Vector, b: Vector): Double = math.sqrt(super.distance(a, b))
}

object EuclideanDistanceMetric {
  def apply() = new EuclideanDistanceMetric()
}
