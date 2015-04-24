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

/** This class implements a cosine distance metric. The class calculates the distance between
  * the given vectors by dividing the dot product of two vectors by the product of their lengths.
  * We convert the result of division to a usable distance. So, 1 - cos(angle) is actually returned.
  *
  * @see http://en.wikipedia.org/wiki/Cosine_similarity
  */
class CosineDistanceMetric extends DistanceMetric {
  override def distance(a: Vector, b: Vector): Double = {
    checkValidArguments(a, b)

    val dotProd: Double = a.dot(b)
    val denominator: Double = a.magnitude * b.magnitude
    if (dotProd == 0 && denominator == 0) {
      0
    } else {
      1 - dotProd / denominator
    }
  }
}

object CosineDistanceMetric {
  def apply() = new CosineDistanceMetric()
}
