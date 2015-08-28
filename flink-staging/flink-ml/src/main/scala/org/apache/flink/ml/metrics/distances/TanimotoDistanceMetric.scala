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

/** This class implements a Tanimoto distance metric. The class calculates the distance between
  * the given vectors. The vectors are assumed as bit-wise vectors. We convert the result of
  * division to a usable distance. So, 1 - similarity is actually returned.
  *
  * @see http://en.wikipedia.org/wiki/Jaccard_index
  */
class TanimotoDistanceMetric extends DistanceMetric {
  override def distance(a: Vector, b: Vector): Double = {
    checkValidArguments(a, b)

    val dotProd: Double = a.dot(b)
    1 - dotProd / (a.magnitude * a.magnitude + b.magnitude * b.magnitude - dotProd)
  }
}

object TanimotoDistanceMetric {
  def apply() = new TanimotoDistanceMetric()
}
