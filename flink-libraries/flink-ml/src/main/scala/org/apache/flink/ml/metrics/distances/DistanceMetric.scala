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

/** DistanceMeasure interface is used for object which determines distance between two points.
  */
trait DistanceMetric extends Serializable {
  /** Returns the distance between the arguments.
    *
    * @param a a Vector defining a multi-dimensional point in some space
    * @param b a Vector defining a multi-dimensional point in some space
    * @return a scalar double of the distance
    */
  def distance(a: Vector, b: Vector): Double

  protected def checkValidArguments(a: Vector, b: Vector) = {
    require(a.size == b.size, "The each size of vectors must be same to calculate distance.")
  }
}
