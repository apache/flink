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

package org.apache.flink.ml.nn

import org.apache.flink.ml.math.{Vector => FlinkVector}
import org.apache.flink.ml.metrics.distances.DistanceMetric
import org.apache.flink.util.Collector

import scala.collection.immutable.Vector
import scala.collection.mutable

class basicKNN {

  /**
    *
    * @param training training set
    * @param testing test set
    * @param k number of neighbors to search for
    * @param metric distance used when computing the nearest neighbors
    * @param out collector of output
    * @tparam T FlinkVector
    */
  def knnQueryBasic[T <: FlinkVector](
                                       training: Vector[T],
                                       testing: Vector[(Long, T)],
                                       k: Int, metric: DistanceMetric,
                                       out: Collector[(FlinkVector, FlinkVector, Long, Double)]) {

    val queue = mutable.PriorityQueue[(FlinkVector, FlinkVector, Long, Double)]()(
      Ordering.by(_._4))

    for ((id, vector) <- testing) {
      for (b <- training) {
        // (training vector, input vector, input key, distance)
        queue.enqueue((b, vector, id, metric.distance(b, vector)))
        if (queue.size > k) {
          queue.dequeue()
        }
      }
      for (v <- queue) {
        out.collect(v)
      }
    }
  }

}
