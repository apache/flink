package org.apache.flink.ml.nn

import org.apache.flink.ml.math.{Vector => FlinkVector}
import org.apache.flink.ml.metrics.distances.DistanceMetric
import org.apache.flink.util.Collector

import scala.collection.immutable.Vector
import scala.collection.mutable

class basicknn {

  /**
    *
    * @param training training set
    * @param testing test set
    * @param k number of neighbors to search for
    * @param metric distance used when computing the nearest neighbors
    * @param queue a priority queue for basicknn query
    * @param out collector of output
    * @tparam T FlinkVector
    */
  def knnQueryBasic[T <: FlinkVector](
                                       training: Vector[T],
                                       testing: Vector[(Long, T)],
                                       k: Int, metric: DistanceMetric,
                                       queue: mutable.PriorityQueue[(FlinkVector,
                                         FlinkVector, Long, Double)],
                                       out: Collector[(FlinkVector, FlinkVector, Long, Double)]) {

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
