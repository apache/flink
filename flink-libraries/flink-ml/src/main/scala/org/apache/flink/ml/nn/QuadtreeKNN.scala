
package org.apache.flink.ml.nn

import org.apache.flink.ml.math.{DenseVector, Vector => FlinkVector}
import org.apache.flink.ml.metrics.distances.DistanceMetric
import org.apache.flink.util.Collector

import scala.collection.immutable.Vector
import scala.collection.mutable

/**
  * Class containing a method to do an exact knn query using a quadtree
  */

class QuadtreeKNN() {

  def knnQueryWithQuadTree[T <: FlinkVector](
                                              training: Vector[T],
                                              testing: Vector[(Long, T)],
                                              k: Int, metric: DistanceMetric,
                                              out: Collector[(FlinkVector,
                                                FlinkVector, Long, Double)]) {
    /// find a bounding box
    val MinArr = Array.tabulate(training.head.size)(x => x)
    val MaxArr = Array.tabulate(training.head.size)(x => x)

    val minVecTrain = MinArr.map(i => training.map(x => x(i)).min - 0.01)
    val minVecTest = MinArr.map(i => testing.map(x => x._2(i)).min - 0.01)
    val maxVecTrain = MaxArr.map(i => training.map(x => x(i)).max + 0.01)
    val maxVecTest = MaxArr.map(i => testing.map(x => x._2(i)).max + 0.01)

    val Min = DenseVector(MinArr.map(i => Array(minVecTrain(i), minVecTest(i)).min))
    val Max = DenseVector(MinArr.map(i => Array(maxVecTrain(i), maxVecTest(i)).max))

    //default value of max elements/box is set to max(20,k)
    val maxPerBox = Array(k, 20).max
    val trainingQuadTree = new QuadTree(Min, Max, metric, maxPerBox)

    val queue = mutable.PriorityQueue[(FlinkVector, FlinkVector, Long, Double)]()(
      Ordering.by(_._4))

    for (v <- training) {
      trainingQuadTree.insert(v)
    }

    for ((id, vector) <- testing) {
      //  Find siblings' objects and do local kNN there
      val siblingObjects =
        trainingQuadTree.searchNeighborsSiblingQueue(vector)

      // do KNN query on siblingObjects and get max distance of kNN
      // then rad is good choice for a neighborhood to do a refined
      // local kNN search
      val knnSiblings = siblingObjects.map(v => metric.distance(vector, v)
      ).sortWith(_ < _).take(k)

      val rad = knnSiblings.last
      val trainingFiltered = trainingQuadTree.searchNeighbors(vector, rad)

      for (b <- trainingFiltered) {
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
