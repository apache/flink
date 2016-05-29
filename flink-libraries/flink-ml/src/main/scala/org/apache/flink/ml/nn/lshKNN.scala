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

import org.apache.flink.ml.metrics.distances.DistanceMetric
import org.apache.flink.util.Collector

import scala.collection.immutable.Vector
import scala.collection.mutable.{ArrayBuffer, HashMap}
import org.apache.flink.ml.math.{Vector => FlinkVector}

/**
  * Locality Sensitive Hashing (LSH) based approximate knn query
  *
  * This method projects onto alpha number of random directions
  * and parameters of the hashing functions are tweaked so that
  * each bucket has at least k points and each testing point
  * lies in a bucket
  *
  * backgrond paper:
  * http://www.slaney.org/malcolm/yahoo/Slaney2008-LSHTutorial.pdf
  *
  */
class lshKNN extends basicKNN {

  val alpha = 5
  // number of hash functions
  val b = 1.0
  // b and W0 are parameters for the hashing functions
  val W0 = 1.0
  val r = scala.util.Random

  /**
    *
    * @param training training set
    * @param testing test set
    * @param k number of neighbors to search for
    * @param metric distance used when computing the nearest neighbors
    * @param out collector of output
    * @tparam T FlinkVector
    */
  def lshknnQuery[T <: FlinkVector](
                                     training: Vector[T],
                                     testing: Vector[(Long, T)],
                                     k: Int, metric: DistanceMetric,
                                     out: Collector[(FlinkVector, FlinkVector, Long, Double)]) {

    val dim = training.head.size
    val rSeq = ArrayBuffer.fill(alpha)(ArrayBuffer.fill(dim)
    (r.nextDouble).asInstanceOf[FlinkVector])

    val hashMapTrain = HashMap[ArrayBuffer[Int],
      scala.collection.mutable.Set[FlinkVector]]()

    var numColl = 0

    var W = W0
    var increaseW = false
    // do a first check to see whether we increase/decrease W0

    // edge case: may not have k collisions
    // preprocess both training and test set so that
    // (1) each training bucket has at least k points and
    // (2) each testing bucket is a collision.  Start W to be 1.0 and
    // decrease/increase by factor of 2.

    fillMapTrain(training, hashMapTrain, W, rSeq)
    increaseW = updateW(hashMapTrain, k, increaseW)

    hashMapTrain.clear()

    if (increaseW) {
      while (increaseW) {
        W = 2.0 * W
        hashMapTrain.clear()
        fillMapTrain(training, hashMapTrain, W, rSeq)
        increaseW = updateW(hashMapTrain, k, increaseW)

      }
      W = 0.5 * W
    } else {
      while (!increaseW) {
        W = 0.5 * W
        hashMapTrain.clear()
        fillMapTrain(training, hashMapTrain, W, rSeq)
        increaseW = updateW(hashMapTrain, k, increaseW)
      }
      W = 2.0 * W
    }

    // Now W optimally satisfies (1)
    increaseW = false
    // test for (2)
    for (v <- testing.map(x => x._2)) {
      var candidatePoints = new ArrayBuffer[FlinkVector]
      val hash = lshHash(v, rSeq, b, W)
      if (hashMapTrain.contains(hash)) {
        numColl += hashMapTrain(hash).size
        candidatePoints ++= hashMapTrain(hash)
      }
      increaseW = candidatePoints.length < k
    }

    // optimally increase W so that (2) is satisfied
    if (increaseW) {
      while (increaseW) {
        W = 2.0 * W
        hashMapTrain.clear()
        fillMapTrain(training, hashMapTrain, W, rSeq)

        for (v <- testing.map(x => x._2)) {
          var candidatePoints = new ArrayBuffer[FlinkVector]
          val hash = lshHash(v, rSeq, b, W)
          if (hashMapTrain.contains(hash)) {
            numColl += hashMapTrain(hash).size
            candidatePoints ++= hashMapTrain(hash)
          }
          increaseW = candidatePoints.length < k
        }
      }
      W = 0.5 * W
    }

    // Now W satisfies both (1) and (2)
    hashMapTrain.clear()
    fillMapTrain(training, hashMapTrain, W, rSeq)

    for ((id, v) <- testing) {
      var candidatePoints = new ArrayBuffer[FlinkVector]

      val hash = lshHash(v, rSeq, b, W)
      if (hashMapTrain.contains(hash)) {
        numColl += hashMapTrain(hash).size
        candidatePoints ++= hashMapTrain(hash)
      }
      knnQueryBasic(candidatePoints.toVector, Vector((id, v)), k, metric, out)

    }
  }

  /**
    *
    * @param training training set
    * @param hashMapTrain hashed training set
    * @param W parameter for LSH functions
    * @param rSeq sequence of random points to project onto
    * @tparam T FlinkVector
    */
  def fillMapTrain[T <: FlinkVector](training: Vector[T], hashMapTrain:
  HashMap[ArrayBuffer[Int],
    scala.collection.mutable.Set[FlinkVector]], W: Double, rSeq: ArrayBuffer[T]) {
    for (v <- training) {
      val hash = lshHash(v, rSeq, b, W)
      if (hashMapTrain.contains(hash)) {
        hashMapTrain(hash) += v
      } else {
        hashMapTrain(hash) = scala.collection.mutable.Set(v)
      }
    }
  }

  /**
    *
    * @param hashMapTrain hashed training set
    * @param k number of neighbors for knn search
    * @param increaseW boolean to determine whether to increas W during iteration
    * @return an update of increaseW
    */
  def updateW(hashMapTrain: HashMap[ArrayBuffer[Int],
    scala.collection.mutable.Set[FlinkVector]], k: Int, increaseW: Boolean): Boolean = {
    var res = increaseW
    for (key <- hashMapTrain.keys) {
      if (hashMapTrain(key).size < k) {
        res = true
      }
    }
    res
  }

  /**
    *
    * @param in A point, typically a single test query point
    * @param randomDirections directions to project onto for LSH hashing
    * @param bParam shifting parameter for LSH
    * @param wParam determines the number of distinct buckets, how far away points go
    * @tparam T FlinkVector
    * @return Array of buckets
    */
  def lshHash[T <: FlinkVector](in: T, randomDirections: ArrayBuffer[T],
                                bParam: Double, wParam: Double):
  ArrayBuffer[Int] = {
    randomDirections.map { r =>
      ((in.dot(r) + bParam) / wParam).floor.toInt
    }
  }

}
