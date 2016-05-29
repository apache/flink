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

import org.apache.flink.ml.math.{Vector => FlinkVector, DenseVector, Breeze}
import org.apache.flink.ml.metrics.distances.DistanceMetric
import org.apache.flink.util.Collector

import scala.collection.immutable.Vector
import scala.collection.mutable.Set

import Breeze._

/**
  * z-value based approach for doing an approximate knn query
  *
  * This method hashes the points to one-dimensional space using
  * the z-value hashing function to do a quick knn search
  *
  * Since the z-value involves formin a binary string out of
  * the coordinate values, this only applies to dimension ~ 30
  *
  * For dimensions larger than 30, LSH is used
  *
  * Relevant papers include:
  * MapReduce z-value based knn:
  * https://www.cs.utah.edu/~lifeifei/papers/mrknnj.pdf
  *
  * non-distributed z-value based knn:
  * http://cs.sjtu.edu.cn/~yaobin/papers/icde10_knn.pdf
  *
  */

class zKNN extends basicKNN {

  val alpha = 2
  // number of times we randomly shift points
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
  def zknnQuery[T <: FlinkVector](
                                   training: Vector[T],
                                   testing: Vector[(Long, T)],
                                   k: Int, metric: DistanceMetric,
                                   out: Collector[(FlinkVector, FlinkVector, Long, Double)]) {

    // bitMult is used to maximize the number of
    // bits in the binary string when computing z-values
    val dim = training.head.size
    val bitMult = 30 / (dim + 1)

    val gamma = k

    // normalize test and training sets, zip training set.
    val testTrainMinMax = getNormalizingParameters(training.union(testing.map(test => test._2)))
    val trainingNorm = training.zipWithIndex.map { train =>
      (train._2, normalizePoint(train._1, testTrainMinMax._1, testTrainMinMax._2))
    }

    //rSeq is a collection of alpha many random vectors
    val rSeq = Seq.fill(alpha)(DenseVector(Seq.fill(training.head.size)
    (r.nextDouble).toArray).asBreeze)

    // map each random vector rVec to the training point set shifted by rVec along with
    // the shifted training point's z-value.
    val zTrainShiftSort = rSeq.map { rVec =>
      trainingNorm.map { trainPoint => (trainPoint._1, trainPoint._2,
        zValue((Math.pow(2, bitMult) * (trainPoint._2.asBreeze + rVec)).fromBreeze))
      }.sortBy(x => x._3).toArray //Array for O(1) access when slicing below
    }

    for ((id, v) <- testing) {
      var candidates: Set[(Int, FlinkVector)] = Set()
      val vNormalized = normalizePoint(v, testTrainMinMax._1, testTrainMinMax._2)

      for (i <- 0 until alpha) {
        val zQueryShifted = zValue((Math.pow(2, bitMult) *
          (vNormalized.asBreeze + rSeq(i))).fromBreeze)

        // get 2*gamma points about query point q, gamma points above and below based on z value
        // if there aren't gamma points above, still grab 2*gamma points
        if (zQueryShifted < zTrainShiftSort(i).head._3) {
          candidates ++= zTrainShiftSort(i).slice(0, 2 * gamma).map(x => (x._1, x._2))
        } else if (zQueryShifted > zTrainShiftSort(i)(zTrainShiftSort(i).length - 1)._3) {
          candidates ++= zTrainShiftSort(i).slice(
            zTrainShiftSort(i).length - 1 - 2 * gamma,
            zTrainShiftSort(i).length).map(x => (x._1, x._2))
        } else {
          // main case
          // do a binary search to get the index and grab 2*gamma nearby points
          val ind = getIndexSortedArr(zTrainShiftSort(i).map(x => (x._2, x._3)),
            (vNormalized, zQueryShifted))
          val posLen = zTrainShiftSort(i).length - ind
          val negLen = ind

          // grab 2*gamma points about index, consider edge cases when not
          // gamma points left/right of index
          if (posLen >= gamma && negLen >= gamma) {
            candidates ++= zTrainShiftSort(i).slice(ind - gamma, ind + gamma).map(
              x => (x._1, x._2))
          } else if (posLen < gamma && posLen + negLen >= 2 * gamma) {
            candidates ++= zTrainShiftSort(i).slice(ind - 2 * gamma - posLen,
              ind + posLen).map(x => (x._1, x._2))
          } else if (negLen < gamma && posLen + negLen >= 2 * gamma) {
            candidates ++= zTrainShiftSort(i).slice(ind - negLen,
              ind + 2 * gamma - negLen).map(x => (x._1, x._2))
          } else {
            throw new IllegalArgumentException(s" Error: gamma is too large!")
          }
        }
      }

      // grab original training set points and do a basic knn query
      val candidatesDenorm: Vector[FlinkVector] = candidates.map { x => training(x._1) }.toVector
      knnQueryBasic(candidatesDenorm, Vector((id, v)), k, metric, out)
    }

  }

  /** Gets parameters for later use in normalizePoint
    *
    * @param vec a collection of points (FlinkVectors)
    * @tparam T FlinkVector
    * @return outputs the min and max values in each coordinate direction
    */
  def getNormalizingParameters[T <: FlinkVector](vec: Vector[T]):
  (FlinkVector, FlinkVector) = {

    val tabArr = Array.tabulate(vec.head.size)(x => x)

    val minVec = tabArr.map(i => vec.map(x => x(i)).min)
    val maxVec = tabArr.map(i => vec.map(x => x(i)).max)

    (DenseVector(minVec), DenseVector(maxVec))
  }

  /** Normalizes points to unit cube.  This is done to remove negative numbers
    * when computing the z-value and to have more control over the size of the
    * bit string when computing z-values
    *
    * @param p Point to be normalized
    * @param minArr min values of the that collection
    * @param maxArr max values of that collection
    * @tparam T FlinkVector
    * @return normalized version of vec where all points lie in the unit cube
    */
  def normalizePoint[T <: FlinkVector](p: T, minArr: T, maxArr: T):
  FlinkVector = {

    val dim = p.size
    val tabArr = Array.tabulate(dim)(x => x)
    DenseVector(tabArr.map(
      i => (p(i) - minArr(i)) / (maxArr(i) - minArr(i))))
  }

  /** Given a sorted set, does a binary search to find the index for which a point
    * not in a sorted set  -- in this case a set of z-values -- is closest to
    *
    * @param arr sorted array of points and their z-values
    * @param P a point and it's z-value not in the array
    * @tparam T FlinkVector
    * @return Gives the index in arr for which P is closest to
    */
  def getIndexSortedArr[T <: FlinkVector](arr: Array[(T, Int)],
                                          P: (T, Int)): Int = {
    def getIndexHelper(arr: Array[(T, Int)],
                       P: ((T, Int)), low: Int, high: Int): Int = {
      val i = (high + low) / 2
      if (P._2 >= arr(i)._2 && P._2 <= arr(i + 1)._2) {
        i
      } else if (P._2 < arr(i)._2) {
        getIndexHelper(arr, P, low, i)
      } else {
        getIndexHelper(arr, P, i, high)
      }
    }
    getIndexHelper(arr, P, 0, arr.length - 1)
  }

  /** Computes the z-value of a point.  This is done by interleaving the
    * bits of each coordinate.  For example the binary representation of
    * (2,6) is (010, 110) and the z-value is
    * 011100 = 28
    *
    * @param in a point (i.e. FlinkVector)
    * @tparam T FlinkVector
    * @return the z-value of that point
    */
  def zValue[T <: FlinkVector](in: T): Int = {
    Integer.parseInt(interleave(in.asBreeze.map(x => x.toInt.toBinaryString)), 2)
  }

  /** Helper function for zValue that does the actual interleaving
    * of the bit strings of each coordinate
    *
    * @param in a point (FlinkVector) represented as a binary string
    * @return the binary string needed to compute the z-value
    */
  def interleave(in: breeze.linalg.Vector[String]): String = {
    // get max length
    val maxLen = in.map(str => str.length).max
    val L = in.length
    var res = ""

    for (i <- 0 until maxLen) {
      for (j <- 0 until L) {
        if (i >= in(L - 1 - j).length) {
          res = 0 + res
        } else {
          res = in(L - 1 - j)(in(L - 1 - j).length - 1 - i) + res
        }
      }
    }
    res
  }

}
