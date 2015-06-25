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

package org.apache.flink.ml.feature.extraction

import org.apache.flink.api.scala._
import org.apache.flink.ml.math.SparseVector
import org.apache.flink.test.util.FlinkTestBase
import org.scalatest.{FlatSpec, Matchers}

class FeatureHasherSuite
    extends FlatSpec
    with Matchers
    with FlinkTestBase {

  behavior of "Flink's Feature Hasher"

  import FeatureHasherData._

  it should "transform a sequence of strings into a sparse feature vector of given size" in {
    val env = ExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(2)

    for (numFeatures <- numFeaturesTest) {
      val inputDS = env.fromCollection(input)

      val transformer = FeatureHasher()
          .setNumFeatures(numFeatures)

      val results = transformer.transform(inputDS).collect

      for ((result, expectedResult) <- results zip expectedResults(numFeatures)) {
        result.equalsVector(expectedResult) should be(true)
      }
    }
  }

  it should "transform a sequence of strings into a sparse feature vector of given size," +
      "with non negative entries" in {
    val env = ExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(2)

    for (numFeatures <- numFeaturesTest) {
      val inputDS = env.fromCollection(input)

      val transformer = FeatureHasher()
          .setNumFeatures(numFeatures).setNonNegative(true)

      val transformedDS = transformer.transform(inputDS)
      val results = transformedDS.collect()

      for ((result, expectedResult) <- results zip expectedResultsNonNegative(numFeatures)) {
        result.equalsVector(expectedResult) should be(true)
      }
    }
  }

  it should "fail when the parameter numFeatures is set to a value less than 1" in {
    intercept[IllegalArgumentException] {
      val transformer = FeatureHasher()
          .setNumFeatures(0).setNonNegative(true)
    }

    intercept[IllegalArgumentException] {
      val transformer = FeatureHasher()
          .setNumFeatures(0).setNonNegative(false)
    }
  }
}

object FeatureHasherData {

  val input = Seq(
    "Two households both alike in dignity".split(" ").toSeq,
    "In fair Verona where we lay our scene".split(" ").toSeq,
    "From ancient grudge break to new mutiny".split(" ").toSeq,
    "Where civil blood makes civil hands unclean".split(" ").toSeq,
    "From forth the fatal loins of these two foes".split(" ").toSeq
  )

  /* 2^30 features can't be tested right now because the implementation of Vector.equalsVector
  performs an index wise comparison on the two vectors, which takes approx. forever */
  val numFeaturesTest = Seq(Math.pow(2, 4).toInt, Math.pow(2, 5).toInt, 1234,
    Math.pow(2, 16).toInt, Math.pow(2, 20).toInt) //, Math.pow(2, 30).toInt)

  val expectedResults = List(
    16 -> List(
      SparseVector.fromCOO(16, Map(0 -> 1.0, 1 -> 1.0, 2 -> -1.0, 14 -> -1.0)),
      SparseVector.fromCOO(16, Map(1 -> -1.0, 2 -> -1.0, 5 -> 1.0, 10 -> 1.0, 12 -> -1.0, 13 -> 1.0,
        14 -> -1.0, 15 -> 1.0)),
      SparseVector.fromCOO(16, Map(7 -> 2.0, 8 -> 1.0, 9 -> -1.0, 11 -> 1.0, 13 -> -1.0,
        15 -> -1.0)),
      SparseVector.fromCOO(16, Map(2 -> -3.0, 7 -> -1.0, 8 -> -1.0, 10 -> 1.0, 14 -> -1.0)),
      SparseVector.fromCOO(16, Map(0 -> 1.0, 2 -> 1.0, 6 -> 1.0, 8 -> 1.0, 9 -> -1.0, 12 -> 1.0,
        14 -> -2.0, 15 -> 1.0))),
    32 -> List(
      SparseVector.fromCOO(32, Map(0 -> 1.0, 14 -> -1.0, 17 -> 1.0, 18 -> -1.0)),
      SparseVector.fromCOO(32, Map(1 -> -1.0, 10 -> 1.0, 12 -> -1.0, 15 -> 1.0, 18 -> -1.0,
        21 -> 1.0, 29 -> 1.0, 30 -> -1.0)),
      SparseVector.fromCOO(32, Map(7 -> 2.0, 8 -> 1.0, 9 -> -1.0, 13 -> -1.0, 15 -> -1.0,
        27 -> 1.0)),
      SparseVector.fromCOO(32, Map(2 -> -3.0, 7 -> -1.0, 10 -> 1.0, 14 -> -1.0, 24 -> -1.0)),
      SparseVector.fromCOO(32, Map(0 -> 1.0, 2 -> 1.0, 8 -> 1.0, 12 -> 1.0, 15 -> 1.0, 22 -> 1.0,
        25 -> -1.0, 30 -> -2.0))),
    1234 -> List(
      SparseVector.fromCOO(1234, Map(287 -> -1.0, 344 -> -1.0, 594 -> 1.0, 637 -> 1.0, 864 -> -1.0,
        897 -> 1.0)),
      SparseVector.fromCOO(1234, Map(40 -> -1.0, 114 -> 1.0, 334 -> -1.0, 351 -> 1.0, 479 -> -1.0,
        647 -> 1.0, 789 -> 1.0, 860 -> -1.0)),
      SparseVector.fromCOO(1234, Map(89 -> 1.0, 165 -> -1.0, 291 -> -1.0, 739 -> 1.0, 851 -> -1.0,
        1078 -> 1.0, 1231 -> 1.0)),
      SparseVector.fromCOO(1234, Map(274 -> -1.0, 450 -> 1.0, 781 -> -1.0, 1008 -> -2.0,
        1160 -> -1.0, 1206 -> -1.0)),
      SparseVector.fromCOO(1234, Map(354 -> 1.0, 368 -> 1.0, 696 -> 1.0, 857 -> -1.0, 874 -> -1.0,
        1061 -> 1.0, 1078 -> 1.0, 1126 -> -1.0, 1224 -> 1.0))),
    65536 -> List(
      SparseVector.fromCOO(65536, Map(24645 -> 1.0, 29029 -> -1.0, 42257 -> 1.0, 46560 -> 1.0,
        60782 -> -1.0, 65042 -> -1.0)),
      SparseVector.fromCOO(65536, Map(6301 -> 1.0, 13546 -> 1.0, 29426 -> -1.0, 37793 -> -1.0,
        39471 -> 1.0, 51454 -> -1.0, 53077 -> 1.0, 65004 -> -1.0)),
      SparseVector.fromCOO(65536, Map(12712 -> 1.0, 12775 -> 1.0, 15501 -> -1.0, 25991 -> 1.0,
        34607 -> -1.0, 43099 -> 1.0, 56905 -> -1.0)),
      SparseVector.fromCOO(65536, Map(3576 -> -1.0, 6882 -> -1.0, 7338 -> 1.0, 39682 -> -2.0,
        59911 -> -1.0, 63086 -> -1.0)),
      SparseVector.fromCOO(65536, Map(12712 -> 1.0, 20662 -> 1.0, 20780 -> 1.0, 24734 -> -1.0,
        25154 -> 1.0, 27375 -> 1.0, 38528 -> 1.0, 39897 -> -1.0, 62142 -> -1.0))),
    1048576 -> List(
      SparseVector.fromCOO(1048576, Map(701920 -> 1.0, 745541 -> 1.0, 781678 -> -1.0,
        785938 -> -1.0, 828689 -> 1.0, 880997 -> -1.0)),
      SparseVector.fromCOO(1048576, Map(29426 -> -1.0, 53077 -> 1.0, 170543 -> 1.0, 248062 -> -1.0,
        299937 -> -1.0, 654828 -> -1.0, 858269 -> 1.0, 865514 -> 1.0)),
      SparseVector.fromCOO(1048576, Map(174171 -> 1.0, 450121 -> -1.0, 471464 -> 1.0,
        474253 -> -1.0, 537063 -> 1.0, 681351 -> 1.0, 689967 -> -1.0)),
      SparseVector.fromCOO(1048576, Map(72418 -> -1.0, 390766 -> -1.0, 518663 -> -1.0,
        724472 -> -1.0, 760578 -> -2.0, 859306 -> 1.0)),
      SparseVector.fromCOO(1048576, Map(286878 -> -1.0, 471464 -> 1.0, 479532 -> 1.0,
        520894 -> -1.0, 610486 -> 1.0, 811586 -> 1.0, 824960 -> 1.0, 879343 -> 1.0,
        1022937 -> -1.0))),
    1073741824 -> List(
      SparseVector.fromCOO(1073741824, Map(131900689 -> 1.0, 161213806 -> -1.0, 476839442 -> -1.0,
        613070304 -> 1.0, 650862661 -> 1.0, 706572645 -> -1.0)),
      SparseVector.fromCOO(1073741824, Map(286431791 -> 1.0, 386530796 -> -1.0, 420508402 -> -1.0,
        610571169 -> -1.0, 792533149 -> 1.0, 809317610 -> 1.0, 824233813 -> 1.0,
        897829118 -> -1.0)),
      SparseVector.fromCOO(1073741824, Map(152217691 -> 1.0, 197669351 -> 1.0, 404176013 -> -1.0,
        422208903 -> 1.0, 445067688 -> 1.0, 883351113 -> -1.0, 1023051567 -> -1.0)),
      SparseVector.fromCOO(1073741824, Map(104668330 -> 1.0, 142678754 -> -1.0, 329643630 -> -1.0,
        472583672 -> -1.0, 694675975 -> -1.0, 1037802242 -> -2.0)),
      SparseVector.fromCOO(1073741824, Map(59007134 -> -1.0, 182014134 -> 1.0, 189555266 -> 1.0,
        225923372 -> 1.0, 358389376 -> 1.0, 389542590 -> -1.0, 445067688 -> 1.0, 834497263 -> 1.0,
        920624089 -> -1.0)))
  ) toMap

  val expectedResultsNonNegative = List(
    16 -> List(
      SparseVector.fromCOO(16, Map(0 -> 1.0, 1 -> 1.0, 2 -> 1.0, 14 -> 1.0)),
      SparseVector.fromCOO(16, Map(1 -> 1.0, 2 -> 1.0, 5 -> 1.0, 10 -> 1.0, 12 -> 1.0, 13 -> 1.0,
        14 -> 1.0, 15 -> 1.0)),
      SparseVector.fromCOO(16, Map(7 -> 2.0, 8 -> 1.0, 9 -> 1.0, 11 -> 1.0, 13 -> 1.0, 15 -> 1.0)),
      SparseVector.fromCOO(16, Map(2 -> 3.0, 7 -> 1.0, 8 -> 1.0, 10 -> 1.0, 14 -> 1.0)),
      SparseVector.fromCOO(16, Map(0 -> 1.0, 2 -> 1.0, 6 -> 1.0, 8 -> 1.0, 9 -> 1.0, 12 -> 1.0,
        14 -> 2.0, 15 -> 1.0))),
    32 -> List(
      SparseVector.fromCOO(32, Map(0 -> 1.0, 14 -> 1.0, 17 -> 1.0, 18 -> 1.0)),
      SparseVector.fromCOO(32, Map(1 -> 1.0, 10 -> 1.0, 12 -> 1.0, 15 -> 1.0, 18 -> 1.0, 21 -> 1.0,
        29 -> 1.0, 30 -> 1.0)),
      SparseVector.fromCOO(32, Map(7 -> 2.0, 8 -> 1.0, 9 -> 1.0, 13 -> 1.0, 15 -> 1.0, 27 -> 1.0)),
      SparseVector.fromCOO(32, Map(2 -> 3.0, 7 -> 1.0, 10 -> 1.0, 14 -> 1.0, 24 -> 1.0)),
      SparseVector.fromCOO(32, Map(0 -> 1.0, 2 -> 1.0, 8 -> 1.0, 12 -> 1.0, 15 -> 1.0, 22 -> 1.0,
        25 -> 1.0, 30 -> 2.0))),
    1234 -> List(
      SparseVector.fromCOO(1234, Map(287 -> 1.0, 344 -> 1.0, 594 -> 1.0, 637 -> 1.0, 864 -> 1.0,
        897 -> 1.0)),
      SparseVector.fromCOO(1234, Map(40 -> 1.0, 114 -> 1.0, 334 -> 1.0, 351 -> 1.0, 479 -> 1.0,
        647 -> 1.0, 789 -> 1.0, 860 -> 1.0)),
      SparseVector.fromCOO(1234, Map(89 -> 1.0, 165 -> 1.0, 291 -> 1.0, 739 -> 1.0, 851 -> 1.0,
        1078 -> 1.0, 1231 -> 1.0)),
      SparseVector.fromCOO(1234, Map(274 -> 1.0, 450 -> 1.0, 781 -> 1.0, 1008 -> 2.0, 1160 -> 1.0,
        1206 -> 1.0)),
      SparseVector.fromCOO(1234, Map(354 -> 1.0, 368 -> 1.0, 696 -> 1.0, 857 -> 1.0, 874 -> 1.0,
        1061 -> 1.0, 1078 -> 1.0, 1126 -> 1.0, 1224 -> 1.0))),
    65536 -> List(
      SparseVector.fromCOO(65536, Map(24645 -> 1.0, 29029 -> 1.0, 42257 -> 1.0, 46560 -> 1.0,
        60782 -> 1.0, 65042 -> 1.0)),
      SparseVector.fromCOO(65536, Map(6301 -> 1.0, 13546 -> 1.0, 29426 -> 1.0, 37793 -> 1.0,
        39471 -> 1.0, 51454 -> 1.0, 53077 -> 1.0, 65004 -> 1.0)),
      SparseVector.fromCOO(65536, Map(12712 -> 1.0, 12775 -> 1.0, 15501 -> 1.0, 25991 -> 1.0,
        34607 -> 1.0, 43099 -> 1.0, 56905 -> 1.0)),
      SparseVector.fromCOO(65536, Map(3576 -> 1.0, 6882 -> 1.0, 7338 -> 1.0, 39682 -> 2.0,
        59911 -> 1.0, 63086 -> 1.0)),
      SparseVector.fromCOO(65536, Map(12712 -> 1.0, 20662 -> 1.0, 20780 -> 1.0, 24734 -> 1.0,
        25154 -> 1.0, 27375 -> 1.0, 38528 -> 1.0, 39897 -> 1.0, 62142 -> 1.0))),
    1048576 -> List(
      SparseVector.fromCOO(1048576, Map(701920 -> 1.0, 745541 -> 1.0, 781678 -> 1.0, 785938 -> 1.0,
        828689 -> 1.0, 880997 -> 1.0)),
      SparseVector.fromCOO(1048576, Map(29426 -> 1.0, 53077 -> 1.0, 170543 -> 1.0, 248062 -> 1.0,
        299937 -> 1.0, 654828 -> 1.0, 858269 -> 1.0, 865514 -> 1.0)),
      SparseVector.fromCOO(1048576, Map(174171 -> 1.0, 450121 -> 1.0, 471464 -> 1.0, 474253 -> 1.0,
        537063 -> 1.0, 681351 -> 1.0, 689967 -> 1.0)),
      SparseVector.fromCOO(1048576, Map(72418 -> 1.0, 390766 -> 1.0, 518663 -> 1.0, 724472 -> 1.0,
        760578 -> 2.0, 859306 -> 1.0)),
      SparseVector.fromCOO(1048576, Map(286878 -> 1.0, 471464 -> 1.0, 479532 -> 1.0, 520894 -> 1.0,
        610486 -> 1.0, 811586 -> 1.0, 824960 -> 1.0, 879343 -> 1.0, 1022937 -> 1.0))),
    1073741824 -> List(
      SparseVector.fromCOO(1073741824, Map(131900689 -> 1.0, 161213806 -> 1.0, 476839442 -> 1.0,
        613070304 -> 1.0, 650862661 -> 1.0, 706572645 -> 1.0)),
      SparseVector.fromCOO(1073741824, Map(286431791 -> 1.0, 386530796 -> 1.0, 420508402 -> 1.0,
        610571169 -> 1.0, 792533149 -> 1.0, 809317610 -> 1.0, 824233813 -> 1.0, 897829118 -> 1.0)),
      SparseVector.fromCOO(1073741824, Map(152217691 -> 1.0, 197669351 -> 1.0, 404176013 -> 1.0,
        422208903 -> 1.0, 445067688 -> 1.0, 883351113 -> 1.0, 1023051567 -> 1.0)),
      SparseVector.fromCOO(1073741824, Map(104668330 -> 1.0, 142678754 -> 1.0, 329643630 -> 1.0,
        472583672 -> 1.0, 694675975 -> 1.0, 1037802242 -> 2.0)),
      SparseVector.fromCOO(1073741824, Map(59007134 -> 1.0, 182014134 -> 1.0, 189555266 -> 1.0,
        225923372 -> 1.0, 358389376 -> 1.0, 389542590 -> 1.0, 445067688 -> 1.0, 834497263 -> 1.0,
        920624089 -> 1.0)))
  ) toMap
}
