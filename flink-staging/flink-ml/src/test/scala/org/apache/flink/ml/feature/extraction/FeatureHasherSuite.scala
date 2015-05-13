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

import org.apache.flink.api.scala.{ExecutionEnvironment, _}
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

      val transformer = FeatureHasher[String]()
          .setNumFeatures(numFeatures)

      val transformedDS = transformer.transform(inputDS)
      val results = transformedDS.collect()

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

      val transformer = FeatureHasher[String]()
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
      val transformer = FeatureHasher[String]()
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
      SparseVector.fromCOO(16, Map(0 -> 1.0, 2 -> -1.0, 7 -> 0.0, 8 -> 1.0, 10 -> 1.0)),
      SparseVector.fromCOO(16, Map(1 -> -1.0, 2 -> 0.0, 3 -> 1.0, 4 -> 2.0, 11 -> -1.0,
        15 -> -1.0)),
      SparseVector.fromCOO(16, Map(1 -> 2.0, 4 -> 1.0, 8 -> 2.0, 12 -> -1.0, 15 -> 1.0)),
      SparseVector.fromCOO(16, Map(1 -> 1.0, 2 -> -2.0, 4 -> -1.0, 11 -> -1.0, 13 -> -2.0)),
      SparseVector.fromCOO(16, Map(0 -> 1.0, 1 -> 0.0, 2 -> 1.0, 4 -> -1.0, 14 -> -2.0,
        15 -> 0.0))),
    32 -> List(
      SparseVector.fromCOO(32, Map(0 -> 1.0, 18 -> -1.0, 23 -> 0.0, 24 -> 1.0, 26 -> 1.0)),
      SparseVector.fromCOO(32, Map(2 -> -1.0, 11 -> -1.0, 17 -> -1.0, 18 -> 1.0, 19 -> 1.0, 20 ->
          2.0, 31 -> -1.0)),
      SparseVector.fromCOO(32, Map(1 -> 1.0, 4 -> 1.0, 8 -> 2.0, 12 -> -1.0, 15 -> 1.0, 17 -> 1.0)),
      SparseVector.fromCOO(32, Map(1 -> 1.0, 2 -> -1.0, 11 -> -1.0, 18 -> -1.0, 20 -> -1.0, 29 ->
          -2.0)),
      SparseVector.fromCOO(32, Map(0 -> 1.0, 1 -> 1.0, 2 -> 1.0, 4 -> -1.0, 17 -> -1.0,
        30 -> -2.0, 31 -> 0.0))),
    1234 -> List(
      SparseVector.fromCOO(1234, Map(86 -> -1.0, 154 -> 1.0, 575 -> -1.0, 666 -> 1.0, 900 -> 1.0,
        1229 -> 1.0)),
      SparseVector.fromCOO(1234, Map(47 -> -1.0, 132 -> 2.0, 214 -> 1.0, 323 -> -1.0,
        1039 -> 0.0, 1192 -> -1.0)),
      SparseVector.fromCOO(1234, Map(387 -> 1.0, 414 -> 1.0, 457 -> 1.0, 536 -> 1.0, 809 -> 1.0,
        894 -> -1.0, 1020 -> 1.0)),
      SparseVector.fromCOO(1234, Map(372 -> -1.0, 549 -> -1.0, 829 -> -2.0, 848 -> -1.0, 866 ->
          -1.0, 949 -> 1.0)),
      SparseVector.fromCOO(1234, Map(80 -> -1.0, 388 -> 1.0, 541 -> -1.0, 592 -> -1.0,
        809 -> 1.0, 916 -> 1.0, 1190 -> -1.0, 1211 -> -1.0, 1229 -> 1.0))),
    65536 -> List(
      SparseVector.fromCOO(65536, Map(17938 -> -1.0, 19160 -> 1.0, 26330 -> 1.0, 32544 -> 1.0,
        40055 -> -1.0, 44023 -> 1.0)),
      SparseVector.fromCOO(65536, Map(15362 -> -1.0, 21483 -> -1.0, 40338 -> 1.0, 44241 -> -1.0,
        46239 -> -1.0, 46900 -> 1.0, 52403 -> 1.0, 52980 -> 1.0)),
      SparseVector.fromCOO(65536, Map(34447 -> 1.0, 36513 -> 1.0, 51364 -> 1.0, 53192 -> 1.0,
        56748 -> -1.0, 57640 -> 1.0, 58001 -> 1.0)),
      SparseVector.fromCOO(65536, Map(11467 -> -1.0, 17394 -> -1.0, 18484 -> -1.0, 47906 -> -1.0,
        61309 -> -2.0, 63041 -> 1.0)),
      SparseVector.fromCOO(65536, Map(2848 -> 1.0, 9407 -> -1.0, 11441 -> -1.0, 15652 -> -1.0,
        17438 -> -1.0, 18786 -> 1.0, 32575 -> 1.0, 36513 -> 1.0, 63230 -> -1.0))),
    1048576 -> List(
      SparseVector.fromCOO(1048576, Map(98080 -> 1.0, 629879 -> -1.0, 895991 -> 1.0,
        943834 -> 1.0, 1000978 -> -1.0, 1002200 -> 1.0)),
      SparseVector.fromCOO(1048576, Map(15362 -> -1.0, 283627 -> -1.0, 373919 -> -1.0, 765137 ->
          -1.0, 838835 -> 1.0, 898868 -> 1.0, 904948 -> 1.0, 957842 -> 1.0)),
      SparseVector.fromCOO(1048576, Map(122284 -> -1.0, 385681 -> 1.0, 575652 -> 1.0,
        757409 -> 1.0, 774088 -> 1.0, 909608 -> 1.0, 951951 -> 1.0)),
      SparseVector.fromCOO(1048576, Map(149556 -> -1.0, 244514 -> -1.0, 521793 -> 1.0, 651133 ->
          -2.0, 994507 -> -1.0, 1000434 -> -1.0)),
      SparseVector.fromCOO(1048576, Map(84322 -> 1.0, 212260 -> -1.0, 402623 -> -1.0, 535729 ->
          -1.0, 653054 -> -1.0, 757409 -> 1.0, 985888 -> 1.0, 1000478 -> -1.0, 1015615 -> 1.0)))
  ) toMap

  val expectedResultsNonNegative = List(
    16 -> List(
      SparseVector.fromCOO(16, Map(0 -> 1.0, 2 -> 1.0, 7 -> 0.0, 8 -> 1.0, 10 -> 1.0)),
      SparseVector.fromCOO(16, Map(1 -> 1.0, 2 -> 0.0, 3 -> 1.0, 4 -> 2.0, 11 -> 1.0, 15 -> 1.0)),
      SparseVector.fromCOO(16, Map(1 -> 2.0, 4 -> 1.0, 8 -> 2.0, 12 -> 1.0, 15 -> 1.0)),
      SparseVector.fromCOO(16, Map(1 -> 1.0, 2 -> 2.0, 4 -> 1.0, 11 -> 1.0, 13 -> 2.0)),
      SparseVector.fromCOO(16, Map(0 -> 1.0, 1 -> 0.0, 2 -> 1.0, 4 -> 1.0, 14 -> 2.0, 15 -> 0.0))),
    32 -> List(
      SparseVector.fromCOO(32, Map(0 -> 1.0, 18 -> 1.0, 23 -> 0.0, 24 -> 1.0, 26 -> 1.0)),
      SparseVector.fromCOO(32, Map(2 -> 1.0, 11 -> 1.0, 17 -> 1.0, 18 -> 1.0, 19 -> 1.0,
        20 -> 2.0, 31 -> 1.0)),
      SparseVector.fromCOO(32, Map(1 -> 1.0, 4 -> 1.0, 8 -> 2.0, 12 -> 1.0, 15 -> 1.0, 17 -> 1.0)),
      SparseVector.fromCOO(32, Map(1 -> 1.0, 2 -> 1.0, 11 -> 1.0, 18 -> 1.0, 20 -> 1.0, 29 -> 2.0)),
      SparseVector.fromCOO(32, Map(0 -> 1.0, 1 -> 1.0, 2 -> 1.0, 4 -> 1.0, 17 -> 1.0, 30 -> 2.0,
        31 -> 0.0))),
    1234 -> List(
      SparseVector.fromCOO(1234, Map(86 -> 1.0, 154 -> 1.0, 575 -> 1.0, 666 -> 1.0, 900 -> 1.0,
        1229 -> 1.0)),
      SparseVector.fromCOO(1234, Map(47 -> 1.0, 132 -> 2.0, 214 -> 1.0, 323 -> 1.0, 1039 -> 0.0,
        1192 -> 1.0)),
      SparseVector.fromCOO(1234, Map(387 -> 1.0, 414 -> 1.0, 457 -> 1.0, 536 -> 1.0, 809 -> 1.0,
        894 -> 1.0, 1020 -> 1.0)),
      SparseVector.fromCOO(1234, Map(372 -> 1.0, 549 -> 1.0, 829 -> 2.0, 848 -> 1.0, 866 -> 1.0,
        949 -> 1.0)),
      SparseVector.fromCOO(1234, Map(80 -> 1.0, 388 -> 1.0, 541 -> 1.0, 592 -> 1.0, 809 -> 1.0,
        916 -> 1.0, 1190 -> 1.0, 1211 -> 1.0, 1229 -> 1.0))),
    65536 -> List(
      SparseVector.fromCOO(65536, Map(17938 -> 1.0, 19160 -> 1.0, 26330 -> 1.0, 32544 -> 1.0,
        40055 -> 1.0, 44023 -> 1.0)),
      SparseVector.fromCOO(65536, Map(15362 -> 1.0, 21483 -> 1.0, 40338 -> 1.0, 44241 -> 1.0,
        46239 -> 1.0, 46900 -> 1.0, 52403 -> 1.0, 52980 -> 1.0)),
      SparseVector.fromCOO(65536, Map(34447 -> 1.0, 36513 -> 1.0, 51364 -> 1.0, 53192 -> 1.0,
        56748 -> 1.0, 57640 -> 1.0, 58001 -> 1.0)),
      SparseVector.fromCOO(65536, Map(11467 -> 1.0, 17394 -> 1.0, 18484 -> 1.0, 47906 -> 1.0,
        61309 -> 2.0, 63041 -> 1.0)),
      SparseVector.fromCOO(65536, Map(2848 -> 1.0, 9407 -> 1.0, 11441 -> 1.0, 15652 -> 1.0, 17438
          -> 1.0, 18786 -> 1.0, 32575 -> 1.0, 36513 -> 1.0, 63230 -> 1.0))),
    1048576 -> List(
      SparseVector.fromCOO(1048576, Map(98080 -> 1.0, 629879 -> 1.0, 895991 -> 1.0,
        943834 -> 1.0, 1000978 -> 1.0, 1002200 -> 1.0)),
      SparseVector.fromCOO(1048576, Map(15362 -> 1.0, 283627 -> 1.0, 373919 -> 1.0,
        765137 -> 1.0, 838835 -> 1.0, 898868 -> 1.0, 904948 -> 1.0, 957842 -> 1.0)),
      SparseVector.fromCOO(1048576, Map(122284 -> 1.0, 385681 -> 1.0, 575652 -> 1.0,
        757409 -> 1.0, 774088 -> 1.0, 909608 -> 1.0, 951951 -> 1.0)),
      SparseVector.fromCOO(1048576, Map(149556 -> 1.0, 244514 -> 1.0, 521793 -> 1.0,
        651133 -> 2.0, 994507 -> 1.0, 1000434 -> 1.0)),
      SparseVector.fromCOO(1048576, Map(84322 -> 1.0, 212260 -> 1.0, 402623 -> 1.0,
        535729 -> 1.0, 653054 -> 1.0, 757409 -> 1.0, 985888 -> 1.0, 1000478 -> 1.0,
        1015615 -> 1.0)))
  ) toMap
}
