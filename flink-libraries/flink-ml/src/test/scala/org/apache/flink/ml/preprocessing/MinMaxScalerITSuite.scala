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
package org.apache.flink.ml.preprocessing

import breeze.linalg.{max, min}
import org.apache.flink.api.scala._
import org.apache.flink.ml.common.LabeledVector
import org.apache.flink.ml.math.Breeze._
import org.apache.flink.ml.math.{DenseVector, Vector}
import org.apache.flink.ml.util.FlinkTestBase
import org.scalatest.{FlatSpec, Matchers}


class MinMaxScalerITSuite
  extends FlatSpec
  with Matchers
  with FlinkTestBase {

  behavior of "Flink's MinMax Scaler"

  import MinMaxScalerData._

  it should "scale the vectors' values to be restricted in the [0.0,1.0] range" in {

    val env = ExecutionEnvironment.getExecutionEnvironment

    val dataSet = env.fromCollection(data)
    val minMaxScaler = MinMaxScaler()
    minMaxScaler.fit(dataSet)
    val scaledVectors = minMaxScaler.transform(dataSet).collect

    scaledVectors.length should equal(data.length)

    //ensure data lies in the user-specified range
    for (vector <- scaledVectors) {
      val test = vector.asBreeze.forall(fv => {
        fv >= 0.0 && fv <= 1.0
      })
      test shouldEqual true
    }

    var expectedMin = data.head.asBreeze
    var expectedMax = data.head.asBreeze

    for (v <- data.tail) {
      val tempVector = v.asBreeze
      expectedMin = min(expectedMin, tempVector)
      expectedMax = max(expectedMax, tempVector)
    }

    //ensure that estimated Min and Max vectors equal the expected ones
    val estimatedMinMax = minMaxScaler.metricsOption.get.collect()
    estimatedMinMax.head shouldEqual(expectedMin, expectedMax)

    //handle the case where a feature takes only one value
    val expectedRangePerFeature = (expectedMax - expectedMin)
    for (i <- 0 until expectedRangePerFeature.size) {
      if (expectedRangePerFeature(i) == 0.0) {
        expectedRangePerFeature(i)= 1.0
      }
    }

    //ensure that vectors where scaled correctly
    for (i <- 0 until data.length) {
      var expectedVector = data(i).asBreeze - expectedMin
      expectedVector :/= expectedRangePerFeature
      expectedVector = expectedVector :* (1.0 - 0.0)

      expectedVector.fromBreeze.toSeq should contain theSameElementsInOrderAs scaledVectors(i)
    }
  }

  it should "scale vectors' values in the [-1.0,1.0] range" in {

    val env = ExecutionEnvironment.getExecutionEnvironment

    val dataSet = env.fromCollection(labeledData)
    val minMaxScaler = MinMaxScaler().setMin(-1.0)
    minMaxScaler.fit(dataSet)
    val scaledVectors = minMaxScaler.transform(dataSet).collect

    scaledVectors.length should equal(labeledData.length)

    //ensure data lies in the user-specified range
    for (labeledVector <- scaledVectors) {
      val test = labeledVector.vector.asBreeze.forall(lv => {
        lv >= -1.0 && lv <= 1.0
      })
      test shouldEqual true
    }

    var expectedMin = labeledData.head.vector.asBreeze
    var expectedMax = labeledData.head.vector.asBreeze

    for (v <- labeledData.tail) {
      val tempVector = v.vector.asBreeze
      expectedMin = min(expectedMin, tempVector)
      expectedMax = max(expectedMax, tempVector)
    }

    //ensure that estimated Min and Max vectors equal the expected ones
    val estimatedMinMax = minMaxScaler.metricsOption.get.collect()
    estimatedMinMax.head shouldEqual(expectedMin, expectedMax)

    //handle the case where a feature takes only one value
    val expectedRangePerFeature = (expectedMax - expectedMin)
    for (i <- 0 until expectedRangePerFeature.size) {
      if (expectedRangePerFeature(i) == 0.0) {
        expectedRangePerFeature(i)= 1.0
      }
    }

    //ensure that LabeledVectors where scaled correctly
    for (i <- 0 until labeledData.length) {
      var expectedVector = labeledData(i).vector.asBreeze - expectedMin
      expectedVector :/= expectedRangePerFeature
      expectedVector = (expectedVector :* (1.0 + 1.0)) - 1.0

      labeledData(i).label shouldEqual scaledVectors(i).label
      expectedVector.fromBreeze.toSeq should contain theSameElementsInOrderAs scaledVectors(i)
        .vector
    }
  }
}


object MinMaxScalerData {

  val data: Seq[Vector] = List(
    DenseVector(Array(2104.00, 3.00, 0.0)),
    DenseVector(Array(1600.00, 3.00, 0.0)),
    DenseVector(Array(2400.00, 3.00, 0.0)),
    DenseVector(Array(1416.00, 2.00, 0.0)),
    DenseVector(Array(3000.00, 4.00, 0.0)),
    DenseVector(Array(1985.00, 4.00, 0.0)),
    DenseVector(Array(1534.00, 3.00, 0.0)),
    DenseVector(Array(1427.00, 3.00, 0.0)),
    DenseVector(Array(1380.00, 3.00, 0.0)),
    DenseVector(Array(1494.00, 3.00, 0.0)),
    DenseVector(Array(1940.00, 4.00, 0.0)),
    DenseVector(Array(2000.00, 3.00, 0.0)),
    DenseVector(Array(1890.00, 3.00, 0.0)),
    DenseVector(Array(4478.00, 5.00, 0.0)),
    DenseVector(Array(1268.00, 3.00, 0.0)),
    DenseVector(Array(2300.00, 4.00, 0.0)),
    DenseVector(Array(1320.00, 2.00, 0.0)),
    DenseVector(Array(1236.00, 3.00, 0.0)),
    DenseVector(Array(2609.00, 4.00, 0.0)),
    DenseVector(Array(3031.00, 4.00, 0.0)),
    DenseVector(Array(1767.00, 3.00, 0.0)),
    DenseVector(Array(1888.00, 2.00, 0.0)),
    DenseVector(Array(1604.00, 3.00, 0.0)),
    DenseVector(Array(1962.00, 4.00, 0.0)),
    DenseVector(Array(3890.00, 3.00, 0.0)),
    DenseVector(Array(1100.00, 3.00, 0.0)),
    DenseVector(Array(1458.00, 3.00, 0.0)),
    DenseVector(Array(2526.00, 3.00, 0.0)),
    DenseVector(Array(2200.00, 3.00, 0.0)),
    DenseVector(Array(2637.00, 3.00, 0.0)),
    DenseVector(Array(1839.00, 2.00, 0.0)),
    DenseVector(Array(1000.00, 1.00, 0.0)),
    DenseVector(Array(2040.00, 4.00, 0.0)),
    DenseVector(Array(3137.00, 3.00, 0.0)),
    DenseVector(Array(1811.00, 4.00, 0.0)),
    DenseVector(Array(1437.00, 3.00, 0.0)),
    DenseVector(Array(1239.00, 3.00, 0.0)),
    DenseVector(Array(2132.00, 4.00, 0.0)),
    DenseVector(Array(4215.00, 4.00, 0.0)),
    DenseVector(Array(2162.00, 4.00, 0.0)),
    DenseVector(Array(1664.00, 2.00, 0.0)),
    DenseVector(Array(2238.00, 3.00, 0.0)),
    DenseVector(Array(2567.00, 4.00, 0.0)),
    DenseVector(Array(1200.00, 3.00, 0.0)),
    DenseVector(Array(852.00, 2.00, 0.0)),
    DenseVector(Array(1852.00, 4.00, 0.0)),
    DenseVector(Array(1203.00, 3.00, 0.0))
  )

  val labeledData: Seq[LabeledVector] = List(
    LabeledVector(1.0, DenseVector(Array(2104.00, 3.00, 0.0))),
    LabeledVector(1.0, DenseVector(Array(1600.00, 3.00, 0.0))),
    LabeledVector(1.0, DenseVector(Array(2400.00, 3.00, 0.0))),
    LabeledVector(0.0, DenseVector(Array(1416.00, 2.00, 0.0))),
    LabeledVector(1.0, DenseVector(Array(3000.00, 4.00, 0.0))),
    LabeledVector(1.0, DenseVector(Array(1985.00, 4.00, 0.0))),
    LabeledVector(1.0, DenseVector(Array(1534.00, 3.00, 0.0))),
    LabeledVector(1.0, DenseVector(Array(1427.00, 3.00, 0.0))),
    LabeledVector(1.0, DenseVector(Array(1380.00, 3.00, 0.0))),
    LabeledVector(1.0, DenseVector(Array(1494.00, 3.00, 0.0))),
    LabeledVector(1.0, DenseVector(Array(1940.00, 4.00, 0.0))),
    LabeledVector(1.0, DenseVector(Array(2000.00, 3.00, 0.0))),
    LabeledVector(1.0, DenseVector(Array(1890.00, 3.00, 0.0))),
    LabeledVector(1.0, DenseVector(Array(4478.00, 5.00, 0.0))),
    LabeledVector(1.0, DenseVector(Array(1268.00, 3.00, 0.0))),
    LabeledVector(1.0, DenseVector(Array(2300.00, 4.00, 0.0))),
    LabeledVector(0.0, DenseVector(Array(1320.00, 2.00, 0.0))),
    LabeledVector(1.0, DenseVector(Array(1236.00, 3.00, 0.0))),
    LabeledVector(1.0, DenseVector(Array(2609.00, 4.00, 0.0))),
    LabeledVector(1.0, DenseVector(Array(3031.00, 4.00, 0.0))),
    LabeledVector(1.0, DenseVector(Array(1767.00, 3.00, 0.0))),
    LabeledVector(0.0, DenseVector(Array(1888.00, 2.00, 0.0))),
    LabeledVector(1.0, DenseVector(Array(1604.00, 3.00, 0.0))),
    LabeledVector(1.0, DenseVector(Array(1962.00, 4.00, 0.0))),
    LabeledVector(1.0, DenseVector(Array(3890.00, 3.00, 0.0))),
    LabeledVector(1.0, DenseVector(Array(1100.00, 3.00, 0.0))),
    LabeledVector(1.0, DenseVector(Array(1458.00, 3.00, 0.0))),
    LabeledVector(1.0, DenseVector(Array(2526.00, 3.00, 0.0))),
    LabeledVector(1.0, DenseVector(Array(2200.00, 3.00, 0.0))),
    LabeledVector(1.0, DenseVector(Array(2637.00, 3.00, 0.0))),
    LabeledVector(0.0, DenseVector(Array(1839.00, 2.00, 0.0))),
    LabeledVector(0.0, DenseVector(Array(1000.00, 1.00, 0.0))),
    LabeledVector(1.0, DenseVector(Array(2040.00, 4.00, 0.0))),
    LabeledVector(1.0, DenseVector(Array(3137.00, 3.00, 0.0))),
    LabeledVector(1.0, DenseVector(Array(1811.00, 4.00, 0.0))),
    LabeledVector(1.0, DenseVector(Array(1437.00, 3.00, 0.0))),
    LabeledVector(1.0, DenseVector(Array(1239.00, 3.00, 0.0))),
    LabeledVector(1.0, DenseVector(Array(2132.00, 4.00, 0.0))),
    LabeledVector(1.0, DenseVector(Array(4215.00, 4.00, 0.0))),
    LabeledVector(1.0, DenseVector(Array(2162.00, 4.00, 0.0))),
    LabeledVector(0.0, DenseVector(Array(1664.00, 2.00, 0.0))),
    LabeledVector(1.0, DenseVector(Array(2238.00, 3.00, 0.0))),
    LabeledVector(1.0, DenseVector(Array(2567.00, 4.00, 0.0))),
    LabeledVector(1.0, DenseVector(Array(1200.00, 3.00, 0.0))),
    LabeledVector(0.0, DenseVector(Array(852.00, 2.00, 0.0))),
    LabeledVector(1.0, DenseVector(Array(1852.00, 4.00, 0.0))),
    LabeledVector(1.0, DenseVector(Array(1203.00, 3.00, 0.0)))
  )
}
