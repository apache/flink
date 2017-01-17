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

import breeze.linalg
import breeze.numerics.sqrt
import breeze.numerics.sqrt._
import org.apache.flink.api.scala._
import org.apache.flink.ml.common.LabeledVector
import org.apache.flink.ml.math.{Vector => FlinkVector, DenseVector}
import org.apache.flink.ml.math.Breeze._
import org.apache.flink.ml.util.FlinkTestBase
import org.scalatest._


class StandardScalerITSuite
  extends FlatSpec
  with Matchers
  with FlinkTestBase {

  behavior of "Flink's Standard Scaler"

  import StandardScalerData._

  def checkVectors(
      scaledVectors: Seq[FlinkVector],
      expectedMean: Double,
      expectedStd: Double): Unit = {
    scaledVectors.length should equal(data.length)

    val numberOfFeatures = scaledVectors.head.size
    var scaledMean: linalg.Vector[Double] = linalg.DenseVector.zeros(numberOfFeatures)
    var scaledStd: linalg.Vector[Double] = linalg.DenseVector.zeros(numberOfFeatures)

    for (vector <- scaledVectors) {
      scaledMean += vector.asBreeze
    }
    scaledMean /= scaledVectors.size.asInstanceOf[Double]

    for (vector <- scaledVectors) {
      val temp = vector.asBreeze - scaledMean
      scaledStd += temp :* temp
    }
    scaledStd /= scaledVectors.size.asInstanceOf[Double]
    scaledStd = sqrt(scaledStd)

    for (i <- 0 until numberOfFeatures) {
      scaledMean(i) should be(expectedMean +- 1e-9)
      scaledStd(i) should be(expectedStd +- 1e-9)
    }
  }

  it should "scale the vectors to have mean equal to 0 and std equal to 1" in {

    val env = ExecutionEnvironment.getExecutionEnvironment

    val dataSet = env.fromCollection(data)
    val scaler = StandardScaler()
    scaler.fit(dataSet)
    val scaledVectors = scaler.transform(dataSet).collect()

    checkVectors(scaledVectors, 0.0, 1.0)
  }

  it should "scale the vectors to have mean equal to 10 and standard deviation equal to 2" in {

    val env = ExecutionEnvironment.getExecutionEnvironment

    val dataSet = env.fromCollection(data)
    val scaler = StandardScaler().setMean(10.0).setStd(2.0)
    scaler.fit(dataSet)
    val scaledVectors = scaler.transform(dataSet).collect()

    checkVectors(scaledVectors, 10.0, 2.0)
  }

  it should "work with LabeledVector" in {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val dataSet = env.fromCollection(data).map(v => LabeledVector(1.0, v))
    val scaler = StandardScaler()
    scaler.fit(dataSet)
    val scaledVectors = scaler.transform(dataSet).map(_.vector).collect()

    checkVectors(scaledVectors, 0.0, 1.0)
  }

  it should "work with (FlinkVector, Double) tuples" in {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val dataSet = env.fromCollection(data).map(v => (v, 1.0))
    val scaler = StandardScaler()
    scaler.fit(dataSet)
    val scaledVectors = scaler.transform(dataSet).map(_._1).collect()

    checkVectors(scaledVectors, 0.0, 1.0)
  }
}

object StandardScalerData {

  val data: Seq[FlinkVector] = List(
    DenseVector(Array(2104.00, 3.00)),
    DenseVector(Array(1600.00, 3.00)),
    DenseVector(Array(2400.00, 3.00)),
    DenseVector(Array(1416.00, 2.00)),
    DenseVector(Array(3000.00, 4.00)),
    DenseVector(Array(1985.00, 4.00)),
    DenseVector(Array(1534.00, 3.00)),
    DenseVector(Array(1427.00, 3.00)),
    DenseVector(Array(1380.00, 3.00)),
    DenseVector(Array(1494.00, 3.00)),
    DenseVector(Array(1940.00, 4.00)),
    DenseVector(Array(2000.00, 3.00)),
    DenseVector(Array(1890.00, 3.00)),
    DenseVector(Array(4478.00, 5.00)),
    DenseVector(Array(1268.00, 3.00)),
    DenseVector(Array(2300.00, 4.00)),
    DenseVector(Array(1320.00, 2.00)),
    DenseVector(Array(1236.00, 3.00)),
    DenseVector(Array(2609.00, 4.00)),
    DenseVector(Array(3031.00, 4.00)),
    DenseVector(Array(1767.00, 3.00)),
    DenseVector(Array(1888.00, 2.00)),
    DenseVector(Array(1604.00, 3.00)),
    DenseVector(Array(1962.00, 4.00)),
    DenseVector(Array(3890.00, 3.00)),
    DenseVector(Array(1100.00, 3.00)),
    DenseVector(Array(1458.00, 3.00)),
    DenseVector(Array(2526.00, 3.00)),
    DenseVector(Array(2200.00, 3.00)),
    DenseVector(Array(2637.00, 3.00)),
    DenseVector(Array(1839.00, 2.00)),
    DenseVector(Array(1000.00, 1.00)),
    DenseVector(Array(2040.00, 4.00)),
    DenseVector(Array(3137.00, 3.00)),
    DenseVector(Array(1811.00, 4.00)),
    DenseVector(Array(1437.00, 3.00)),
    DenseVector(Array(1239.00, 3.00)),
    DenseVector(Array(2132.00, 4.00)),
    DenseVector(Array(4215.00, 4.00)),
    DenseVector(Array(2162.00, 4.00)),
    DenseVector(Array(1664.00, 2.00)),
    DenseVector(Array(2238.00, 3.00)),
    DenseVector(Array(2567.00, 4.00)),
    DenseVector(Array(1200.00, 3.00)),
    DenseVector(Array(852.00, 2.00)),
    DenseVector(Array(1852.00, 4.00)),
    DenseVector(Array(1203.00, 3.00))
  )
}
