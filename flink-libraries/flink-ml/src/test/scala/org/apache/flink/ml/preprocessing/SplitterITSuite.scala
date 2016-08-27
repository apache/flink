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

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.ml.util.FlinkTestBase
import org.scalatest.{Matchers, FlatSpec}
import org.apache.flink.api.scala.utils._


class SplitterITSuite extends FlatSpec
  with Matchers
  with FlinkTestBase {

  behavior of "Flink's DataSet Splitter"

  import MinMaxScalerData._

 it should "result in datasets with no elements in common and all elements used" in {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val dataSet = env.fromCollection(data)

    val splitDataSets = Splitter.randomSplit(dataSet.zipWithUniqueId, 0.5)

   (splitDataSets(0).union(splitDataSets(1)).count()) should equal(dataSet.count())


   splitDataSets(0).join(splitDataSets(1)).where(0).equalTo(0).count() should equal(0)
  }

  it should "result in datasets of an expected size when precise" in {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val dataSet = env.fromCollection(data)

    val splitDataSets = Splitter.randomSplit(dataSet, 0.5, true)

    val expectedLength = data.size.toDouble * 0.5

    splitDataSets(0).count().toDouble should equal(expectedLength +- 1.0)
  }

  it should "result in expected number of datasets" in {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val dataSet = env.fromCollection(data)

    val fracArray = Array(0.5, 0.25, 0.25)

    val splitDataSets = Splitter.multiRandomSplit(dataSet, fracArray)

    splitDataSets.length should equal(fracArray.length)
  }

  it should "produce TrainTestDataSets in which training size is greater than testing size" in {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val dataSet = env.fromCollection(data)

    val dataSetArray = Splitter.kFoldSplit(dataSet, 4)

    (dataSetArray(1).testing.count() < dataSetArray(1).training.count()) should be(true)

  }

  it should "throw an exception if sample fraction ins nonsensical" in {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val dataSet = env.fromCollection(data)

    intercept[IllegalArgumentException] {
      val splitDataSets = Splitter.randomSplit(dataSet, -0.2)
    }

    intercept[IllegalArgumentException] {
      val splitDataSets = Splitter.randomSplit(dataSet, -1.2)
    }

  }
}
