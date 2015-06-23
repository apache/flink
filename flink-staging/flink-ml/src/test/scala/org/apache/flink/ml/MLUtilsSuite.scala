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

package org.apache.flink.ml

import java.io.File

import org.apache.flink.ml.statistics.{ContinuousHistogram, DiscreteHistogram}

import scala.io.Source

import org.scalatest.{FlatSpec, Matchers}

import org.apache.flink.api.scala._
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.ml.common.LabeledVector
import org.apache.flink.ml.math.SparseVector
import org.apache.flink.test.util.FlinkTestBase
import org.apache.flink.testutils.TestFileUtils

class MLUtilsSuite extends FlatSpec with Matchers with FlinkTestBase {

  behavior of "The RichExecutionEnvironment"

  it should "read a libSVM/SVMLight input file" in {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val content =
      """
        |1 2:10.0 4:4.5 8:4.2 # foo
        |-1 1:9.0 4:-4.5 7:2.4 # bar
        |0.4 3:1.0 8:-5.6 10:1.0
        |-42.1 2:2.0 4:-6.1 3:5.1 # svm
      """.stripMargin

    val expectedLabeledVectors = Set(
      LabeledVector(1, SparseVector.fromCOO(10, (1, 10), (3, 4.5), (7, 4.2))),
      LabeledVector(-1, SparseVector.fromCOO(10, (0, 9), (3, -4.5), (6, 2.4))),
      LabeledVector(0.4, SparseVector.fromCOO(10, (2, 1), (7, -5.6), (9, 1))),
      LabeledVector(-42.1, SparseVector.fromCOO(10, (1, 2), (3, -6.1), (2, 5.1)))
    )

    val inputFilePath = TestFileUtils.createTempFile(content)

    val svmInput = env.readLibSVM(inputFilePath)

    val labeledVectors = svmInput.collect()

    labeledVectors.size should be(expectedLabeledVectors.size)

    for(lVector <- labeledVectors) {
      expectedLabeledVectors.contains(lVector) should be(true)
    }

  }

  it should "write a libSVM/SVMLight output file" in {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val labeledVectors = Seq(
      LabeledVector(1.0, SparseVector.fromCOO(10, (1, 10), (3, 4.5), (7, 4.2))),
      LabeledVector(-1.0, SparseVector.fromCOO(10, (0, 9), (3, -4.5), (6, 2.4))),
      LabeledVector(0.4, SparseVector.fromCOO(10, (2, 1), (7, -5.6), (9, 1))),
      LabeledVector(-42.1, SparseVector.fromCOO(10, (1, 2), (3, -6.1), (2, 5.1)))
    )

    val expectedLines = List(
      "1.0 2:10.0 4:4.5 8:4.2",
      "-1.0 1:9.0 4:-4.5 7:2.4",
      "0.4 3:1.0 8:-5.6 10:1.0",
      "-42.1 2:2.0 3:5.1 4:-6.1"
    )

    val labeledVectorsDS = env.fromCollection(labeledVectors)

    val tempDir = new File(System.getProperty("java.io.tmpdir"))

    val tempFile = new File(tempDir, TestFileUtils.randomFileName())

    val outputFilePath = tempFile.getAbsolutePath

    labeledVectorsDS.writeAsLibSVM(outputFilePath)

    env.execute()

    val src = Source.fromFile(tempFile)

    var counter = 0

    for(l <- src.getLines()) {
      expectedLines.exists(_.equals(l)) should be(true)
      counter += 1
    }

    counter should be(expectedLines.size)

    tempFile.delete()
  }

  it should "create a discrete histogram" in {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val discreteData = Seq(1.0, 2.0, 3.0, 5.0, 1.0, 7.0, 9.0, 1.0, 0.0, 1.0, 4.0, 6.0, 7.0, 9.0,
      4.0, 3.0, 1.0, 4.0, 6.0, 8.0, 4.0, 3.0, 6.0, 8.0, 4.0, 3.0, 6.0, 8.0, 9.0, 7.0, 8.0, 2.0, 3.0,
      6.0, 0.0)
    val h = MLUtils
      .createDiscreteHistogram(env.fromCollection(discreteData))
      .collect().toArray.apply(0)

    h.count(0) should equal(discreteData.count(x => x == 0))
    h.count(1) should equal(discreteData.count(x => x == 1))
    h.count(2) should equal(discreteData.count(x => x == 2))
    h.count(3) should equal(discreteData.count(x => x == 3))
    h.count(4) should equal(discreteData.count(x => x == 4))
    h.count(5) should equal(discreteData.count(x => x == 5))
    h.count(6) should equal(discreteData.count(x => x == 6))
    h.count(7) should equal(discreteData.count(x => x == 7))
    h.count(8) should equal(discreteData.count(x => x == 8))
    h.count(9) should equal(discreteData.count(x => x == 9))
  }

  it should "create a continuous histogram" in {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val discreteData = Seq(1.0, 2.0, 3.0, 5.0, 1.0, 7.0, 9.0, 1.0, 0.0, 1.0, 4.0, 6.0, 7.0, 9.0,
      4.0, 3.0, 1.0, 4.0, 6.0, 8.0, 4.0, 3.0, 6.0, 8.0, 4.0, 3.0, 6.0, 8.0, 9.0, 7.0, 8.0, 2.0, 3.0,
      6.0, 0.0)
    val h = env.fromCollection(discreteData).setParallelism(4)
      .createHistogram(5)
      .collect().toArray.apply(0)

    h.count(h.quantile(0.2)) should equal(7)
    h.count(h.quantile(0.1)) should equal(4)
    h.count(h.quantile(0.5)) should equal(18)
  }
}
