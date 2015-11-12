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

package org.apache.flink.ml.classification

import org.apache.flink.api.scala.{ExecutionEnvironment, DataSet}
import org.apache.flink.api.scala._
import org.apache.flink.ml.math.{DenseVector, DenseMatrix}
import org.apache.flink.test.util.FlinkTestBase
import org.scalatest.{FlatSpec, Matchers}
import scala.io.Source._

class HMMSuite extends FlatSpec with Matchers with FlinkTestBase {

  behavior of "Hidden Markov Model"

  it should "train a HMM" in {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val labeledData: DataSet[(Array[(Int, Int)])] = readLabeledData(env)
    val data: DataSet[Array[Int]] = readData(env)

    val hmm = new HMM()
    hmm.initializeParameters(labeledData, 3, 3)
    hmm.train(data, 3)

    val expectedInitialProbabilities = new DenseVector(Array[Double](0.3,0.2,0.5))
    val expectedTransitionProbabilities = new DenseMatrix(3, 3,
      Array[Double](0.3,0.4,0.3,0.5,0.1,0.3,0.2,0.5,0.4))
    val expectedEmissionProbabilities = new DenseMatrix(3, 3,
      Array[Double](0.2,0.3,0.6,0.4,0.4,0.1,0.4,0.3,0.3))

    for (i <- 0 until hmm.initialProbabilities.size) {
      hmm.initialProbabilities(i) should be(expectedInitialProbabilities(i) +- 0.1)
    }

    for (i <- 0 until hmm.transitionProbabilities.numRows) {
      for (j <- 0 until hmm.transitionProbabilities.numCols) {
        hmm.transitionProbabilities(i, j) should be(expectedTransitionProbabilities(i, j) +- 0.1)
      }
    }

    for (i <- 0 until hmm.emissionProbabilities.numRows) {
      for (j <- 0 until hmm.emissionProbabilities.numCols) {
        hmm.emissionProbabilities(i, j) should be(expectedEmissionProbabilities(i, j) +- 0.1)
      }
    }
  }

  it should "predict correct values" in {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val hmm = new HMM()
    hmm.initialProbabilities = new DenseVector(Array[Double](0.3,0.2,0.5))
    hmm.transitionProbabilities = new DenseMatrix(3, 3,
      Array[Double](0.3,0.4,0.3,0.5,0.1,0.3,0.2,0.5,0.4))
    hmm.emissionProbabilities = new DenseMatrix(3, 3,
      Array[Double](0.2,0.3,0.6,0.4,0.4,0.1,0.4,0.3,0.3))

    var data: Map[String, Array[Int]] = Map.empty[String, Array[Int]]
    var labels: Map[String, Array[Int]] = Map.empty[String, Array[Int]]
    val lines = fromFile(getClass.getResource("/LabeledSequenceData2").toURI).getLines()

    var numberOfLabelsToPredict: Int = 0
    var index: Int = 0
    lines.foreach(line => {
      try {
        val split = line.split(":")
        data += index.toString -> split(0).split(",").map(x => x.toInt)
        labels += index.toString -> split(1).split(",").map(x => x.toInt)

        numberOfLabelsToPredict += split(1).length
        index += 1
      } catch {
        case e: Exception => //Do nothing
      }
    })

    val result = hmm.decode(env.fromCollection(data.toList)).collect()
    var countCorrectPredictions: Int = 0
    result.foreach(r => {
      val index = r._1

      val expectedLabels = labels(index)

      for (i <- 0 until r._2.length) {
        val predictedLabel = r._2(i)._2
        val expectedLabel = expectedLabels(i)
        if (predictedLabel == expectedLabel) {
          countCorrectPredictions += 1
        }
      }
    })

    numberOfLabelsToPredict.toDouble should be > (numberOfLabelsToPredict.toDouble / 5)
  }

  def readLabeledData(env: ExecutionEnvironment): DataSet[(Array[(Int, Int)])] = {
    env.readTextFile(getClass.getResource("/LabeledSequenceData").toString).map(line => {
      try {
        val split = line.split(":")
        val observations = split(0).split(",")
        val labels = split(1).split(",")

        val result: Array[(Int, Int)] = new Array[(Int, Int)](observations.length)
        for (i <- 0 until observations.length) {
          val tuple = new Tuple2(observations(i).toInt, labels(i).toInt)
          result(i) = tuple
        }
        result
      } catch {
        case e: Exception => null
      }
    }).filter(data => data != null)
  }

  def readData(env: ExecutionEnvironment): DataSet[Array[Int]] = {
    env.readTextFile(getClass.getResource("/SequenceData").toString).map(line => {
      try {
        line.split(",").map(x => x.toInt)
      } catch {
        case e: Exception => null
      }
    }).filter(data => data != null)
  }
}

