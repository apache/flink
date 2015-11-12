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

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.DataSet
import org.apache.flink.configuration.Configuration
import org.apache.flink.ml.math.{DenseVector, DenseMatrix}
import org.apache.flink.api.scala._

import scala.collection.mutable

/**
 * Implementation of the Viterbi Algorithm
 */
object Viterbi {

  /**
   * decodes the observations with the given Hidden Markov Model using the Viterbi Algorithm
   * @param hmm Hidden Markov Model
   * @param observations observations
   * @return decoded observations
   */
  def decode(hmm: HMM, observations: DataSet[(String, Array[Int])]):
  DataSet[(String, Array[(Int, Int)])] = {

    val hmmModelParams: DataSet[(DenseVector, DenseMatrix, DenseMatrix)] =
      observations.getExecutionEnvironment.fromElements((hmm.initialProbabilities,
        hmm.transitionProbabilities, hmm.emissionProbabilities))

    observations.map(new RichMapFunction[(String, Array[Int]), (String, Array[(Int, Int)])]() {

      var initialProbabilities: DenseVector = null
      var transitionProbabilities: DenseMatrix = null
      var emissionProbabilities: DenseMatrix = null
      var numberOfStates: Int = 0

      override def open(config: Configuration): Unit = {
        val modelParams = getRuntimeContext().getBroadcastVariable[(DenseVector,
         DenseMatrix, DenseMatrix)]("HMMParams").get(0)
        initialProbabilities = modelParams._1
        transitionProbabilities = modelParams._2
        emissionProbabilities = modelParams._3
        numberOfStates = transitionProbabilities.numRows
      }

      def map(observations: (String, Array[Int])): (String, Array[(Int, Int)]) = {
        val v: Array[Array[Double]] = Array.ofDim[Double](observations._2.length, numberOfStates)
        var path: Map[Int, mutable.MutableList[Int]] = Map.empty[Int, mutable.MutableList[Int]]

        for (state <- 0 until numberOfStates) {
          val p: mutable.MutableList[Int] = mutable.MutableList[Int](state)
          path += state -> p
          v(0)(state) = initialProbabilities(state) *
            emissionProbabilities(state, observations._2(0))
        }

        for (o <- 1 until observations._2.length) {
          var newPath: Map[Int, mutable.MutableList[Int]] =
            Map.empty[Int, mutable.MutableList[Int]]
          for (state <- 0 until numberOfStates) {

            var maxValue: Double = -1
            var bestState = -1
            for (prevState <- 0 until numberOfStates) {
              val value = v(o - 1)(prevState) * transitionProbabilities(prevState, state) *
                emissionProbabilities(state, observations._2(o))
              if (value > maxValue) {
                maxValue = value
                bestState = prevState
              }
            }
            v(o)(state) = maxValue
            newPath += (state -> path(bestState).clone())
            newPath(state) += state
          }

          path = newPath
        }

        var bestPath: mutable.MutableList[Int] = null
        var bestValue: Double = -1

        for (state <- 0 until numberOfStates) {
          val value = v(observations._2.length - 1)(state)
          if (value > bestValue) {
            bestValue = value
            bestPath = path(state)
          }
        }

        val result: (String, Array[(Int, Int)]) = (observations._1,
          new Array[(Int, Int)](observations._2.length))

        for (i <- 0 until observations._2.length) {
          result._2(i) = (observations._2(i), bestPath(i))
        }

        result
      }
    }).withBroadcastSet(hmmModelParams, "HMMParams")
  }
}
