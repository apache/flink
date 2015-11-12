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

import org.apache.flink.api.scala.DataSet

import org.apache.flink.api.scala._
import org.apache.flink.ml.math.{DenseMatrix, DenseVector}
import org.apache.flink.ml.optimization.BaumWelch
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

class HMM {

  /**
   * initial probabilities of the states
   */
  var initialProbabilities: DenseVector = null


  /**
   * transition probabilities
   */
  var transitionProbabilities: DenseMatrix = null


  /**
   * emission probabilities
   */
  var emissionProbabilities: DenseMatrix = null

  val LOG = LoggerFactory.getLogger(this.getClass)

  /**
   *
   * Initializes the parameters of the HMM with labeled training data
   *
   * @param trainingData labeled training data with the first element of the tuple as
   *                     the observation and the second element as the label
   * @param numberOfStates number of states
   * @param numberOfObVocab number of the observation vocabulary
   */
  def initializeParameters(trainingData: DataSet[Array[Tuple2[Int, Int]]],
                           numberOfStates: Int, numberOfObVocab: Int) = {

    val params =

      trainingData.flatMap((data, collector: Collector[(String, Int, Int, Long)]) => {

      var transitions : Map[String, Long] = Map.empty[String, Long].withDefaultValue(0)
      var emissions : Map[String, Long] = Map.empty[String, Long].withDefaultValue(0)

      var currentOb = data(0)

      //xi -> yi
      var emission: String = currentOb._2 + "->" + currentOb._1

      emissions += emission -> 1

      val initialState = ("i", currentOb._2, 0, 1L)

      for (i <- 1 until data.length) {
        val nextOb= data(i)
        val transition = currentOb._2 + "->" + nextOb._2
        currentOb = nextOb
        emission = currentOb._2 + "->" + currentOb._1
        transitions += (transition -> (transitions(transition) + 1L))
        emissions += (emission -> (emissions(emission) + 1L))
      }

      //emit initial state
      collector.collect(initialState)

      //emit transitions
      transitions.foreach(t => {val split = t._1.split("->");

        collector.collect(("t", split(0).toInt,split(1).toInt, t._2))})

        //emit emissions
       emissions.foreach(e => {val split = e._1.split("->");
       collector.collect(("e", split(0).toInt, split(1).toInt, e._2))})
    })
        //group each tuple types and sum them up
      .groupBy(0, 1,2).sum(3).map(tuple => (tuple._1, tuple._2,
        List((tuple._3, tuple._4)), tuple._4))
        .groupBy(0, 1).reduce((tuple1, tuple2) =>
        (tuple1._1, tuple1._2, tuple1._3 ::: tuple2._3, tuple1._4 + tuple2._4)).collect()


    initialProbabilities = new DenseVector(new Array[Double](numberOfStates))
    transitionProbabilities = new DenseMatrix(numberOfStates, numberOfStates,
      new Array[Double](numberOfStates * numberOfStates))
    emissionProbabilities = new DenseMatrix(numberOfStates, numberOfObVocab,
      new Array[Double](numberOfStates * numberOfObVocab))


    val numberOfTrainingInstances: Long = trainingData.count()
    params.foreach(param => {
      param._1 match {
        case "i" => {
          initialProbabilities.update(param._2, param._4.toDouble / numberOfTrainingInstances)
        }
        case "t" => param._3.foreach(nextState =>
          transitionProbabilities.update(param._2, nextState._1, nextState._2.toDouble / param._4))
        case "e" => param._3.foreach(nextState =>
          emissionProbabilities.update(param._2, nextState._1, nextState._2.toDouble / param._4))
      }
    })
  }

  /**
   * trains the model parameters with the given observations
   * @param observations training data
   * @param numberOfIterations number of iterations
   */
  def train(observations: DataSet[Array[Int]], numberOfIterations: Int): Unit = {
    BaumWelch.train(this, observations, numberOfIterations)
  }

  def decode(observations: DataSet[(String, Array[Int])]): DataSet[(String, Array[(Int, Int)])] = {
    Viterbi.decode(this, observations)
  }


  override def toString: String = {
    "initial probabilities \n" + initialProbabilities.toString +
      "\n \ntransition probabilities \n" +
      transitionProbabilities.toString + " \n \nemission probabilities\n" +
      emissionProbabilities.toString
  }
}

