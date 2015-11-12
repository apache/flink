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

package org.apache.flink.ml.optimization

import org.apache.flink.api.common.functions.{RichFlatMapFunction, RichMapFunction, MapFunction}
import org.apache.flink.api.scala.{ExecutionEnvironment, DataSet}
import org.apache.flink.configuration.Configuration
import org.apache.flink.ml.classification.HMM
import org.apache.flink.ml.math.{DenseVector, DenseMatrix}
import org.apache.flink.util.Collector
import org.apache.flink.api.scala._
import org.slf4j.LoggerFactory

/**
 * Implementation of a parallel Baum-Welch algorithm based on the paper
 * Lin, Jimmy, and Chris Dyer. "Data-intensive text processing with MapReduce.
 * "Synthesis Lectures on Human Language Technologies 3.1 (2010): 1-177.
 *
 */
object BaumWelch {

  /**
   * trains the Hidden Markov Model with the given number of iterations
   *
   * @param hmm Hidden Markov Model to train
   * @param observations observations which are used to train the Hidden Markov Model
   * @param numberOfIterations number of iterations to train the Hidden Markov Model
   */
  def train(hmm: HMM, observations: DataSet[Array[Int]], numberOfIterations: Int) {
    train(hmm, observations, hmm.initialProbabilities.size,
      hmm.emissionProbabilities.numCols, numberOfIterations)
  }

  /**
   * trains the Hidden Markov Model with the given number of iterations
   *
   * @param hmm Hidden Markov Model to train
   * @param observations training data
   * @param numberOfStates number of states of the Hidden Markov Model
   * @param sizeOfObservationVocab size of the observation vocabulary
   * @param numberOfIterations number of iterations to train the Hidden Markov Model
   */
  protected def train(hmm: HMM, observations: DataSet[Array[Int]], numberOfStates: Int,
                      sizeOfObservationVocab: Int, numberOfIterations: Int) {

    val hmmModelParams = observations.getExecutionEnvironment.fromElements(
      (hmm.initialProbabilities, hmm.transitionProbabilities, hmm.emissionProbabilities))

    val paramsResult = hmmModelParams.iterate(numberOfIterations) {
      previousHmmModelParams: DataSet[(DenseVector, DenseMatrix, DenseMatrix)] => {

        val results = trainDistributed(previousHmmModelParams, observations)

        combineResults(results, numberOfStates, sizeOfObservationVocab)
      }
    }

    val params = paramsResult.collect()(0)

    hmm.initialProbabilities = params._1
    hmm.transitionProbabilities = params._2
    hmm.emissionProbabilities = params._3
  }

  /**
   * trains dsitributed the Hidden Markov Model
   *
   * @param previousHmmModelParams previous parameters of the Hidden markov Model
   * @param observations training data
   * @return results of the distributed trainings
   */
  def trainDistributed(previousHmmModelParams: DataSet[(DenseVector, DenseMatrix, DenseMatrix)],
                       observations: DataSet[Array[Int]]): DataSet[(String, Int, Int, Double)] = {
    observations.flatMap(new RichFlatMapFunction[Array[Int], (String, Int, Int, Double)]() {

      var initialProbabilities: DenseVector = null
      var transitionProbabilities: DenseMatrix = null
      var emissionProbabilities: DenseMatrix = null
      var numberOfStates: Int = 0
      var sizeOfObservationVocab: Int = 0

      override def open(config: Configuration): Unit = {
        val modelParams = getRuntimeContext()
        .getBroadcastVariable[(DenseVector, DenseMatrix, DenseMatrix)]("HMMParams").get(0)
        initialProbabilities = modelParams._1
        transitionProbabilities = modelParams._2
        emissionProbabilities = modelParams._3
        numberOfStates = transitionProbabilities.numRows
        sizeOfObservationVocab = emissionProbabilities.numCols
      }


      override def flatMap(in: Array[Int],
                           collector: Collector[(String, Int, Int, Double)]): Unit = {
        val f = forward(initialProbabilities, transitionProbabilities,
          emissionProbabilities, numberOfStates, in)
        val b = backward(initialProbabilities, transitionProbabilities,
          emissionProbabilities, numberOfStates, in)

        //update initial probabilities
        var probSum: Double = sumProbOfObservation(f, b, 0)
        for (state <- 0 until numberOfStates) {
          var prob: Double = 0
          if (probSum != 0) {
            prob = gamma(state, f, b, 0, probSum)
          }
          collector.collect(("i", state, 0, prob))
        }

        //update transition probabilities
        for (state1 <- 0 until numberOfStates) {
          for (state2 <- 0 until numberOfStates) {
            var sumProb: Double = 0
            var sumGamma: Double = 0
            for (o <- 0 until in.length) {
              probSum = sumProbOfObservation(f, b, o)
              if (probSum != 0) {
                sumProb += calculateTransitionProb(f, b, transitionProbabilities,
                  emissionProbabilities, in, state1, state2, o, probSum)
                sumGamma += gamma(state1, f, b, o, probSum)
              }
            }

            var prob: Double = 0
            if (sumGamma != 0) {
              prob = sumProb / sumGamma
            }
            collector.collect(("t", state1, state2, prob))
          }
        }

        //update emission probabilities
        for (state <- 0 until numberOfStates) {
          for (obVocab <- 0 until sizeOfObservationVocab) {
            var sumProb: Double = 0
            var sumGamma: Double = 0
            for (o <- 0 until in.length) {
              probSum = sumProbOfObservation(f, b, o)
              if (probSum != 0) {
                var g = gamma(state, f, b, o, probSum)
                if (obVocab == in(o)) {
                  sumProb += g
                }
                sumGamma += g
              }
            }

            var prob: Double = 0
            if (sumGamma != 0) {
              prob = sumProb / sumGamma
            }
            collector.collect(("e", state, obVocab, prob))
          }
        }
      }
    }).withBroadcastSet(previousHmmModelParams, "HMMParams")
  }


  /**
   * combines the results of the distributed training
   *
   * @param results results of the distributed trainings
   * @param numberOfStates number of states of the Hidden Markov Model
   * @param sizeOfObservationVocab size of the obervation Vocabulary of the Hidden Markov Model
   * @return new model parameters of the Hidden Markov Model
   */
  def combineResults(results: DataSet[(String, Int, Int, Double)], numberOfStates: Int,
                     sizeOfObservationVocab: Int):
  DataSet[(DenseVector, DenseMatrix, DenseMatrix)] = {
    results.groupBy(0, 1, 2).sum(3).map(t => (t._1, t._2, List((t._3, t._4)), t._4))
      .groupBy(0, 1).reduce((tuple1, tuple2) =>
      (tuple1._1, tuple1._2, tuple1._3 ::: tuple2._3, tuple1._4 + tuple2._4))
      .reduceGroup(iterator => {
        var initialProbabilities: DenseVector = null
        var transitionProbabilities: DenseMatrix = null
        var emissionProbabilities: DenseMatrix = null
        var count: Double = 0
        iterator.foreach(p => {
          if (p._1.equals("i")) {
            if (initialProbabilities == null) {
              initialProbabilities = new DenseVector(new Array[Double](numberOfStates))
            }
            initialProbabilities(p._2.toInt) = p._4
            count += p._4
          } else if (p._1.equals("t")) {
            if (transitionProbabilities == null) {
              transitionProbabilities = new DenseMatrix(numberOfStates, numberOfStates,
                new Array[Double](numberOfStates * numberOfStates))
            }
            p._3.foreach(t => transitionProbabilities(p._2.toInt, t._1.toInt) = t._2 / p._4)
          } else if (p._1.equals("e")) {
            if (emissionProbabilities == null) {
              emissionProbabilities = new DenseMatrix(numberOfStates, sizeOfObservationVocab,
                new Array[Double](numberOfStates * sizeOfObservationVocab))
            }
            p._3.foreach(t => emissionProbabilities(p._2.toInt, t._1.toInt) = t._2 / p._4)
          }
        })

        for (i <- 0 until initialProbabilities.size) {
          initialProbabilities(i) = initialProbabilities(i) / count
        }
        (initialProbabilities, transitionProbabilities, emissionProbabilities)
      })
  }

  /**
   * forward prodecure of the Baum-Welch algorithm
   *
   * @param initialProbabilities initial probabilities
   * @param transitionProbabilities transition probabilities
   * @param emissionProbabilities emission probabilities
   * @param numberOfStates number of states
   * @param observations training data
   * @return forward probabilities
   */
  def forward(initialProbabilities: DenseVector, transitionProbabilities: DenseMatrix,
              emissionProbabilities: DenseMatrix, numberOfStates: Int,
              observations: Array[Int]): Array[Array[Double]] = {
    val f: Array[Array[Double]] = Array.ofDim[Double](numberOfStates, observations.length)

    //initial probabilities
    for (state <- 0 until f.length) {
      f(state)(0) = initialProbabilities(state) * emissionProbabilities(state, observations(0))
    }

    for (o <- 0 until observations.length - 1) {
      for (state1 <- 0 until numberOfStates) {
        for (state2 <- 0 until numberOfStates) {
          f(state1)(o + 1) += f(state2)(o) * transitionProbabilities(state1, state2) *
            emissionProbabilities(state1, observations(o + 1))
        }
      }
    }
    f
  }

  /**
   * backward prodecure of the Baum-Welch algorithm
   *
   * @param initialProbabilities initial probabilities
   * @param transitionProbabilities transition probabilities
   * @param emissionProbabilities emission probabilities
   * @param numberOfStates number of states
   * @param observations training data
   * @return backwardprobabilities
   */
  def backward(initialProbabilities: DenseVector, transitionProbabilities: DenseMatrix,
               emissionProbabilities: DenseMatrix,
               numberOfStates: Int, observations: Array[Int]): Array[Array[Double]] = {
    val b: Array[Array[Double]] = Array.ofDim[Double](numberOfStates, observations.length)

    //initial probabilities
    for (state <- 0 until b.length) {
      b(state)(observations.length - 1) = 1
    }

    for (o <- observations.length - 2 to 0 by -1) {
      for (state1 <- 0 until numberOfStates) {
        for (state2 <- 0 until numberOfStates) {
          b(state1)(o) += b(state2)(o + 1) * transitionProbabilities(state1, state2) *
            emissionProbabilities(state2, observations(o + 1))
        }
      }
    }
    b
  }

  /**
   * calculates the transition probability of state1 -> state2
   *
   * @param f forward probabilities
   * @param b backward probabilities
   * @param transitionProbabilities transition probabilities
   * @param emissionProbabilities emission probabilities
   * @param obs training subset
   * @param state1 state1
   * @param state2 state2
   * @param ob current observation
   * @param sumProbOfObservation sum of the observation probability
   * @return transition probabilities of state1 -> state2
   */
  def calculateTransitionProb(f: Array[Array[Double]], b: Array[Array[Double]],
                              transitionProbabilities: DenseMatrix,
                              emissionProbabilities: DenseMatrix,
                              obs: Array[Int], state1: Int,
                              state2: Int, ob: Int,
                              sumProbOfObservation: Double): Double = {
    var prob = f(state1)(ob) * transitionProbabilities(state1, state2)
    if (ob < b(state2).length - 1) {
      prob *= emissionProbabilities(state2, obs(ob + 1)) * b(state2)(ob + 1)
    }

    prob / sumProbOfObservation
  }

  /**
   * calculates the gamma value
   *
   * @param state state for which the gamma value will be calculates
   * @param f forward probabilities
   * @param b backward probabilities
   * @param ob current observation
   * @param sumProbOfObservation sum of the observation probability
   * @return gamma value for the given state and observation
   */
  def gamma(state: Int, f: Array[Array[Double]], b: Array[Array[Double]],
            ob: Int, sumProbOfObservation: Double): Double = {
    (f(state)(ob) * b(state)(ob)) / sumProbOfObservation
  }

  /**
   * calculates the sum of the probabilities of the given observation
   *
   * @param f forward probabilities
   * @param b backward probabilities
   * @param ob current observation
   * @return sum of the observation probability
   */
  def sumProbOfObservation(f: Array[Array[Double]], b: Array[Array[Double]], ob: Int): Double = {
    var result: Double = 0

    for (state <- 0 until f.length) {
      result += f(state)(ob) * b(state)(ob)
    }

    result
  }
}
