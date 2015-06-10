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
package org.apache.flink.ml.clustering

import org.apache.flink.api.scala._
import org.apache.flink.test.util.FlinkTestBase
import org.scalatest.{Matchers, FlatSpec}

class GMMSuite extends FlatSpec with Matchers with FlinkTestBase{
  behavior of "The GMM implementation"

  it should "train a gmm model" in {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val trainingDS = env.fromCollection(GMMClustering.trainingData)
    val testingDS = env.fromCollection(GMMClustering.testingData)
    val gaussiansDS = env.fromCollection(GMMClustering.initialGaussians)

    val learner = GMM()
      .setInitialGaussians(gaussiansDS)
      .setNumGaussians(2)
      .setNumIterations(30)

    val gModel = learner.fit(trainingDS)
    val resultedPredictionsDS = gModel.transform(testingDS)
    val resultedGaussiansDS = gModel.gmm

    val expectedPredictions = env.fromCollection(GMMClustering.expectedPrediction)
    val expectedGaussians = env.fromCollection(GMMClustering.expectedGaussians)

    val gausResult = resultedGaussiansDS.join(expectedGaussians).where(l => l.id).equalTo(r => r.id)
      .map(pair => {
      val resC = pair._1
      val expC = pair._2
      (resC.prior, expC.prior, resC.mu, expC.mu)
    }).collect()

   /* val predResult = resultedPredictionsDS.join(expectedPredictions).where(l => (l.gaussianID, l.pointID)).equalTo(r => (r.gaussianID, r.pointID))
      .map(pair => {
      val resP = pair._1
      val expP = pair._2
      (resP.probability == expP.probability)
    }).collect()*/

    gausResult.foreach {
      case (gaussian) =>
        gaussian._1 should be(gaussian._2)
        gaussian._3.valuesIterator.zip(gaussian._4.valuesIterator).foreach {
          case (resultMu, expectedMu) =>
            resultMu should be(expectedMu)
        }
    }

    /*predResult.foreach { case (prediction) =>
      prediction should be(true)

    }*/
  }
}
