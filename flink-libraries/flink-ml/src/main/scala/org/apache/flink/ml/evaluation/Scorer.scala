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
package org.apache.flink.ml.evaluation

import org.apache.flink.api.scala._
import org.apache.flink.ml.common.{ParameterMap, WithParameters}
import org.apache.flink.ml.pipeline._

//TODO: Need to generalize type of Score (and evaluateOperation)
class Scorer[
  PredictorType[PredictorInstanceType] <: WithParameters,
  PreparedTesting
] (val score: Score[PredictorType, PreparedTesting])
  extends WithParameters {
  def evaluate[InputTesting, PredictorInstance <: PredictorType[PredictorInstance]](
      testing: DataSet[InputTesting],
      predictorInstance: PredictorInstance,
      evaluateParameters: ParameterMap = ParameterMap.Empty)(implicit
    prepareOperation: PrepareOperation[PredictorInstance, InputTesting, PreparedTesting]):
    DataSet[Double] = {

    val resultingParameters = predictorInstance.parameters ++ evaluateParameters
    val preparedTesting = prepareOperation.prepare(predictorInstance, testing, resultingParameters)
    score.evaluate(preparedTesting)
  }
}

object Scorer {

  /**
    * The default prepare operation for ranking evaluation.
    * Creates a tuple of the test [[DataSet]] and the rankings.
    */
  implicit def defaultRankingEvalPrepare[Instance <: RankingPredictor[Instance]]
  (implicit ev: RankingPredictOperation[Instance]) =
    new PrepareOperation[Instance,
      (Int,Int,Double), (DataSet[(Int,Int,Double)], DataSet[(Int,Int,Int)])] {
      override def prepare(als: Instance,
                           test: DataSet[(Int, Int, Double)],
                           parameters: ParameterMap):
      (DataSet[(Int, Int, Double)], DataSet[(Int, Int, Int)]) = {
        (test, als.evaluateRankings(test, parameters))
      }
    }
}
