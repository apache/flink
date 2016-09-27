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

package org.apache.flink.ml.pipeline

import org.apache.flink.api.scala._
import org.apache.flink.ml.common.ParameterMap
import org.apache.flink.ml.util.FlinkTestBase
import org.scalatest.{FlatSpec, Matchers}

class RankingPredictorTest extends FlatSpec with Matchers with FlinkTestBase {

  behavior of "Evaluation of ranking prediction"
  it should "make ranking predictions correctly based on rating predictions" in {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val mockedPredictor = new PredictDataSetOperation[
      MockFromRatingPredictor, (Int, Int), (Int ,Int, Double)] {
      override def predictDataSet(
        instance: MockFromRatingPredictor,
        predictParameters: ParameterMap,
        input: DataSet[(Int, Int)])
      : DataSet[(Int, Int, Double)] = env.fromCollection(Seq(
        (1,1,0.7),
        (1,2,0.9),
        (1,3,0.8),
        (1,4,1.0),
        (2,1,0.9),
        (2,2,0.8),
        (2,3,1.0),
        (2,4,0.1),
        (2,5,0.9)
      ))
    }
    val rankingFromRatingPredictOperation = new RankingFromRatingPredictOperation(mockedPredictor)
    class MockFromRatingPredictor extends RankingPredictor[MockFromRatingPredictor]
      with TrainingRatingsProvider {
      override def getTrainingItems: DataSet[Int] =
        env.fromCollection(Seq(1,2,3))
      override def getTrainingData: DataSet[(Int, Int, Double)] =
        env.fromCollection(Seq((3,1,1.0),(3,2,1.0),(3,3,1.0),(3,4,1.0), (3,5,1.0)))
    }

    val mockPredictor = new MockFromRatingPredictor()
    val users = env.fromCollection(Seq(1,2))
    val rankings = mockPredictor.predictRankings(3,users,new ParameterMap)(
      rankingFromRatingPredictOperation).collect()
    rankings.toSet shouldBe Set(
      (1,4,1),
      (1,2,2),
      (1,3,3),
      (2,3,1),
      (2,5,2),
      (2,1,3)
    )
  }
  it should "use default itemsgetter if necessary" in {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val mockedPredictor = new PredictDataSetOperation[
      MockFromRatingPredictor, (Int, Int), (Int ,Int, Double)] {
      override def predictDataSet(
        instance: MockFromRatingPredictor,
        predictParameters: ParameterMap,
        input: DataSet[(Int, Int)])
      : DataSet[(Int, Int, Double)] = env.fromCollection(Seq(
        (1,1,0.7),
        (1,2,0.9),
        (1,3,0.8),
        (1,4,1.0),
        (2,1,0.9),
        (2,2,0.8),
        (2,3,1.0),
        (2,4,0.1),
        (2,5,0.9)
      ))
    }
    val rankingFromRatingPredictOperation = new RankingFromRatingPredictOperation(mockedPredictor)
    class MockFromRatingPredictor extends RankingPredictor[MockFromRatingPredictor]
      with TrainingRatingsProvider {
      override def getTrainingData: DataSet[(Int, Int, Double)] =
        env.fromCollection(Seq((3,1,1.0),(3,2,1.0),(3,3,1.0),(3,4,1.0), (3,5,1.0)))
    }

    val mockPredictor = new MockFromRatingPredictor()
    val users = env.fromCollection(Seq(1,2))
    val rankings = mockPredictor.predictRankings(3,users,new ParameterMap)(
      rankingFromRatingPredictOperation).collect()
    rankings.toSet shouldBe Set(
      (1,4,1),
      (1,2,2),
      (1,3,3),
      (2,3,1),
      (2,5,2),
      (2,1,3)
    )
  }
  it should "exclude pairs from ranking predictions if asked to" in {
    val env = ExecutionEnvironment.getExecutionEnvironment
    var calledArguments :DataSet[(Int,Int)] = null
    val mockedPredictor = new PredictDataSetOperation[
      MockPredictor, (Int, Int), (Int ,Int, Double)] {
      override def predictDataSet(
        instance: MockPredictor,
        predictParameters: ParameterMap,
        input: DataSet[(Int, Int)])
      : DataSet[(Int, Int, Double)] = {
        calledArguments = input
        env.fromCollection(Seq(
          (1,1,0.7),
          (1,2,0.9),
          (1,3,0.8),
          (1,4,1.0),
          (2,1,0.9),
          (2,2,0.8),
          (2,3,1.0),
          (2,4,0.1),
          (2,5,0.9)
        ))
      }
    }
    val rankingFromRatingPredictOperation = new RankingFromRatingPredictOperation(mockedPredictor)
    class MockPredictor extends RankingPredictor[MockPredictor] with TrainingRatingsProvider{
      override def getTrainingData: DataSet[(Int, Int, Double)] =
        env.fromCollection(Seq((3,1,1.0),(3,2,1.0),(2,3,1.0)))
      override def getTrainingItems: DataSet[Int] =
        env.fromCollection(Seq(1,2,3,4,5))
    }
    val mockPredictor = new MockPredictor()
    val users = env.fromCollection(Seq(1,2))
    val par = new ParameterMap
    mockPredictor.predictRankings(3,users,par)(rankingFromRatingPredictOperation).collect()
    calledArguments.collect().toSet shouldBe Set(
      (1,1),
      (1,2),
      (1,3),
      (1,4),
      (1,5),
      (2,1),
      (2,2),
      (2,4),
      (2,5)
    )
    par.add(RankingPredictor.ExcludeKnown, false)
    mockPredictor.predictRankings(3,users,par)(rankingFromRatingPredictOperation).collect()
    calledArguments.collect().toSet shouldBe Set(
      (1,1),
      (1,2),
      (1,3),
      (1,4),
      (1,5),
      (2,1),
      (2,2),
      (2,3),
      (2,4),
      (2,5)
    )
  }
}
