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
import org.apache.flink.ml.common.ParameterMap
import org.apache.flink.ml.pipeline.{PredictDataSetOperation, RankingFromRatingPredictOperation,
RankingPredictor, TrainingRatingsProvider}
import org.apache.flink.ml.util.FlinkTestBase
import org.scalatest.{FlatSpec, Matchers}

class RankingScoreITSuite extends FlatSpec with Matchers with FlinkTestBase {

  def log2(d : Double) = scala.math.log(d)/scala.math.log(2)

  behavior of "ndcgScore"

  it should "calculate idcgs correctly" in {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val test = env.fromCollection(Seq(
      (1,1,0.7),
      (1,2,0.9),
      (1,3,0.8),
      (1,4,1.0),
      (2,1,0.9),
      (2,2,0.6)
    ))
    val idcgs = RankingScores.ndcgScore(3).calculateIdcgs(test).collect()
    idcgs.toSet shouldEqual Set(
      (1, 1/log2(2) + 0.9/log2(3) + 0.8/log2(4) ),
      (2, 0.9/log2(2) + 0.6/log2(3))
    )
  }
  it should "calculate dcgs correctly" in {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val test = env.fromCollection(Seq(
      (1,1,0.7),
      (1,2,0.9),
      (1,3,0.8),
      (1,4,1.0),
      (2,1,0.9),
      (2,2,0.6),
      (3,1,0.9)
    ))
    val predictions = env.fromCollection(Seq(
      (1,10,1),
      (1,3,2),
      (2,5,1),
      (2,6,2),
      (3,1,1),
      (3,2,2)
    ))
    val dcgs = RankingScores.ndcgScore(4).calculateDcgs(predictions, test).collect()
    dcgs.toSet shouldEqual Set(
      (1, 0.8/log2(3) ),
      (2, 0.0),
      (3, 0.9/log2(2) )
    )
  }
  it should "calculate ndcgs correctly" in {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val idcgs = env.fromCollection(Seq(
      (1,2.0),
      (2,4.0),
      (3,1.0),
      (4,2.5),
      (5,3.0)
    ))
    val dcgs = env.fromCollection(Seq(
      (1,1.0),
      (2,0.0),
      (3,1.0)
    ))
    val ndcgs = RankingScores.ndcgScore(4).calculateNdcgs(dcgs, idcgs).collect()
    ndcgs.toSet shouldEqual Set(
      (1,0.5),
      (2,0.0),
      (3,1.0)
    )
  }
  it should "calculate ndcg correctly" in {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val test = env.fromCollection(Seq(
      (1,1,0.7),
      (1,2,0.9),
      (1,3,0.8),
      (1,4,1.0),
      (2,1,0.9),
      (2,2,0.6),
      (3,1,0.9)
    ))
    val predictions = env.fromCollection(Seq(
      (1,10,1),
      (1,3,2),
      (2,5,1),
      (2,6,2),
      (3,1,1),
      (3,2,2)
    ))
    val ndcgs = RankingScores.ndcgScore(4).evaluate((test, predictions)).collect()
    ndcgs.toSet shouldEqual Set(
      (
        (0.8/log2(3))/(1.0/log2(2) + 0.9/log2(3) + 0.8/log2(4) + 0.7/log2(5)) + //item 1 dcg/idcg
        0 + //item 2 dcg/idcg
        (0.9/log2(2))/(0.9/log2(2)) //item 3 dcg/idcg
      ) / 3
    )
  }

  behavior of "precision and recall scores"

  it should "join recommendations with test correctly" in {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val recommendations = env.fromCollection(Seq(
      (1,1,1),
      (1,2,2),
      (1,3,3),
      (2,4,1),
      (2,5,2),
      (2,1,3)
    ))
    val test = env.fromCollection(Seq(
      (1,1,1.0),
      (1,10,2.0),
      (2,1,1.0),
      (2,3,1.0),
      (2,12,1.0),
      (2,5,1.0),
      (2,13,1.0),
      (2,7,1.0)
    ))
    val joined = RankingScores.PrecisionAndRecallScoresUtilities.joinWithTest(recommendations, test)
      .collect()
    joined.toSet shouldEqual Set(
      (1,1,Some(1),"true_positive"),
      (1,2,Some(2),"false_positive"),
      (1,3,Some(3),"false_positive"),
      (1,10,None,"false_negative"),
      (2,1,Some(3),"true_positive"),
      (2,3,None,"false_negative"),
      (2,4,Some(1),"false_positive"),
      (2,5,Some(2),"true_positive"),
      (2,7,None,"false_negative"),
      (2,12,None,"false_negative"),
      (2,13,None,"false_negative")
    )
  }
  it should "count the types per user correctly" in {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val setToCount = env.fromCollection(Seq(
      (1,1,Some(1),"true_positive"),
      (1,2,Some(2),"false_positive"),
      (1,3,Some(3),"false_positive"),
      (1,10,None,"false_negative"),
      (2,1,Some(3),"true_positive"),
      (2,3,None,"false_negative"),
      (2,4,Some(1),"false_positive"),
      (2,5,Some(2),"true_positive"),
      (2,7,None,"false_negative"),
      (2,12,None,"false_negative"),
      (2,13,None,"false_negative")
    ))
    val counted = RankingScores.PrecisionAndRecallScoresUtilities.countTypes(setToCount).collect()
    counted.toSet shouldEqual Set(
      (1,1,2,1),
      (2,2,1,4)
    )
  }
  it should "calculate precisions correctly" in {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val recommendations = env.fromCollection(Seq(
      (1,1,1),
      (1,2,2),
      (1,3,3),
      (2,4,1),
      (2,5,2),
      (2,1,3)
    ))
    val test = env.fromCollection(Seq(
      (1,1,1.0),
      (1,10,2.0),
      (2,1,1.0),
      (2,3,1.0),
      (2,12,1.0),
      (2,5,1.0),
      (2,13,1.0),
      (2,7,1.0)
    ))
    val precisions = RankingScores.PrecisionAndRecallScoresUtilities
      .precisions(recommendations, test).collect()
    precisions.toSet shouldEqual Set(
      (1,1.0/3.0),
      (2,2.0/3.0)
    )
  }
  it should "calculate recalls correctly" in {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val recommendations = env.fromCollection(Seq(
      (1,1,1),
      (1,2,2),
      (1,3,3),
      (2,4,1),
      (2,5,2),
      (2,1,3)
    ))
    val test = env.fromCollection(Seq(
      (1,1,1.0),
      (1,10,2.0),
      (2,1,1.0),
      (2,3,1.0),
      (2,12,1.0),
      (2,5,1.0),
      (2,13,1.0),
      (2,7,1.0)
    ))
    val recalls = RankingScores.PrecisionAndRecallScoresUtilities.recalls(recommendations, test)
      .collect()
    recalls.toSet shouldEqual Set(
      (1,1.0/2.0),
      (2,2.0/6.0)
    )
  }
  it should "count the types per user up to k correctly" in {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val setToCount = env.fromCollection(Seq(
      (1,1,Some(1),"true_positive"),
      (1,2,None,"false_negative"),
      (1,3,Some(3),"true_positive"),
      (1,10,Some(2),"false_positive"),
      (2,1,Some(3),"true_positive"),
      (2,3,None,"false_negative"),
      (2,4,Some(1),"false_positive"),
      (2,5,Some(2),"true_positive"),
      (2,7,None,"false_negative"),
      (2,12,None,"false_negative"),
      (2,13,None,"false_negative")
    ))
    val counted = RankingScores.PrecisionAndRecallScoresUtilities.countTypesUpToK(setToCount)
      .collect()
    counted.toSet shouldEqual Set(
      (1,1,1,0,2),
      (1,2,1,1,2),
      (1,3,2,1,1),
      (2,1,0,1,6),
      (2,2,1,1,5),
      (2,3,2,1,4)
    )
  }
  it should "calculate mean average precision per user correctly" in {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val recommendations = env.fromCollection(Seq(
      (1,1,1),
      (1,2,2),
      (1,3,3),
      (2,4,1),
      (2,5,2),
      (2,1,3)
    ))
    val test = env.fromCollection(Seq(
      (1,1,1.0),
      (1,10,2.0),
      (2,1,1.0),
      (2,3,1.0),
      (2,12,1.0),
      (2,5,1.0),
      (2,13,1.0),
      (2,7,1.0)
    ))
    val precisionAndRecall = RankingScores
      .PrecisionAndRecallScoresUtilities
      .meanAveragePrecisionAndRecall(recommendations, test).collect()

    //    (1,1,1,0,1)
    //    (1,2,1,1,1)
    //    (1,3,1,2,1)
    //    (2,1,0,1,6)
    //    (2,2,1,1,5)
    //    (2,3,2,1,4)

    precisionAndRecall.toSet shouldEqual Set(
      (1, (1.0/1.0+1.0/2.0+1.0/3.0)/3, (1.0/2.0 + 1.0/2.0 + 1.0/2.0)/3),
      (2, (0.0/1.0+1.0/2.0+2.0/3.0)/3, (0.0/6.0 + 1.0/6.0 + 2.0/6.0)/3)
    )
  }

  behavior of "ranking scores and scorer"
  it should "be possible to evaluate a RankingPredictor using a scorer" in {
    val env = ExecutionEnvironment.getExecutionEnvironment

    class MockFromRatingPredictor extends RankingPredictor[MockFromRatingPredictor]
      with TrainingRatingsProvider {
      override def getTrainingItems: DataSet[Int] =
        env.fromCollection(Seq(1,2,3))
      override def getTrainingData: DataSet[(Int, Int, Double)] =
        env.fromCollection(Seq((3,1,1.0),(3,2,1.0),(3,3,1.0),(3,4,1.0), (3,5,1.0)))
    }

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
        (2,2,0.8),
        (2,3,1.0)
      ))
    }

    implicit val rankingFromRatingPredictOperation =
      new RankingFromRatingPredictOperation(mockedPredictor)

    val predictor = new MockFromRatingPredictor

    val test = env.fromCollection(Seq(
      (1,1,1.0),
      (1,10,2.0),
      (2,1,1.0),
      (2,2,1.0)
    ))

    val scorer = new Scorer(RankingScores.ndcgScore(100))
    // todo: fix implicit scoping
    val result = scorer.evaluate(test, predictor, new ParameterMap)(
      Scorer.defaultRankingEvalPrepare)
    result.collect().toSet shouldEqual Set(
      (
        (1.0/log2(5))/(2.0/log2(2)+1.0/log2(3)) +
        (1.0/log2(3))/(1.0/log2(2)+1.0/log2(3))
      ) / 2
    )
  }
}
