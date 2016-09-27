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

import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._
import org.apache.flink.ml._
import org.apache.flink.ml.pipeline._
import org.apache.flink.ml.RichNumericDataSet
import org.apache.flink.util.Collector

import scala.reflect.ClassTag

trait Score[
  PredictorType[PredictorInstanceType],
  PreparedTesting
] {
  def evaluate(test: PreparedTesting): DataSet[Double]
}

abstract class RankingScore extends Score[
  RankingPredictor,
  (DataSet[(Int, Int, Double)], DataSet[(Int,Int,Int)])
  ] with Serializable{
  // We will either have to add a k:Int parameter into the test tuple,
  // count the predictions in the DataSet[Int,Int,Int], or add
  // scoreParameters to the Score trait, and there are reasons to do
  // both. Let's discuss this later.
  // todo: revisit this

  override def evaluate(testAndPredictions: (DataSet[(Int, Int, Double)], DataSet[(Int, Int, Int)]))
  : DataSet[Double]
}

/**
 * Evaluation score
 *
 * Can be used to calculate a performance score for an algorithm, when provided with a DataSet
 * of (truth, prediction) tuples
 *
 * @tparam PredictionType output type
 */
trait PairwiseScore[PredictionType] extends Score[
  Predictor,
  DataSet[(PredictionType, PredictionType)]
] {
  def evaluate(trueAndPredicted: DataSet[(PredictionType, PredictionType)]): DataSet[Double]
}

/** Traits to allow us to determine at runtime if a Score is a loss (lower is better) or a
  * performance score (higher is better)
  */
trait Loss

trait PerformanceScore

/**
 * Metrics expressible as a mean of a function taking output pairs as input
 *
 * @param scoringFct function to apply to all elements
 * @tparam PredictionType output type
 */
abstract class MeanScore[PredictionType: TypeInformation: ClassTag](
    scoringFct: (PredictionType, PredictionType) => Double)
    (implicit yyt: TypeInformation[(PredictionType, PredictionType)])
  extends PairwiseScore[PredictionType] with Serializable {

  override def evaluate(trueAndPredicted: DataSet[(PredictionType, PredictionType)]): DataSet[Double] = {
    trueAndPredicted.map(yy => scoringFct(yy._1, yy._2)).mean()
  }
}

/** Scores aimed at evaluating the performance of regression algorithms
  *
  */
object RegressionScores {
  /**
   * Mean Squared loss function
   *
   * Calculates (y1 - y2)^2^ and returns the mean.
   *
   * @return a Loss object
   */
  def squaredLoss = new MeanScore[Double]((y1,y2) => (y1 - y2) * (y1 - y2)) with Loss

  /**
   * Mean Zero One Loss Function also usable for score information
   *
   * Assigns 1 if sign of outputs differ and 0 if the signs are equal, and returns the mean
   *
   * @return a Loss object
   */
  def zeroOneSignumLoss = new MeanScore[Double]({ (y1, y2) =>
    val sy1 = y1.signum
    val sy2 = y2.signum
    if (sy1 == sy2) 0 else 1
  }) with Loss

  /** Calculates the coefficient of determination, $R^2^$
    *
    * $R^2^$ indicates how well the data fit the a calculated model
    * Reference: [[http://en.wikipedia.org/wiki/Coefficient_of_determination]]
    */
  def r2Score = new PairwiseScore[Double] with PerformanceScore {
    override def evaluate(trueAndPredicted: DataSet[(Double, Double)]): DataSet[Double] = {
      val onlyTrue = trueAndPredicted.map(truthPrediction => truthPrediction._1)
      val meanTruth = onlyTrue.mean()

      val ssRes = trueAndPredicted
        .map(tp => (tp._1 - tp._2) * (tp._1 - tp._2)).reduce(_ + _)
      val ssTot = onlyTrue
        .mapWithBcVariable(meanTruth) {
          case (truth: Double, meanTruth: Double) => (truth - meanTruth) * (truth - meanTruth)
        }.reduce(_ + _)

      val r2 = ssRes
        .mapWithBcVariable(ssTot) {
          case (ssRes: Double, ssTot: Double) =>
          // We avoid dividing by 0 and just assign 0.0
          if (ssTot == 0.0) {
            0.0
          }
          else {
            1 - (ssRes / ssTot)
          }
      }
      r2
    }
  }
}

/** Scores aimed at evaluating the performance of classification algorithms
  *
  */
object ClassificationScores {
  /** Calculates the fraction of correct predictions
    *
    */
  def accuracyScore = {
    new MeanScore[Double]((y1, y2) => if (y1.approximatelyEquals(y2)) 1 else 0)
      with PerformanceScore
  }

  /**
   * Mean Zero One Loss Function
   *
   * Assigns 1 if outputs differ and 0 if they are equal, and returns the mean.
   *
   * @return a Loss object
   */
  def zeroOneLoss = {
    new MeanScore[Double]((y1, y2) => if (y1.approximatelyEquals(y2)) 0 else 1) with Loss
  }
}

object RankingScores extends Serializable {
  def ndcgScore(topK: Int) =
    new RankingScore {
      // These could be private, however it makes sense to make them
      // user facing, too.
      // todo: revisit this
      def calculateIdcgs(test: DataSet[(Int, Int, Double)])
      : DataSet[(Int, Double)] = {
        test
          .groupBy(0)
          .sortGroup(2, Order.DESCENDING)
          .reduceGroup(elements => {
            val bufferedElements = elements.buffered
            val user = bufferedElements.head._1
            val idcg = bufferedElements
              .map(_._3)
              .zip((1 to topK).toIterator)
              .map { case (rel, rank) => rel / (scala.math.log(rank + 1) / scala.math.log(2)) }
              .sum
            (user, idcg)
          })
      }

      def calculateDcgs(predictions: DataSet[(Int, Int, Int)], test: DataSet[(Int, Int, Double)])
      : DataSet[(Int, Double)] =
        predictions
          .leftOuterJoin(test)
          .where(0, 1)
          .equalTo(0, 1)
          .apply((l, r) => (l, Option(r)))
          .groupBy(_._1._1)
          .reduceGroup(elements => {
            val bufferedElements = elements.toList
            val user = bufferedElements.head._1._1
            val dcg: Double = bufferedElements
              .map {
                case ((u1, i1, rank), Some((u2, i2, rel))) =>
                  rel / (scala.math.log(rank + 1) / scala.math.log(2))
                case (l, None) =>
                  0.0
              }
              .sum
            (user, dcg)
          })

      def calculateNdcgs(dcgs: DataSet[(Int, Double)], idcgs: DataSet[(Int, Double)])
      : DataSet[(Int, Double)] =
        idcgs
          .join(dcgs)
          .where(0)
          .equalTo(0)
          .apply((idcg, dcg) => (idcg._1, dcg._2 / idcg._2))

      def averageNdcg(test: DataSet[(Int, Int, Double)], rankingPredictions: DataSet[(Int, Int, Int)])
      : DataSet[Double] = {
        val idcgs = calculateIdcgs(test)
        val dcgs = calculateDcgs(rankingPredictions, test)
        val ndcgs = calculateNdcgs(dcgs, idcgs)
        ndcgs.map(_._2).mean()
      }

      override def evaluate(testAndPredictions: (DataSet[(Int, Int, Double)], DataSet[(Int, Int, Int)]))
      : DataSet[Double] = {
        val (test, predictions) = testAndPredictions
        averageNdcg(test, predictions)
      }
    }

  object PrecisionAndRecallScoresUtilities extends Serializable {
    // The reason that these are public facing functions is that if
    // one tries to calculate precision and also recall, the
    // heavy lifting has to be done twice, while the two metrics
    // calculate basically the same think, up to a certain .map()
    // at the end of the calculation. This way the users can
    // efficiently calculate these two metrics themselves without
    // calculating things twice. Flink calculates them at the same
    // time and returns a tuple - we cant do this, since the eval
    // api expects single double as return type.
    // todo: revisit this

    def joinWithTest(rankingPredictions: DataSet[(Int, Int, Int)], test: DataSet[(Int, Int, Double)])
    : DataSet[(Int, Int, Option[Int], String)] = {
      rankingPredictions
        .fullOuterJoin(test)
        .where(0, 1)
        .equalTo(0, 1)
        .apply((l, r, o: Collector[(Int, Int, Option[Int], String)]) => {
          (Option(l), Option(r)) match {
            // todo: use sealed trait instead of strings
            case (None, Some(rr)) => o.collect((rr._1, rr._2, None, "false_negative"))
            case (Some(ll), None) => o.collect((ll._1, ll._2, Some(ll._3), "false_positive"))
            case (Some(ll), Some(rr)) => o.collect((ll._1, ll._2, Some(ll._3), "true_positive"))
            case default => ()
          }
        })
    }

    def countTypes(joined: DataSet[(Int, Int, Option[Int], String)])
    : DataSet[(Int, Int, Int, Int)] = {
      joined
        .groupBy(0)
        .reduceGroup(elements => {
          val bufferedElements = elements.toList
          val user = bufferedElements.head._1
          var truePositive, falsePositive, falseNegative = 0
          for ((_, _, _, t) <- bufferedElements) {
            t match {
              case "true_positive" => truePositive += 1
              case "false_positive" => falsePositive += 1
              case "false_negative" => falseNegative += 1
            }
          }
          (user, truePositive, falsePositive, falseNegative)
        })
    }


    def countTypesUpToK(joined: DataSet[(Int, Int, Option[Int], String)])
    : DataSet[(Int, Int, Int, Int, Int)] =
      joined
        .map(x => x match {
          case (user, item, Some(rank), t) => (user, item, rank, t)
          case (user, item, None, t) => (user, item, 0, t)
        })
        .groupBy(0)
        .sortGroup(2, Order.ASCENDING)
        // todo: something else instead of strings and case classes instead of (long) tuples
        .reduceGroup((elements, collector: Collector[(Int, Int, Int, Int, Int)]) => {
        val bufferedElements = elements.toList
        val user = bufferedElements.head._1
        var truePositive, falsePositive, falseNegative = 0
        val allTruePositive = bufferedElements.count(_._4 == "true_positive")
        for ((_, _, rank, t) <- bufferedElements) {
          t match {
            case "true_positive" => truePositive += 1
            case "false_positive" => falsePositive += 1
            case "false_negative" => falseNegative += 1
          }
          if (rank != 0) {
            collector.collect((user, rank, truePositive, falsePositive, falseNegative + (allTruePositive - truePositive)))
          }
        }
      })

    def precisions(rankingPredictions: DataSet[(Int, Int, Int)], test: DataSet[(Int, Int, Double)])
    : DataSet[(Int, Double)] = {
      val countedPerUser = countTypes(joinWithTest(rankingPredictions, test))

      val calculatePrecision = (user: Int, truePositive: Int, falsePositive: Int, falseNegative: Int) =>
        truePositive.toDouble / (truePositive.toDouble + falsePositive.toDouble)

      countedPerUser.map(x => (x._1, calculatePrecision.tupled(x)))
    }

    def recalls(rankingPredictions: DataSet[(Int, Int, Int)], test: DataSet[(Int, Int, Double)])
    : DataSet[(Int, Double)] = {
      val countedPerUser = countTypes(joinWithTest(rankingPredictions, test))

      val calculateRecall = (user: Int, truePositive: Int, falsePositive: Int, falseNegative: Int) =>
        truePositive.toDouble / (truePositive.toDouble + falseNegative.toDouble)

      countedPerUser.map(x => (x._1, calculateRecall.tupled(x)))
    }

    def meanAveragePrecisionAndRecall(rankingPredictions: DataSet[(Int, Int, Int)], test: DataSet[(Int, Int, Double)])
    : DataSet[(Int, Double, Double)] = {
      val counted = countTypesUpToK(joinWithTest(rankingPredictions, test))

      val calculatePrecision = (user: Int, truePositive: Int, falsePositive: Int, falseNegative: Int) =>
        truePositive.toDouble / (truePositive.toDouble + falsePositive.toDouble)

      val calculateRecall = (user: Int, truePositive: Int, falsePositive: Int, falseNegative: Int) =>
        truePositive.toDouble / (truePositive.toDouble + falseNegative.toDouble)

      counted
        .map(x => (x._1, x._3, x._4, x._5))
        .map(x => (x._1, calculatePrecision.tupled(x), calculateRecall.tupled(x)))
        .groupBy(0)
        .reduceGroup(elements => {
          val bufferedElements = elements.toList
          (bufferedElements.head._1,
            bufferedElements.map(_._2).sum / bufferedElements.length,
            bufferedElements.map(_._3).sum / bufferedElements.length)
        })
    }
  }

  def precisionScore(topK: Int) = new RankingScore {
    override def evaluate(testAndPredictions: (DataSet[(Int, Int, Double)], DataSet[(Int, Int, Int)]))
    : DataSet[Double] = {
      val (test, predictions) = testAndPredictions
      val precs = PrecisionAndRecallScoresUtilities.precisions(predictions, test)
      precs.map(_._2).mean()
    }
  }

  def recallScore(topK: Int) = new RankingScore {
    override def evaluate(testAndPredictions: (DataSet[(Int, Int, Double)], DataSet[(Int, Int, Int)]))
    : DataSet[Double] = {
      val (test, predictions) = testAndPredictions
      val recs = PrecisionAndRecallScoresUtilities.recalls(predictions, test)
      recs.map(_._2).mean()
    }
  }

  def meanPrecisionScore(topK: Int) = new RankingScore {
    override def evaluate(testAndPredictions: (DataSet[(Int, Int, Double)], DataSet[(Int, Int, Int)]))
    : DataSet[Double] = {
      val (test, predictions) = testAndPredictions
      val meanAvgPrecAndRecall = PrecisionAndRecallScoresUtilities
        .meanAveragePrecisionAndRecall(predictions, test)
      meanAvgPrecAndRecall.map(_._2).mean()
    }
  }

  def meanRecallScore(topK: Int) = new RankingScore {
    override def evaluate(testAndPredictions: (DataSet[(Int, Int, Double)], DataSet[(Int, Int, Int)]))
    : DataSet[Double] = {
      val (test, predictions) = testAndPredictions
      val meanAvgPrecAndRecall = PrecisionAndRecallScoresUtilities
        .meanAveragePrecisionAndRecall(predictions, test)
      meanAvgPrecAndRecall.map(_._3).mean()
    }
  }
}
