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

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._
import org.apache.flink.ml._

import scala.reflect.ClassTag

/**
 * Evaluation score
 *
 * Takes a whole data set and then computes the evaluation score on them (obviously, again encoded
 * in a DataSet)
 *
 * @tparam PredictionType output type
 */
trait Score[PredictionType] {
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
class MeanScore[PredictionType: TypeInformation: ClassTag](
    scoringFct: (PredictionType, PredictionType) => Double)
    (implicit yyt: TypeInformation[(PredictionType, PredictionType)])
  extends Score[PredictionType] with Serializable {
  def evaluate(trueAndPredicted: DataSet[(PredictionType, PredictionType)]): DataSet[Double] =
    trueAndPredicted.map(yy => scoringFct(yy._1, yy._2)).mean()
}



object Score {
  /**
   * Squared loss function
   *
   * returns (y1 - y2)'
   *
   * @return a Loss object
   */
  def squaredLoss = new MeanScore[Double]((y1,y2) => (y1 - y2) * (y1 - y2)) with Loss

  /**
   * Zero One Loss Function
   *
   * returns 1 if outputs differ and 0 if they are equal
   *
   * @tparam T output type
   * @return a Loss object
   */
  def zeroOneLoss[T: TypeInformation: ClassTag] = {
    //TODO: If T == Double, == comparison could be problematic
    new MeanScore[T]((y1, y2) => if (y1 == y2) 0 else 1) with PerformanceScore
  }

  /**
   * Zero One Loss Function also usable for score information
   *
   * returns 1 if sign of outputs differ and 0 if the signs are equal
   *
   * @return a Loss object
   */
  def zeroOneSignumLoss = new MeanScore[Double]({ (y1, y2) =>
    val sy1 = scala.math.signum(y1)
    val sy2 = scala.math.signum(y2)
    if (sy1 == sy2) 0 else 1
  }) with PerformanceScore

}
