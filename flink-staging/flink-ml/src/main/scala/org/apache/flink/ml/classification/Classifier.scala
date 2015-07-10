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

import org.apache.flink.api.scala._
import org.apache.flink.ml.common.LabeledVector
import org.apache.flink.ml.evaluation.{Scorer, ClassificationScores}
import org.apache.flink.ml.pipeline.{SimpleScoreDataSetOperation, EvaluateDataSetOperation, Predictor}

/** Trait that classification algorithms should implement
  *
  * @tparam Self Type of the implementing class
  */
trait Classifier[Self] extends Predictor[Self]{
  that: Self =>

}

object Classifier {

  implicit def ClassifierSimpleScoreDataSetOperation[Instance <: Classifier[Instance]]
      (implicit evaluateOperation: EvaluateDataSetOperation[Instance, LabeledVector, Double])
    : SimpleScoreDataSetOperation[Instance, LabeledVector, Double] = {
    new SimpleScoreDataSetOperation[Instance, LabeledVector, Double] {
      override def simpleScoreDataSet(
          instance: Instance,
          testing: DataSet[LabeledVector]): DataSet[Double] = {
        val scorer = Scorer(ClassificationScores.accuracyScore)
        scorer.evaluate(testing, instance)
      }
    }
  }
}
