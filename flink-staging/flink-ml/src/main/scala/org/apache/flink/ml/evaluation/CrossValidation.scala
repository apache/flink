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
import org.apache.flink.ml.RichDataSet
import java.util.Random

import org.apache.flink.ml.pipeline.{EvaluateDataSetOperation, FitOperation, Predictor}

object CrossValidation {
  def crossValScore[P <: Predictor[P], T](
      predictor: P,
      data: DataSet[T],
      scorerOption: Option[Scorer] = None,
      cv: FoldGenerator = KFold(),
      seed: Long = new Random().nextLong())(implicit fitOperation: FitOperation[P, T],
      evaluateDataSetOperation: EvaluateDataSetOperation[P, T, Double]): Array[DataSet[Double]] = {
    val folds = cv.folds(data, 1)

    val scores = folds.map {
      case (training: DataSet[T], testing: DataSet[T]) =>
        predictor.fit(training)
        if (scorerOption.isEmpty) {
          predictor.score(testing)
        } else {
          val s = scorerOption.get
          s.evaluate(testing, predictor)
        }
    }
    // TODO: Undecided on the return type: Array[DS[Double]] or DS[Double] i.e. reduce->union?
    // Or: Return mean and std?
    scores//.reduce((right: DataSet[Double], left: DataSet[Double]) => left.union(right)).mean()
  }
}

abstract class FoldGenerator {

  /** Takes a DataSet as input and creates splits (folds) of the data into
    * (training, testing) pairs.
    *
    * @param input The DataSet that will be split into folds
    * @param seed Seed for replicable splitting of the data
    * @tparam T The type of the DataSet
    * @return An Array containing K (training, testing) tuples, where training and testing are
    *         DataSets
    */
  def folds[T](
      input: DataSet[T],
      seed: Long = new Random().nextLong()): Array[(DataSet[T], DataSet[T])]
}

class KFold(numFolds: Int) extends FoldGenerator{

  /** Takes a DataSet as input and creates K splits (folds) of the data into non-overlapping
    * (training, testing) pairs.
    *
    * Code based on Apache Spark implementation
    * @param input The DataSet that will be split into folds
    * @param seed Seed for replicable splitting of the data
    * @tparam T The type of the DataSet
    * @return An Array containing K (training, testing) tuples, where training and testing are
    *         DataSets
    */
  override def folds[T](
      input: DataSet[T],
      seed: Long = new Random().nextLong()): Array[(DataSet[T], DataSet[T])] = {
    val numFoldsF = numFolds.toFloat
    (1 to numFolds).map { fold =>
      val lb = (fold - 1) / numFoldsF
      val ub = fold / numFoldsF
      val validation = input.sampleBounded(lb, ub, complement = false, seed = seed)
      val training = input.sampleBounded(lb, ub, complement = true, seed = seed)
      (training, validation)
    }.toArray
  }
}

object KFold {
  def apply(): KFold = new KFold(10) //TODO: Keep default of 10?
  def apply(folds: Int): KFold = new KFold(folds)
}
