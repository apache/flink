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

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.{DataSet, _}
import org.apache.flink.ml._
import org.apache.flink.ml.common._
import org.apache.flink.ml.math.{Vector => FlinkVector}
import org.apache.flink.ml.recommendation.ALS
import org.apache.flink.ml.recommendation.ALS.Factors
import org.apache.flink.util.Collector

import scala.collection.mutable

/** Predictor trait for Flink's pipeline operators.
  *
  * A [[Predictor]] calculates predictions for testing data based on the model it learned during
  * the fit operation (training phase). In order to do that, the implementing class has to provide
  * a [[FitOperation]] and a [[PredictDataSetOperation]] implementation for the correct types. The
  * implicit values should be put into the scope of the companion object of the implementing class
  * to make them retrievable for the Scala compiler.
  *
  * The pipeline mechanism has been inspired by scikit-learn
  *
  * @tparam Self Type of the implementing class
  */
trait Predictor[Self] extends Estimator[Self] with WithParameters {
  that: Self =>

  /** Predict testing data according the learned model. The implementing class has to provide
    * a corresponding implementation of [[PredictDataSetOperation]] which contains the prediction
    * logic.
    *
    * @param testing Testing data which shall be predicted
    * @param predictParameters Additional parameters for the prediction
    * @param predictor [[PredictDataSetOperation]] which encapsulates the prediction logic
    * @tparam Testing Type of the testing data
    * @tparam Prediction Type of the prediction data
    * @return
    */
  def predict[Testing, Prediction](
      testing: DataSet[Testing],
      predictParameters: ParameterMap = ParameterMap.Empty)(implicit
      predictor: PredictDataSetOperation[Self, Testing, Prediction])
    : DataSet[Prediction] = {
    FlinkMLTools.registerFlinkMLTypes(testing.getExecutionEnvironment)
    predictor.predictDataSet(this, predictParameters, testing)
  }

  /** Computes a prediction value for each example in the testing data and returns a pair of true
    * label value and prediction value. It is important that the implementation chooses a Testing
    * type from which it can extract the true label value.
    *
    * @param testing
    * @param evaluateParameters
    * @param evaluator
    * @tparam Testing
    * @tparam PredictionValue
    * @return
    */
  def evaluate[Testing, PredictionValue](
      testing: DataSet[Testing],
      evaluateParameters: ParameterMap = ParameterMap.Empty)
      (implicit evaluator: EvaluateDataSetOperation[Self, Testing, PredictionValue])
    : DataSet[(PredictionValue, PredictionValue)] = {
    FlinkMLTools.registerFlinkMLTypes(testing.getExecutionEnvironment)
    evaluator.evaluateDataSet(this, evaluateParameters, testing)
  }
}

trait RankingPredictor[Self] extends Estimator[Self] with WithParameters {
  that: Self =>

  def predictRankings(
    k: Int,
    users: DataSet[Int],
    predictParameters: ParameterMap = ParameterMap.Empty)(implicit
    rankingPredictOperation : RankingPredictOperation[Self])
  : DataSet[(Int,Int,Int)] =
    rankingPredictOperation.predictRankings(this, k, users, predictParameters)

  def evaluateRankings(
    testing: DataSet[(Int,Int,Double)],
    evaluateParameters: ParameterMap = ParameterMap.Empty)(implicit
    rankingPredictOperation : RankingPredictOperation[Self])
  : DataSet[(Int,Int,Int)] = {
    // todo: do not burn 100 topK into code
    predictRankings(100, testing.map(_._1).distinct(), evaluateParameters)
  }
}

trait RankingPredictOperation[Instance] {
  def predictRankings(
    instance: Instance,
    k: Int,
    users: DataSet[Int],
    predictParameters: ParameterMap = ParameterMap.Empty)
  : DataSet[(Int, Int, Int)]
}

/**
  * Trait for providing auxiliary data for ranking evaluations.
  *
  * They are useful e.g. for excluding items found in the training [[DataSet]]
  * from the recommended top K items.
  */
trait TrainingRatingsProvider {

  def getTrainingData: DataSet[(Int, Int, Double)]

  /**
    * Retrieving the training items.
    * Although this can be calculated from the training data, it requires a costly
    * [[DataSet.distinct]] operation, while in matrix factor models the set items could be
    * given more efficiently from the item factors.
    */
  def getTrainingItems: DataSet[Int] = {
    getTrainingData.map(_._2).distinct()
  }
}

/**
  * Ranking predictions for the most common case.
  * If we can predict ratings, we can compute top K lists by sorting the predicted ratings.
  */
class RankingFromRatingPredictOperation[Instance <: TrainingRatingsProvider]
(val ratingPredictor: PredictDataSetOperation[Instance, (Int, Int), (Int, Int, Double)])
  extends RankingPredictOperation[Instance] {

  private def getUserItemPairs(users: DataSet[Int], items: DataSet[Int], exclude: DataSet[(Int, Int)])
  : DataSet[(Int, Int)] = {
    users.cross(items)
      .leftOuterJoin(exclude).where(0, 1).equalTo(0, 1)
      .apply((l, r, o: Collector[(Int, Int)]) => {
        Option(r) match {
          case Some(_) => ()
          case None => o.collect(l)
        }
      })
  }

  private def rankingsFromRatings(
                   topK: Int,
                   scores: DataSet[(Int, Int, Double)])
  : DataSet[(Int, Int, Int)] = {
    scores
      .groupBy(0)
      .combineGroup((elements, collector: Collector[(Int, Array[(Int, Double)])]) => {
        val bufferedElements = elements.buffered
        val head = bufferedElements.head
        var topKitems = mutable.SortedSet[(Int, Int, Double)]()(
          Ordering[(Double, Int)].reverse.on(x => (x._3, x._2)))
        for (e <- bufferedElements) {
          topKitems += e
          if (topKitems.size > topK) {
            topKitems = topKitems.dropRight(1)
          }
        }
        collector.collect((head._1, topKitems.toArray.map(x => (x._2, x._3))))
      })
      .withForwardedFields("0")
      .groupBy(0)
      .reduce((l, r) => {
        var topKitems = mutable.SortedSet[(Int, Double)]()(
          Ordering[(Double, Int)].reverse.on(x => (x._2, x._1)))
        topKitems ++= l._2 ++= r._2
        (l._1, topKitems.take(topK).toArray)
      })
      .flatMap(x => for (e <- x._2.zip(1 to topK)) yield (x._1, e._1._1, e._2))
  }

  override def predictRankings(
                                instance: Instance,
                                k: Int,
                                users: DataSet[Int],
                                predictParameters: ParameterMap = ParameterMap.Empty)
  : DataSet[(Int, Int, Int)] = {
    val trainData = instance.getTrainingData
    val items = instance.getTrainingItems
    val exclude: DataSet[(Int, Int)] =
      if (predictParameters.get(RankingPredictor.ExcludeKnown).getOrElse(false))
        trainData.map(x => (x._1, x._2))
      else
        trainData.getExecutionEnvironment.fromCollection(Seq())
    val userItemPairs = getUserItemPairs(users, items, exclude)
    val scores = ratingPredictor.predictDataSet(instance, predictParameters, userItemPairs)
    this.rankingsFromRatings(k, scores)
  }
}

object RankingPredictor {

  case object ExcludeKnown extends Parameter[Boolean] {
    val defaultValue: Option[Boolean] = Some(true)
  }

}

object Predictor {

  /** Default [[PredictDataSetOperation]] which takes a [[PredictOperation]] to calculate a tuple
    * of testing element and its prediction value.
    *
    * Note: We have to put the TypeInformation implicit values for Testing and PredictionValue after
    * the PredictOperation implicit parameter. Otherwise, if it's defined as a context bound, then
    * the Scala compiler does not find the implicit [[PredictOperation]] value.
    *
    * @param predictOperation
    * @param testingTypeInformation
    * @param predictionValueTypeInformation
    * @tparam Instance
    * @tparam Model
    * @tparam Testing
    * @tparam PredictionValue
    * @return
    */
  implicit def defaultPredictDataSetOperation[
      Instance <: Estimator[Instance],
      Model,
      Testing,
      PredictionValue](
      implicit predictOperation: PredictOperation[Instance, Model, Testing, PredictionValue],
      testingTypeInformation: TypeInformation[Testing],
      predictionValueTypeInformation: TypeInformation[PredictionValue])
    : PredictDataSetOperation[Instance, Testing, (Testing, PredictionValue)] = {
    new PredictDataSetOperation[Instance, Testing, (Testing, PredictionValue)] {
      override def predictDataSet(
          instance: Instance,
          predictParameters: ParameterMap,
          input: DataSet[Testing])
        : DataSet[(Testing, PredictionValue)] = {
        val resultingParameters = instance.parameters ++ predictParameters

        val model = predictOperation.getModel(instance, resultingParameters)

        implicit val resultTypeInformation = createTypeInformation[(Testing, PredictionValue)]

        input.mapWithBcVariable(model){
          (element, model) => {
            (element, predictOperation.predict(element, model))
          }
        }
      }
    }
  }

  /** Default [[EvaluateDataSetOperation]] which takes a [[PredictOperation]] to calculate a tuple
    * of true label value and predicted label value.
    *
    * Note: We have to put the TypeInformation implicit values for Testing and PredictionValue after
    * the PredictOperation implicit parameter. Otherwise, if it's defined as a context bound, then
    * the Scala compiler does not find the implicit [[PredictOperation]] value.
    *
    * @param predictOperation
    * @param testingTypeInformation
    * @param predictionValueTypeInformation
    * @tparam Instance
    * @tparam Model
    * @tparam Testing
    * @tparam PredictionValue
    * @return
    */
  implicit def defaultEvaluateDataSetOperation[
      Instance <: Estimator[Instance],
      Model,
      Testing,
      PredictionValue](
      implicit predictOperation: PredictOperation[Instance, Model, Testing, PredictionValue],
      testingTypeInformation: TypeInformation[Testing],
      predictionValueTypeInformation: TypeInformation[PredictionValue])
    : EvaluateDataSetOperation[Instance, (Testing, PredictionValue), PredictionValue] = {
    new EvaluateDataSetOperation[Instance, (Testing, PredictionValue), PredictionValue] {
      override def evaluateDataSet(
          instance: Instance,
          evaluateParameters: ParameterMap,
          testing: DataSet[(Testing, PredictionValue)])
        : DataSet[(PredictionValue,  PredictionValue)] = {
        val resultingParameters = instance.parameters ++ evaluateParameters
        val model = predictOperation.getModel(instance, resultingParameters)

        implicit val resultTypeInformation = createTypeInformation[(Testing, PredictionValue)]

        testing.mapWithBcVariable(model){
          (element, model) => {
            (element._2, predictOperation.predict(element._1, model))
          }
        }
      }
    }
  }

  /** [[EvaluateDataSetOperation]] which takes a [[PredictOperation]] to calculate a tuple
    * of true label value and predicted label value, when provided with a DataSet of
    * [[LabeledVector]].
    *
    * @param predictOperation An implicit PredictOperation that takes a Flink Vector and returns
    *                         a Double
    * @tparam Instance The [[Predictor]] instance that calls the function
    * @tparam Model The model that the calling [[Predictor]] uses for predictions
    * @return An EvaluateDataSetOperation for LabeledVector
    */
  implicit def LabeledVectorEvaluateDataSetOperation[Instance <: Predictor[Instance],Model]
      (implicit predictOperation: PredictOperation[Instance, Model, FlinkVector, Double])
    : EvaluateDataSetOperation[Instance, LabeledVector, Double] = {
    new EvaluateDataSetOperation[Instance, LabeledVector, Double] {
      override def evaluateDataSet(
          instance: Instance,
          evaluateParameters: ParameterMap,
          testing: DataSet[LabeledVector])
        : DataSet[(Double,  Double)] = {
        val resultingParameters = instance.parameters ++ evaluateParameters
        val model = predictOperation.getModel(instance, resultingParameters)

        testing.mapWithBcVariable(model){
          (element, model) => {
            (element.label, predictOperation.predict(element.vector, model))
          }
        }
      }
    }
  }

}

/** Type class for the predict operation of [[Predictor]]. This predict operation works on DataSets.
  *
  * [[Predictor]]s either have to implement this trait or the [[PredictOperation]] trait. The
  * implementation has to be made available as an implicit value or function in the scope of
  * their companion objects.
  *
  * The first type parameter is the type of the implementing [[Predictor]] class so that the Scala
  * compiler includes the companion object of this class in the search scope for the implicit
  * values.
  *
  * @tparam Self Type of [[Predictor]] implementing class
  * @tparam Testing Type of testing data
  * @tparam Prediction Type of predicted data
  */
trait PredictDataSetOperation[Self, Testing, Prediction] extends Serializable{

  /** Calculates the predictions for all elements in the [[DataSet]] input
    *
    * @param instance The Predictor instance that we will use to make the predictions
    * @param predictParameters The parameters for the prediction
    * @param input The DataSet containing the unlabeled examples
    * @return
    */
  def predictDataSet(
      instance: Self,
      predictParameters: ParameterMap,
      input: DataSet[Testing])
    : DataSet[Prediction]
}

/** Type class for predict operation. It takes an element and the model and then computes the
  * prediction value for this element.
  *
  * It is sufficient for a [[Predictor]] to only implement this trait to support the evaluate and
  * predict method.
  *
  * @tparam Instance The concrete type of the [[Predictor]] that we will use for predictions
  * @tparam Model The representation of the predictive model for the algorithm, for example a
  *               Vector of weights
  * @tparam Testing The type of the example that we will use to make the predictions (input)
  * @tparam Prediction The type of the label that the prediction operation will produce (output)
  *
  */
trait PredictOperation[Instance, Model, Testing, Prediction] extends Serializable{

  /** Defines how to retrieve the model of the type for which this operation was defined
    *
    * @param instance The Predictor instance that we will use to make the predictions
    * @param predictParameters The parameters for the prediction
    * @return A DataSet with the model representation as its only element
    */
  def getModel(instance: Instance, predictParameters: ParameterMap): DataSet[Model]

  /** Calculates the prediction for a single element given the model of the [[Predictor]].
    *
    * @param value The unlabeled example on which we make the prediction
    * @param model The model representation of the prediciton algorithm
    * @return A label for the provided example of type [[Prediction]]
    */
  def predict(value: Testing, model: Model): Prediction
}

/**
  * Operation for preparing a testing [[DataSet]] for evaluation.
  *
  * The most commonly [[EvaluateDataSetOperation]] is used, but evaluation of
  * ranking recommendations need input in a different form.
  */
trait PrepareOperation[Instance, Testing, Prepared] extends Serializable {

  def prepare(
               als: Instance,
               test : DataSet[Testing],
               parameters : ParameterMap)
  : Prepared
}

/** Type class for the evaluate operation of [[Predictor]]. This evaluate operation works on
  * DataSets.
  *
  * It takes a [[DataSet]] of some type. For each element of this [[DataSet]] the evaluate method
  * computes the prediction value and returns a tuple of true label value and prediction value.
  *
  * @tparam Instance The concrete type of the Predictor instance that we will use to make the
  *                  predictions
  * @tparam Testing The type of the example that we will use to make the predictions (input)
  * @tparam Prediction The type of the label that the prediction operation will produce (output)
  *
  */
trait EvaluateDataSetOperation[Instance, Testing, Prediction] extends
  PrepareOperation[Instance, Testing, DataSet[(Prediction,Prediction)]] {

  override def prepare(als: Instance,
                       test: DataSet[Testing],
                       parameters: ParameterMap)
  : DataSet[(Prediction, Prediction)] =
    evaluateDataSet(als, parameters, test)

  def evaluateDataSet(
      instance: Instance,
      evaluateParameters: ParameterMap,
      testing: DataSet[Testing])
    : DataSet[(Prediction, Prediction)]
}
