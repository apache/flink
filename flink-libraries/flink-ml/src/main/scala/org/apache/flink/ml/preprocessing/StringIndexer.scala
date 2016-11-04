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

package org.apache.flink.ml.preprocessing

import org.apache.flink.api.scala._
import org.apache.flink.ml.common.{Parameter, ParameterMap}
import org.apache.flink.ml.pipeline.{FitOperation, TransformDataSetOperation, Transformer}
import org.apache.flink.ml.preprocessing.StringIndexer.HandleInvalid
import org.apache.flink.api.scala.utils._
import org.apache.flink.api.scala.extensions.acceptPartialFunctions
import scala.collection.immutable.Seq

/**
  * StringIndexer maps a DataSet[String] to a DataSet[(String,Int)] where each label is
  * associated with an index.The indices are in [0,numLabels) and are ordered by label
  * frequencies. The most frequent label has index 0.
  *
  * @example
  * {{{
  *                    val trainingDS: DataSet[String] = env.fromCollection(data)
  *                    val transformer = StringIndexer().setHandleInvalid("skip")
  *
  *                    transformer.fit(trainingDS)
  *                    val transformedDS = transformer.transform(trainingDS)
  * }}}
  *
  *
  * You can manage unseen labels using HandleInvalid parameter. If HandleInvalid is
  * set to "skip" (see example),then each line containing an unseen label is skipped.
  * Otherwise an exception is raised.
  *
  * =Parameters=
  *
  * -[[HandleInvalid]]: Define how to handle unseen labels: by default is "skip"
  *
  *
  */
class StringIndexer extends Transformer[StringIndexer] {

  private[preprocessing] var metricsOption: Option[DataSet[(String, Long)]] = None


  /**
    * Set the value to handle unseen labels
    * @param value set to "skip" if you want to filter line with unseen labels
    * @return StringIndexer instance with HandleInvalid value
    */
  def setHandleInvalid(value: String): this.type ={
    parameters.add( HandleInvalid, value )
    this
  }

}

object StringIndexer {

  case object HandleInvalid extends Parameter[String] {
    val defaultValue: Option[String] = Some( "skip" )
  }

  // ==================================== Factory methods ========================================

  def apply(): StringIndexer ={
    new StringIndexer( )
  }

  // ====================================== Operations ===========================================

  /**
    * Trains [[StringIndexer]] by learning the count of each labels in the input DataSet.
    *
    * @return [[FitOperation]] training the [[StringIndexer]] on string labels
    */
  implicit def fitStringIndexer ={
    new FitOperation[StringIndexer, String] {
      def fit(instance: StringIndexer, fitParameters: ParameterMap,
        input: DataSet[String]): Unit ={
        val metrics = extractIndices( input )
        instance.metricsOption = Some( metrics )
      }
    }
  }

  /**
    * Count the frequency of each label, sort them in a decreasing order and assign an index
    *
    * @param input input Dataset containing labels
    * @return a map that returns for each label (key) its index (value)
    */
  private def extractIndices(input: DataSet[String]): DataSet[(String, Long)] ={

    val mapping = input
      .mapWith( s => (s, 1) )
      .groupBy( 0 )
      .reduce( (a, b) => (a._1, a._2 + b._2) )
      .partitionByRange( 1 )
      .zipWithIndex
      .mapWith { case (id, (label, count)) => (label, id) }

    mapping

  }

  /**
    * [[TransformDataSetOperation]] which returns a new dataset with an index added for
    * each label. If you encounter a label that has not be fitted during FitOperation,
    * line is skipped if setHandleInvalid is set to "skip".
    * Otherwise, an error is thrown.
    *
    * @return a new DataSet[(String,Int)]
    */
  implicit def transformStringDataset ={
    new TransformDataSetOperation[StringIndexer, String, (String, Int)] {
      def transformDataSet(instance: StringIndexer,
        transformParameters: ParameterMap,
        input: DataSet[String]) ={

        val resultingParameters = instance.parameters ++ transformParameters
        val handleInvalid = resultingParameters( HandleInvalid )

        def toHandle(label: String) = handleInvalid match {
          case "skip" => Seq.empty[(String, Int)]
          case _ => throw new Exception( s"label ${label} has not be fitted during " +
            s"the fit phase - " + s"Use setHandleInvalid with skip parameter to " +
            s"filter non fitted labels, or fit your data with label ${label}" )
        }

        instance.metricsOption match {
          case Some( metrics ) => {
            input.flatMap { l =>
              val count = metrics.get( l )
              count match {
                case Some( value ) => Seq( (l, value) )
                case None => toHandle( l )
              }
            }
          }
          case None =>
            throw new RuntimeException( "The StringIndexer has to be fitted to the data. " +
              "This is necessary to determine the count" )
        }
      }
    }
  }
}
