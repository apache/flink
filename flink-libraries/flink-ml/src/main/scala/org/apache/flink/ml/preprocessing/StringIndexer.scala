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

import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.extensions.acceptPartialFunctions
import org.apache.flink.ml.common.{Parameter, ParameterMap}
import org.apache.flink.ml.pipeline.{FitOperation, TransformDataSetOperation, Transformer}
import org.apache.flink.ml.preprocessing.StringIndexer.HandleInvalid
import org.apache.flink.api.scala.utils._

import scala.collection.immutable.Seq


/** Encodes a DataSet[String] of labels to a Dataset[(String,Long)] of (labels,indices).
  *
  * StringIndexer is the same than Spark's StringIndexer.
  * It takes a label of type String and maps it to a a tuple of type (String,Long),
  * where the first element is the label and the second element is the index of this label.
  * These indices are in [0,numLabels), ordered by label frequencies so that
  * the most frequent label has index 0.
  *
  * This transformer can only be used to map a DataSet[String] to a DataSet[(String,Double)].
  *
  * After fitting a Dataset[String], StringIndexer manages unseen labels with 2 strategies:
  * - if HandleInvalid has argument "skip", unseen labels are skipped
  * - otherwise, an error is thrown
  *
  * By default, HandleInvalid has value "skip".
  *
  * @example
  * {{{
  *                  val trainingDS: DataSet[Vector] = env.fromCollection(data)
  *                  val transformer = StringIndexer().setHandleInvalid("skip")
  *
  *                  transformer.fit(trainingDS)
  *                  val transformedDS = transformer.transform(trainingDS)
  * }}}
  *
  * =Parameters=
  *
  * - [[HandleInvalid]]: If "skip",unseen labels are skipped during transformation phase.
  *
  */

class StringIndexer extends Transformer[StringIndexer] {

  private[preprocessing] var metricsOption: Option[DataSet[(String, Long)]] = None

  /**
    * Sets the value of HandleInvalid - Default value is "skip".
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

  // ==================================== Factory methods ==========================================

  def apply(): StringIndexer ={
    new StringIndexer( )
  }

  // ====================================== Operations =============================================

  /**
    * Trains [[StringIndexer]] ordering labels by frequencies in the input DataSet
    * @return [[FitOperation]] training the [[StringIndexer]] on [[String]]
    */
  implicit def fitStringIndexer ={
    new FitOperation[StringIndexer, String] {
      def fit(instance: StringIndexer, fitParameters: ParameterMap, input: DataSet[String]): Unit ={
        val metrics = extractIndices( input )
        instance.metricsOption = Some( metrics )
      }
    }
  }

  /**
    * Sort labels by frequencies and assign an index
    * @param input Dataset[String] containing labels
    * @return Dataset[(String,Long)] where each label is associated with an index
    */
  private def extractIndices(input: DataSet[String]): DataSet[(String, Long)] ={

    val mapping = input
      .mapWith( s => (s, 1) )
      .groupBy( 0 )
      .sum( 1 )
      .partitionByRange( x => -x._2 )
      .sortPartition( 1, Order.DESCENDING )
      .zipWithIndex
      .mapWith { case (id, (label, count)) => (label, id) }

    mapping.print( )

    mapping
  }

  /**
    * [[TransformDataSetOperation]] returns a Dataset[(String,Long)]
    * If HandleInvalid has value "skip", unseen labels are ignored and the corresponding
    * lines are skipped. Otherwise an Exception is thrown
    */

  implicit def transformStringDataset ={
    new TransformDataSetOperation[StringIndexer, String, (String, Long)] {
      def transformDataSet(instance: StringIndexer,
        transformParameters: ParameterMap,
        input: DataSet[String]) ={

        val resultingParameters = instance.parameters ++ transformParameters
        val handleInvalid = resultingParameters( HandleInvalid )

        def toHandle(label: String) = handleInvalid match {
          case "skip" => Seq.empty[(String, Long)]
          case _ => throw new Exception( s"label ${label} has not be fitted during the fit phase - " +
            s"Use setHandleInvalid with skip parameter to filter non fitted labels, or fit your data with " +
            s"label ${label}" )
        }

        instance.metricsOption match {
          case Some( metrics ) => {
            input
              .leftOuterJoin( metrics ).where( "*" ).equalTo( 0 ) {
              (left, right) =>
                val joinIndex = if (right == null) None else Some( right._2 )
                (left, joinIndex)
            }
              .flatMapWith { case (label, index) =>
                index match {
                  case Some( value ) => Seq( (label, value) )
                  case _ => toHandle( label )
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
