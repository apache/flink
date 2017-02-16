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

package org.apache.flink.table.plan.nodes.dataset.forwarding

import org.apache.flink.api.common.typeinfo.{BasicArrayTypeInfo, BasicTypeInfo, TypeInformation => TypeInfo}
import org.apache.flink.api.common.typeutils.CompositeType
import org.apache.flink.api.java.DataSet
import org.apache.flink.table.api.TableException
import org.apache.flink.types.Row

object FieldForwardingUtils {

  def compositeTypeField = (fields: Seq[(String, TypeInfo[_])]) => fields

  private def throwMissedWrapperException(customWrapper: TypeInfo[_]) = {
    throw new TableException(s"Implementation for $customWrapper wrapper is missing.")
  }

  /**
    * Wrapper for {@link getForwardedFields}
    */
  def getForwardedInput(
      inputType: TypeInfo[_],
      outputType: TypeInfo[_],
      forwardIndices: Seq[Int],
      customWrapper: TypeInfo[_] =>
        Seq[(String, TypeInfo[_])] = throwMissedWrapperException): String = {

    getForwardedFields(inputType,
      outputType,
      forwardIndices.zip(forwardIndices),
      customWrapper)
  }

  /**
    * Wraps provided indices with proper names.
    * e.g. _1 for Tuple, f0 for Row, fieldName for POJO and named Row
    *
    * @param inputType      information of input data
    * @param outputType     information of output data
    * @param forwardIndices tuple of (input, output) indices of a forwarded field
    * @param customWrapper  used for figuring out proper type in specific cases,
    *                       e.g. {@see DataSetSingleRowJoin}
    * @return string with forwarded fields mapped from input to output
    */
  def getForwardedFields(
      inputType: TypeInfo[_],
      outputType: TypeInfo[_],
      forwardIndices: Seq[(Int, Int)],
      customWrapper: TypeInfo[_] =>
        Seq[(String, TypeInfo[_])] = throwMissedWrapperException): String = {

    def chooseWrapper(
        typeInformation: TypeInfo[_]): Seq[(String, TypeInfo[_])] = {

      typeInformation match {
        case composite: CompositeType[_] =>
          val fields = extractFields(composite)
          compositeTypeField(fields)
        case basic: BasicTypeInfo[_] =>
          Seq((s"*", basic))
        case array: BasicArrayTypeInfo[_, _] =>
          Seq((s"*", array))
        case _ =>
          customWrapper(typeInformation)
      }
    }

    val wrapInput = chooseWrapper(inputType)
    val wrapOutput = chooseWrapper(outputType)

    forwardFields(forwardIndices, wrapInput, wrapOutput)
  }

  private def extractFields(
      composite: CompositeType[_]): Seq[(String, TypeInfo[_])] = {

    val types = for {
      i <- 0 until composite.getArity
    } yield { composite.getTypeAt(i) }

    composite.getFieldNames.zip(types)
  }

  private def forwardFields(
      forwardIndices: Seq[(Int, Int)],
      wrappedInput: Int => (String, TypeInfo[_]),
      wrappedOutput: Int => (String, TypeInfo[_])): String = {

    implicit class Field2ForwardField(left: (String, TypeInfo[_])) {
      def ->(right: (String, TypeInfo[_])): String = if (left.equals(right)) {
        s"${left._1}"
      } else {
        if (left._2.equals(right._2)) {
          s"${left._1}->${right._1}"
        } else {
          null
        }
      }
    }

    forwardIndices map {
      case (in, out) =>
        wrappedInput(in) -> wrappedOutput(out)
    } filterNot(_ == null) mkString ";"
  }

  def getDummyForwardedFields(
      leftDataSet: DataSet[Row],
      rightDataSet: DataSet[Row],
      returnType: TypeInfo[Row]): (String, String) = {

    val leftFields = getDummyForwardedFields(leftDataSet, returnType)
    val rightFields = getDummyForwardedFields(rightDataSet, returnType)
    (leftFields, rightFields)
  }

  def getDummyForwardedFields(
      dataSet: DataSet[Row],
      returnType: TypeInfo[Row]): String ={

    val `type` = dataSet.getType
    val indices = 0 until `type`.getTotalFields
    getForwardedInput(`type`, returnType, indices)
  }

}
