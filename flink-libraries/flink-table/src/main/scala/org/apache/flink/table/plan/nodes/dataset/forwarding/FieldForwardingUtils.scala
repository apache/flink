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

import org.apache.flink.api.common.typeinfo.AtomicType
import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeinfo.{TypeInformation => TypeInfo}
import org.apache.flink.api.common.typeutils.CompositeType
import org.apache.flink.table.api.TableException
import org.apache.flink.types.Row

object FieldForwardingUtils {

  private def throwMissedTypeInfoException(info: TypeInfo[_]) = {
    throw new TableException(s"Implementation for $info wrapper is missing.")
  }

  /**
    * Wrapper for {@link getForwardedInput}
    *
    * @param leftType left input [[TypeInformation]]
    * @param rightType right input [[TypeInformation]]
    * @param returnType information of output data
    * @return string with forwarded fields mapped from input to output
    */
  def getForwardedInput(
      leftType: TypeInfo[Row],
      rightType: TypeInfo[Row],
      returnType: TypeInfo[Row]): (String, String) = {

    val leftFields = getForwardedInput(leftType, returnType)
    val rightFields = getForwardedInput(rightType, returnType)
    (leftFields, rightFields)
  }

  /**
    * Wrapper for {@link getForwardedInput}
    * Generates default indices by inputTypeInfo
    *
    * @param inputType input [[TypeInformation]]
    * @param returnType information of output data
    * @return string with forwarded fields mapped from input to output
    */
  def getForwardedInput(
      inputType: TypeInfo[Row],
      returnType: TypeInfo[Row]): String ={

    val indices = 0 until inputType.getTotalFields
    getForwardedInput(inputType, returnType, indices)
  }

  /**
    * Wrapper for {@link getForwardedFields}
    * Generates default indices by zipping forwardIndices with itself
    *
    * @param inputType      information of input data
    * @param outputType     information of output data
    * @param forwardIndices direct mapping of fields
    * @return string with forwarded fields mapped from input to output
    */
  def getForwardedInput(
      inputType: TypeInfo[_],
      outputType: TypeInfo[_],
      forwardIndices: Seq[Int]): String = {

    getForwardedFields(
      inputType,
      outputType,
      forwardIndices.zip(forwardIndices))
  }

  /**
    * Wraps provided indices with proper names.
    * e.g. _1 for Tuple, f0 for Row, fieldName for POJO and named Row
    *
    * @param inputType      information of input data
    * @param outputType     information of output data
    * @param forwardIndices tuple of (input, output) indices of a forwarded field
    * @return string with forwarded fields mapped from input to output
    */
  def getForwardedFields(
      inputType: TypeInfo[_],
      outputType: TypeInfo[_],
      forwardIndices: Seq[(Int, Int)]): String = {

    def chooseWrapper(
        typeInformation: TypeInfo[_]): Seq[(String, TypeInfo[_])] = {

      typeInformation match {
        case composite: CompositeType[_] =>
          extractFields(composite)
        case basic: BasicTypeInfo[_] =>
          Seq((s"*", basic))
        case array: BasicArrayTypeInfo[_, _] =>
          Seq((s"*", array))
        case atomic: AtomicType[_] =>
          Seq((s"*", atomic))
        case _ =>
          throwMissedTypeInfoException(typeInformation)
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
          throw new TableException("The logic of identifying " +
            "the mapping of forwarded fields is broken")
        }
      }
    }

    forwardIndices map { case (in, out) =>
      wrappedInput(in) -> wrappedOutput(out)
    } mkString ";"
  }
}
