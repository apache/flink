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

import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.common.typeutils.CompositeType
import org.apache.flink.table.api.TableException

object FieldForwardingUtils {

  def compositeTypeField = (fields: Seq[String]) => fields

  private def throwMissedWrapperException(wrapperCustomCase: TypeInformation[_]) = {
    throw new TableException(s"Implementation for $wrapperCustomCase index wrapper is missing.")
  }

  /**
    * Wrapper for {@link getForwardedFields}
    */
  def getForwardedInput(
      inputType: TypeInformation[_],
      outputType: TypeInformation[_],
      forwardIndices: Seq[Int],
      wrapperCustomCase: TypeInformation[_] =>
        (Int) => String = throwMissedWrapperException): String = {

    getForwardedFields(inputType,
      outputType,
      forwardIndices.zip(forwardIndices),
      wrapperCustomCase)
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
      inputType: TypeInformation[_],
      outputType: TypeInformation[_],
      forwardIndices: Seq[(Int, Int)],
      customWrapper: TypeInformation[_] =>
        (Int) => String = throwMissedWrapperException): String = {

    def chooseWrapper(typeInformation: TypeInformation[_]): (Int) => String = {
      typeInformation match {
        case composite: CompositeType[_] =>
          compositeTypeField(composite.getFieldNames)
        case basic: BasicTypeInfo[_] =>
          (_: Int) => s"*"
        case _ =>
          customWrapper(typeInformation)
      }
    }

    val wrapInput = chooseWrapper(inputType)
    val wrapOutput = chooseWrapper(outputType)

    forwardFields(forwardIndices, wrapInput, wrapOutput)
  }

  private def forwardFields(
      forwardIndices: Seq[(Int, Int)],
      wrapInput: (Int) => String,
      wrapOutput: (Int) => String): String = {

    implicit class String2ForwardFields(left: String) {
      def ->(right: String): String = if (left == right) {
        left
      } else {
        s"$left->$right"
      }
    }

    def wrapIndices(inputIndex: Int, outputIndex: Int): String = {
      wrapInput(inputIndex) -> wrapOutput(outputIndex)
    }

    forwardIndices map { case (in, out) => wrapIndices(in, out) } mkString ";"
  }
}
