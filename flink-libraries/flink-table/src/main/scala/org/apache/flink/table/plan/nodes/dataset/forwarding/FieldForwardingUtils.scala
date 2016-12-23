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

import org.apache.calcite.rex.RexProgram
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.common.typeutils.CompositeType
import org.apache.flink.table.api.TableException

import scala.collection.JavaConversions._

object FieldForwardingUtils {

  def compositeTypeField = (fields: Seq[String]) => (v: Int) => fields(v)

  private def throwMissedWrapperException(wrapperCustomCase: TypeInformation[_]) = {
    throw new TableException(s"Implementation for $wrapperCustomCase index wrapper is missing.")
  }

  /**
    * Wrapper for {@link getForwardedFields}
    * @param inputType
    * @param outputType
    * @param forwardIndices
    * @param wrapperCustomCase
    * @param calcProgram
    * @return
    */
  def getForwardedInput(
      inputType: TypeInformation[_],
      outputType: TypeInformation[_],
      forwardIndices: Seq[Int],
      wrapperCustomCase: TypeInformation[_] => (Int) => String = throwMissedWrapperException,
      calcProgram: Option[RexProgram] = None) = {

    getForwardedFields(inputType,
      outputType,
      forwardIndices.zip(forwardIndices),
      wrapperCustomCase,
      calcProgram)
  }

  /**
    * Wraps provided indices with proper names, e.g. _1 for tuple, f0 for row, fieldName for POJO.
    * @param inputType
    * @param outputType
    * @param forwardIndices - tuple of input-output indices of a forwarded field
    * @param wrapperCustomCase - used for  figuring out proper type in specific cases,
    *                          e.g. {@see DataSetSingleRowJoin}
    * @param calcProgram - used for  figuring out proper type in specific cases,
    *                    e.g. {@see DataSetCalc}
    * @return - string with forwarded fields mapped from input to output
    */
  def getForwardedFields(
      inputType: TypeInformation[_],
      outputType: TypeInformation[_],
      forwardIndices: Seq[(Int, Int)],
      wrapperCustomCase: TypeInformation[_] => (Int) => String = throwMissedWrapperException,
      calcProgram: Option[RexProgram] = None): String = {

    def chooseWrapper(typeInformation: TypeInformation[_]): (Int) => String = {
      typeInformation match {
        case composite: CompositeType[_] =>
          // POJOs' fields are sorted, so we can not access them by their positional index.
          // So we collect field names from
          // outputRowType. For all other types we get field names from inputDS.
          val typeFieldList = composite.getFieldNames
          var fieldWrapper: (Int) => String = compositeTypeField(typeFieldList)
          if (calcProgram.isDefined) {
            val projectFieldList = calcProgram.get.getOutputRowType.getFieldNames
            if (typeFieldList.toSet == projectFieldList.toSet) {
              fieldWrapper = compositeTypeField(projectFieldList)
            }
          }
          fieldWrapper
        case basic: BasicTypeInfo[_] => (v: Int) => s"*"
        case _ => wrapperCustomCase(typeInformation)
      }
    }

    val wrapInput = chooseWrapper(inputType)
    val wrapOutput = chooseWrapper(outputType)

    implicit def string2ForwardFields(left: String) = new AnyRef {
      def ->(right: String): String = left + "->" + right
      def simplify(): String = if (left.split("->").head == left.split("->").last) {
        left.split("->").head
      } else {
        left
      }
    }

    def wrapIndices(inputIndex: Int, outputIndex: Int): String = {
      wrapInput(inputIndex) -> wrapOutput(outputIndex) simplify()
    }

    forwardIndices map { case (in, out) => wrapIndices(in, out) } mkString ";"
  }
}
