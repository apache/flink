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

package org.apache.flink.table.planner.plan.nodes.common

import org.apache.calcite.rex._
import org.apache.flink.api.dag.Transformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.operators.OneInputStreamOperator
import org.apache.flink.streaming.api.transformations.OneInputTransformation
import org.apache.flink.table.data.RowData
import org.apache.flink.table.functions.python.{PythonFunctionInfo, PythonFunctionKind}
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.plan.nodes.common.CommonPythonCalc.ARROW_PYTHON_SCALAR_FUNCTION_OPERATOR_NAME
import org.apache.flink.table.planner.plan.nodes.common.CommonPythonCalc.PYTHON_SCALAR_FUNCTION_OPERATOR_NAME
import org.apache.flink.table.planner.plan.utils.PythonUtil.containsPythonCall
import org.apache.flink.table.runtime.typeutils.RowDataTypeInfo
import org.apache.flink.table.types.logical.RowType

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable

trait CommonPythonCalc extends CommonPythonBase {

  private def extractPythonScalarFunctionInfos(
      rexCalls: Array[RexCall]): (Array[Int], Array[PythonFunctionInfo]) = {
    // using LinkedHashMap to keep the insert order
    val inputNodes = new mutable.LinkedHashMap[RexNode, Integer]()
    val pythonFunctionInfos = rexCalls.map(createPythonFunctionInfo(_, inputNodes))

    val udfInputOffsets = inputNodes.toArray
      .map(_._1)
      .collect {
        case inputRef: RexInputRef => inputRef.getIndex
        case fac: RexFieldAccess => fac.getField.getIndex
      }
    (udfInputOffsets, pythonFunctionInfos)
  }

  private def getPythonScalarFunctionOperator(
      config: Configuration,
      inputRowTypeInfo: RowDataTypeInfo,
      outputRowTypeInfo: RowDataTypeInfo,
      udfInputOffsets: Array[Int],
      pythonFunctionInfos: Array[PythonFunctionInfo],
      forwardedFields: Array[Int],
      isArrow: Boolean)= {
    val clazz = if (isArrow) {
      loadClass(ARROW_PYTHON_SCALAR_FUNCTION_OPERATOR_NAME)
    } else {
      loadClass(PYTHON_SCALAR_FUNCTION_OPERATOR_NAME)
    }
    val ctor = clazz.getConstructor(
      classOf[Configuration],
      classOf[Array[PythonFunctionInfo]],
      classOf[RowType],
      classOf[RowType],
      classOf[Array[Int]],
      classOf[Array[Int]])
    ctor.newInstance(
      config,
      pythonFunctionInfos,
      inputRowTypeInfo.toRowType,
      outputRowTypeInfo.toRowType,
      udfInputOffsets,
      forwardedFields)
      .asInstanceOf[OneInputStreamOperator[RowData, RowData]]
  }

  def createPythonOneInputTransformation(
      inputTransform: Transformation[RowData],
      calcProgram: RexProgram,
      name: String,
      config: Configuration): OneInputTransformation[RowData, RowData] = {
    val pythonRexCalls = calcProgram.getProjectList
      .map(calcProgram.expandLocalRef)
      .collect { case call: RexCall => call }
      .toArray

    val forwardedFields: Array[Int] = calcProgram.getProjectList
      .map(calcProgram.expandLocalRef)
      .collect { case inputRef: RexInputRef => inputRef.getIndex }
      .toArray

    val (pythonUdfInputOffsets, pythonFunctionInfos) =
      extractPythonScalarFunctionInfos(pythonRexCalls)

    val inputLogicalTypes =
      inputTransform.getOutputType.asInstanceOf[RowDataTypeInfo].getLogicalTypes
    val pythonOperatorInputTypeInfo = inputTransform.getOutputType.asInstanceOf[RowDataTypeInfo]
    val pythonOperatorResultTyeInfo = new RowDataTypeInfo(
      forwardedFields.map(inputLogicalTypes(_)) ++
        pythonRexCalls.map(node => FlinkTypeFactory.toLogicalType(node.getType)): _*)

    val pythonOperator = getPythonScalarFunctionOperator(
      config,
      pythonOperatorInputTypeInfo,
      pythonOperatorResultTyeInfo,
      pythonUdfInputOffsets,
      pythonFunctionInfos,
      forwardedFields,
      calcProgram.getExprList.asScala.exists(containsPythonCall(_, PythonFunctionKind.PANDAS)))

    new OneInputTransformation(
      inputTransform,
      name,
      pythonOperator,
      pythonOperatorResultTyeInfo,
      inputTransform.getParallelism
    )
  }
}

object CommonPythonCalc {
  val PYTHON_SCALAR_FUNCTION_OPERATOR_NAME =
    "org.apache.flink.table.runtime.operators.python.scalar.RowDataPythonScalarFunctionOperator"

  val ARROW_PYTHON_SCALAR_FUNCTION_OPERATOR_NAME =
    "org.apache.flink.table.runtime.operators.python.scalar.arrow." +
      "RowDataArrowPythonScalarFunctionOperator"
}
