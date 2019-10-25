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
import org.apache.calcite.sql.`type`.SqlTypeName
import org.apache.flink.api.dag.Transformation
import org.apache.flink.streaming.api.operators.OneInputStreamOperator
import org.apache.flink.streaming.api.transformations.OneInputTransformation
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.functions.FunctionLanguage
import org.apache.flink.table.functions.python.{PythonFunction, PythonFunctionInfo, SimplePythonFunction}
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.functions.utils.ScalarSqlFunction
import org.apache.flink.table.planner.plan.nodes.common.CommonPythonCalc.PYTHON_SCALAR_FUNCTION_OPERATOR_NAME
import org.apache.flink.table.runtime.typeutils.BaseRowTypeInfo
import org.apache.flink.table.types.logical.RowType

import scala.collection.JavaConversions._
import scala.collection.mutable

trait CommonPythonCalc {

  private lazy val convertLiteralToPython = {
    val clazz = Class.forName("org.apache.flink.api.common.python.PythonBridgeUtils")
    clazz.getMethod("convertLiteralToPython", classOf[RexLiteral], classOf[SqlTypeName])
  }

  private def extractPythonScalarFunctionInfos(
      rexCalls: Array[RexCall]): (Array[Int], Array[PythonFunctionInfo]) = {
    // using LinkedHashMap to keep the insert order
    val inputNodes = new mutable.LinkedHashMap[RexNode, Integer]()
    val pythonFunctionInfos = rexCalls.map(createPythonScalarFunctionInfo(_, inputNodes))

    val udfInputOffsets = inputNodes.toArray
      .map(_._1)
      .collect { case inputRef: RexInputRef => inputRef.getIndex }
    (udfInputOffsets, pythonFunctionInfos)
  }

  private def createPythonScalarFunctionInfo(
      rexCall: RexCall,
      inputNodes: mutable.Map[RexNode, Integer]): PythonFunctionInfo = rexCall.getOperator match {
    case sfc: ScalarSqlFunction if sfc.scalarFunction.getLanguage == FunctionLanguage.PYTHON =>
      val inputs = new mutable.ArrayBuffer[AnyRef]()
      rexCall.getOperands.foreach {
        case pythonRexCall: RexCall if pythonRexCall.getOperator.asInstanceOf[ScalarSqlFunction]
          .scalarFunction.getLanguage == FunctionLanguage.PYTHON =>
          // Continuous Python UDFs can be chained together
          val argPythonInfo = createPythonScalarFunctionInfo(pythonRexCall, inputNodes)
          inputs.append(argPythonInfo)

        case literal: RexLiteral =>
          inputs.append(
            convertLiteralToPython.invoke(null, literal, literal.getType.getSqlTypeName))

        case argNode: RexNode =>
          // For input arguments of RexInputRef, it's replaced with an offset into the input row
          inputNodes.get(argNode) match {
            case Some(existing) => inputs.append(existing)
            case None =>
              val inputOffset = Integer.valueOf(inputNodes.size)
              inputs.append(inputOffset)
              inputNodes.put(argNode, inputOffset)
          }
      }

      // Extracts the necessary information for Python function execution, such as
      // the serialized Python function, the Python env, etc
      val pythonFunction = new SimplePythonFunction(
        sfc.scalarFunction.asInstanceOf[PythonFunction].getSerializedPythonFunction,
        sfc.scalarFunction.asInstanceOf[PythonFunction].getPythonEnv)
      new PythonFunctionInfo(pythonFunction, inputs.toArray)
  }

  private def getPythonScalarFunctionOperator(
      inputRowTypeInfo: BaseRowTypeInfo,
      outputRowTypeInfo: BaseRowTypeInfo,
      udfInputOffsets: Array[Int],
      pythonFunctionInfos: Array[PythonFunctionInfo],
      forwardedFields: Array[Int])= {
    val clazz = Class.forName(PYTHON_SCALAR_FUNCTION_OPERATOR_NAME)
    val ctor = clazz.getConstructor(
      classOf[Array[PythonFunctionInfo]],
      classOf[RowType],
      classOf[RowType],
      classOf[Array[Int]],
      classOf[Array[Int]])
    ctor.newInstance(
      pythonFunctionInfos,
      inputRowTypeInfo.toRowType,
      outputRowTypeInfo.toRowType,
      udfInputOffsets,
      forwardedFields)
      .asInstanceOf[OneInputStreamOperator[BaseRow, BaseRow]]
  }

  def createPythonOneInputTransformation(
      inputTransform: Transformation[BaseRow],
      calcProgram: RexProgram,
      name: String): OneInputTransformation[BaseRow, BaseRow] = {
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
      inputTransform.getOutputType.asInstanceOf[BaseRowTypeInfo].getLogicalTypes
    val pythonOperatorInputTypeInfo = inputTransform.getOutputType.asInstanceOf[BaseRowTypeInfo]
    val pythonOperatorResultTyeInfo = new BaseRowTypeInfo(
      forwardedFields.map(inputLogicalTypes(_)) ++
        pythonRexCalls.map(node => FlinkTypeFactory.toLogicalType(node.getType)): _*)

    val pythonOperator = getPythonScalarFunctionOperator(
      pythonOperatorInputTypeInfo,
      pythonOperatorResultTyeInfo,
      pythonUdfInputOffsets,
      pythonFunctionInfos,
      forwardedFields)

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
    "org.apache.flink.table.runtime.operators.python.BaseRowPythonScalarFunctionOperator"
}
