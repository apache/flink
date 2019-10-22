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

import org.apache.calcite.plan.RelOptCluster
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rex._
import org.apache.calcite.sql.`type`.SqlTypeName
import org.apache.flink.api.dag.Transformation
import org.apache.flink.streaming.api.operators.OneInputStreamOperator
import org.apache.flink.streaming.api.transformations.OneInputTransformation
import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.functions.FunctionLanguage
import org.apache.flink.table.functions.python.{PythonFunction, PythonFunctionInfo, SimplePythonFunction}
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.codegen.{CalcCodeGenerator, CodeGeneratorContext}
import org.apache.flink.table.planner.functions.utils.ScalarSqlFunction
import org.apache.flink.table.planner.plan.nodes.common.CommonPythonCalc.PYTHON_SCALAR_FUNCTION_OPERATOR_NAME
import org.apache.flink.table.runtime.typeutils.BaseRowTypeInfo
import org.apache.flink.table.types.DataType
import org.apache.flink.table.types.logical.RowType
import org.apache.flink.table.types.utils.TypeConversions

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
      .filter(_.isInstanceOf[RexInputRef])
      .map(_.asInstanceOf[RexInputRef].getIndex)
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
      classOf[DataType],
      classOf[DataType],
      classOf[Array[Int]],
      classOf[Array[Int]])
    ctor.newInstance(
      pythonFunctionInfos,
      TypeConversions.fromLogicalToDataType(inputRowTypeInfo.toRowType),
      TypeConversions.fromLogicalToDataType(outputRowTypeInfo.toRowType),
      udfInputOffsets,
      forwardedFields)
      .asInstanceOf[OneInputStreamOperator[BaseRow, BaseRow]]
  }

  private def createPythonOneInputTransformation(
      inputTransform: Transformation[BaseRow],
      calcProgram: RexProgram,
      name: String) = {
    val pythonRexCalls = calcProgram.getProjectList
      .map(calcProgram.expandLocalRef)
      .filter(_.isInstanceOf[RexCall])
      .map(_.asInstanceOf[RexCall])
      .toArray

    val forwardedFields: Array[Int] = calcProgram.getProjectList
      .map(calcProgram.expandLocalRef)
      .filter(_.isInstanceOf[RexInputRef])
      .map(_.asInstanceOf[RexInputRef].getIndex)
      .toArray

    val resultProjectList = {
      var idx = 0
      calcProgram.getProjectList
        .map(calcProgram.expandLocalRef)
        .map {
          case pythonCall: RexCall =>
            val inputRef = new RexInputRef(forwardedFields.length + idx, pythonCall.getType)
            idx += 1
            inputRef
          case node => node
        }
    }

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

    val pythonInputTransform = new OneInputTransformation(
      inputTransform,
      name,
      pythonOperator,
      pythonOperatorResultTyeInfo,
      inputTransform.getParallelism
    )
    (pythonInputTransform, pythonOperatorResultTyeInfo, resultProjectList)
  }

  private def createProjectionRexProgram(
      inputRowType: RowType,
      outputRelData: RelDataType,
      projectList: mutable.Buffer[RexNode],
      cluster: RelOptCluster) = {
    val factory = cluster.getTypeFactory.asInstanceOf[FlinkTypeFactory]
    val inputRelData = factory.createFieldTypeFromLogicalType(inputRowType)
    RexProgram.create(inputRelData, projectList, null, outputRelData, cluster.getRexBuilder)
  }

  protected def createOneInputTransformation(
      inputTransform: Transformation[BaseRow],
      inputsContainSingleton: Boolean,
      calcProgram: RexProgram,
      name: String,
      config : TableConfig,
      ctx : CodeGeneratorContext,
      cluster: RelOptCluster,
      rowType: RelDataType,
      opName: String): OneInputTransformation[BaseRow, BaseRow] = {
    val (pythonInputTransform, pythonOperatorResultTyeInfo, resultProjectList) =
      createPythonOneInputTransformation(inputTransform, calcProgram, name)

    if (inputsContainSingleton) {
      pythonInputTransform.setParallelism(1)
      pythonInputTransform.setMaxParallelism(1)
    }

    val onlyFilter = resultProjectList.zipWithIndex.forall { case (rexNode, index) =>
      rexNode.isInstanceOf[RexInputRef] && rexNode.asInstanceOf[RexInputRef].getIndex == index
    }

    if (onlyFilter) {
      pythonInputTransform
    } else {
      // After executing python OneInputTransformation, the order of the output fields
      // is Python Call after the forwarding fields, so in the case of sequential changes,
      // a calc is needed to adjust the order.
      val outputType = FlinkTypeFactory.toLogicalRowType(rowType)
      val rexProgram = createProjectionRexProgram(
        pythonOperatorResultTyeInfo.toRowType, rowType, resultProjectList, cluster)
      val substituteOperator = CalcCodeGenerator.generateCalcOperator(
        ctx,
        cluster,
        pythonInputTransform,
        outputType,
        config,
        rexProgram,
        None,
        retainHeader = true,
        opName
      )

      new OneInputTransformation(
        pythonInputTransform,
        name,
        substituteOperator,
        BaseRowTypeInfo.of(outputType),
        pythonInputTransform.getParallelism)
    }
  }
}

object CommonPythonCalc {
  val PYTHON_SCALAR_FUNCTION_OPERATOR_NAME =
    "org.apache.flink.table.runtime.operators.python.BaseRowPythonScalarFunctionOperator"
}
