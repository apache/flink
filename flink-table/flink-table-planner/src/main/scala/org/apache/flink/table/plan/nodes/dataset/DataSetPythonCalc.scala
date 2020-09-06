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

package org.apache.flink.table.plan.nodes.dataset

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.Calc
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rex._
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.java.DataSet
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.configuration.Configuration
import org.apache.flink.table.api.internal.BatchTableEnvImpl
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.functions.python.{PythonFunctionInfo, PythonFunctionKind}
import org.apache.flink.table.plan.nodes.CommonPythonCalc
import org.apache.flink.table.plan.nodes.dataset.DataSetPythonCalc.{ARROW_PYTHON_SCALAR_FUNCTION_FLAT_MAP_NAME, PYTHON_SCALAR_FUNCTION_FLAT_MAP_NAME}
import org.apache.flink.table.plan.schema.RowSchema
import org.apache.flink.table.plan.util.PythonUtil.containsPythonCall
import org.apache.flink.table.types.logical.RowType
import org.apache.flink.table.types.utils.TypeConversions
import org.apache.flink.types.Row

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

/**
  * Flink RelNode for Python ScalarFunctions.
  */
class DataSetPythonCalc(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    input: RelNode,
    rowRelDataType: RelDataType,
    calcProgram: RexProgram,
    ruleDescription: String)
  extends DataSetCalcBase(
    cluster,
    traitSet,
    input,
    rowRelDataType,
    calcProgram,
    ruleDescription)
    with CommonPythonCalc {

  private lazy val inputSchema = new RowSchema(input.getRowType)

  override def copy(traitSet: RelTraitSet, child: RelNode, program: RexProgram): Calc = {
    new DataSetPythonCalc(cluster, traitSet, child, getRowType, program, ruleDescription)
  }

  override def translateToPlan(tableEnv: BatchTableEnvImpl): DataSet[Row] = {

    val inputDS = getInput.asInstanceOf[DataSetRel].translateToPlan(tableEnv)

    val flatMapFunctionResultTypeInfo = new RowTypeInfo(
      getForwardedFields(calcProgram).map(inputSchema.fieldTypeInfos.get(_)) ++
        getPythonRexCalls(calcProgram).map(node => FlinkTypeFactory.toTypeInfo(node.getType)): _*)

    // construct the Python ScalarFunction flatMap function
    val flatMapFunctionInputRowType = TypeConversions.fromLegacyInfoToDataType(
      inputSchema.typeInfo).getLogicalType.asInstanceOf[RowType]
    val flatMapFunctionOutputRowType = TypeConversions.fromLegacyInfoToDataType(
      flatMapFunctionResultTypeInfo).getLogicalType.asInstanceOf[RowType]
    val flatMapFunction = getPythonScalarFunctionFlatMap(
      getConfig(tableEnv.execEnv, tableEnv.getConfig),
      flatMapFunctionInputRowType,
      flatMapFunctionOutputRowType,
      calcProgram)

    inputDS.flatMap(flatMapFunction).name(calcOpName(calcProgram, getExpressionString))
  }

  private[flink] def getPythonScalarFunctionFlatMap(
    config: Configuration,
    inputRowType: RowType,
    outputRowType: RowType,
    calcProgram: RexProgram) = {
    val clazz = if (calcProgram.getExprList.asScala.exists(
      containsPythonCall(_, PythonFunctionKind.PANDAS))) {
      loadClass(ARROW_PYTHON_SCALAR_FUNCTION_FLAT_MAP_NAME)
    } else {
      loadClass(PYTHON_SCALAR_FUNCTION_FLAT_MAP_NAME)
    }
    val ctor = clazz.getConstructor(
      classOf[Configuration],
      classOf[Array[PythonFunctionInfo]],
      classOf[RowType],
      classOf[RowType],
      classOf[Array[Int]],
      classOf[Array[Int]])
    val (udfInputOffsets, pythonFunctionInfos) =
      extractPythonScalarFunctionInfos(getPythonRexCalls(calcProgram))
    ctor.newInstance(
      config,
      pythonFunctionInfos,
      inputRowType,
      outputRowType,
      udfInputOffsets,
      getForwardedFields(calcProgram))
      .asInstanceOf[RichFlatMapFunction[Row, Row]]
  }
}

object DataSetPythonCalc {
  val PYTHON_SCALAR_FUNCTION_FLAT_MAP_NAME =
    "org.apache.flink.table.runtime.functions.python.PythonScalarFunctionFlatMap"

  val ARROW_PYTHON_SCALAR_FUNCTION_FLAT_MAP_NAME =
    "org.apache.flink.table.runtime.functions.python.arrow.ArrowPythonScalarFunctionFlatMap"
}
