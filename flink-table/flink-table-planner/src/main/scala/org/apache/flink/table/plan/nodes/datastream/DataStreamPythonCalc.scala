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

package org.apache.flink.table.plan.nodes.datastream

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.Calc
import org.apache.calcite.rex.RexProgram
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.table.api.StreamQueryConfig
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.plan.nodes.CommonPythonCalc
import org.apache.flink.table.plan.schema.RowSchema
import org.apache.flink.table.planner.StreamPlanner
import org.apache.flink.table.runtime.types.{CRow, CRowTypeInfo}
import org.apache.flink.table.types.logical.RowType
import org.apache.flink.table.types.utils.TypeConversions

import scala.collection.JavaConversions._

/**
  * RelNode for Python ScalarFunctions.
  */
class DataStreamPythonCalc(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    input: RelNode,
    inputSchema: RowSchema,
    schema: RowSchema,
    calcProgram: RexProgram,
    ruleDescription: String)
  extends DataStreamCalcBase(
    cluster,
    traitSet,
    input,
    inputSchema,
    schema,
    calcProgram,
    ruleDescription)
  with CommonPythonCalc {

  override def copy(traitSet: RelTraitSet, child: RelNode, program: RexProgram): Calc = {
    new DataStreamPythonCalc(
      cluster,
      traitSet,
      child,
      inputSchema,
      schema,
      program,
      ruleDescription)
  }

  override def translateToPlan(
      planner: StreamPlanner,
      queryConfig: StreamQueryConfig): DataStream[CRow] = {
    val inputDataStream =
      getInput.asInstanceOf[DataStreamRel].translateToPlan(planner, queryConfig)
    val inputParallelism = inputDataStream.getParallelism

    val pythonOperatorResultTypeInfo = new RowTypeInfo(
      getForwardedFields(calcProgram).map(inputSchema.fieldTypeInfos.get(_)) ++
        getPythonRexCalls(calcProgram).map(node => FlinkTypeFactory.toTypeInfo(node.getType)): _*)

    // construct the Python operator
    val pythonOperatorInputRowType = TypeConversions.fromLegacyInfoToDataType(
      inputSchema.typeInfo).getLogicalType.asInstanceOf[RowType]
    val pythonOperatorOutputRowType = TypeConversions.fromLegacyInfoToDataType(
      pythonOperatorResultTypeInfo).getLogicalType.asInstanceOf[RowType]
    val pythonOperator = getPythonScalarFunctionOperator(
      getConfig(planner.getConfig),
      pythonOperatorInputRowType,
      pythonOperatorOutputRowType,
      calcProgram)

    inputDataStream
      .transform(
        calcOpName(calcProgram, getExpressionString),
        CRowTypeInfo(pythonOperatorResultTypeInfo),
        pythonOperator)
      // keep parallelism to ensure order of accumulate and retract messages
      .setParallelism(inputParallelism)
  }
}
