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
import org.apache.calcite.rel.core.JoinRelType
import org.apache.calcite.rex.{RexCall, RexNode}
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.table.api.TableException
import org.apache.flink.table.functions.utils.TableSqlFunction
import org.apache.flink.table.plan.nodes.CommonPythonCorrelate
import org.apache.flink.table.plan.nodes.logical.FlinkLogicalTableFunctionScan
import org.apache.flink.table.plan.schema.RowSchema
import org.apache.flink.table.planner.StreamPlanner
import org.apache.flink.table.runtime.types.{CRow, CRowTypeInfo}
import org.apache.flink.table.types.logical.RowType
import org.apache.flink.table.types.utils.TypeConversions

/**
  * Flink RelNode which matches along with join a Python user defined table function.
  */
class DataStreamPythonCorrelate(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputSchema: RowSchema,
    input: RelNode,
    scan: FlinkLogicalTableFunctionScan,
    condition: Option[RexNode],
    schema: RowSchema,
    joinSchema: RowSchema,
    joinType: JoinRelType,
    ruleDescription: String)
  extends DataStreamCorrelateBase(
    cluster,
    traitSet,
    inputSchema,
    input,
    scan,
    condition,
    schema,
    joinType)
  with CommonPythonCorrelate {

  if (condition.isDefined) {
    throw new TableException("Currently Python correlate does not support conditions in left join.")
  }

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    new DataStreamPythonCorrelate(
      cluster,
      traitSet,
      inputSchema,
      inputs.get(0),
      scan,
      condition,
      schema,
      joinSchema,
      joinType,
      ruleDescription)
  }

  override def translateToPlan(planner: StreamPlanner): DataStream[CRow] = {
    val inputDataStream =
      getInput.asInstanceOf[DataStreamRel].translateToPlan(planner)

    val pythonTableFuncRexCall = scan.getCall.asInstanceOf[RexCall]

    val (pythonUdtfInputOffsets, pythonFunctionInfo) =
      extractPythonTableFunctionInfo(pythonTableFuncRexCall)

    val pythonOperatorInputRowType = TypeConversions.fromLegacyInfoToDataType(
      inputSchema.typeInfo).getLogicalType.asInstanceOf[RowType]

    val pythonOperatorOutputRowType = TypeConversions.fromLegacyInfoToDataType(
      schema.typeInfo).getLogicalType.asInstanceOf[RowType]

    val sqlFunction = pythonTableFuncRexCall.getOperator.asInstanceOf[TableSqlFunction]

    val pythonOperator = getPythonTableFunctionOperator(
      getConfig(planner.getExecutionEnvironment, planner.getConfig),
      pythonOperatorInputRowType,
      pythonOperatorOutputRowType,
      pythonFunctionInfo,
      pythonUdtfInputOffsets,
      joinType)

    inputDataStream
      .transform(
        correlateOpName(
          inputSchema.relDataType,
          pythonTableFuncRexCall,
          sqlFunction,
          schema.relDataType,
          getExpressionString),
        CRowTypeInfo(schema.typeInfo),
        pythonOperator)
      // keep parallelism to ensure order of accumulate and retract messages
      .setParallelism(inputDataStream.getParallelism)
  }
}
