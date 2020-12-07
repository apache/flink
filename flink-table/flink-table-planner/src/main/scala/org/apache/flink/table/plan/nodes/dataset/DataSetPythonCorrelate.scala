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
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.JoinRelType
import org.apache.calcite.rex.{RexCall, RexNode}
import org.apache.flink.api.java.DataSet
import org.apache.flink.table.api.internal.BatchTableEnvImpl
import org.apache.flink.table.functions.utils.TableSqlFunction
import org.apache.flink.table.plan.nodes.CommonPythonCorrelate
import org.apache.flink.table.plan.nodes.logical.FlinkLogicalTableFunctionScan
import org.apache.flink.table.plan.schema.RowSchema
import org.apache.flink.table.types.logical.RowType
import org.apache.flink.table.types.utils.TypeConversions
import org.apache.flink.types.Row

/**
  * Flink RelNode which matches along with join a Python user defined table function.
  */
class DataSetPythonCorrelate(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputNode: RelNode,
    scan: FlinkLogicalTableFunctionScan,
    condition: Option[RexNode],
    relRowType: RelDataType,
    joinRowType: RelDataType,
    joinType: JoinRelType,
    ruleDescription: String)
  extends DataSetCorrelateBase(
    cluster,
    traitSet,
    inputNode,
    scan,
    condition,
    relRowType,
    joinRowType,
    joinType,
    ruleDescription)
  with CommonPythonCorrelate {

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    new DataSetPythonCorrelate(
      cluster,
      traitSet,
      inputs.get(0),
      scan,
      condition,
      relRowType,
      joinRowType,
      joinType,
      ruleDescription)
  }

  override def translateToPlan(tableEnv: BatchTableEnvImpl): DataSet[Row] = {
    val inputDS = inputNode.asInstanceOf[DataSetRel].translateToPlan(tableEnv)

    val pythonTableFuncRexCall = scan.getCall.asInstanceOf[RexCall]

    val (pythonUdtfInputOffsets, pythonFunctionInfo) =
      extractPythonTableFunctionInfo(pythonTableFuncRexCall)

    val pythonOperatorInputRowType = TypeConversions.fromLegacyInfoToDataType(
      new RowSchema(getInput.getRowType).typeInfo).getLogicalType.asInstanceOf[RowType]

    val pythonOperatorOutputRowType = TypeConversions.fromLegacyInfoToDataType(
      new RowSchema(getRowType).typeInfo).getLogicalType.asInstanceOf[RowType]

    val sqlFunction = pythonTableFuncRexCall.getOperator.asInstanceOf[TableSqlFunction]

    val flatMapFunction = getPythonTableFunctionFlatMap(
      getConfig(tableEnv.execEnv, tableEnv.getConfig),
      pythonOperatorInputRowType,
      pythonOperatorOutputRowType,
      pythonFunctionInfo,
      pythonUdtfInputOffsets,
      joinType)

    inputDS
      .flatMap(flatMapFunction)
      .name(correlateOpName(
        inputNode.getRowType,
        pythonTableFuncRexCall,
        sqlFunction,
        relRowType,
        getExpressionString)
      )
  }
}
