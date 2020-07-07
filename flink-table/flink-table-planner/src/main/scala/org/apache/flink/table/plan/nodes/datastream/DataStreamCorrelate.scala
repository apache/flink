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

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.table.functions.utils.TableSqlFunction
import org.apache.flink.table.plan.nodes.logical.FlinkLogicalTableFunctionScan
import org.apache.flink.table.plan.schema.RowSchema
import org.apache.flink.table.planner.StreamPlanner
import org.apache.flink.table.runtime.CRowCorrelateProcessRunner
import org.apache.flink.table.runtime.types.{CRow, CRowTypeInfo}

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.core.JoinRelType
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rex.{RexCall, RexNode}

/**
  * Flink RelNode which matches along with join a Java/Scala user defined table function.
  */
class DataStreamCorrelate(
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
    joinType) {

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    new DataStreamCorrelate(
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

    val config = planner.getConfig

    // we do not need to specify input type
    val inputDS = getInput.asInstanceOf[DataStreamRel].translateToPlan(planner)

    val funcRel = scan.asInstanceOf[FlinkLogicalTableFunctionScan]
    val rexCall = funcRel.getCall.asInstanceOf[RexCall]
    val sqlFunction = rexCall.getOperator.asInstanceOf[TableSqlFunction]
    val pojoFieldMapping = Some(sqlFunction.getPojoFieldMapping)
    val udtfTypeInfo = sqlFunction.getRowTypeInfo.asInstanceOf[TypeInformation[Any]]

    val process = generateFunction(
      config,
      inputSchema,
      udtfTypeInfo,
      schema,
      joinType,
      rexCall,
      pojoFieldMapping,
      ruleDescription,
      classOf[ProcessFunction[CRow, CRow]])

    val collector = generateCollector(
      config,
      inputSchema,
      udtfTypeInfo,
      schema,
      condition,
      pojoFieldMapping)

    val processFunc = new CRowCorrelateProcessRunner(
      process.name,
      process.code,
      collector.name,
      collector.code,
      CRowTypeInfo(process.returnType))

    val inputParallelism = inputDS.getParallelism

    inputDS
      .process(processFunc)
      // preserve input parallelism to ensure that acc and retract messages remain in order
      .setParallelism(inputParallelism)
      .name(correlateOpName(
        inputSchema.relDataType,
        rexCall,
        sqlFunction,
        schema.relDataType,
        getExpressionString)
      )
  }

}
