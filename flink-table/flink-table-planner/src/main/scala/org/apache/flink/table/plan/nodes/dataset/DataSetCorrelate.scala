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

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.DataSet
import org.apache.flink.table.api.internal.BatchTableEnvImpl
import org.apache.flink.table.functions.utils.TableSqlFunction
import org.apache.flink.table.plan.nodes.logical.FlinkLogicalTableFunctionScan
import org.apache.flink.table.plan.schema.RowSchema
import org.apache.flink.table.runtime.CorrelateFlatMapRunner
import org.apache.flink.types.Row

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.JoinRelType
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rex.{RexCall, RexNode}

/**
  * Flink RelNode which matches along with join a Java/Scala user defined table function.
  */
class DataSetCorrelate(
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
    ruleDescription) {

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    new DataSetCorrelate(
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

    val config = tableEnv.getConfig

    // we do not need to specify input type
    val inputDS = inputNode.asInstanceOf[DataSetRel].translateToPlan(tableEnv)

    val funcRel = scan.asInstanceOf[FlinkLogicalTableFunctionScan]
    val rexCall = funcRel.getCall.asInstanceOf[RexCall]
    val sqlFunction = rexCall.getOperator.asInstanceOf[TableSqlFunction]
    val pojoFieldMapping = Some(sqlFunction.getPojoFieldMapping)
    val udtfTypeInfo = sqlFunction.getRowTypeInfo.asInstanceOf[TypeInformation[Any]]

    val flatMap = generateFunction(
      config,
      new RowSchema(getInput.getRowType),
      udtfTypeInfo,
      new RowSchema(getRowType),
      joinType,
      rexCall,
      pojoFieldMapping,
      ruleDescription,
      classOf[FlatMapFunction[Row, Row]])

    val collector = generateCollector(
      config,
      new RowSchema(getInput.getRowType),
      udtfTypeInfo,
      new RowSchema(getRowType),
      condition,
      pojoFieldMapping)

    val mapFunc = new CorrelateFlatMapRunner[Row, Row](
      flatMap.name,
      flatMap.code,
      collector.name,
      collector.code,
      flatMap.returnType)

    inputDS
      .flatMap(mapFunc)
      .name(correlateOpName(
        inputNode.getRowType,
        rexCall,
        sqlFunction,
        relRowType,
        getExpressionString)
      )
  }
}
