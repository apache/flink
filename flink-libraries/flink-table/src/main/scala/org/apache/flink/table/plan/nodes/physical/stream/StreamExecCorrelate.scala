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
package org.apache.flink.table.plan.nodes.physical.stream

import org.apache.flink.streaming.api.transformations.StreamTransformation
import org.apache.flink.table.api.StreamTableEnvironment
import org.apache.flink.table.codegen.{CodeGeneratorContext, CorrelateCodeGenerator}
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.functions.utils.TableSqlFunction
import org.apache.flink.table.plan.nodes.exec.RowStreamExecNode
import org.apache.flink.table.plan.nodes.logical.FlinkLogicalTableFunctionScan
import org.apache.flink.table.plan.nodes.physical.FlinkPhysicalRel
import org.apache.flink.table.plan.util.CorrelateUtil
import org.apache.flink.table.runtime.AbstractProcessStreamOperator

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.{RelNode, RelWriter, SingleRel}
import org.apache.calcite.rex.{RexCall, RexNode, RexProgram}
import org.apache.calcite.sql.SemiJoinType

/**
  * Flink RelNode which matches along with join a user defined table function.
  */
class StreamExecCorrelate(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    child: RelNode,
    projectProgram: Option[RexProgram],
    scan: FlinkLogicalTableFunctionScan,
    condition: Option[RexNode],
    relDataType: RelDataType,
    joinType: SemiJoinType,
    ruleDescription: String)
  extends SingleRel(cluster, traitSet, child)
  with StreamPhysicalRel
  with RowStreamExecNode {

  require(joinType == SemiJoinType.INNER || joinType == SemiJoinType.LEFT)

  override def deriveRowType(): RelDataType = relDataType

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    copy(traitSet, inputs.get(0), projectProgram, relDataType)
  }

  /**
    * Note: do not passing member 'child' because singleRel.replaceInput may update 'input' rel.
    */
  def copy(
      traitSet: RelTraitSet,
      newChild: RelNode,
      projectProgram: Option[RexProgram],
      outputType: RelDataType): RelNode = {
    new StreamExecCorrelate(
      cluster,
      traitSet,
      newChild,
      projectProgram,
      scan,
      condition,
      outputType,
      joinType,
      ruleDescription)
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    val rexCall = scan.getCall.asInstanceOf[RexCall]
    val sqlFunction = rexCall.getOperator.asInstanceOf[TableSqlFunction]
    super.explainTerms(pw)
      .item("invocation", scan.getCall)
      .item("correlate", CorrelateUtil.correlateToString(
        child.getRowType, rexCall, sqlFunction, getExpressionString))
      .item("select", CorrelateUtil.selectToString(relDataType))
      .item("rowType", relDataType)
      .item("joinType", joinType)
      .itemIf("condition", condition.orNull, condition.isDefined)
  }

  override def isDeterministic: Boolean = CorrelateUtil.isDeterministic(scan, condition)

  //~ ExecNode methods -----------------------------------------------------------

  override def getFlinkPhysicalRel: FlinkPhysicalRel = this

  override def translateToPlanInternal(
      tableEnv: StreamTableEnvironment): StreamTransformation[BaseRow] = {

    val inputTransformation = getInputNodes.get(0).translateToPlan(tableEnv)
      .asInstanceOf[StreamTransformation[BaseRow]]
    val operatorCtx = CodeGeneratorContext(tableEnv.getConfig, supportReference = true)
      .setOperatorBaseClass(classOf[AbstractProcessStreamOperator[_]])
    val transformation = CorrelateCodeGenerator.generateCorrelateTransformation(
      tableEnv,
      operatorCtx,
      inputTransformation,
      child.getRowType,
      projectProgram,
      scan,
      condition,
      relDataType,
      joinType,
      inputTransformation.getParallelism,
      retainHeader = true,
      getExpressionString,
      ruleDescription)
    transformation.setResources(getResource.getReservedResourceSpec,
      getResource.getPreferResourceSpec)
    transformation
  }
}
