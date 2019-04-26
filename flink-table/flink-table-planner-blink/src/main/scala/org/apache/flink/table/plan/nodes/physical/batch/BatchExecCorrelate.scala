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
package org.apache.flink.table.plan.nodes.physical.batch

import org.apache.flink.runtime.operators.DamBehavior
import org.apache.flink.streaming.api.transformations.StreamTransformation
import org.apache.flink.table.api.{BatchTableEnvironment, TableConfigOptions}
import org.apache.flink.table.codegen.{CodeGeneratorContext, CorrelateCodeGenerator}
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.functions.utils.TableSqlFunction
import org.apache.flink.table.plan.nodes.exec.{BatchExecNode, ExecNode}
import org.apache.flink.table.plan.nodes.logical.FlinkLogicalTableFunctionScan
import org.apache.flink.table.plan.util.RelExplainUtil

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.Correlate
import org.apache.calcite.rel.{RelNode, RelWriter, SingleRel}
import org.apache.calcite.rex.{RexCall, RexNode, RexProgram}
import org.apache.calcite.sql.SemiJoinType

import java.util

import scala.collection.JavaConversions._

/**
  * Batch physical RelNode for [[Correlate]] (user defined table function).
  */
class BatchExecCorrelate(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputRel: RelNode,
    scan: FlinkLogicalTableFunctionScan,
    condition: Option[RexNode],
    projectProgram: Option[RexProgram],
    outputRowType: RelDataType,
    joinType: SemiJoinType)
  extends SingleRel(cluster, traitSet, inputRel)
  with BatchPhysicalRel
  with BatchExecNode[BaseRow] {

  require(joinType == SemiJoinType.INNER || joinType == SemiJoinType.LEFT)

  override def deriveRowType(): RelDataType = outputRowType

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    copy(traitSet, inputs.get(0), projectProgram, outputRowType)
  }

  /**
    * Note: do not passing member 'child' because singleRel.replaceInput may update 'input' rel.
    */
  def copy(
      traitSet: RelTraitSet,
      child: RelNode,
      projectProgram: Option[RexProgram],
      outputType: RelDataType): RelNode = {
    new BatchExecCorrelate(
      cluster,
      traitSet,
      child,
      scan,
      condition,
      projectProgram,
      outputType,
      joinType)
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    val rexCall = scan.getCall.asInstanceOf[RexCall]
    val sqlFunction = rexCall.getOperator.asInstanceOf[TableSqlFunction]
    super.explainTerms(pw)
      .item("invocation", scan.getCall)
      .item("correlate", RelExplainUtil.correlateToString(
        input.getRowType, rexCall, sqlFunction, getExpressionString))
      .item("select", outputRowType.getFieldNames.mkString(","))
      .item("rowType", outputRowType)
      .item("joinType", joinType)
      .itemIf("condition", condition.orNull, condition.isDefined)
  }

  //~ ExecNode methods -----------------------------------------------------------

  override def getDamBehavior: DamBehavior = DamBehavior.PIPELINED

  override def getInputNodes: util.List[ExecNode[BatchTableEnvironment, _]] =
    getInputs.map(_.asInstanceOf[ExecNode[BatchTableEnvironment, _]])

  override def replaceInputNode(
      ordinalInParent: Int,
      newInputNode: ExecNode[BatchTableEnvironment, _]): Unit = {
    replaceInput(ordinalInParent, newInputNode.asInstanceOf[RelNode])
  }

  /**
    * Internal method, translates the [[org.apache.flink.table.plan.nodes.exec.BatchExecNode]]
    * into a Batch operator.
    *
    * @param tableEnv The [[BatchTableEnvironment]] of the translated Table.
    */
  override def translateToPlanInternal(
      tableEnv: BatchTableEnvironment): StreamTransformation[BaseRow] = {
    val inputTransformation = getInputNodes.get(0).translateToPlan(tableEnv)
      .asInstanceOf[StreamTransformation[BaseRow]]
    val operatorCtx = CodeGeneratorContext(tableEnv.getConfig)
    CorrelateCodeGenerator.generateCorrelateTransformation(
      tableEnv,
      operatorCtx,
      inputTransformation,
      input.getRowType,
      projectProgram,
      scan,
      condition,
      outputRowType,
      joinType,
      tableEnv.getConfig.getConf.getInteger(TableConfigOptions.SQL_RESOURCE_DEFAULT_PARALLELISM),
      retainHeader = false,
      getExpressionString,
      "BatchExecCorrelate")
  }

}
