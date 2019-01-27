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

import org.apache.flink.streaming.api.transformations.{OneInputTransformation, StreamTransformation}
import org.apache.flink.table.api.StreamTableEnvironment
import org.apache.flink.table.calcite.{FlinkTypeFactory, RelTimeIndicatorConverter}
import org.apache.flink.table.codegen.{CalcCodeGenerator, CodeGeneratorContext}
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.plan.nodes.exec.RowStreamExecNode
import org.apache.flink.table.plan.nodes.logical.FlinkLogicalCalc
import org.apache.flink.table.plan.nodes.physical.FlinkPhysicalRel
import org.apache.flink.table.plan.util.CalcUtil
import org.apache.flink.table.runtime.AbstractProcessStreamOperator

import org.apache.calcite.plan.{RelOptCluster, RelOptCost, RelOptPlanner, RelTraitSet}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.Calc
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rel.{RelNode, RelWriter}
import org.apache.calcite.rex.RexProgram

/**
  * Flink RelNode which matches along with LogicalCalc.
  */
class StreamExecCalc(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    input: RelNode,
    relDataType: RelDataType,
    calcProgram: RexProgram,
    val ruleDescription: String)
  extends Calc(cluster, traitSet, input, calcProgram)
  with StreamPhysicalRel
  with RowStreamExecNode {

  override def deriveRowType(): RelDataType = relDataType

  override def copy(traitSet: RelTraitSet, child: RelNode, program: RexProgram): Calc = {
    new StreamExecCalc(
      cluster,
      traitSet,
      child,
      relDataType,
      program,
      ruleDescription)
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    pw.input("input", getInput)
      .item("select", CalcUtil.selectionToString(calcProgram, getExpressionString))
      .itemIf("where",
        CalcUtil.conditionToString(calcProgram, getExpressionString),
        calcProgram.getCondition != null)
  }

  override def isDeterministic: Boolean = CalcUtil.isDeterministic(program)

  override def computeSelfCost(planner: RelOptPlanner, metadata: RelMetadataQuery): RelOptCost = {
    FlinkLogicalCalc.computeCost(calcProgram, planner, metadata, this)
  }

  //~ ExecNode methods -----------------------------------------------------------

  override def getFlinkPhysicalRel: FlinkPhysicalRel = this

  override def translateToPlanInternal(
      tableEnv: StreamTableEnvironment): StreamTransformation[BaseRow] = {
    val config = tableEnv.getConfig
    val inputTransform = getInputNodes.get(0).translateToPlan(tableEnv)
      .asInstanceOf[StreamTransformation[BaseRow]]
    // materialize time attributes in condition
    val condition = if (calcProgram.getCondition != null) {
      val materializedCondition = RelTimeIndicatorConverter.convertExpression(
        calcProgram.expandLocalRef(calcProgram.getCondition),
        input.getRowType,
        cluster.getRexBuilder)
      Some(materializedCondition)
    } else {
      None
    }
    val ctx = CodeGeneratorContext(config, supportReference = true).setOperatorBaseClass(
      classOf[AbstractProcessStreamOperator[BaseRow]])
    val substituteStreamOperator = CalcCodeGenerator.generateCalcOperator(
      ctx,
      cluster,
      input.getRowType,
      inputTransform,
      getRowType,
      config,
      calcProgram,
      condition,
      retainHeader = true,
      ruleDescription = ruleDescription
    )
    val transformation = new OneInputTransformation(
      inputTransform,
      CalcUtil.calcToString(calcProgram, getExpressionString),
      substituteStreamOperator,
      FlinkTypeFactory.toInternalBaseRowTypeInfo(getRowType),
      inputTransform.getParallelism)
    transformation.setResources(getResource.getReservedResourceSpec,
      getResource.getPreferResourceSpec)
    transformation
  }
}
