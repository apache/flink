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

import org.apache.calcite.plan.{RelOptCluster, RelOptCost, RelOptPlanner, RelTraitSet}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rel.{RelNode, RelWriter, SingleRel}
import org.apache.calcite.rex.RexProgram
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.table.api.StreamTableEnvironment
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.codegen.CodeGenerator
import org.apache.flink.table.plan.nodes.CommonCalc
import org.apache.flink.types.Row

/**
  * Flink RelNode which matches along with FlatMapOperator.
  *
  */
class DataStreamCalc(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    input: RelNode,
    rowRelDataType: RelDataType,
    private[flink] val calcProgram: RexProgram,
    ruleDescription: String)
  extends SingleRel(cluster, traitSet, input)
  with CommonCalc
  with DataStreamRel {

  override def deriveRowType() = rowRelDataType

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    new DataStreamCalc(
      cluster,
      traitSet,
      inputs.get(0),
      getRowType,
      calcProgram,
      ruleDescription
    )
  }

  override def toString: String = calcToString(calcProgram, getExpressionString)

  override def explainTerms(pw: RelWriter): RelWriter = {
    super.explainTerms(pw)
      .item("select", selectionToString(calcProgram, getExpressionString))
      .itemIf("where",
        conditionToString(calcProgram, getExpressionString),
        calcProgram.getCondition != null)
  }

  override def computeSelfCost (planner: RelOptPlanner, metadata: RelMetadataQuery): RelOptCost = {
    val child = this.getInput
    val rowCnt = metadata.getRowCount(child)
    computeSelfCost(calcProgram, planner, rowCnt)
  }

  override def estimateRowCount(metadata: RelMetadataQuery): Double = {
    val child = this.getInput
    val rowCnt = metadata.getRowCount(child)
    estimateRowCount(calcProgram, rowCnt)
  }

  override def translateToPlan(tableEnv: StreamTableEnvironment): DataStream[Row] = {

    val config = tableEnv.getConfig

    val inputDataStream = getInput.asInstanceOf[DataStreamRel].translateToPlan(tableEnv)

    val generator = new CodeGenerator(config, false, inputDataStream.getType)

    val body = functionBody(
      generator,
      inputDataStream.getType,
      getRowType,
      calcProgram,
      config)

    val genFunction = generator.generateFunction(
      ruleDescription,
      classOf[FlatMapFunction[Row, Row]],
      body,
      FlinkTypeFactory.toInternalRowTypeInfo(getRowType))

    val mapFunc = calcMapFunction(genFunction)
    inputDataStream.flatMap(mapFunc).name(calcOpName(calcProgram, getExpressionString))
  }
}
