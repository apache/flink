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

import org.apache.calcite.plan.{RelOptCluster, RelOptCost, RelOptPlanner, RelTraitSet}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.Calc
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rel.{RelNode, RelWriter}
import org.apache.calcite.rex._
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.java.DataSet
import org.apache.flink.table.api.BatchTableEnvironment
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.codegen.CodeGenerator
import org.apache.flink.table.plan.nodes.CommonCalc
import org.apache.flink.types.Row

/**
  * Flink RelNode which matches along with LogicalCalc.
  *
  */
class DataSetCalc(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    input: RelNode,
    rowRelDataType: RelDataType,
    calcProgram: RexProgram,
    ruleDescription: String)
  extends Calc(cluster, traitSet, input, calcProgram)
  with CommonCalc
  with DataSetRel {

  override def deriveRowType(): RelDataType = rowRelDataType

  override def copy(traitSet: RelTraitSet, child: RelNode, program: RexProgram): Calc = {
    new DataSetCalc(cluster, traitSet, child, getRowType, program, ruleDescription)
  }

  override def toString: String = calcToString(calcProgram, getExpressionString)

  override def explainTerms(pw: RelWriter): RelWriter = {
    pw.input("input", getInput)
      .item("select", selectionToString(calcProgram, getExpressionString))
      .itemIf("where",
        conditionToString(calcProgram, getExpressionString),
        calcProgram.getCondition != null)
  }

  override def computeSelfCost(planner: RelOptPlanner, metadata: RelMetadataQuery): RelOptCost = {
    val child = this.getInput
    val rowCnt = metadata.getRowCount(child)

    computeSelfCost(calcProgram, planner, rowCnt)
  }

  override def estimateRowCount(metadata: RelMetadataQuery): Double = {
    val child = this.getInput
    val rowCnt = metadata.getRowCount(child)

    estimateRowCount(calcProgram, rowCnt)
  }

  override def translateToPlan(tableEnv: BatchTableEnvironment): DataSet[Row] = {

    val config = tableEnv.getConfig

    val inputDS = getInput.asInstanceOf[DataSetRel].translateToPlan(tableEnv)

    val returnType = FlinkTypeFactory.toInternalRowTypeInfo(getRowType)

    val generator = new CodeGenerator(config, false, inputDS.getType)

    val body = functionBody(
      generator,
      inputDS.getType,
      getRowType,
      calcProgram,
      config)

    val genFunction = generator.generateFunction(
      ruleDescription,
      classOf[FlatMapFunction[Row, Row]],
      body,
      returnType)

    val mapFunc = calcMapFunction(genFunction)
    inputDS.flatMap(mapFunc).name(calcOpName(calcProgram, getExpressionString))
  }
}
