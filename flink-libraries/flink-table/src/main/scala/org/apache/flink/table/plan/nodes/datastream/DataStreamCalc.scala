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
import org.apache.calcite.rel.core.Calc
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rel.{RelNode, RelWriter}
import org.apache.calcite.rex.RexProgram
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.table.api.{StreamQueryConfig, StreamTableEnvironment}
import org.apache.flink.table.codegen.FunctionCodeGenerator
import org.apache.flink.table.calcite.{FlinkTypeFactory, RelTimeIndicatorConverter}
import org.apache.flink.table.plan.nodes.CommonCalc
import org.apache.flink.table.plan.schema.RowSchema
import org.apache.flink.table.runtime.CRowProcessRunner
import org.apache.flink.table.runtime.types.{CRow, CRowTypeInfo}

import scala.collection.JavaConverters._

/**
  * Flink RelNode which matches along with FlatMapOperator.
  *
  */
class DataStreamCalc(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    input: RelNode,
    inputSchema: RowSchema,
    schema: RowSchema,
    calcProgram: RexProgram,
    ruleDescription: String)
  extends Calc(cluster, traitSet, input, calcProgram)
  with CommonCalc
  with DataStreamRel {

  override def deriveRowType(): RelDataType = schema.relDataType

  override def copy(traitSet: RelTraitSet, child: RelNode, program: RexProgram): Calc = {
    new DataStreamCalc(
      cluster,
      traitSet,
      child,
      inputSchema,
      schema,
      program,
      ruleDescription)
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

  override def translateToPlan(
      tableEnv: StreamTableEnvironment,
      queryConfig: StreamQueryConfig): DataStream[CRow] = {

    val config = tableEnv.getConfig

    val inputDataStream =
      getInput.asInstanceOf[DataStreamRel].translateToPlan(tableEnv, queryConfig)

    // materialize time attributes in condition
    val condition = if (calcProgram.getCondition != null) {
      val materializedCondition = RelTimeIndicatorConverter.convertExpression(
        calcProgram.expandLocalRef(calcProgram.getCondition),
        inputSchema.relDataType,
        cluster.getRexBuilder)
      Some(materializedCondition)
    } else {
      None
    }

    // filter out time attributes
    val projection = calcProgram.getProjectList.asScala
      .map(calcProgram.expandLocalRef)

    val generator = new FunctionCodeGenerator(config, false, inputSchema.typeInfo)

    val genFunction = generateFunction(
      generator,
      ruleDescription,
      inputSchema,
      schema,
      projection,
      condition,
      config,
      classOf[ProcessFunction[CRow, CRow]])

    val inputParallelism = inputDataStream.getParallelism

    val processFunc = new CRowProcessRunner(
      genFunction.name,
      genFunction.code,
      CRowTypeInfo(schema.typeInfo))

    inputDataStream
      .process(processFunc)
      .name(calcOpName(calcProgram, getExpressionString))
      // keep parallelism to ensure order of accumulate and retract messages
      .setParallelism(inputParallelism)
  }
}
