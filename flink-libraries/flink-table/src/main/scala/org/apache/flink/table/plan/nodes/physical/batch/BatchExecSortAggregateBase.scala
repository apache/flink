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

import org.apache.flink.table.api.BatchTableEnvironment
import org.apache.flink.table.api.functions.{AggregateFunction, UserDefinedFunction}
import org.apache.flink.table.api.types.RowType
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.codegen.operator.OperatorCodeGenerator._
import org.apache.flink.table.codegen.{CodeGeneratorContext, GeneratedOperator}
import org.apache.flink.table.dataformat.{BinaryRow, GenericRow, JoinedRow}
import org.apache.flink.table.plan.cost.FlinkBatchCost._
import org.apache.flink.table.plan.cost.FlinkCostFactory
import org.apache.flink.table.runtime.AbstractStreamOperatorWithMetrics

import org.apache.calcite.plan.{RelOptCluster, RelOptCost, RelOptPlanner, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.tools.RelBuilder

abstract class BatchExecSortAggregateBase(
    cluster: RelOptCluster,
    relBuilder: RelBuilder,
    traitSet: RelTraitSet,
    inputNode: RelNode,
    aggCallToAggFunction: Seq[(AggregateCall, UserDefinedFunction)],
    rowRelDataType: RelDataType,
    inputRelDataType: RelDataType,
    grouping: Array[Int],
    auxGrouping: Array[Int],
    isMerge: Boolean,
    isFinal: Boolean)
  extends BatchExecGroupAggregateBase(
    cluster,
    relBuilder,
    traitSet,
    inputNode,
    aggCallToAggFunction,
    rowRelDataType,
    inputRelDataType,
    grouping,
    auxGrouping,
    isMerge,
    isFinal) {

  override def computeSelfCost(planner: RelOptPlanner, mq: RelMetadataQuery): RelOptCost = {
    val inputRows = mq.getRowCount(getInput())
    if (inputRows == null) {
      return null
    }
    // sort is not done here
    val cpuCost = FUNC_CPU_COST * inputRows * aggCallToAggFunction.size
    val averageRowSize: Double = mq.getAverageRowSize(this)
    val memCost = averageRowSize
    val costFactory = planner.getCostFactory.asInstanceOf[FlinkCostFactory]
    costFactory.makeCost(mq.getRowCount(this), cpuCost, 0, 0, memCost)
  }

  private[flink] def codegenWithKeys(
      ctx: CodeGeneratorContext,
      tableEnv: BatchTableEnvironment,
      inputType: RowType,
      outputType: RowType): GeneratedOperator = {
    val config = tableEnv.config
    val inputTerm = CodeGeneratorContext.DEFAULT_INPUT1_TERM

    // register udaggs
    aggCallToAggFunction.map(_._2).filter(a => a.isInstanceOf[AggregateFunction[_, _]])
        .map(a => ctx.addReusableFunction(a))

    val lastKeyTerm = "lastKey"
    val currentKeyTerm = "currentKey"
    val currentKeyWriterTerm = "currentKeyWriter"

    val keyProjectionCode = genGroupKeyProjectionCode("SortAgg", ctx,
      groupKeyRowType, getGrouping, inputType, inputTerm, currentKeyTerm, currentKeyWriterTerm)

    val keyNotEquals = genGroupKeyChangedCheckCode(currentKeyTerm, lastKeyTerm)

    val (initAggBufferCode, doAggregateCode, aggOutputExpr) = genSortAggCodes(
      isMerge, isFinal, ctx, config, builder, getGrouping, getAuxGrouping, inputRelDataType,
      aggCallToAggFunction, aggregates, udaggs, inputTerm, inputType,
      aggBufferNames, aggBufferTypes, outputType)

    val joinedRow = "joinedRow"
    ctx.addOutputRecord(outputType, classOf[JoinedRow], joinedRow)
    val binaryRow = classOf[BinaryRow].getName
    ctx.addReusableMember(s"$binaryRow $lastKeyTerm = null;")

    val processCode =
      s"""
         |hasInput = true;
         |${ctx.reuseInputUnboxingCode(Set(inputTerm))}
         |
         |// project key from input
         |$keyProjectionCode
         |if ($lastKeyTerm == null) {
         |  $lastKeyTerm = $currentKeyTerm.copy();
         |
         |  // init agg buffer
         |  $initAggBufferCode
         |} else if ($keyNotEquals) {
         |
         |  // write output
         |  ${aggOutputExpr.code}
         |
         |  ${generatorCollect(s"$joinedRow.replace($lastKeyTerm, ${aggOutputExpr.resultTerm})")}
         |
         |  $lastKeyTerm = $currentKeyTerm.copy();
         |
         |  // init agg buffer
         |  $initAggBufferCode
         |}
         |
         |// do doAggregateCode
         |$doAggregateCode
         |""".stripMargin.trim

    val endInputCode =
      s"""
         |if (hasInput) {
         |  // write last output
         |  ${aggOutputExpr.code}
         |  ${generatorCollect(s"$joinedRow.replace($lastKeyTerm, ${aggOutputExpr.resultTerm})")}
         |}
       """.stripMargin

    val className = if (isFinal) "SortAggregateWithKeys" else "LocalSortAggregateWithKeys"
    val baseClass = classOf[AbstractStreamOperatorWithMetrics[_]].getName
    generateOperator(
      ctx, className, baseClass, processCode, endInputCode, inputRelDataType, config)
  }
}
