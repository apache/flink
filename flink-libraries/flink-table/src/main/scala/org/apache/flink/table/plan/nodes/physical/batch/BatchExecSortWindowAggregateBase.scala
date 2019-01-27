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
import org.apache.flink.table.api.functions.UserDefinedFunction
import org.apache.flink.table.api.types.{RowType}
import org.apache.flink.table.api.window.TimeWindow
import org.apache.flink.table.calcite.FlinkRelBuilder.NamedWindowProperty
import org.apache.flink.table.codegen.CodeGeneratorContext._
import org.apache.flink.table.codegen._
import org.apache.flink.table.plan.cost.FlinkBatchCost._
import org.apache.flink.table.plan.cost.FlinkCostFactory
import org.apache.flink.table.plan.logical.LogicalWindow
import org.apache.flink.table.runtime.AbstractStreamOperatorWithMetrics

import org.apache.calcite.plan.{RelOptCluster, RelOptCost, RelOptPlanner, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.tools.RelBuilder

abstract class BatchExecSortWindowAggregateBase(
    window: LogicalWindow,
    inputTimestampIndex: Int,
    inputTimestampType: RelDataType,
    namedProperties: Seq[NamedWindowProperty],
    cluster: RelOptCluster,
    relBuilder: RelBuilder,
    traitSet: RelTraitSet,
    inputNode: RelNode,
    aggCallToAggFunction: Seq[(AggregateCall, UserDefinedFunction)],
    rowRelDataType: RelDataType,
    inputRelDataType: RelDataType,
    grouping: Array[Int],
    auxGrouping: Array[Int],
    assignPane: Boolean = false,
    isMerge: Boolean,
    isFinal: Boolean)
  extends BatchExecWindowAggregateBase(
    window,
    inputTimestampIndex,
    inputTimestampType,
    namedProperties,
    cluster,
    relBuilder,
    traitSet,
    inputNode,
    aggCallToAggFunction,
    rowRelDataType,
    inputRelDataType,
    grouping,
    auxGrouping,
    assignPane,
    isMerge,
    isFinal) {

  private[flink] def codegenWithoutKeys(
      ctx: CodeGeneratorContext,
      tableEnv: BatchTableEnvironment,
      inputType: RowType,
      outputType: RowType,
      buffLimitSize: Int,
      windowStart: Long,
      windowSize: Long,
      slideSize: Long): GeneratedOperator = {

    val config = tableEnv.config

    val inputTerm = CodeGeneratorContext.DEFAULT_INPUT1_TERM
    registerUDAGGs(ctx, aggCallToAggFunction)

    val timeWindowType = classOf[TimeWindow].getName
    val currentWindow = CodeGenUtils.newName("currentWindow")
    ctx.addReusableMember(s"transient $timeWindowType $currentWindow = null;")

    val windowsGrouping = CodeGenUtils.newName("windowsGrouping")
    val enablePreAcc = choosePreAcc(assignPane, window, slideSize, windowSize) || isMerge
    val windowElementType = getWindowsGroupingElementInfo(enablePreAcc)

    val (triggerWindowAgg, endWindowAgg) = genWindowAggCodes(
      enablePreAcc, ctx, config, windowSize, slideSize, windowsGrouping, buffLimitSize,
      windowElementType, inputTimestampIndex, currentWindow, None, outputType)

    val (processInput, endProcessInput) =
      if (enablePreAcc) {
        genPreAccumulate(ctx, config, window, windowStart, slideSize, windowSize, inputTerm,
          inputType, outputType, windowsGrouping, windowElementType, None, triggerWindowAgg,
          endWindowAgg)
      } else {
        (s"""
            |hasInput = true;
            |$windowsGrouping.addInputToBuffer(($BINARY_ROW)$inputTerm);
            |$triggerWindowAgg
         """.stripMargin, endWindowAgg)
      }

    val processCode =
      s"""
         |if (!$inputTerm.isNullAt($inputTimestampIndex)) {
         |  $processInput
         |}
         |""".stripMargin

    val endInputCode =
      s"""
         |if (hasInput) {
         |  $endProcessInput
         |}""".stripMargin

    val className = if (isFinal) "SortWinAggWithoutKeys" else "LocalSortWinAggWithoutKeys"
    val baseClass = classOf[AbstractStreamOperatorWithMetrics[_]].getName
    generateOperator(
      ctx, className, baseClass, processCode, endInputCode, inputRelDataType, config)
  }

  private[flink] def codegenWithKeys(
      ctx: CodeGeneratorContext,
      tableEnv: BatchTableEnvironment,
      inputType: RowType,
      outputType: RowType,
      buffLimitSize: Int,
      windowStart: Long,
      windowSize: Long,
      slideSize: Long): GeneratedOperator = {

    val config = tableEnv.config
    registerUDAGGs(ctx, aggCallToAggFunction)

    val inputTerm = CodeGeneratorContext.DEFAULT_INPUT1_TERM

    val currentKey = CodeGenUtils.newName("currentKey")
    val currentKeyWriter = CodeGenUtils.newName("currentKeyWriter")
    val lastKey = CodeGenUtils.newName("lastKey")
    ctx.addReusableMember(s"transient $BINARY_ROW $lastKey = null;")

    val keyProjectionCode = genGroupKeyProjectionCode("GlobalSortWindowAggGroupKey", ctx,
      groupKeyRowType, grouping, inputType, inputTerm, currentKey, currentKeyWriter)

    val keyNotEqualsCode = genGroupKeyChangedCheckCode(currentKey, lastKey)

    // gen code to merge pre-accumulated results
    val timeWindowType = classOf[TimeWindow].getName
    val currentWindow = CodeGenUtils.newName("currentWindow")
    ctx.addReusableMember(s"transient $timeWindowType $currentWindow = null;")

    val windowsGrouping = CodeGenUtils.newName("windowsGrouping")
    val enablePreAcc = choosePreAcc(assignPane, window, slideSize, windowSize) || isMerge
    val windowElementType = getWindowsGroupingElementInfo(enablePreAcc)

    val (triggerWindowAgg, endWindowAgg) = genWindowAggCodes(
      enablePreAcc, ctx, config, windowSize, slideSize, windowsGrouping, buffLimitSize,
      windowElementType, inputTimestampIndex, currentWindow, Some(lastKey), outputType)

    val (processCurrentKeyInput, endProcessCurrentKeyInput) =
      if (enablePreAcc) {
        genPreAccumulate(ctx, config, window, windowStart, slideSize, windowSize, inputTerm,
          inputType, outputType, windowsGrouping, windowElementType, Some(lastKey),
          triggerWindowAgg, endWindowAgg)
      } else {
        (s"""
          |hasInput = true;
          |$windowsGrouping.addInputToBuffer(($BINARY_ROW)$inputTerm);
          |$triggerWindowAgg
         """.stripMargin, endWindowAgg)
      }

    val processCode =
      s"""
         |if (!$inputTerm.isNullAt($inputTimestampIndex)) {
         |  // reusable input fields access
         |  ${ctx.reuseInputUnboxingCode(Set(inputTerm))}
         |  // project key from input
         |  $keyProjectionCode
         |  // find next group and aggregate
         |  if ($lastKey == null) {
         |   $lastKey = $currentKey.copy();
         |  } else if ($keyNotEqualsCode) {
         |    $endProcessCurrentKeyInput
         |    $lastKey = $currentKey.copy();
         |  }
         |  // assign each input with an aligned window start timestamp
         |  // and do accumulate if possible
         |  // buffer it into current group buffer
         |  // and do aggregation for all trigger windows if exits
         |  $processCurrentKeyInput
         |}
         |""".stripMargin.trim

    val endInputCode =
      s"""
         |if (hasInput) {
         |  $endProcessCurrentKeyInput
         |}
         """.stripMargin

    val className = if (isFinal) "SortWinAggWithKeys" else "LocalSortWinAggWithKeys"
    val baseClass = classOf[AbstractStreamOperatorWithMetrics[_]].getName
    generateOperator(
      ctx, className, baseClass, processCode, endInputCode, inputRelDataType, config)
  }

  override def computeSelfCost(planner: RelOptPlanner, mq: RelMetadataQuery): RelOptCost = {
    val inputRowCnt = mq.getRowCount(getInput)
    if (inputRowCnt == null) {
      return null
    }
    // does not take pane optimization into consideration here
    // sort is not done here
    val cpu = FUNC_CPU_COST * inputRowCnt * aggCallToAggFunction.size
    val averageRowSize: Double = mq.getAverageRowSize(this)
    val memCost = averageRowSize
    val costFactory = planner.getCostFactory.asInstanceOf[FlinkCostFactory]
    costFactory.makeCost(mq.getRowCount(this), cpu, 0, 0, memCost)
  }
}
