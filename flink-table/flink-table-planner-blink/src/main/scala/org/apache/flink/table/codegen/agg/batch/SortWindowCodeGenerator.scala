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

package org.apache.flink.table.codegen.agg.batch

import org.apache.flink.streaming.api.operators.OneInputStreamOperator
import org.apache.flink.table.`type`.RowType
import org.apache.flink.table.api.window.TimeWindow
import org.apache.flink.table.calcite.FlinkRelBuilder.NamedWindowProperty
import org.apache.flink.table.codegen.CodeGenUtils.BINARY_ROW
import org.apache.flink.table.codegen.agg.batch.AggCodeGenHelper.genGroupKeyChangedCheckCode
import org.apache.flink.table.codegen.{CodeGenUtils, CodeGeneratorContext, ProjectionCodeGenerator}
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.functions.AggregateFunction
import org.apache.flink.table.generated.GeneratedOperator
import org.apache.flink.table.plan.logical.{LogicalWindow, SlidingGroupWindow, TumblingGroupWindow}
import org.apache.flink.table.plan.util.AggregateInfoList
import org.apache.flink.table.runtime.TableStreamOperator

import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.tools.RelBuilder

/**
  * Tumbling window: like [[SortAggCodeGenerator]].
  *
  * Sliding window:
  * 1.enableAssignPane + 1 phase:
  * -- distribute by (key)
  *   -- sort by (key + ts)
  *     -- assign pane + sort agg + assign window + sort agg
  * 2.enableAssignPane + 2 phase:
  * -- sort by (key + ts)
  *   -- assign pane + sort agg 
  *     -- distribute by (key)
  *       -- sort by (key + pane)
  *         -- assign window + sort agg
  * 3.disableAssignPane + 1 phase:
  * -- distribute by (key)
  *   -- sort by (key +ts)
  *     -- assign window + sort agg.
  */
class SortWindowCodeGenerator(
    ctx: CodeGeneratorContext,
    relBuilder: RelBuilder,
    window: LogicalWindow,
    inputTimeFieldIndex: Int,
    inputTimeIsDate: Boolean,
    namedProperties: Seq[NamedWindowProperty],
    aggInfoList: AggregateInfoList,
    inputRowType: RelDataType,
    inputType: RowType,
    outputType: RowType,
    buffLimitSize: Int,
    windowStart: Long,
    windowSize: Long,
    slideSize: Long,
    grouping: Array[Int],
    auxGrouping: Array[Int],
    enableAssignPane: Boolean = true,
    isMerge: Boolean,
    isFinal: Boolean)
  extends WindowCodeGenerator(
    relBuilder,
    window,
    inputTimeFieldIndex,
    inputTimeIsDate,
    namedProperties,
    aggInfoList,
    inputRowType,
    grouping,
    auxGrouping,
    enableAssignPane,
    isMerge,
    isFinal) {

  def genWithoutKeys(): GeneratedOperator[OneInputStreamOperator[BaseRow, BaseRow]] = {
    val inputTerm = CodeGenUtils.DEFAULT_INPUT1_TERM

    aggCallToAggFunction
        .map(_._2).filter(a => a.isInstanceOf[AggregateFunction[_, _]])
        .map(a => ctx.addReusableFunction(a))

    val timeWindowType = classOf[TimeWindow].getName
    val currentWindow = CodeGenUtils.newName("currentWindow")
    ctx.addReusableMember(s"transient $timeWindowType $currentWindow = null;")

    val windowsGrouping = CodeGenUtils.newName("windowsGrouping")
    val enablePreAcc = choosePreAcc || isMerge
    val windowElementType = getWindowsGroupingElementInfo(enablePreAcc)

    val (triggerWindowAgg, endWindowAgg) = genWindowAggCodes(
      enablePreAcc,
      ctx,
      windowSize,
      slideSize,
      windowsGrouping,
      buffLimitSize,
      windowElementType,
      inputTimeFieldIndex,
      currentWindow,
      None,
      outputType)

    val (processInput, endProcessInput) =
      if (enablePreAcc) {
        genPreAccumulate(ctx,
          windowStart,
          slideSize,
          windowSize,
          inputTerm,
          inputType,
          outputType,
          windowsGrouping,
          windowElementType,
          None,
          triggerWindowAgg,
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
         |if (!$inputTerm.isNullAt($inputTimeFieldIndex)) {
         |  $processInput
         |}
         |""".stripMargin

    val endInputCode =
      s"""
         |if (hasInput) {
         |  $endProcessInput
         |}""".stripMargin

    val className = if (isFinal) "SortWinAggWithoutKeys" else "LocalSortWinAggWithoutKeys"
    val baseClass = classOf[TableStreamOperator[_]].getName
    AggCodeGenHelper.generateOperator(
      ctx, className, baseClass, processCode, endInputCode, inputType)
  }

  def genWithKeys(): GeneratedOperator[OneInputStreamOperator[BaseRow, BaseRow]] = {
    aggCallToAggFunction
        .map(_._2).filter(a => a.isInstanceOf[AggregateFunction[_, _]])
        .map(a => ctx.addReusableFunction(a))

    val inputTerm = CodeGenUtils.DEFAULT_INPUT1_TERM

    val currentKey = CodeGenUtils.newName("currentKey")
    val currentKeyWriter = CodeGenUtils.newName("currentKeyWriter")
    val lastKey = CodeGenUtils.newName("lastKey")
    ctx.addReusableMember(s"transient $BINARY_ROW $lastKey = null;")

    val keyProjectionCode = ProjectionCodeGenerator.generateProjectionExpression(
      ctx,
      inputType,
      groupKeyRowType,
      grouping,
      inputTerm = inputTerm,
      outRecordTerm = currentKey,
      outRecordWriterTerm = currentKeyWriter).code

    val keyNotEqualsCode = genGroupKeyChangedCheckCode(currentKey, lastKey)

    // gen code to merge pre-accumulated results
    val timeWindowType = classOf[TimeWindow].getName
    val currentWindow = CodeGenUtils.newName("currentWindow")
    ctx.addReusableMember(s"transient $timeWindowType $currentWindow = null;")

    val windowsGrouping = CodeGenUtils.newName("windowsGrouping")
    val enablePreAcc = choosePreAcc || isMerge
    val windowElementType = getWindowsGroupingElementInfo(enablePreAcc)

    val (triggerWindowAgg, endWindowAgg) = genWindowAggCodes(
      enablePreAcc,
      ctx,
      windowSize,
      slideSize,
      windowsGrouping,
      buffLimitSize,
      windowElementType,
      inputTimeFieldIndex,
      currentWindow,
      Some(lastKey),
      outputType)

    val (processCurrentKeyInput, endProcessCurrentKeyInput) =
      if (enablePreAcc) {
        genPreAccumulate(ctx,
          windowStart,
          slideSize,
          windowSize,
          inputTerm,
          inputType,
          outputType,
          windowsGrouping,
          windowElementType,
          Some(lastKey),
          triggerWindowAgg,
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
         |if (!$inputTerm.isNullAt($inputTimeFieldIndex)) {
         |  // reusable input fields access
         |  ${ctx.reuseInputUnboxingCode(inputTerm)}
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
    val baseClass = classOf[TableStreamOperator[_]].getName
    AggCodeGenHelper.generateOperator(
      ctx, className, baseClass, processCode, endInputCode, inputType)
  }

  private def choosePreAcc: Boolean = {
    // pre accumulate by pane
    enableAssignPane ||
        // pre accumulate by window
        window.isInstanceOf[TumblingGroupWindow] ||
        (window.isInstanceOf[SlidingGroupWindow] && slideSize == windowSize)
  }
}
