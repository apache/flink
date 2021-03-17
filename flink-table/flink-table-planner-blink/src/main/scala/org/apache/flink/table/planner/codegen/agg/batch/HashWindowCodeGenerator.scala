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

package org.apache.flink.table.planner.codegen.agg.batch

import org.apache.flink.api.java.tuple.{Tuple2 => JTuple2}
import org.apache.flink.api.java.typeutils.ListTypeInfo
import org.apache.flink.runtime.operators.sort.QuickSort
import org.apache.flink.streaming.api.operators.OneInputStreamOperator
import org.apache.flink.table.api.Types
import org.apache.flink.table.data.RowData
import org.apache.flink.table.data.binary.BinaryRowData
import org.apache.flink.table.planner.codegen.CodeGenUtils.{BINARY_ROW, newName}
import org.apache.flink.table.planner.codegen.OperatorCodeGenerator.generateCollect
import org.apache.flink.table.planner.codegen._
import org.apache.flink.table.planner.codegen.agg.batch.AggCodeGenHelper.genGroupKeyChangedCheckCode
import org.apache.flink.table.planner.codegen.agg.batch.HashAggCodeGenHelper.{genHashAggOutputExpr, genRetryAppendToMap, prepareHashAggKVTypes, prepareHashAggMap}
import org.apache.flink.table.planner.expressions.PlannerNamedWindowProperty
import org.apache.flink.table.planner.plan.logical.{LogicalWindow, SlidingGroupWindow, TumblingGroupWindow}
import org.apache.flink.table.planner.plan.utils.AggregateInfoList
import org.apache.flink.table.runtime.generated.GeneratedOperator
import org.apache.flink.table.runtime.operators.TableStreamOperator
import org.apache.flink.table.runtime.operators.aggregate.BytesHashMapSpillMemorySegmentPool
import org.apache.flink.table.runtime.operators.sort.BinaryKVInMemorySortBuffer
import org.apache.flink.table.runtime.operators.window.TimeWindow
import org.apache.flink.table.runtime.types.TypeInfoLogicalTypeConverter.fromTypeInfoToLogicalType
import org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer
import org.apache.flink.table.runtime.util.KeyValueIterator
import org.apache.flink.table.runtime.util.collections.binary.BytesMap
import org.apache.flink.table.types.logical.{LogicalType, RowType}
import org.apache.flink.util.MutableObjectIterator

import org.apache.calcite.tools.RelBuilder
import org.apache.commons.math3.util.ArithmeticUtils

import scala.collection.JavaConversions._

/**
  * Tumbling window: like [[HashAggCodeGenerator]].
  *
  * Sliding window:
  * 1.enableAssignPane + 2 phase:
  * -- assign pane + hash agg
  *     -- distribute by (key)
  *       -- global hash agg(key + pane)
  *         -- sort by (key + pane)
  *           -- assign window + sort agg
  * 2.disableAssignPane + 1 phase:
  * -- distribute by (key)
  *   -- assign window + hash agg[(key + window) -> agg buffer].
  */
class HashWindowCodeGenerator(
    ctx: CodeGeneratorContext,
    relBuilder: RelBuilder,
    window: LogicalWindow,
    inputTimeFieldIndex: Int,
    inputTimeIsDate: Boolean,
    namedProperties: Seq[PlannerNamedWindowProperty],
    aggInfoList: AggregateInfoList,
    inputRowType: RowType,
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

  private lazy val aggBufferRowType = RowType.of(aggBufferTypes.flatten, aggBufferNames.flatten)

  private lazy val aggMapKeyRowType = RowType.of(
    (groupKeyRowType.getChildren :+ timestampInternalType).toArray,
    (groupKeyRowType.getFieldNames :+ "assignedTs").toArray)

  def gen(
      inputType: RowType,
      outputType: RowType,
      buffLimitSize: Int,
      windowStart: Long,
      windowSize: Long,
      slideSize: Long): GeneratedOperator[OneInputStreamOperator[RowData, RowData]] = {
    val className = if (isFinal) "HashWinAgg" else "LocalHashWinAgg"
    val suffix = if (grouping.isEmpty) "WithoutKeys" else "WithKeys"

    val inputTerm = CodeGenUtils.DEFAULT_INPUT1_TERM
    // add logger
    val logTerm = CodeGenUtils.newName("LOG")
    ctx.addReusableLogger(logTerm, className)

    // gen code to do aggregate using aggregate map
    val aggMapKey = newName("aggMapKey")
    val aggMapKeyWriter = newName("aggMapKeyWriter")
    val (processElementPerWindow, outputResultFromMap) = genHashWindowAggCodes(
      buffLimitSize,
      windowSize,
      slideSize,
      inputTerm,
      inputType,
      outputType,
      aggMapKey,
      logTerm)

    val (processCode, outputWhenEndInputCode) = if (isFinal && isMerge) {
      // prepare for aggregate map key's projection
      val projAggMapKeyCode = ProjectionCodeGenerator.generateProjectionExpression(ctx,
        inputType,
        aggMapKeyRowType,
        grouping :+ grouping.length,
        inputTerm = inputTerm,
        outRecordTerm = aggMapKey,
        outRecordWriterTerm = aggMapKeyWriter).code

      val processCode =
        s"""
           |if (!$inputTerm.isNullAt($inputTimeFieldIndex)) {
           |  hasInput = true;
           |  // input field access for group key projection, window/pane assign
           |  // and aggregate map update
           |  ${ctx.reuseInputUnboxingCode(inputTerm)}
           |  // build aggregate map key
           |  $projAggMapKeyCode
           |  // look up aggregate map and aggregate
           |  $processElementPerWindow
           |}
       """.stripMargin

      (processCode, outputResultFromMap)
    } else {
      // gen code to assign windows/pane to current input
      val assignTimestampExprs = genTimestampAssignExprs(
        enableAssignPane, windowStart, windowSize, slideSize, window, inputTerm, inputType)

      val processCode =
        if (!isSlidingWindowWithOverlapping(enableAssignPane, window, slideSize, windowSize)) {
          // Each input will be assigned with only one window, in the cases of
          // Tumbling window, Sliding window with slideSize >= windowSize or with pane optimization.
          assert(assignTimestampExprs.size == 1)
          val assignTimestampExpr = assignTimestampExprs.head

          // prepare for aggregate map key's projection
          val accessAssignedTimestampExpr = GeneratedExpression(
            assignTimestampExpr.resultTerm, "false", "", timestampInternalType)
          val prepareInitAggMapKeyExpr = prepareAggMapKeyExpr(inputTerm, inputType,
            Some(accessAssignedTimestampExpr), aggMapKeyRowType, aggMapKey, aggMapKeyWriter)
          val processAggregate =
            s"""
               |  // build aggregate map key
               |  ${prepareInitAggMapKeyExpr.code}
               |  // aggregate by each input with assigned timestamp
               |  $processElementPerWindow
           """.stripMargin

          // gen code to filter invalid windows in the case of jumping window
          val processEachInput = if (isJumpingWindow(slideSize, windowSize)) {
            val checkValidWindow = s"${getInputTimeValue(inputTerm, inputTimeFieldIndex)} < " +
                s"${assignTimestampExpr.resultTerm} + ${windowSize}L"
            s"""
               |if ($checkValidWindow) {
               |  // build aggregate map key
               |  ${prepareInitAggMapKeyExpr.code}
               |  // aggregate by each input with assigned timestamp
               |  $processAggregate
               |}
           """.stripMargin
          } else {
            processAggregate
          }
          s"""
             |if (!$inputTerm.isNullAt($inputTimeFieldIndex)) {
             |  hasInput = true;
             |  // input field access for group key projection, window/pane assign
             |   // and aggregate map update
             |  ${ctx.reuseInputUnboxingCode(inputTerm)}
             |  // assign timestamp(window or pane)
             |  ${assignTimestampExpr.code}
             |  // process each input
             |  $processEachInput
             |}""".stripMargin
        } else {
          // Otherwise, each input will be assigned with overlapping windows.
          assert(assignTimestampExprs.size > 1)
          val assignedWindows = newName("assignedWindows")
          ctx.addReusableMember(
            s"transient java.util.List<java.lang.Long> $assignedWindows" +
                s" = new java.util.ArrayList<java.lang.Long>();")
          val prepareCodes = for (expr <- assignTimestampExprs) yield {
            s"""
               |${expr.code}
               |$assignedWindows.add(${expr.resultTerm});
               """.stripMargin
          }
          val code =
            s"""
               |$assignedWindows.clear();
               |${prepareCodes.mkString("\n").trim}
               """.stripMargin
          val assignTimestampExpr =
            new GeneratedExpression(assignedWindows, "false", code,
              fromTypeInfoToLogicalType(new ListTypeInfo(Types.LONG)))

          // gen code to filter invalid overlapping windows
          val assignedTimestamp = newName("assignedTimestamp")
          val timestampTerm = s"${getInputTimeValue(inputTerm, inputTimeFieldIndex)}"
          val checkValidWindow = s"$timestampTerm >= $assignedTimestamp " +
              s" && $timestampTerm < $assignedTimestamp + ${windowSize}L"

          // prepare for aggregate map key's projection
          val prepareInitAggMapKeyExpr = prepareAggMapKeyExpr(
            inputTerm, inputType, None, aggMapKeyRowType, aggMapKey, aggMapKeyWriter)
          val realAssignedValue = if (inputTimeIsDate) {
            convertToIntValue(s"$assignedTimestamp")
          } else {
            assignedTimestamp
          }
          val updateAssignedTsCode = s"$aggMapKey.set$timestampInternalTypeName(${
            grouping.length
          }, $realAssignedValue);"

          s"""
             |if (!$inputTerm.isNullAt($inputTimeFieldIndex)) {
             |  hasInput = true;
             |  // input field access for group key projection, window/pane assign
             |  // and aggregate map update
             |  ${ctx.reuseInputUnboxingCode(inputTerm)}
             |  // assign windows/pane
             |  ${assignTimestampExpr.code}
             |  // build aggregate map key
             |  ${prepareInitAggMapKeyExpr.code}
             |  // we assigned all the possible overlapping windows in this case,
             |  // so need filtering the invalid window here
             |  for (Long $assignedTimestamp : ${assignTimestampExpr.resultTerm}) {
             |    if ($checkValidWindow) {
             |     // update input's assigned timestamp
             |     $updateAssignedTsCode
             |     $processElementPerWindow
             |    } else {
             |     break;
             |    }
             |  }
             |}
       """.stripMargin
        }
      (processCode, outputResultFromMap)
    }

    val baseClass = classOf[TableStreamOperator[_]].getName
    val endInputCode = if (isFinal) {
      s"""
         |$outputWhenEndInputCode
         """.stripMargin
    } else {
      outputWhenEndInputCode
    }
    AggCodeGenHelper.generateOperator(
      ctx, className + suffix, baseClass, processCode, endInputCode, inputType)
  }

  private def genTimestampAssignExprs(
      assignPane: Boolean,
      windowStart: Long,
      windowSize: Long,
      slideSize: Long,
      window: LogicalWindow,
      inputTerm: String,
      inputType: RowType): Seq[GeneratedExpression] = {
    window match {
      case SlidingGroupWindow(_, timeField, _, _) =>
        if (assignPane) {
          val paneSize = ArithmeticUtils.gcd(windowSize, slideSize)
          Seq(genAlignedWindowStartExpr(
            ctx, inputTerm, inputType, timeField, windowStart, paneSize))
        } else if (slideSize >= windowSize) {
          Seq(genAlignedWindowStartExpr(
            ctx, inputTerm, inputType, timeField, windowStart, slideSize))
        } else {
          val maxNumOverlapping = math.ceil(windowSize * 1.0 / slideSize).toInt
          val exprs = for (index <- 0 until maxNumOverlapping) yield {
            genAlignedWindowStartExpr(
              ctx, inputTerm, inputType, timeField, windowStart, slideSize, index)
          }
          exprs
        }
      case TumblingGroupWindow(_, timeField, _) =>
        Seq(genAlignedWindowStartExpr(
          ctx, inputTerm, inputType, timeField, windowStart, windowSize))
      case _ =>
        throw new RuntimeException(s"Bug. Assign pane for $window is not supported.")
    }
  }

  private def prepareAggMapKeyExpr(
      inputTerm: String,
      inputType: LogicalType,
      assignedTimestampExpr: Option[GeneratedExpression],
      currentKeyType: RowType,
      currentKeyTerm: String,
      currentKeyWriterTerm: String): GeneratedExpression = {
    val codeGen = new ExprCodeGenerator(ctx, false)
        .bindInput(inputType, inputTerm = inputTerm)
    val expr = if (assignedTimestampExpr.isDefined) {
      val assignedLongTimestamp = assignedTimestampExpr.get
      if (inputTimeIsDate) {
        val dateTerm = ctx.addReusableLocalVariable("int", "dateTerm")
        val convertDateCode =
          s"""
             |  ${assignedLongTimestamp.code}
             |  $dateTerm = ${convertToIntValue(assignedLongTimestamp.resultTerm)};
           """.stripMargin
        GeneratedExpression(
          dateTerm,
          assignedLongTimestamp.nullTerm,
          convertDateCode,
          timestampInternalType)
      } else {
        assignedLongTimestamp
      }
    } else {
      val value = if (inputTimeIsDate) "-1" else "-1L"
      GeneratedExpression(value, "false", "", timestampInternalType)
    }
    // There will be only assigned timestamp field when no grouping window aggregate case.
    codeGen.generateResultExpression(
      grouping.map(
        idx => GenerateUtils.generateFieldAccess(ctx, inputType, inputTerm, idx)) :+ expr,
      currentKeyType.asInstanceOf[RowType],
      classOf[BinaryRowData],
      outRow = currentKeyTerm,
      outRowWriter = Some(currentKeyWriterTerm))
  }

  private def genGroupWindowHashAggCodes(
      isMerge: Boolean,
      isFinal: Boolean,
      windowSize: Long,
      slideSize: Long,
      aggMapKeyTypesTerm: String,
      aggBufferTypesTerm: String,
      bufferLimitSize: Int,
      aggregateMapTerm: String,
      inputTerm: String,
      inputType: RowType,
      outputType: RowType,
      currentAggBufferTerm: String): (GeneratedExpression, GeneratedExpression, String) = {
    // build mapping for DeclarativeAggregationFunction binding references
    val offset = if (isMerge) grouping.length + 1 else grouping.length
    val argsMapping = AggCodeGenHelper.buildAggregateArgsMapping(
      isMerge, offset, inputType, auxGrouping, aggInfos, aggBufferTypes)
    val aggBuffMapping = HashAggCodeGenHelper.buildAggregateAggBuffMapping(aggBufferTypes)
    // gen code to create empty agg buffer
    val initedAggBuffer = HashAggCodeGenHelper.genReusableEmptyAggBuffer(
      ctx, builder, inputTerm, inputType, auxGrouping, aggInfos, aggBufferRowType)
    if (auxGrouping.isEmpty) {
      // init aggBuffer in open function when there is no auxGrouping
      ctx.addReusableOpenStatement(initedAggBuffer.code)
    }
    // gen code to update agg buffer from the aggregate map
    val doAggregateExpr = HashAggCodeGenHelper.genAggregate(
      isMerge,
      ctx,
      builder,
      inputType,
      inputTerm,
      auxGrouping,
      aggInfos,
      argsMapping,
      aggBuffMapping,
      currentAggBufferTerm,
      aggBufferRowType)

    // gen code to set hash agg result
    val aggOutputCode = if (isFinal && enableAssignPane) {
      // gen output by sort and merge pre accumulate results
      genOutputByMerging(
        windowSize,
        slideSize,
        bufferLimitSize,
        outputType,
        aggregateMapTerm,
        argsMapping,
        aggBuffMapping,
        aggMapKeyTypesTerm,
        aggBufferTypesTerm,
        aggMapKeyRowType,
        aggBufferRowType)
    } else {
      genOutputDirectly(
        windowSize,
        inputTerm,
        inputType,
        outputType,
        aggregateMapTerm,
        argsMapping,
        aggBuffMapping)
    }

    (initedAggBuffer, doAggregateExpr, aggOutputCode)
  }

  private def genOutputByMerging(
      windowSize: Long,
      slideSize: Long,
      bufferLimitSize: Int,
      outputType: RowType,
      aggregateMapTerm: String,
      argsMapping: Array[Array[(Int, LogicalType)]],
      aggBuffMapping: Array[Array[(Int, LogicalType)]],
      aggKeyTypeTerm: String,
      aggBufferTypeTerm: String,
      aggMapKeyType: RowType,
      aggBufferType: RowType): String = {
    val keyComputerTerm = CodeGenUtils.newName("keyComputer")
    val recordComparatorTerm = CodeGenUtils.newName("recordComparator")
    val prepareSorterCode = HashAggCodeGenHelper.genKVSorterPrepareCode(
      ctx, keyComputerTerm, recordComparatorTerm, aggMapKeyType)

    val memPoolTypeTerm = classOf[BytesHashMapSpillMemorySegmentPool].getName
    val binaryRowSerializerTypeTerm = classOf[BinaryRowDataSerializer].getName

    val sorterBufferType = classOf[BinaryKVInMemorySortBuffer].getName
    val sorterBufferTerm = newName("buffer")

    val createSorterBufferCode =
      s"""
         |   $prepareSorterCode
         |   $sorterBufferType $sorterBufferTerm = $sorterBufferType.createBuffer(
         |      $keyComputerTerm,
         |      new $binaryRowSerializerTypeTerm($aggKeyTypeTerm.length),
         |      new $binaryRowSerializerTypeTerm($aggBufferTypeTerm.length),
         |      $recordComparatorTerm,
         |      $aggregateMapTerm.getRecordAreaMemorySegments(),
         |      $aggregateMapTerm.getNumElements(),
         |      new $memPoolTypeTerm($aggregateMapTerm.getBucketAreaMemorySegments())
         |   );
       """.stripMargin

    val reuseAggMapKeyTerm = newName("reusedKey")
    val reuseAggBufferTerm = newName("reusedValue")
    val reuseKVTerm = newName("reusedKV")
    val binaryRow = classOf[BinaryRowData].getName
    val kvType = classOf[JTuple2[_,_]].getName
    ctx.addReusableMember(
      s"transient $binaryRow $reuseAggMapKeyTerm = new $binaryRow(${aggMapKeyType.getFieldCount});")
    ctx.addReusableMember(
      s"transient $binaryRow $reuseAggBufferTerm = new $binaryRow(${aggBufferType.getFieldCount});")
    ctx.addReusableMember(
      s"transient $kvType<$binaryRow, $binaryRow> $reuseKVTerm = " +
          s"new  $kvType<$binaryRow, $binaryRow>($reuseAggMapKeyTerm, $reuseAggBufferTerm);"
    )

    // ---------------------------------------------------------------------------------------------
    // gen code to create a buffer to group all the elements having the same grouping key
    val windowElementType = getWindowsGroupingElementInfo()
    // project into aggregate map key and value into prepared window element
    val bufferWindowElementTerm = newName("prepareWinElement")
    val bufferWindowElementWriterTerm = newName("prepareWinElementWriter")
    val exprCodegen = new ExprCodeGenerator(ctx, false)
    // TODO refine this. Is it possible to reuse grouping key projection?
    val accessKeyExprs = for (idx <- 0 until aggMapKeyType.getFieldCount - 1) yield
      GenerateUtils.generateFieldAccess(
        ctx, aggMapKeyType, reuseAggMapKeyTerm, idx)
    val accessTimestampExpr = GenerateUtils.generateFieldAccess(
      ctx,
      aggMapKeyType,
      reuseAggMapKeyTerm,
      aggMapKeyType.getFieldCount - 1)
    val accessValExprs = for (idx <- 0 until aggBufferType.getFieldCount) yield
      GenerateUtils.generateFieldAccess(ctx, aggBufferType, reuseAggBufferTerm, idx)
    val accessExprs = (accessKeyExprs :+ GeneratedExpression(
      accessTimestampExpr.resultTerm,
      "false",
      accessTimestampExpr.code,
      timestampInternalType)) ++ accessValExprs
    val buildWindowsGroupingElementExpr = exprCodegen.generateResultExpression(
      accessExprs,
      windowElementType,
      classOf[BinaryRowData],
      outRow = bufferWindowElementTerm,
      outRowWriter = Some(bufferWindowElementWriterTerm))

    // ---------------------------------------------------------------------------------------------
    // gen code to apply aggregate functions to grouping window elements
    val timeWindowType = classOf[TimeWindow].getName
    val currentWindow = newName("currentWindow")
    ctx.addReusableMember(s"transient $timeWindowType $currentWindow = null;")

    // gen code to assign window and aggregate
    val windowsGrouping = CodeGenUtils.newName("windowsGrouping")
    val (processCode, endCode) = if (grouping.isEmpty) {
      val (triggerWindowAgg, endWindowAgg) = genWindowAggCodes(
        enablePreAcc = true,
        ctx,
        windowSize,
        slideSize,
        windowsGrouping,
        bufferLimitSize,
        windowElementType,
        inputTimeFieldIndex,
        currentWindow,
        None,
        outputType)
      val process =
        s"""
           |// prepare windows grouping input
           |${buildWindowsGroupingElementExpr.code}
           |$windowsGrouping
           |  .addInputToBuffer(($BINARY_ROW)${buildWindowsGroupingElementExpr.resultTerm});
           |$triggerWindowAgg
         """.stripMargin
      (process, endWindowAgg)
    } else {
      // project grouping keys from aggregate map's key
      val groupKeyTerm = newName("groupKey")
      val groupKeyWriterTerm = newName("groupKeyWriter")
      val projGroupingKeyCode = ProjectionCodeGenerator.generateProjectionExpression(ctx,
        aggMapKeyType,
        groupKeyRowType,
        grouping.indices.toArray,
        inputTerm = reuseAggMapKeyTerm,
        outRecordTerm = groupKeyTerm,
        outRecordWriterTerm = groupKeyWriterTerm).code


      ("GroupingKeyFromAggMapKey", ctx,
        groupKeyRowType, grouping.indices.toArray,
        aggMapKeyType, reuseAggMapKeyTerm, groupKeyTerm, groupKeyWriterTerm)

      // gen code to check group key changed
      val lastKeyTerm = newName("lastKey")
      ctx.addReusableMember(s"transient $BINARY_ROW $lastKeyTerm = null;")
      val keyNotEqualsCode = genGroupKeyChangedCheckCode(groupKeyTerm, lastKeyTerm)

      val (triggerWindowAgg, endWindowAgg) = genWindowAggCodes(
        enablePreAcc = true,
        ctx,
        windowSize,
        slideSize,
        windowsGrouping,
        bufferLimitSize,
        windowElementType,
        inputTimeFieldIndex,
        currentWindow,
        Some(lastKeyTerm),
        outputType)

      val process =
        s"""
           |// project agg grouping key
           |$projGroupingKeyCode
           |// prepare windows grouping input
           |${buildWindowsGroupingElementExpr.code}
           |if ($lastKeyTerm == null) {
           |  $lastKeyTerm = $groupKeyTerm.copy();
           |} else if ($keyNotEqualsCode) {
           |  $endWindowAgg
           |  $lastKeyTerm = $groupKeyTerm.copy();
           |}
           |$windowsGrouping
           |  .addInputToBuffer(($BINARY_ROW)${buildWindowsGroupingElementExpr.resultTerm});
           |$triggerWindowAgg
         """.stripMargin
      val end =
        s"""
           | $endWindowAgg
           | $lastKeyTerm = null;
       """.stripMargin
      (process, end)
    }

    val sortType = classOf[QuickSort].getName
    val bufferIteratorType = classOf[MutableObjectIterator[_]].getName
    s"""
       | if (hasInput) {
       |  // sort by grouping keys and assigned timestamp
       |  $createSorterBufferCode
       |  new $sortType().sort($sorterBufferTerm);
       |  // merge and get result
       |  $bufferIteratorType<$kvType<$binaryRow, $binaryRow>> iterator =
       |    $sorterBufferTerm.getIterator();
       |  while (iterator.next($reuseKVTerm) != null) {
       |      // reusable input fields access
       |      ${ctx.reuseInputUnboxingCode(bufferWindowElementTerm)}
       |      $processCode
       |   }
       |  $endCode
       | }
       """.stripMargin
  }

  private def genOutputDirectly(
      windowSize: Long,
      inputTerm: String,
      inputType: RowType,
      outputType: RowType,
      aggregateMapTerm: String,
      argsMapping: Array[Array[(Int, LogicalType)]],
      aggBuffMapping: Array[Array[(Int, LogicalType)]]): String = {
    val outputTerm = "hashAggOutput"
    ctx.addReusableOutputRecord(outputType, getOutputRowClass, outputTerm)
    val (reuseAggMapKeyTerm, reuseAggBufferTerm) =
      HashAggCodeGenHelper.prepareTermForAggMapIteration(
        ctx, outputTerm, outputType, getOutputRowClass)

    val windowAggOutputExpr = if (isFinal) {
      // project group key if exists
      val (groupKey, projGroupKeyCode) = if (!grouping.isEmpty) {
        val groupKey = newName("groupKey")
        val keyProjectionCode = ProjectionCodeGenerator.generateProjectionExpression(
          ctx,
          aggMapKeyRowType,
          groupKeyRowType,
          grouping.indices.toArray,
          inputTerm = reuseAggMapKeyTerm,
          outRecordTerm = groupKey,
          outRecordWriterTerm = newName("groupKeyWriter")).code
        (Some(groupKey), keyProjectionCode)
      } else {
        (None, "")
      }

      // gen agg result
      val resultExpr = genHashAggOutputExpr(
        isMerge,
        isFinal,
        ctx,
        builder,
        auxGrouping,
        aggInfos,
        argsMapping,
        aggBuffMapping,
        outputTerm,
        outputType,
        inputTerm,
        inputType,
        groupKey,
        reuseAggBufferTerm,
        aggBufferRowType)

      // update current window
      val timeWindowType = classOf[TimeWindow].getName
      val currentWindow = newName("currentWindow")
      ctx.addReusableMember(s"transient $timeWindowType $currentWindow = null;")
      val assignedTsIndex = grouping.length
      val currentWindowCode =
        s"""
           |$currentWindow = $timeWindowType.of(
           |${convertToLongValue(s"$reuseAggMapKeyTerm.get$timestampInternalTypeName" +
            s"($assignedTsIndex)")},
           |${convertToLongValue(s"$reuseAggMapKeyTerm.get$timestampInternalTypeName" +
            s"($assignedTsIndex)")}
           | + ${windowSize}L);
        """.stripMargin

      val winResExpr =
        genWindowAggOutputWithWindowPorps(ctx, outputType, currentWindow, resultExpr)

      val output =
        s"""
           |// update current window
           |$currentWindowCode
           |// project current group keys if exist
           |$projGroupKeyCode
           |// build agg output
           |${winResExpr.code}
         """.stripMargin
      new GeneratedExpression(
        winResExpr.resultTerm, "false", output, outputType)
    } else {
      genHashAggOutputExpr(
        isMerge,
        isFinal,
        ctx,
        builder,
        auxGrouping,
        aggInfos,
        argsMapping,
        aggBuffMapping,
        outputTerm,
        outputType,
        inputTerm,
        inputType,
        Some(reuseAggMapKeyTerm),
        reuseAggBufferTerm,
        aggBufferRowType)
    }

    // -------------------------------------------------------------------------------------------
    // gen code to iterating the aggregate map and output to downstream
    val iteratorType = classOf[KeyValueIterator[_, _]].getCanonicalName
    val rowDataType = classOf[RowData].getCanonicalName
    val iteratorTerm = CodeGenUtils.newName("iterator")
    s"""
       |$iteratorType<$rowDataType, $rowDataType> $iteratorTerm =
       |  $aggregateMapTerm.getEntryIterator();
       |while ($iteratorTerm.advanceNext()) {
       |   $reuseAggMapKeyTerm = ($rowDataType) $iteratorTerm.getKey();
       |   $reuseAggBufferTerm = ($rowDataType) $iteratorTerm.getValue();
       |   ${ctx.reuseInputUnboxingCode(reuseAggBufferTerm)}
       |   ${windowAggOutputExpr.code}
       |   ${generateCollect(windowAggOutputExpr.resultTerm)}
       |}
       """.stripMargin
  }

  private def genHashWindowAggCodes(
      buffLimitSize: Int,
      windowSize: Long,
      slideSize: Long,
      inputTerm: String,
      inputType: RowType,
      outputType: RowType,
      aggMapKey: String,
      logTerm: String): (String, String) = {
    // prepare aggregate map
    val aggMapKeyTypesTerm = CodeGenUtils.newName("aggMapKeyTypes")
    val aggBufferTypesTerm = CodeGenUtils.newName("aggBufferTypes")
    prepareHashAggKVTypes(
      ctx, aggMapKeyTypesTerm, aggBufferTypesTerm, aggMapKeyRowType, aggBufferRowType)
    val aggregateMapTerm = CodeGenUtils.newName("aggregateMap")
    prepareHashAggMap(ctx, aggMapKeyTypesTerm, aggBufferTypesTerm, aggregateMapTerm)

    val binaryRowTypeTerm = classOf[BinaryRowData].getName
    // gen code to do aggregate by window using aggregate map
    val currentAggBufferTerm =
      ctx.addReusableLocalVariable(binaryRowTypeTerm, "currentAggBuffer")
    val (initedAggBufferExpr, doAggregateExpr, outputResultFromMap) = genGroupWindowHashAggCodes(
      isMerge,
      isFinal,
      windowSize,
      slideSize,
      aggMapKeyTypesTerm,
      aggBufferTypesTerm,
      buffLimitSize,
      aggregateMapTerm,
      inputTerm,
      inputType,
      outputType,
      currentAggBufferTerm)

    // -------------------------------------------------------------------------------------------
    val lazyInitAggBufferCode = if (auxGrouping.nonEmpty) {
      s"""
         |// lazy init agg buffer (with auxGrouping)
         |${initedAggBufferExpr.code}
       """.stripMargin
    } else {
      ""
    }

    val lookupInfoTypeTerm = classOf[BytesMap.LookupInfo[_, _]].getCanonicalName
    val lookupInfo = ctx.addReusableLocalVariable(lookupInfoTypeTerm, "lookupInfo")
    val dealWithAggHashMapOOM = if (isFinal) {
      s"""throw new java.io.IOException("Hash window aggregate map OOM.");"""
    } else {
      val retryAppend = genRetryAppendToMap(
        aggregateMapTerm, aggMapKey, initedAggBufferExpr, lookupInfo, currentAggBufferTerm)
      val logMapOutput = CodeGenUtils.genLogInfo(
        logTerm, s"BytesHashMap out of memory with {} entries, output directly.",
        s"$aggregateMapTerm.getNumElements()")
      s"""
         |$logMapOutput
         | // hash map out of memory, output directly
         |$outputResultFromMap
         | // retry append
         |$retryAppend
          """.stripMargin
    }

    val process =
      s"""
         |// look up output buffer using current key (grouping keys ..., assigned timestamp)
         |$lookupInfo = ($lookupInfoTypeTerm) $aggregateMapTerm.lookup($aggMapKey);
         |$currentAggBufferTerm = ($binaryRowTypeTerm) $lookupInfo.getValue();
         |if (!$lookupInfo.isFound()) {
         |  $lazyInitAggBufferCode
         |  // append empty agg buffer into aggregate map for current group key
         |  try {
         |    $currentAggBufferTerm =
         |      $aggregateMapTerm.append($lookupInfo, ${initedAggBufferExpr.resultTerm});
         |  } catch (java.io.EOFException exp) {
         |    $dealWithAggHashMapOOM
         |  }
         |}
         |// aggregate buffer fields access
         |${ctx.reuseInputUnboxingCode(currentAggBufferTerm)}
         |// do aggregate and update agg buffer
         |${doAggregateExpr.code}
       """.stripMargin.trim

    (process, outputResultFromMap)
  }
}
