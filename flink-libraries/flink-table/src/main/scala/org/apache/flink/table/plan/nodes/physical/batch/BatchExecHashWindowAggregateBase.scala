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

import org.apache.flink.api.java.tuple.{Tuple2 => JTuple2}
import org.apache.flink.api.java.typeutils.ListTypeInfo
import org.apache.flink.runtime.operators.sort.QuickSort
import org.apache.flink.table.api.functions.UserDefinedFunction
import org.apache.flink.table.api.types.{DataType, InternalType, RowType, TypeConverters}
import org.apache.flink.table.api.window.TimeWindow
import org.apache.flink.table.api.{BatchTableEnvironment, TableConfig, Types}
import org.apache.flink.table.calcite.FlinkRelBuilder.NamedWindowProperty
import org.apache.flink.table.codegen.CodeGenUtils.{generateFieldAccess, newName}
import org.apache.flink.table.codegen.CodeGeneratorContext.BINARY_ROW
import org.apache.flink.table.codegen._
import org.apache.flink.table.codegen.agg.BatchExecHashAggregateCodeGen
import org.apache.flink.table.codegen.operator.OperatorCodeGenerator
import org.apache.flink.table.dataformat.BinaryRow
import org.apache.flink.table.plan.cost.FlinkBatchCost._
import org.apache.flink.table.plan.cost.FlinkCostFactory
import org.apache.flink.table.plan.logical.{LogicalWindow, SlidingGroupWindow, TumblingGroupWindow}
import org.apache.flink.table.runtime.AbstractStreamOperatorWithMetrics
import org.apache.flink.table.runtime.sort.BinaryKVInMemorySortBuffer
import org.apache.flink.table.runtime.util.{BytesHashMap, BytesHashMapSpillMemorySegmentPool}
import org.apache.flink.table.typeutils.BinaryRowSerializer
import org.apache.flink.util.MutableObjectIterator

import org.apache.calcite.plan.{RelOptCluster, RelOptCost, RelOptPlanner, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.tools.RelBuilder
import org.apache.commons.math3.util.ArithmeticUtils

abstract class BatchExecHashWindowAggregateBase(
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
    isMerge: Boolean,
    isFinal: Boolean)
  with BatchExecHashAggregateCodeGen {

  lazy val aggBufferRowType: RowType = new RowType(
    aggBufferTypes.flatten.toArray[DataType], aggBufferNames.flatten)

  lazy val aggMapKeyRowType: RowType = new RowType(
    groupKeyRowType.getFieldTypes :+ timestampInternalType,
    groupKeyRowType.getFieldNames :+ "assignedTs")

  private[flink] def codegen(
      ctx: CodeGeneratorContext,
      tableEnv: BatchTableEnvironment,
      inputType: RowType,
      outputType: RowType,
      buffLimitSize: Int,
      reservedAggMapMemory: Long,
      preferredAggMapMemory: Long,
      windowStart: Long,
      windowSize: Long,
      slideSize: Long): GeneratedOperator = {

    val className = if (isFinal) "HashWinAgg" else "LocalHashWinAgg"
    val suffix = if (grouping.isEmpty) "WithoutKeys" else "WithKeys"

    val config = tableEnv.config
    val inputTerm = CodeGeneratorContext.DEFAULT_INPUT1_TERM
    // add logger
    val logTerm = CodeGenUtils.newName("LOG")
    ctx.addReusableLogger(logTerm, className)

    // gen code to do aggregate using aggregate map
    val aggMapKey = newName("aggMapKey")
    val aggMapKeyWriter = newName("aggMapKeyWriter")
    val (processElementPerWindow, outputResultFromMap) = genHashWindowAggCodes(ctx, config,
      reservedAggMapMemory, preferredAggMapMemory, buffLimitSize, windowSize,
      slideSize, inputTerm, inputType,
      outputType, aggMapKey, logTerm)

    val (processCode, outputWhenEndInputCode) = if (isFinal && isMerge) {
      // prepare for aggregate map key's projection
      val projAggMapKeyCode = genGroupKeyProjectionCode("GlobalHashWinAgg", ctx, aggMapKeyRowType,
        grouping :+ grouping.length ,inputType, inputTerm, aggMapKey, aggMapKeyWriter)

      val processCode =
        s"""
           |if (!$inputTerm.isNullAt($inputTimestampIndex)) {
           |  hasInput = true;
           |  // input field access for group key projection, window/pane assign
           |  // and aggregate map update
           |  ${ctx.reuseInputUnboxingCode(Set(inputTerm))}
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
        assignPane, ctx, config, windowStart, windowSize, slideSize, window, inputTerm, inputType)

      val processCode =
        if (!isSlidingWindowWithOverlapping(assignPane, window, slideSize, windowSize)) {
          // Each input will be assigned with only one window, in the cases of
          // Tumbling window, Sliding window with slideSize >= windowSize or with pane optimization.
          assert(assignTimestampExprs.size == 1)
          val assignTimestampExpr = assignTimestampExprs.head

          // prepare for aggregate map key's projection
          val accessAssignedTimestampExpr = GeneratedExpression(
            assignTimestampExpr.resultTerm, "false", "", timestampInternalType, literal = true)
          val prepareInitAggMapKeyExpr = prepareAggMapKeyExpr(ctx, inputTerm, inputType,
            Some(accessAssignedTimestampExpr), aggMapKeyRowType, aggMapKey, aggMapKeyWriter)
          val processAggregate =
            s"""
               |  // build aggregate map key
               |  ${prepareInitAggMapKeyExpr.code}
               |  // aggregate by each input with assigned timestamp
               |  $processElementPerWindow
           """.stripMargin

          // gen code to filter invalid windows in the case of jumping window
          val processEachInput = if (isJumpingWindow(window, slideSize, windowSize)) {
            val checkValidWindow = s"${getInputTimeValue(inputTerm, inputTimestampIndex)} < " +
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
             |if (!$inputTerm.isNullAt($inputTimestampIndex)) {
             |  hasInput = true;
             |  // input field access for group key projection, window/pane assign
             |   // and aggregate map update
             |  ${ctx.reuseInputUnboxingCode(Set(inputTerm))}
             |  // assign timestamp(window or pane)
             |  ${assignTimestampExpr.code}
             |  // process each input
             |  $processEachInput
             |}""".stripMargin
        } else {
          // Otherwise, each input will be assigned with overlapping windows.
          assert(assignTimestampExprs.size > 1)
          val assignedWindows = newName("assignedWindows")
          ctx.addReusableMember(s"transient java.util.List<java.lang.Long> $assignedWindows;",
            s"$assignedWindows = new java.util.ArrayList<java.lang.Long>();")
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
              TypeConverters.createInternalTypeFromTypeInfo(new ListTypeInfo(Types.LONG)))

          // gen code to filter invalid overlapping windows
          val assignedTimestamp = newName("assignedTimestamp")
          val timestampTerm = s"${getInputTimeValue(inputTerm, inputTimestampIndex)}"
          val checkValidWindow = s"$timestampTerm >= $assignedTimestamp " +
              s" && $timestampTerm < $assignedTimestamp + ${windowSize}L"

          // prepare for aggregate map key's projection
          val prepareInitAggMapKeyExpr = prepareAggMapKeyExpr(
            ctx, inputTerm, inputType, None, aggMapKeyRowType, aggMapKey, aggMapKeyWriter)
          val realAssignedValue = if (timestampIsDate) {
            convertToIntValue(s"$assignedTimestamp")
          } else {
            assignedTimestamp
          }
          val updateAssignedTsCode = s"$aggMapKey.set$timestampInternalTypeName(${
            grouping.length
          }, $realAssignedValue);"

          s"""
             |if (!$inputTerm.isNullAt($inputTimestampIndex)) {
             |  hasInput = true;
             |  // input field access for group key projection, window/pane assign
             |  // and aggregate map update
             |  ${ctx.reuseInputUnboxingCode(Set(inputTerm))}
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

    val baseClass = classOf[AbstractStreamOperatorWithMetrics[_]].getName
    val endInputCode = if (isFinal) {
      s"""
         |$outputWhenEndInputCode
         """.stripMargin
    } else {
      outputWhenEndInputCode
    }
    generateOperator(
      ctx, className + suffix, baseClass, processCode, endInputCode, inputRelDataType, config)
  }

  private def genTimestampAssignExprs(
      assignPane: Boolean,
      ctx: CodeGeneratorContext,
      config: TableConfig,
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
            ctx, config, inputTerm, inputType, timeField, windowStart, paneSize))
        } else if (slideSize >= windowSize) {
          Seq(genAlignedWindowStartExpr(
            ctx, config, inputTerm, inputType, timeField, windowStart, slideSize))
        } else {
          val maxNumOverlapping = math.ceil(windowSize * 1.0 / slideSize).toInt
          val exprs = for (index <- 0 until maxNumOverlapping) yield {
            genAlignedWindowStartExpr(
              ctx, config, inputTerm, inputType, timeField, windowStart, slideSize, index)
          }
          exprs
        }
      case TumblingGroupWindow(_, timeField, _) =>
        Seq(genAlignedWindowStartExpr(
          ctx, config, inputTerm, inputType, timeField, windowStart, windowSize))
      case _ =>
        throw new RuntimeException(s"Bug. Assign pane for $window is not supported.")
    }
  }

  // -------------------------------------------------------------------------------------------
  private def prepareAggMapKeyExpr(
      ctx: CodeGeneratorContext,
      inputTerm: String,
      inputType: InternalType,
      assignedTimestampExpr: Option[GeneratedExpression],
      currentKeyType: RowType,
      currentKeyTerm: String,
      currentKeyWriterTerm: String): GeneratedExpression = {
    val codeGen = new ExprCodeGenerator(ctx, false, nullCheck = true)
        .bindInput(inputType, inputTerm = inputTerm)
    val expr = if (assignedTimestampExpr.isDefined) {
      val assignedLongTimestamp = assignedTimestampExpr.get
      if (timestampIsDate) {
        val dateTerm = ctx.newReusableField("dateTerm", "int")
        val convertDateCode =
          s"""
             |  ${assignedLongTimestamp.code}
             |  $dateTerm = ${convertToIntValue(assignedLongTimestamp.resultTerm)};
           """.stripMargin
        GeneratedExpression(
          dateTerm,
          assignedLongTimestamp.nullTerm,
          convertDateCode,
          timestampInternalType,
          assignedLongTimestamp.literal)
      } else {
        assignedLongTimestamp
      }
    } else {
      val value = if (timestampIsDate) "-1" else "-1L"
      GeneratedExpression(value, "false", "", timestampInternalType, literal = true)
    }
    // There will be only assigned timestamp field when no grouping window aggregate case.
    codeGen.generateResultExpression(
      grouping.map(
        idx => generateFieldAccess(
          ctx, inputType, inputTerm, idx, nullCheck = true)) :+ expr,
      currentKeyType.asInstanceOf[RowType],
      classOf[BinaryRow],
      outRow = currentKeyTerm,
      outRowWriter = Some(currentKeyWriterTerm))
  }

  private def genGroupWindowHashAggCodes(
      ctx: CodeGeneratorContext,
      config: TableConfig,
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
    val argsMapping = buildAggregateArgsMapping(
      isMerge, offset, inputRelDataType,  auxGrouping, aggregateCalls, aggBufferTypes)
    val aggBuffMapping = buildAggregateAggBuffMapping(aggBufferTypes)
    // gen code to create empty agg buffer
    val initedAggBuffer = genReusableEmptyAggBuffer(
      ctx, config, builder, inputTerm, inputType, auxGrouping, aggregates, aggBufferRowType)
    if (auxGrouping.isEmpty) {
      // init aggBuffer in open function when there is no auxGrouping
      ctx.addReusableOpenStatement(initedAggBuffer.code)
    }
    // gen code to update agg buffer from the aggregate map
    val doAggregateExpr = genAggregate(isMerge, ctx, config, builder, inputRelDataType,
      inputType, inputTerm, auxGrouping, aggregates, aggCallToAggFunction,
      argsMapping, aggBuffMapping, currentAggBufferTerm, aggBufferRowType)

    // gen code to set hash agg result
    val aggOutputCode = if (isFinal && assignPane) {
      // gen output by sort and merge pre accumulate results
      genOutputByMerging(ctx, config,
        windowSize, slideSize, bufferLimitSize,
        outputType, aggregateMapTerm, argsMapping, aggBuffMapping,
        aggMapKeyTypesTerm, aggBufferTypesTerm, aggMapKeyRowType, aggBufferRowType)
    } else {
      genOutputDirectly(ctx, config, windowSize, inputTerm, inputType, outputType,
        aggregateMapTerm, argsMapping, aggBuffMapping)
    }

    (initedAggBuffer, doAggregateExpr, aggOutputCode)
  }

  private def genOutputByMerging(
      ctx: CodeGeneratorContext,
      config: TableConfig,
      windowSize: Long,
      slideSize: Long,
      bufferLimitSize: Int,
      outputType: RowType,
      aggregateMapTerm: String,
      argsMapping: Array[Array[(Int, InternalType)]],
      aggBuffMapping: Array[Array[(Int, InternalType)]],
      aggKeyTypeTerm: String,
      aggBufferTypeTerm: String,
      aggMapKeyType: RowType,
      aggBufferType: RowType): String = {

    val keyComputerTerm = CodeGenUtils.newName("keyComputer")
    val recordComparatorTerm = CodeGenUtils.newName("recordComparator")
    val prepareSorterCode = genKVSorterPrepareCode(
      ctx, keyComputerTerm, recordComparatorTerm, aggMapKeyType)

    val memPoolTypeTerm = classOf[BytesHashMapSpillMemorySegmentPool].getName
    val binaryRowSerializerTypeTerm = classOf[BinaryRowSerializer].getName

    val sorterBufferType = classOf[BinaryKVInMemorySortBuffer].getName
    val sorterBufferTerm = newName("buffer")

    val createSorterBufferCode =
      s"""
         |   $prepareSorterCode
         |   $sorterBufferType $sorterBufferTerm = $sorterBufferType.createBuffer(
         |      $keyComputerTerm,
         |      new $binaryRowSerializerTypeTerm($aggKeyTypeTerm),
         |      new $binaryRowSerializerTypeTerm($aggBufferTypeTerm),
         |      $recordComparatorTerm,
         |      $aggregateMapTerm.getRecordAreaMemorySegments(),
         |      $aggregateMapTerm.getNumElements(),
         |      new $memPoolTypeTerm($aggregateMapTerm.getBucketAreaMemorySegments())
         |   );
       """.stripMargin

    val reuseAggMapKeyTerm = newName("reusedKey")
    val reuseAggBufferTerm = newName("reusedValue")
    val reuseKVTerm = newName("reusedKV")
    val binaryRow = classOf[BinaryRow].getName
    val kvType = classOf[JTuple2[_,_]].getName
    ctx.addReusableMember(
      s"transient $binaryRow $reuseAggMapKeyTerm = new $binaryRow(${aggMapKeyType.getArity});")
    ctx.addReusableMember(
      s"transient $binaryRow $reuseAggBufferTerm = new $binaryRow(${aggBufferType.getArity});")
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
    val exprCodegen = new ExprCodeGenerator(ctx, false, config.getNullCheck)
    // TODO refine this. Is it possible to reuse grouping key projection?
    val accessKeyExprs = for (idx <- 0 until aggMapKeyType.getArity - 1) yield
      CodeGenUtils.generateFieldAccess(
        ctx, aggMapKeyType, reuseAggMapKeyTerm, idx, nullCheck = true)
    val accessTimestampExpr = CodeGenUtils.generateFieldAccess(
      ctx, aggMapKeyType,
      reuseAggMapKeyTerm, aggMapKeyType.getArity - 1, nullCheck = false)
    val accessValExprs = for (idx <- 0 until aggBufferType.getArity) yield
      CodeGenUtils.generateFieldAccess(
        ctx, aggBufferType, reuseAggBufferTerm, idx, nullCheck = true)
    val accessExprs = (accessKeyExprs :+ GeneratedExpression(
      accessTimestampExpr.resultTerm,
      "false",
      accessTimestampExpr.code,
      timestampInternalType,
      literal = true)) ++ accessValExprs
    val buildWindowsGroupingElementExpr = exprCodegen.generateResultExpression(
      accessExprs,
      windowElementType,
      classOf[BinaryRow],
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
        enablePreAcc = true, ctx, config, windowSize, slideSize, windowsGrouping, bufferLimitSize,
        windowElementType, inputTimestampIndex, currentWindow, None, outputType)
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
      val projGroupingKeyCode = genGroupKeyProjectionCode("GroupingKeyFromAggMapKey", ctx,
        groupKeyRowType, grouping.indices.toArray,
        aggMapKeyType, reuseAggMapKeyTerm, groupKeyTerm, groupKeyWriterTerm)

      // gen code to check group key changed
      val lastKeyTerm = newName("lastKey")
      ctx.addReusableMember(s"transient $BINARY_ROW $lastKeyTerm = null;")
      val keyNotEqualsCode = genGroupKeyChangedCheckCode(groupKeyTerm, lastKeyTerm)

      val (triggerWindowAgg, endWindowAgg) = genWindowAggCodes(
        enablePreAcc = true, ctx, config, windowSize, slideSize, windowsGrouping, bufferLimitSize,
        windowElementType, inputTimestampIndex, currentWindow, Some(lastKeyTerm), outputType)

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
       |      ${ctx.reuseInputUnboxingCode(Set(bufferWindowElementTerm))}
       |      $processCode
       |   }
       |  $endCode
       | }
       """.stripMargin
  }

  private def genOutputDirectly(
      ctx: CodeGeneratorContext,
      config: TableConfig,
      windowSize: Long,
      inputTerm: String,
      inputType: RowType,
      outputType: RowType,
      aggregateMapTerm: String,
      argsMapping: Array[Array[(Int, InternalType)]],
      aggBuffMapping: Array[Array[(Int, InternalType)]]): String = {
    val outputTerm = "hashAggOutput"
    ctx.addOutputRecord(outputType, getOutputRowClass, outputTerm)
    val (reuseAggMapEntryTerm, reuseAggMapKeyTerm, reuseAggBufferTerm) =
      prepareTermForAggMapIteration(
        ctx, outputTerm, outputType, aggMapKeyRowType, aggBufferRowType)

    val windowAggOutputExpr = if (isFinal) {
      // project group key if exists
      val (groupKey, projGroupKeyCode) = if (!grouping.isEmpty) {
        val groupKey = newName("groupKey")
        val keyProjectionCode = genGroupKeyProjectionCode(
          "GroupKeyProj", ctx, groupKeyRowType, grouping.indices.toArray, aggMapKeyRowType,
          reuseAggMapKeyTerm, groupKey, newName("groupKeyWriter"))
        (Some(groupKey), keyProjectionCode)
      } else {
        (None, "")
      }

      // gen agg result
      val resultExpr = genHashAggOutputExpr(
        isMerge, isFinal, ctx, config, builder, inputRelDataType, auxGrouping, aggregates,
        argsMapping, aggBuffMapping, outputTerm, outputType, inputTerm, inputType, groupKey,
        reuseAggBufferTerm, aggBufferRowType)

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
      genHashAggOutputExpr(isMerge, isFinal, ctx, config, builder, inputRelDataType, auxGrouping,
        aggregates, argsMapping, aggBuffMapping, outputTerm, outputType, inputTerm, inputType,
        Some(reuseAggMapKeyTerm), reuseAggBufferTerm, aggBufferRowType)
    }

    // -------------------------------------------------------------------------------------------
    // gen code to iterating the aggregate map and output to downstream
    val mapEntryTypeTerm = classOf[BytesHashMap.Entry].getCanonicalName
    s"""
       |org.apache.flink.util.MutableObjectIterator<$mapEntryTypeTerm> iterator =
       |  $aggregateMapTerm.getEntryIterator();
       |while (iterator.next($reuseAggMapEntryTerm) != null) {
       |   ${ctx.reuseInputUnboxingCode(Set(reuseAggBufferTerm))}
       |   ${windowAggOutputExpr.code}
       |   ${OperatorCodeGenerator.generatorCollect(windowAggOutputExpr.resultTerm)}
       |}
       """.stripMargin
  }

  private def genHashWindowAggCodes(
      ctx: CodeGeneratorContext,
      config: TableConfig,
      reservedAggMapMemory: Long,
      preferredAggMapMemory: Long,
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
    prepareHashAggMap(
      ctx, config, reservedAggMapMemory, preferredAggMapMemory
      , aggMapKeyTypesTerm, aggBufferTypesTerm, aggregateMapTerm)

    // gen code to do aggregate by window using aggregate map
    val currentAggBufferTerm =
      ctx.newReusableField("currentAggBuffer", classOf[BinaryRow].getName)
    val (initedAggBufferExpr, doAggregateExpr, outputResultFromMap) = genGroupWindowHashAggCodes(
      ctx, config, isMerge, isFinal, windowSize, slideSize, aggMapKeyTypesTerm, aggBufferTypesTerm,
      buffLimitSize, aggregateMapTerm, inputTerm, inputType, outputType, currentAggBufferTerm)

    // -------------------------------------------------------------------------------------------
    val lazyInitAggBufferCode = if (auxGrouping.nonEmpty) {
      s"""
         |// lazy init agg buffer (with auxGrouping)
         |${initedAggBufferExpr.code}
       """.stripMargin
    } else {
      ""
    }

    val lookupInfo =
      ctx.newReusableField("lookupInfo", classOf[BytesHashMap.LookupInfo].getCanonicalName)
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
         |$lookupInfo = $aggregateMapTerm.lookup($aggMapKey);
         |$currentAggBufferTerm = $lookupInfo.getValue();
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
         |${ctx.reuseInputUnboxingCode(Set(currentAggBufferTerm))}
         |// do aggregate and update agg buffer
         |${doAggregateExpr.code}
       """.stripMargin.trim

    (process, outputResultFromMap)
  }

  override def computeSelfCost(planner: RelOptPlanner, mq: RelMetadataQuery): RelOptCost = {
    val numOfGroupKey = grouping.length
    val inputRowCnt = mq.getRowCount(getInput())
    if (inputRowCnt == null) {
      return null
    }
    // does not take pane optimization into consideration here
    // calculate hash code of groupKeys + timestamp
    val hashCpuCost = HASH_CPU_COST * inputRowCnt * (numOfGroupKey + 1)
    val aggFunctionCpuCost = FUNC_CPU_COST * inputRowCnt * aggCallToAggFunction.size
    val rowCnt = mq.getRowCount(this)
    // assume memory is enough to hold hashTable to simplify the estimation because spill will not
    // happen under the assumption
    //  We aim for a 200% utilization of the bucket table.
    val bucketSize = rowCnt * BytesHashMap.BUCKET_SIZE / BatchPhysicalRel.HASH_COLLISION_WEIGHT
    val recordSize = rowCnt * (BatchPhysicalRel.binaryRowAverageSize(this) + BytesHashMap
        .RECORD_EXTRA_LENGTH)
    val memCost = bucketSize + recordSize
    val costFactory = planner.getCostFactory.asInstanceOf[FlinkCostFactory]
    costFactory.makeCost(rowCnt, hashCpuCost + aggFunctionCpuCost, 0, 0, memCost)
  }
}
