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

import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.dataformat.{BaseRow, BinaryRow, GenericRow, JoinedRow}
import org.apache.flink.table.expressions.ExpressionUtils.extractValue
import org.apache.flink.table.expressions.{Expression, ValueLiteralExpression}
import org.apache.flink.table.functions.{AggregateFunction, UserDefinedFunction}
import org.apache.flink.table.planner.JLong
import org.apache.flink.table.planner.calcite.FlinkRelBuilder.PlannerNamedWindowProperty
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.codegen.CodeGenUtils.{BINARY_ROW, boxedTypeTermForType, newName}
import org.apache.flink.table.planner.codegen.GenerateUtils.generateFieldAccess
import org.apache.flink.table.planner.codegen.GeneratedExpression.{NEVER_NULL, NO_CODE}
import org.apache.flink.table.planner.codegen.OperatorCodeGenerator.generateCollect
import org.apache.flink.table.planner.codegen._
import org.apache.flink.table.planner.codegen.agg.batch.AggCodeGenHelper.{buildAggregateArgsMapping, genAggregateByFlatAggregateBuffer, genFlatAggBufferExprs, genInitFlatAggregateBuffer}
import org.apache.flink.table.planner.codegen.agg.batch.WindowCodeGenerator.{asLong, isTimeIntervalLiteral}
import org.apache.flink.table.planner.expressions.ExpressionBuilder._
import org.apache.flink.table.planner.expressions.RexNodeConverter
import org.apache.flink.table.planner.functions.aggfunctions.DeclarativeAggregateFunction
import org.apache.flink.table.planner.functions.utils.UserDefinedFunctionUtils.getAccumulatorTypeOfAggregateFunction
import org.apache.flink.table.planner.plan.logical.{LogicalWindow, SlidingGroupWindow, TumblingGroupWindow}
import org.apache.flink.table.planner.plan.utils.{AggregateInfoList, AggregateUtil}
import org.apache.flink.table.runtime.operators.window.TimeWindow
import org.apache.flink.table.runtime.operators.window.grouping.{HeapWindowsGrouping, WindowsGrouping}
import org.apache.flink.table.runtime.types.LogicalTypeDataTypeConverter.fromDataTypeToLogicalType
import org.apache.flink.table.runtime.util.RowIterator
import org.apache.flink.table.types.logical.LogicalTypeRoot.INTERVAL_DAY_TIME
import org.apache.flink.table.types.logical._
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks.hasRoot

import org.apache.calcite.avatica.util.DateTimeUtils
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.tools.RelBuilder
import org.apache.commons.math3.util.ArithmeticUtils

import scala.collection.JavaConversions._

abstract class WindowCodeGenerator(
    relBuilder: RelBuilder,
    window: LogicalWindow,
    inputTimeFieldIndex: Int,
    inputTimeIsDate: Boolean,
    namedProperties: Seq[PlannerNamedWindowProperty],
    aggInfoList: AggregateInfoList,
    inputRowType: RelDataType,
    grouping: Array[Int],
    auxGrouping: Array[Int],
    enableAssignPane: Boolean = true,
    val isMerge: Boolean,
    val isFinal: Boolean) {

  lazy val builder: RelBuilder = relBuilder.values(inputRowType)
  lazy val timestampInternalType: LogicalType =
    if (inputTimeIsDate) new IntType() else new BigIntType()
  lazy val timestampInternalTypeName: String = if (inputTimeIsDate) "Int" else "Long"
  lazy val aggCallToAggFunction: Array[(AggregateCall, UserDefinedFunction)] =
    aggInfoList.aggInfos.map(info => (info.agg, info.function))
  lazy val aggregateCalls: Seq[AggregateCall] = aggCallToAggFunction.map(_._1)
  lazy val aggregates: Seq[UserDefinedFunction] = aggCallToAggFunction.map(_._2)

  lazy val aggArgs: Array[Array[Int]] = aggInfoList.aggInfos.map(_.argIndexes)

  // currently put auxGrouping to aggBuffer in code-gen
  lazy val aggBufferNames: Array[Array[String]] = auxGrouping.zipWithIndex.map {
    case (_, index) => Array(s"aux_group$index")
  } ++ aggregates.zipWithIndex.toArray.map {
    case (a: DeclarativeAggregateFunction, index) =>
      val idx = auxGrouping.length + index
      a.aggBufferAttributes.map(attr => s"agg${idx}_${attr.getName}")
    case (_: AggregateFunction[_, _], index) =>
      val idx = auxGrouping.length + index
      Array(s"agg$idx")
  }

  lazy val aggBufferTypes: Array[Array[LogicalType]] = auxGrouping.map { index =>
    Array(FlinkTypeFactory.toLogicalType(inputRowType.getFieldList.get(index).getType))
  } ++ aggregates.map {
    case a: DeclarativeAggregateFunction => a.getAggBufferTypes.map(_.getLogicalType)
    case a: AggregateFunction[_, _] =>
      Array(getAccumulatorTypeOfAggregateFunction(a)).map(fromDataTypeToLogicalType)
  }.toArray[Array[LogicalType]]

  lazy val groupKeyRowType: RowType = RowType.of(
    grouping.map { index =>
      FlinkTypeFactory.toLogicalType(inputRowType.getFieldList.get(index).getType)
    }, grouping.map(inputRowType.getFieldNames.get(_)))

  // get udagg instance names
  lazy val udaggs: Map[AggregateFunction[_, _], String] = aggregates
      .filter(a => a.isInstanceOf[AggregateFunction[_, _]])
      .map(a => a -> CodeGenUtils.udfFieldName(a)).toMap
      .asInstanceOf[Map[AggregateFunction[_, _], String]]

  lazy val windowedGroupKeyType: RowType = RowType.of(
    (groupKeyRowType.getChildren :+ timestampInternalType).toArray,
    (groupKeyRowType.getFieldNames :+ "assignedTs$").toArray)

  def getOutputRowClass: Class[_ <: BaseRow] =
    if (namedProperties.isEmpty && grouping.isEmpty) classOf[GenericRow] else classOf[JoinedRow]

  private[flink] def getWindowsGroupingElementInfo(
      enablePreAccumulate: Boolean = true): RowType = {
    if (enablePreAccumulate) {
      val (groupKeyNames, groupKeyTypes) =
        (groupKeyRowType.getFieldNames, groupKeyRowType.getChildren.toArray(Array[LogicalType]()))
      val (aggBuffNames, aggBuffTypes) =
        (aggBufferNames.flatten, aggBufferTypes.flatten)
      RowType.of(
        (groupKeyTypes :+ timestampInternalType) ++ aggBuffTypes,
        ((groupKeyNames :+ "assignedTs$") ++ aggBuffNames).toArray)
    } else {
      FlinkTypeFactory.toLogicalRowType(inputRowType)
    }
  }

  private[flink] def genCreateWindowsGroupingCode(
      ctx: CodeGeneratorContext,
      inputTimeFieldIndex: Int,
      windowSize: Long,
      slideSize: Long,
      groupingTerm: String,
      bufferLimitSize: Int): Unit = {
    val windowsGrouping = classOf[HeapWindowsGrouping].getName
    ctx.addReusableMember(
      s"""
         |transient $windowsGrouping $groupingTerm = new $windowsGrouping(
         |  $bufferLimitSize, ${windowSize}L, ${slideSize}L,
         |  $inputTimeFieldIndex, $inputTimeIsDate);
       """.stripMargin)
    ctx.addReusableCloseStatement(s"$groupingTerm.close();")
  }

  /**
    * Using [[WindowsGrouping#buildTriggerWindowElementsIterator]]
    * to iterate the windows assigned with the current keyed or all grouped input.
    * Apply aggregate functions within the each window scope in turns.
    */
  private[flink] def genTriggerWindowAggByWindowsGroupingCode(
      ctx: CodeGeneratorContext,
      groupingTerm: String,
      currentWindow: String,
      currentWindowElement: String,
      initAggBufferCode: String,
      doAggregateCode: String,
      outputWinAggResExpr: GeneratedExpression): String = {
    val rowIter = classOf[RowIterator[BinaryRow]].getName
    val statements =
      s"""
         |while ($groupingTerm.hasTriggerWindow()) {
         |  $rowIter elementIterator = $groupingTerm.buildTriggerWindowElementsIterator();
         |  $currentWindow = $groupingTerm.getTriggerWindow();
         |  // init agg buffer
         |  $initAggBufferCode
         |  // do aggregate
         |  boolean hasElement = false;
         |  while(elementIterator.advanceNext()) {
         |    hasElement = true;
         |    $BINARY_ROW $currentWindowElement = ($BINARY_ROW) elementIterator.getRow();
         |    ${ctx.reuseInputUnboxingCode(currentWindowElement)}
         |    $doAggregateCode
         |  }
         |  if (hasElement) {
         |    // write output
         |    ${outputWinAggResExpr.code}
         |    ${generateCollect(outputWinAggResExpr.resultTerm)}
         |  }
         |}""".stripMargin
    val functionName = CodeGenUtils.newName("triggerWindowProcess")
    val functionCode =
      s"""
         |private void $functionName() throws java.lang.Exception {
         |  ${ctx.reuseLocalVariableCode()}
         |  $statements
         |}
       """.stripMargin
    ctx.addReusableMember(functionCode)
    s"$functionName();"
  }

  private[flink] def genTriggerLeftoverWindowAggCode(
      groupingTerm: String, triggerProcessCode: String): String = {
    s"""
       | $groupingTerm.advanceWatermarkToTriggerAllWindows();
       | $triggerProcessCode
       | $groupingTerm.reset();
       """.stripMargin
  }

  private[flink] def genSortWindowAggCodes(
      enablePreAcc: Boolean,
      ctx: CodeGeneratorContext,
      inputTerm: String,
      inputType: RowType,
      outputType: RowType,
      currentKey: Option[String],
      currentWindow: String): (String, String, GeneratedExpression) = {
    // gen code to apply aggregate functions to grouping window elements
    val offset = if (enablePreAcc) grouping.length + 1 else grouping.length
    val argsMapping = buildAggregateArgsMapping(
      enablePreAcc, offset, inputType, auxGrouping, aggArgs, aggBufferTypes)
    val aggBufferExprs = genFlatAggBufferExprs(
      enablePreAcc,
      ctx,
      builder,
      auxGrouping,
      aggregates,
      argsMapping,
      aggBufferNames,
      aggBufferTypes)
    val initAggBufferCode = genInitFlatAggregateBuffer(
      ctx,
      builder,
      inputType,
      inputTerm,
      grouping,
      auxGrouping,
      aggregates,
      udaggs,
      aggBufferExprs)
    val doAggregateCode = genAggregateByFlatAggregateBuffer(
      enablePreAcc,
      ctx,
      builder,
      inputType,
      inputTerm,
      auxGrouping,
      aggCallToAggFunction,
      aggregates,
      udaggs,
      argsMapping,
      aggBufferNames,
      aggBufferTypes,
      aggBufferExprs)

    // --------------------------------------------------------------------------------------------
    // gen code to set group window aggregate output
    val valueRow = CodeGenUtils.newName("valueRow")
    val resultCodegen = new ExprCodeGenerator(ctx, false)
    val setValueResult = if (isFinal) {
      AggCodeGenHelper.genSortAggOutputExpr(
        enablePreAcc,
        isFinal = true,
        ctx,
        builder,
        grouping,
        auxGrouping,
        aggregates,
        aggInfoList.aggInfos.map(_.externalResultType),
        udaggs,
        argsMapping,
        aggBufferNames,
        aggBufferTypes,
        aggBufferExprs,
        outputType)
    } else {
      // output assigned window and agg buffer
      val valueRowType = RowType.of(
        timestampInternalType +: aggBufferExprs.map(_.resultType): _*)
      val wStartCode = if (inputTimeIsDate) {
        convertToIntValue(s"$currentWindow.getStart()")
      } else {
        s"$currentWindow.getStart()"
      }
      resultCodegen.generateResultExpression(
        GeneratedExpression(s"$wStartCode", NEVER_NULL, NO_CODE, timestampInternalType) +:
            aggBufferExprs,
        valueRowType,
        classOf[GenericRow],
        outRow = valueRow)
    }

    // --------------------------------------------------------------------------------------------
    // add grouping keys if exists
    val resultExpr = currentKey match {
      case Some(key) =>
        // generate agg result
        val windowAggResultTerm = CodeGenUtils.newName("windowAggResult")
        ctx.addReusableOutputRecord(outputType, classOf[JoinedRow], windowAggResultTerm)
        val output =
          s"""
             |${setValueResult.code}
             |$windowAggResultTerm.replace($key, ${setValueResult.resultTerm});
         """.stripMargin
        new GeneratedExpression(windowAggResultTerm, NEVER_NULL, output, outputType)
      // all group agg output
      case _ => setValueResult
    }

    // --------------------------------------------------------------------------------------------
    // add window props if needed
    val outputExpr = if (isFinal) {
      genWindowAggOutputWithWindowPorps(ctx, outputType, currentWindow, resultExpr)
    } else {
      resultExpr
    }
    (initAggBufferCode, doAggregateCode, outputExpr)
  }

  private[flink] def genWindowAggCodes(
      enablePreAcc: Boolean,
      ctx: CodeGeneratorContext,
      windowSize: Long,
      slideSize: Long,
      windowsGrouping: String,
      bufferLimitSize: Int,
      windowElementType: RowType,
      inputTimeFieldIndex: Int,
      currentWindow: String,
      groupKey: Option[String],
      outputType: RowType): (String, String) = {
    // gen code to do aggregate by window or pane
    val windowElemTerm = CodeGenUtils.newName("winElement")
    val (initAggBuffCode, doAggCode, outputWinAggResExpr) = genSortWindowAggCodes(
      enablePreAcc = enablePreAcc,
      ctx,
      windowElemTerm,
      windowElementType,
      outputType,
      groupKey,
      currentWindow)

    // gen code to create windows grouping buffer
    genCreateWindowsGroupingCode(
      ctx, inputTimeFieldIndex, windowSize, slideSize, windowsGrouping, bufferLimitSize)

    // merge pre-accumulate result and output
    val processCode = genTriggerWindowAggByWindowsGroupingCode(
      ctx,
      windowsGrouping,
      currentWindow,
      windowElemTerm,
      initAggBuffCode,
      doAggCode,
      outputWinAggResExpr)
    val endCode = genTriggerLeftoverWindowAggCode(windowsGrouping, processCode)

    (processCode, endCode)
  }

  private[flink] def genPreAccumulate(
      ctx: CodeGeneratorContext,
      windowStart: Long,
      slideSize: Long,
      windowSize: Long,
      inputTerm: String,
      inputType: RowType,
      outputType: RowType,
      windowsTerm: String,
      windowElementType: RowType,
      lastKey: Option[String],
      triggerWindowAggCode: String,
      endWindowAggCode: String): (String, String) = {
    // gen code to assign timestamp
    def genAssignTimestampExpr(
        ctx: CodeGeneratorContext,
        inputTerm: String,
        inputType: RowType): GeneratedExpression = {
      if (isFinal && isMerge) {
        // get assigned timestamp by local window agg
        val ret = GenerateUtils.generateFieldAccess(
          ctx,
          windowedGroupKeyType,
          inputTerm,
          windowedGroupKeyType.getFieldCount - 1)
        if (inputTimeIsDate) {
          val timestamp = ctx.addReusableLocalVariable("long", "timestamp")
          val convertToLongCode =
            s"""
               |  ${ret.code}
               |  $timestamp = ${convertToLongValue(ret.resultTerm)};
           """.stripMargin
          GeneratedExpression(timestamp, ret.nullTerm, convertToLongCode, new BigIntType())
        } else {
          ret
        }
      } else {
        // assign timestamp with each input
        window match {
          case SlidingGroupWindow(_, timeField, size, slide) if isTimeIntervalLiteral(size) =>
            val (slideSize, windowSize) = (asLong(slide), asLong(size))
            if (enableAssignPane) {
              val paneSize = ArithmeticUtils.gcd(windowSize, slideSize)
              genAlignedWindowStartExpr(
                ctx, inputTerm, inputType, timeField, windowStart, paneSize)
            } else {
              assert(slideSize >= windowSize)
              genAlignedWindowStartExpr(
                ctx, inputTerm, inputType, timeField, windowStart, slideSize)
            }
          case TumblingGroupWindow(_, timeField, size) =>
            genAlignedWindowStartExpr(
              ctx, inputTerm, inputType, timeField, windowStart, asLong(size))
          case _ =>
            throw new RuntimeException(s"Bug. Assign pane for $window is not supported.")
        }
      }
    }
    val assignedTsExpr = genAssignTimestampExpr(ctx, inputTerm, inputType)

    // gen code to do aggregate by assigned ts
    val lastTimestampTerm = CodeGenUtils.newName("lastTimestamp")
    ctx.addReusableMember(s"transient long $lastTimestampTerm = -1;")
    val preAccResult = CodeGenUtils.newName("prepareWinElement")
    val preAccResultWriter = CodeGenUtils.newName("prepareWinElementWriter")
    ctx.addReusableOutputRecord(
      windowElementType, classOf[BinaryRow], preAccResult, Some(preAccResultWriter))

    val timeWindowType = classOf[TimeWindow].getName
    val currentWindow = newName("currentWindow")
    ctx.addReusableMember(s"transient $timeWindowType $currentWindow = null;")

    // output or merge pre accumulate results by window
    val (initAggBufferCode, doAggregateCode, mergeOrOutput, mergeOrOutputLastPane) =
      if (isFinal && enableAssignPane) {
        // case: global/complete window agg: Sliding window with with pane optimization
        val offset = if (isMerge) grouping.length + 1 else grouping.length
        val argsMapping = buildAggregateArgsMapping(
          isMerge, offset, inputType, auxGrouping, aggArgs, aggBufferTypes)
        val aggBufferExprs = genFlatAggBufferExprs(
          isMerge,
          ctx,
          builder,
          auxGrouping,
          aggregates,
          argsMapping,
          aggBufferNames,
          aggBufferTypes)
        val initAggBufferCode = genInitFlatAggregateBuffer(
          ctx,
          builder,
          inputType,
          inputTerm,
          grouping,
          auxGrouping,
          aggregates,
          udaggs,
          aggBufferExprs)
        val doAggregateCode = genAggregateByFlatAggregateBuffer(
          isMerge,
          ctx,
          builder,
          inputType,
          inputTerm,
          auxGrouping,
          aggCallToAggFunction,
          aggregates,
          udaggs,
          argsMapping,
          aggBufferNames,
          aggBufferTypes,
          aggBufferExprs)

        // project pre accumulated results into a binary row to fit to WindowsGrouping
        val exprCodegen = new ExprCodeGenerator(ctx, false)
        val setResultExprs = grouping.indices.map(
          generateFieldAccess(
            ctx, groupKeyRowType, lastKey.get, _)) ++
            (GeneratedExpression(lastTimestampTerm, NEVER_NULL, NO_CODE, new BigIntType())
                +: aggBufferExprs)
        val setPanedAggResultExpr = exprCodegen.generateResultExpression(
          setResultExprs,
          windowElementType,
          classOf[BinaryRow],
          preAccResult,
          Some(preAccResultWriter))

        // using windows grouping buffer to merge paned agg results
        val merge =
          s"""
             |${setPanedAggResultExpr.code}
             |// buffer into current group buffer
             |$windowsTerm.addInputToBuffer(($BINARY_ROW)${setPanedAggResultExpr.resultTerm});
             |// trigger window aggregate
             |$triggerWindowAggCode
       """.stripMargin
        val mergeLast =
          s"""
             |${setPanedAggResultExpr.code}
             |// buffer into current group buffer
             |$windowsTerm.addInputToBuffer(($BINARY_ROW)${setPanedAggResultExpr.resultTerm});
             |// last pane triggered windows will be triggered again when grouping keys changed
             |$endWindowAggCode
         """.stripMargin

        (initAggBufferCode, doAggregateCode, merge, mergeLast)
      } else {
        // case1: local window agg
        // case2: global window agg: Tumbling window, Sliding window with windowSize == slideSize
        // or without pane optimization
        // case3: complete window agg: Tumbling window, Sliding window with windowSize == slideSize
        val (initAggBuffCode, doAggCode, outputWinAggResExpr) = genSortWindowAggCodes(
          isMerge,
          ctx,
          inputTerm,
          inputType,
          outputType,
          lastKey,
          currentWindow)

        val output =
          s"""
             |// update current window
             |$currentWindow =
             |  $timeWindowType.of($lastTimestampTerm, $lastTimestampTerm + ${windowSize}L);
             |// build window agg output
             |${outputWinAggResExpr.code}
             |// output result
             |${generateCollect(outputWinAggResExpr.resultTerm)}
           """.stripMargin

        (initAggBuffCode, doAggCode, output, output)
      }

    val preAccCode =
      s"""
         | hasInput = true;
         | // aggregate in sort agg way
         | if ($lastTimestampTerm == -1) {
         |    $initAggBufferCode
         |    $lastTimestampTerm = ${assignedTsExpr.resultTerm};
         | } else if ($lastTimestampTerm != ${assignedTsExpr.resultTerm}) {
         |    $mergeOrOutput
         |    // update active timestamp
         |    $lastTimestampTerm = ${assignedTsExpr.resultTerm};
         |    // init agg buffer
         |    $initAggBufferCode
         | }
         | // accumulate
         | $doAggregateCode
         """.stripMargin

    // gen code to filter invalid windows in the case of jumping window
    val processEachInput = if (!isMerge && isJumpingWindow(slideSize, windowSize)) {
      s"""
         |if (${getInputTimeValue(inputTerm, inputTimeFieldIndex)}) <
         |      ${assignedTsExpr.resultTerm} + ${windowSize}L) {
         |  $preAccCode
         |}
           """.stripMargin
    } else {
      preAccCode
    }

    val processFuncName = CodeGenUtils.newName("preAccumulate")
    val inputTypeTerm = boxedTypeTermForType(inputType)
    ctx.addReusableMember(
      s"""
         |private void $processFuncName($inputTypeTerm $inputTerm) throws java.lang.Exception {
         |  ${ctx.reuseLocalVariableCode()}
         |  // assign timestamp (pane/window)
         |  ${ctx.reuseInputUnboxingCode(inputTerm)}
         |  ${assignedTsExpr.code}
         |  $processEachInput
         |}
         """.stripMargin)

    val endProcessFuncName = CodeGenUtils.newName("endPreAccumulate")
    val setLastPaneAggResultCode =
      s"""
         | // merge paned agg results or output directly
         | $mergeOrOutputLastPane
         | $lastTimestampTerm = -1;
       """.stripMargin
    ctx.addReusableMember(
      s"""
         |private void $endProcessFuncName() throws java.lang.Exception {
         |  ${ctx.reuseLocalVariableCode()}
         |  $setLastPaneAggResultCode
         |}
         """.stripMargin)
    (s"$processFuncName($inputTerm);", s"$endProcessFuncName();")
  }

  /**
    * Generate code to set the group window aggregate result.
    * If the group window aggregate has window auxiliary functions' projection with it,
    * two Timestamp typed fields will be added at the last of the output row indicating
    * the window's start and end timestamp property, to which the windowed aggregate result belongs.
    */
  private[flink] def genWindowAggOutputWithWindowPorps(
      ctx: CodeGeneratorContext,
      outputType: RowType,
      currentWindowTerm: String,
      aggResultExpr: GeneratedExpression): GeneratedExpression = {
    // output window property if necessary
    val propSize = namedProperties.size
    if (namedProperties.isEmpty || !isFinal) {
      // group window aggregate without window auxiliary function projection
      // or local window aggregate
      aggResultExpr
    } else {
      // window property fields always added at last when building the LogicalWindowAggregate
      val propOutputType = {
        val outputFields = outputType.getChildren
        val lastFieldPos = outputFields.size - 1
        val propFields =
          for (offset <- lastFieldPos - propSize + 1 to lastFieldPos) yield outputFields(offset)
        RowType.of(propFields: _*)
      }

      // reusable row to set window property fields
      val propTerm = CodeGenUtils.newName("windowProp")
      ctx.addReusableOutputRecord(propOutputType, classOf[GenericRow], propTerm)
      val windowAggResultWithPropTerm = CodeGenUtils.newName("windowAggResultWithProperty")
      ctx.addReusableOutputRecord(outputType, classOf[JoinedRow], windowAggResultWithPropTerm)

      // set window start, end property according to window type
      val (startPos, endPos, rowTimePos) = AggregateUtil.computeWindowPropertyPos(namedProperties)
      val lastPos = propSize - 1

      // get assigned window start timestamp
      def windowProps(size: Expression) = {
        val (startWValue, endWValue, rowTimeValue) = (
            s"$currentWindowTerm.getStart()",
            s"$currentWindowTerm.getEnd()",
            s"$currentWindowTerm.maxTimestamp()")
        val start = if (startPos.isDefined) {
          s"$propTerm.setLong($lastPos + ${startPos.get}, $startWValue);"
        } else ""
        val end = if (endPos.isDefined) {
          s"$propTerm.setLong($lastPos + ${endPos.get}, $endWValue);"
        } else ""
        val rowTime = if (rowTimePos.isDefined) {
          s"$propTerm.setLong($lastPos + ${rowTimePos.get}, $rowTimeValue);"
        } else ""
        (start, end, rowTime)
      }

      // compute window start, window end, window rowTime
      val (setWindowStart, setWindowEnd, setWindowRowTime) = window match {
        case TumblingGroupWindow(_, _, size) if isTimeIntervalLiteral(size) =>
          windowProps(size)
        case SlidingGroupWindow(_, _, size, _) if isTimeIntervalLiteral(size) =>
          windowProps(size)
        case _ =>
          throw new UnsupportedOperationException(
            s"Window $window is not supported in a batch environment.")
      }
      val output =
        s"""
           |${aggResultExpr.code}
           |$setWindowStart
           |$setWindowEnd
           |$setWindowRowTime
           |$windowAggResultWithPropTerm.replace(${aggResultExpr.resultTerm}, $propTerm);
         """.stripMargin
      new GeneratedExpression(
        windowAggResultWithPropTerm, NEVER_NULL, output, outputType)
    }
  }

  private[flink] def isJumpingWindow(slideSize: Long, windowSize: Long) : Boolean = {
    window.isInstanceOf[SlidingGroupWindow] && slideSize > windowSize
  }

  private[flink] def genAlignedWindowStartExpr(
      ctx: CodeGeneratorContext,
      inputTerm: String,
      inputType: RowType,
      timeField: Expression,
      windowStart: Long,
      slideSize: Long,
      index: Int = 0): GeneratedExpression = {
    val exprCodegen = new ExprCodeGenerator(ctx, nullableInput = false)
        .bindInput(inputType, inputTerm = inputTerm)
    val timeStampInLong = reinterpretCast(timeField, typeLiteral(DataTypes.BIGINT()), false)
    val millValue: Long = if (inputTimeIsDate) DateTimeUtils.MILLIS_PER_DAY else 1L
    val timeStampValue = times(timeStampInLong, literal(millValue))
    val remainder = mod(minus(timeStampValue, literal(windowStart)), literal(slideSize))
    // handle both positive and negative cases
    val expr = minus(minus(
      timeStampValue,
      ifThenElse(
        lessThan(remainder, literal(0)),
        plus(remainder, literal(slideSize)),
        remainder)),
      literal(index * slideSize))
    exprCodegen.generateExpression(expr.accept(
      new RexNodeConverter(relBuilder.values(inputRowType))))
  }

  def getGrouping: Array[Int] = grouping

  def getAuxGrouping: Array[Int] = auxGrouping

  def getAggCallList: Seq[AggregateCall] = aggCallToAggFunction.map(_._1)

  def getInputTimeValue(inputTerm: String, index: Int): String = {
    if(inputTimeIsDate) {
      s"""
         |$inputTerm.getInt($index) * ${DateTimeUtils.MILLIS_PER_DAY}L
       """.stripMargin
    } else {
      s"$inputTerm.getLong($index)"
    }
  }

  def convertToIntValue(inputTerm: String): String = {
    if (inputTimeIsDate) {
      s"(int)($inputTerm/${DateTimeUtils.MILLIS_PER_DAY})"
    } else {
      inputTerm
    }
  }

  def convertToLongValue(inputTerm: String): String = {
    if (inputTimeIsDate) {
      s"(long)($inputTerm * ${DateTimeUtils.MILLIS_PER_DAY}L)"
    } else {
      inputTerm
    }
  }

  def isSlidingWindowWithOverlapping(
      enableAssignPane: Boolean,
      window: LogicalWindow,
      slideSize: Long,
      windowSize: Long): Boolean = {
    window.isInstanceOf[SlidingGroupWindow] && slideSize < windowSize && !enableAssignPane
  }
}

object WindowCodeGenerator {

  def getWindowDef(window: LogicalWindow): (Long, Long) = {
    val (windowSize, slideSize): (Long, Long) = window match {
      case TumblingGroupWindow(_, _, size) if isTimeIntervalLiteral(size) =>
        (asLong(size), asLong(size))
      case SlidingGroupWindow(_, _, size, slide) if isTimeIntervalLiteral(size) =>
        (asLong(size), asLong(slide))
      case _ =>
        // count tumbling/sliding window and session window not supported now
        throw new UnsupportedOperationException(s"Window $window is not supported right now.")
    }
    (windowSize, slideSize)
  }

  def asLong(expr: Expression): Long = extractValue(expr, classOf[JLong]).get()

  def isTimeIntervalLiteral(expr: Expression): Boolean = expr match {
    case literal: ValueLiteralExpression if
      hasRoot(literal.getOutputDataType.getLogicalType, INTERVAL_DAY_TIME) => true
    case _ => false
  }
}
