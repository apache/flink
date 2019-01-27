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

import org.apache.flink.table.api.functions.{AggregateFunction, DeclarativeAggregateFunction, UserDefinedFunction}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.types.{DataType, DataTypes, InternalType, PrimitiveType, RowType}
import org.apache.flink.table.api.window.TimeWindow
import org.apache.flink.table.api.{TableConfig, TableException}
import org.apache.flink.table.calcite.FlinkRelBuilder.NamedWindowProperty
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.codegen.CodeGenUtils.{boxedTypeTermForType, newName}
import org.apache.flink.table.codegen.CodeGeneratorContext.BINARY_ROW
import org.apache.flink.table.codegen.GeneratedExpression.{NEVER_NULL, NO_CODE}
import org.apache.flink.table.codegen._
import org.apache.flink.table.codegen.agg.BatchExecAggregateCodeGen
import org.apache.flink.table.codegen.operator.OperatorCodeGenerator.generatorCollect
import org.apache.flink.table.dataformat.{BaseRow, BinaryRow, GenericRow, JoinedRow}
import org.apache.flink.table.expressions.ExpressionUtils.isTimeIntervalLiteral
import org.apache.flink.table.expressions.{Expression, If, Literal}
import org.apache.flink.table.functions.utils.UserDefinedFunctionUtils.getAccumulatorTypeOfAggregateFunction
import org.apache.flink.table.plan.logical.{LogicalWindow, SlidingGroupWindow, TumblingGroupWindow}
import org.apache.flink.table.plan.nodes.exec.RowBatchExecNode
import org.apache.flink.table.plan.nodes.physical.FlinkPhysicalRel
import org.apache.flink.table.plan.util.AggregateUtil.asLong
import org.apache.flink.table.plan.util.{AggregateNameUtil, AggregateUtil}
import org.apache.flink.table.runtime.functions.DateTimeFunctions
import org.apache.flink.table.runtime.util.RowIterator
import org.apache.flink.table.runtime.window.grouping.{AbstractWindowsGrouping, HeapWindowsGrouping}

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.rel.{RelNode, RelWriter, SingleRel}
import org.apache.calcite.sql.`type`.SqlTypeName
import org.apache.calcite.tools.RelBuilder
import org.apache.commons.math3.util.ArithmeticUtils

import scala.collection.JavaConversions._

abstract class BatchExecWindowAggregateBase(
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
    enableAssignPane: Boolean = true,
    val isMerge: Boolean,
    val isFinal: Boolean)
  extends SingleRel(cluster, traitSet, inputNode)
  with BatchExecAggregateCodeGen
  with BatchPhysicalRel
  with RowBatchExecNode {

  if (grouping.isEmpty && auxGrouping.nonEmpty) {
    throw new TableException("auxGrouping should be empty if grouping is emtpy.")
  }

  def getNamedProperties: Seq[NamedWindowProperty] = namedProperties

  lazy val builder: RelBuilder = relBuilder.values(inputRelDataType)
  lazy val timestampIsDate: Boolean = inputTimestampType.getSqlTypeName == SqlTypeName.DATE
  lazy val timestampInternalType: PrimitiveType =
    if (timestampIsDate) DataTypes.INT else DataTypes.LONG
  lazy val timestampInternalTypeName: String = if (timestampIsDate) "Int" else "Long"
  lazy val aggregateCalls: Seq[AggregateCall] = aggCallToAggFunction.map(_._1)
  lazy val aggregates: Seq[UserDefinedFunction] = aggCallToAggFunction.map(_._2)

  // currently put auxGrouping to aggBuffer in code-gen
  lazy val aggBufferNames: Array[Array[String]] = auxGrouping.zipWithIndex.map {
    case (_, index) => Array(s"aux_group$index")
  } ++ aggregates.zipWithIndex.toArray.map {
    case (a: DeclarativeAggregateFunction, index) =>
      val idx = auxGrouping.length + index
      a.aggBufferAttributes.map(attr => s"agg${idx}_${attr.name}").toArray
    case (_: AggregateFunction[_, _], index) =>
      val idx = auxGrouping.length + index
      Array(s"agg$idx")
  }

  lazy val aggBufferTypes: Array[Array[InternalType]] = auxGrouping.map { index =>
    Array(FlinkTypeFactory.toInternalType(inputRelDataType.getFieldList.get(index).getType))
  } ++ aggregates.map {
    case a: DeclarativeAggregateFunction => a.aggBufferSchema.map(_.toInternalType).toArray
    case a: AggregateFunction[_, _] =>
      Array(getAccumulatorTypeOfAggregateFunction(a)).map(_.toInternalType)
  }.toArray[Array[InternalType]]

  lazy val groupKeyRowType = new RowType(
    grouping.map { index =>
      FlinkTypeFactory.toInternalType(inputRelDataType.getFieldList.get(index).getType)
    }.toArray[DataType], grouping.map(inputRelDataType.getFieldNames.get(_)))

  // get udagg instance names
  lazy val udaggs: Map[AggregateFunction[_, _], String] = aggregates
      .filter(a => a.isInstanceOf[AggregateFunction[_, _]])
      .map(a => a -> CodeGeneratorContext.udfFieldName(a)).toMap
      .asInstanceOf[Map[AggregateFunction[_, _], String]]

  lazy val windowedGroupKeyType: RowType = new RowType(
    groupKeyRowType.getFieldTypes :+ timestampInternalType,
    groupKeyRowType.getFieldNames :+ "assignedTs$")

  override def deriveRowType(): RelDataType = rowRelDataType

  override def explainTerms(pw: RelWriter): RelWriter = {
    super.explainTerms(pw)
      .itemIf("groupBy",
        AggregateNameUtil.groupingToString(inputRelDataType, grouping), grouping.nonEmpty)
      .itemIf("auxGrouping",
        AggregateNameUtil.groupingToString(inputRelDataType, auxGrouping), auxGrouping.nonEmpty)
      .item("window", window)
      .itemIf("properties", namedProperties.map(_.name).mkString(", "), namedProperties.nonEmpty)
      .item("select",
        AggregateNameUtil.windowAggregationToString(
          inputRelDataType,
          grouping,
          auxGrouping,
          rowRelDataType,
          aggCallToAggFunction,
          enableAssignPane,
          isMerge = isMerge,
          isGlobal = isFinal))
  }

  override def isDeterministic: Boolean = AggregateUtil.isDeterministic(getAggCallList)

  def getOutputRowClass: Class[_ <: BaseRow] =
    if (namedProperties.isEmpty && grouping.isEmpty) classOf[GenericRow] else classOf[JoinedRow]

  private[flink] def getWindowsGroupingElementInfo(
      enablePreAccumulate: Boolean = true): RowType = {
    if (enablePreAccumulate) {
      val (groupKeyNames, groupKeyTypes) =
        (groupKeyRowType.getFieldNames, groupKeyRowType.getFieldTypes)
      val (aggBuffNames, aggBuffTypes) =
        (aggBufferNames.flatten, aggBufferTypes.flatten)
      new RowType(
        (groupKeyTypes :+ timestampInternalType) ++ aggBuffTypes,
        (groupKeyNames :+ "assignedTs$") ++ aggBuffNames)
    } else {
      FlinkTypeFactory.toInternalRowType(inputRelDataType)
    }
  }

  private[flink] def genCreateWindowsGroupingCode(
      ctx: CodeGeneratorContext,
      timestampIndex: Int,
      windowSize: Long,
      slideSize: Long,
      groupingTerm: String,
      bufferLimitSize: Int): Unit = {
    // TODO using SpillableWindowsGrouping instead
    val windowsGrouping = classOf[HeapWindowsGrouping].getName
    ctx.addReusableMember(
      s"transient $windowsGrouping $groupingTerm " +
        s"= new $windowsGrouping(" +
        s"$bufferLimitSize, ${windowSize}L, ${slideSize}L, $timestampIndex, $timestampIsDate);")
    ctx.addReusableCloseStatement(s"$groupingTerm.close();")
  }

  /**
    * Using [[AbstractWindowsGrouping#buildTriggerWindowElementsIterator]]
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
         |    $BINARY_ROW $currentWindowElement = ($BINARY_ROW)elementIterator.getRow();
         |    ${ctx.reuseInputUnboxingCode(Set(currentWindowElement))}
         |    $doAggregateCode
         |  }
         |  if (hasElement) {
         |    // write output
         |    ${outputWinAggResExpr.code}
         |    ${generatorCollect(outputWinAggResExpr.resultTerm)}
         |  }
         |}""".stripMargin
    val functionName = CodeGenUtils.newName("triggerWindowProcess")
    val functionCode =
      s"""
         |private void $functionName() {
         |  ${ctx.reuseFieldCode()}
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
      isMerge: Boolean,
      isFinal: Boolean,
      ctx: CodeGeneratorContext,
      config: TableConfig,
      inputTerm: String,
      inputType: RowType,
      outputType: RowType,
      currentKey: Option[String],
      currentWindow: String): (String, String, GeneratedExpression) = {

    // gen code to apply aggregate functions to grouping window elements
    val offset = if (isMerge) grouping.length + 1 else grouping.length
    val argsMapping = buildAggregateArgsMapping(
      isMerge, offset, inputRelDataType, auxGrouping, aggregateCalls, aggBufferTypes)
    val aggBufferExprs = genFlatAggBufferExprs(isMerge, ctx, config, builder, auxGrouping,
      aggregates, argsMapping, aggBufferNames, aggBufferTypes)
    val initAggBufferCode = genInitFlatAggregateBuffer(ctx, config, builder, inputType, inputTerm,
      grouping, auxGrouping, aggregates, udaggs, aggBufferExprs)
    val doAggregateCode = genAggregateByFlatAggregateBuffer(
      isMerge, ctx, config, builder, inputType, inputTerm, auxGrouping, aggCallToAggFunction,
      aggregates, udaggs, argsMapping, aggBufferNames, aggBufferTypes, aggBufferExprs)

    // --------------------------------------------------------------------------------------------
    // gen code to set group window aggregate output
    val valueRow = CodeGenUtils.newName("valueRow")
    val resultCodegen = new ExprCodeGenerator(ctx, false, config.getNullCheck)
    val setValueResult = if (isFinal) {
      genSortAggOutputExpr(
        isMerge, isFinal = true, ctx, config, builder, grouping, auxGrouping, aggregates, udaggs,
        argsMapping, aggBufferNames, aggBufferTypes, aggBufferExprs, outputType)
    } else {
      // output assigned window and agg buffer
      val valueRowType = new RowType(
        timestampInternalType +: aggBufferExprs.map(_.resultType): _*)
      val wStartCode = if (timestampIsDate) {
        convertToIntValue(s"$currentWindow.getStart()")
      } else {
        s"$currentWindow.getStart()"
      }
      resultCodegen.generateResultExpression(GeneratedExpression(
        s"$wStartCode", NEVER_NULL, NO_CODE, timestampInternalType, literal = true) +:
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
        ctx.addOutputRecord(outputType, classOf[JoinedRow], windowAggResultTerm)
        val output =
          s"""
             |${setValueResult.code}
             |$windowAggResultTerm.replace($key, ${setValueResult.resultTerm});
         """.stripMargin
        new GeneratedExpression(
          windowAggResultTerm, NEVER_NULL, output, outputType.toInternalType)
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
      config: TableConfig,
      windowSize: Long,
      slideSize: Long,
      windowsGrouping: String,
      bufferLimitSize: Int,
      windowElementType: RowType,
      timestampIndex: Int,
      currentWindow: String,
      groupKey: Option[String],
      outputType: RowType): (String, String) = {
    // gen code to do aggregate by window or pane
    val windowElemTerm = CodeGenUtils.newName("winElement")
    val (initAggBuffCode, doAggCode, outputWinAggResExpr) = genSortWindowAggCodes(
      isMerge = enablePreAcc, isFinal, ctx, config,
      windowElemTerm, windowElementType, outputType, groupKey, currentWindow)

    // gen code to create windows grouping buffer
    genCreateWindowsGroupingCode(
      ctx, timestampIndex, windowSize, slideSize, windowsGrouping, bufferLimitSize)

    // merge pre-accumulate result and output
    val processCode = genTriggerWindowAggByWindowsGroupingCode(
      ctx, windowsGrouping, currentWindow, windowElemTerm,
      initAggBuffCode, doAggCode, outputWinAggResExpr)
    val endCode = genTriggerLeftoverWindowAggCode(windowsGrouping, processCode)

    (processCode, endCode)
  }

  private[flink] def genPreAccumulate(
      ctx: CodeGeneratorContext,
      config: TableConfig,
      window: LogicalWindow,
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
        config: TableConfig,
        inputTerm: String,
        inputType: RowType,
        window: LogicalWindow): GeneratedExpression = {
      if (isFinal && isMerge) {
        // get assigned timestamp by local window agg
        val ret = CodeGenUtils.generateFieldAccess(ctx, windowedGroupKeyType.toInternalType,
          inputTerm,
          windowedGroupKeyType.getArity - 1, nullCheck = false)
        if (timestampIsDate) {
          val timestamp = ctx.newReusableField("timestamp", "long")
          val convertToLongCode =
            s"""
               |  ${ret.code}
               |  $timestamp = ${convertToLongValue(ret.resultTerm)};
           """.stripMargin
          GeneratedExpression(timestamp, ret.nullTerm, convertToLongCode, DataTypes.LONG)
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
                ctx, config, inputTerm, inputType, timeField, windowStart, paneSize)
            } else {
              assert(slideSize >= windowSize)
              genAlignedWindowStartExpr(
                ctx, config, inputTerm, inputType, timeField, windowStart, slideSize)
            }
          case TumblingGroupWindow(_, timeField, size) =>
            genAlignedWindowStartExpr(
              ctx, config, inputTerm, inputType, timeField, windowStart, asLong(size))
          case _ =>
            throw new RuntimeException(s"Bug. Assign pane for $window is not supported.")
        }
      }
    }
    val assignedTsExpr = genAssignTimestampExpr(ctx, config, inputTerm, inputType, window)

    // gen code to do aggregate by assigned ts
    val lastTimestampTerm = CodeGenUtils.newName("lastTimestamp")
    ctx.addReusableMember(s"transient long $lastTimestampTerm = -1;")
    val preAccResult = CodeGenUtils.newName("prepareWinElement")
    val preAccResultWriter = CodeGenUtils.newName("prepareWinElementWriter")
    ctx.addOutputRecord(
      windowElementType, classOf[BinaryRow], preAccResult, Some(preAccResultWriter))

    val timeWindowType = classOf[TimeWindow].getName
    val currentWindow = newName("currentWindow")
    ctx.addReusableMember(s"transient $timeWindowType $currentWindow = null;")

    // output or merge pre accumulate results by window
    val (initAggBufferCode, doAggregateCode, mergeOrOutput, mergeOrOutputLastPane) =
      if (isFinal && enableAssignPane) {
        // case: global/complete window agg: Sliding window with with pane optimization
        val offset = if (isMerge) grouping.length + 1 else grouping.length
        val argsMapping = buildAggregateArgsMapping(isMerge, offset, inputRelDataType, auxGrouping,
          aggCallToAggFunction.map(_._1), aggBufferTypes)
        val aggBufferExprs = genFlatAggBufferExprs(isMerge, ctx, config, builder, auxGrouping,
          aggregates, argsMapping, aggBufferNames, aggBufferTypes)
        val initAggBufferCode = genInitFlatAggregateBuffer(ctx, config, builder, inputType,
          inputTerm, grouping, auxGrouping, aggregates, udaggs, aggBufferExprs)
        val doAggregateCode = genAggregateByFlatAggregateBuffer(
          isMerge, ctx, config, builder, inputType, inputTerm, auxGrouping, aggCallToAggFunction,
          aggregates, udaggs, argsMapping, aggBufferNames, aggBufferTypes, aggBufferExprs)

        // project pre accumulated results into a binary row to fit to WindowsGrouping
        val exprCodegen = new ExprCodeGenerator(ctx, false, config.getNullCheck)
        val setResultExprs = grouping.indices.map(
          CodeGenUtils.generateFieldAccess(
            ctx, groupKeyRowType.toInternalType, lastKey.get, _, config.getNullCheck)) ++
          (GeneratedExpression(lastTimestampTerm, NEVER_NULL, NO_CODE, DataTypes.LONG,
            literal = true) +: aggBufferExprs)
        val setPanedAggResultExpr = exprCodegen.generateResultExpression(
          setResultExprs, windowElementType, classOf[BinaryRow],
          preAccResult, Some(preAccResultWriter))

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
          isMerge, isFinal, ctx, config,
          inputTerm, inputType, outputType, lastKey, currentWindow)

        val output =
          s"""
             |// update current window
             |$currentWindow =
             |  $timeWindowType.of($lastTimestampTerm, $lastTimestampTerm + ${windowSize}L);
             |// build window agg output
             |${outputWinAggResExpr.code}
             |// output result
             |${generatorCollect(outputWinAggResExpr.resultTerm)}
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
    val processEachInput = if (!isMerge && isJumpingWindow(window, slideSize, windowSize)) {
      s"""
         |if (${getInputTimeValue(inputTerm, inputTimestampIndex)}) <
         |      ${assignedTsExpr.resultTerm} + ${windowSize}L) {
         |  $preAccCode
         |}
           """.stripMargin
    } else {
      preAccCode
    }

    val processFuncName = CodeGenUtils.newName("preAccumulate")
    val inputTypeTerm = boxedTypeTermForType(inputType.toInternalType)
    ctx.addReusableMember(
      s"""
         |private void $processFuncName($inputTypeTerm $inputTerm) throws java.io.IOException {
         |  ${ctx.reuseFieldCode()}
         |  // assign timestamp (pane/window)
         |  ${ctx.reuseInputUnboxingCode(Set(inputTerm))}
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
         |private void $endProcessFuncName() throws java.io.IOException {
         |  ${ctx.reuseFieldCode()}
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
    if (propSize == 0 || !isFinal) {
      // group window aggregate without window auxiliary function projection
      // or local window aggregate
      aggResultExpr
    } else {
      // window property fields always added at last when building the LogicalWindowAggregate
      val outputFields = outputType.getFieldTypes
      val lastFieldPos = outputFields.size - 1
      val propFields =
        for (offset <- lastFieldPos - propSize + 1 to lastFieldPos) yield outputFields(offset)
      val propOutputType = new RowType(propFields: _*)
      // reusable row to set window property fields
      val propTerm = CodeGenUtils.newName("windowProp")
      ctx.addOutputRecord(propOutputType, classOf[GenericRow], propTerm)
      val windowAggResultWithPropTerm = CodeGenUtils.newName("windowAggResultWithProperty")
      ctx.addOutputRecord(outputType, classOf[JoinedRow], windowAggResultWithPropTerm)

      // set window start, end property according to window type
      val (startPos, endPos, rowTimePos) = AggregateUtil.computeWindowPropertyPos(namedProperties)
      val lastPos = propSize - 1

      // get assigned window start timestamp
      def windowProps(size: Expression) = {
        val (flag, startWValue, endWValue, rowTimeValue) = ("Long",
            s"$currentWindowTerm.getStart()",
            s"$currentWindowTerm.getEnd()",
            s"$currentWindowTerm.maxTimestamp()")
        val start = if (startPos.isDefined) {
          s"$propTerm.set$flag($lastPos + ${startPos.get}, $startWValue);"
        } else ""
        val end = if (endPos.isDefined) {
          s"$propTerm.set$flag($lastPos + ${endPos.get}, $endWValue);"
        } else ""
        val rowTime = if (rowTimePos.isDefined) {
          s"$propTerm.set$flag($lastPos + ${rowTimePos.get}, $rowTimeValue);"
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
        windowAggResultWithPropTerm, NEVER_NULL, output, outputType.toInternalType)
    }
  }

  private[flink] def getWindowDef(window: LogicalWindow): (Long, Long) = {
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

  private[flink] def choosePreAcc(
      enableAssignPane: Boolean,
      window: LogicalWindow,
      slideSize: Long,
      windowSize: Long): Boolean = {
    // pre accumulate by pane
    enableAssignPane ||
        // pre accumulate by window
        window.isInstanceOf[TumblingGroupWindow] ||
        (window.isInstanceOf[SlidingGroupWindow] && slideSize == windowSize)
  }

  private[flink] def isSlidingWindowWithOverlapping(
      enableAssignPane: Boolean,
      window: LogicalWindow,
      slideSize: Long,
      windowSize: Long): Boolean = {
    window.isInstanceOf[SlidingGroupWindow] && slideSize < windowSize && !enableAssignPane
  }

  private[flink] def isJumpingWindow(
      window: LogicalWindow, slideSize: Long, windowSize: Long) : Boolean = {
    window.isInstanceOf[SlidingGroupWindow] && slideSize > windowSize
  }

  private[flink] def genAlignedWindowStartExpr(
      ctx: CodeGeneratorContext,
      config: TableConfig,
      inputTerm: String,
      inputType: RowType,
      timeField: Expression,
      windowStart: Long,
      slideSize: Long,
      index: Int = 0): GeneratedExpression = {
    val exprCodegen = new ExprCodeGenerator(ctx, nullableInput = false, nullCheck = false)
        .bindInput(inputType.toInternalType, inputTerm = inputTerm)
    val timeStampInLong = timeField.reinterpret(DataTypes.LONG, checkOverflow = false)
    val millValue = if (timestampIsDate) DateTimeFunctions.MILLIS_PER_DAY else 1L
    val timeStampValue = timeStampInLong * Literal(millValue, DataTypes.LONG)
    val remainder = (timeStampValue - windowStart) % slideSize
    // handle both positive and negative cases
    val expr = timeStampValue - If(remainder < 0, remainder + slideSize, remainder) -
        index * slideSize
    exprCodegen.generateExpression(expr.toRexNode(relBuilder.values(inputRelDataType)))
  }

  def getGrouping: Array[Int] = grouping

  def getAuxGrouping: Array[Int] = auxGrouping

  def getWindow: LogicalWindow = window

  def getAggCallList: Seq[AggregateCall] = aggCallToAggFunction.map(_._1)

  def getInputTimeValue(inputTerm: String, index: Int): String = {
    if(timestampIsDate) {
      s"""
         |$inputTerm.getInt($index) * ${DateTimeFunctions.MILLIS_PER_DAY}L
       """.stripMargin
    } else {
      s"$inputTerm.getLong($index)"
    }
  }

  def convertToIntValue(inputTerm: String): String = {
    if (timestampIsDate) {
      s"(int)($inputTerm/${DateTimeFunctions.MILLIS_PER_DAY})"
    } else {
      inputTerm
    }
  }

  def convertToLongValue(inputTerm: String): String = {
    if (timestampIsDate) {
      s"(long)($inputTerm * ${DateTimeFunctions.MILLIS_PER_DAY}L)"
    } else {
      inputTerm
    }
  }

  override def getFlinkPhysicalRel: FlinkPhysicalRel = this
}
