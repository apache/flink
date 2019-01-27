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

package org.apache.flink.table.plan.nodes.physical.stream

import org.apache.flink.annotation.VisibleForTesting
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.cep._
import org.apache.flink.cep.nfa.AfterMatchSkipStrategy
import org.apache.flink.cep.nfa.compiler.NFACompiler
import org.apache.flink.cep.operator.{FlatSelectCepOperator, FlatSelectTimeoutCepOperator, SelectCepOperator, SelectTimeoutCepOperator}
import org.apache.flink.cep.pattern.Pattern
import org.apache.flink.cep.pattern.Quantifier.QuantifierProperty
import org.apache.flink.cep.pattern.conditions.BooleanConditions
import org.apache.flink.streaming.api.operators.co.CoStreamMap
import org.apache.flink.streaming.api.operators.{ChainingStrategy, ProcessOperator}
import org.apache.flink.streaming.api.transformations.{OneInputTransformation, SideOutputTransformation, StreamTransformation, TwoInputTransformation}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.{StreamTableEnvironment, TableConfig, TableException, ValidationException}
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.plan.logical.MatchRecognize
import org.apache.flink.table.plan.nodes.common.CommonMatchRecognize
import org.apache.flink.table.plan.nodes.exec.RowStreamExecNode
import org.apache.flink.table.plan.nodes.physical.FlinkPhysicalRel
import org.apache.flink.table.plan.rules.physical.stream.StreamExecRetractionRules
import org.apache.flink.table.plan.schema.BaseRowSchema
import org.apache.flink.table.plan.util._
import org.apache.flink.table.runtime.BaseRowRowtimeProcessFunction
import org.apache.flink.table.runtime.`match`._
import org.apache.flink.table.typeutils.BaseRowTypeInfo
import org.apache.flink.util.{MathUtils, OutputTag}

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.RelFieldCollation.Direction
import org.apache.calcite.rel._
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rex._
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.sql.SqlMatchRecognize.{AfterOption, RowsPerMatchOption}
import org.apache.calcite.sql.`type`.SqlTypeFamily
import org.apache.calcite.sql.fun.SqlStdOperatorTable._
import org.apache.calcite.tools.RelBuilder

import java.lang.{Boolean => JBoolean, Long => JLong}
import java.util
import java.util.UUID

import _root_.scala.collection.JavaConversions._

/**
  * Flink RelNode which matches along with LogicalMatch.
  */
class StreamExecMatch(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    input: RelNode,
    logicalMatch: MatchRecognize,
    outputSchema: BaseRowSchema,
    inputSchema: BaseRowSchema)
  extends SingleRel(cluster, traitSet, input)
  with CommonMatchRecognize
  with StreamPhysicalRel
  with RowStreamExecNode {

  override def deriveRowType(): RelDataType = outputSchema.relDataType

  override def copy(traitSet: RelTraitSet, inputs: util.List[RelNode]): RelNode = {
    new StreamExecMatch(
      cluster,
      traitSet,
      inputs.get(0),
      logicalMatch,
      outputSchema,
      inputSchema)
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    explainMatch(
      super.explainTerms(pw), logicalMatch, inputSchema.fieldNames.toList, getExpressionString)
  }

  override def isDeterministic: Boolean = {
    MatchUtil.isDeterministic(logicalMatch)
  }

  private def translateTimeBound(interval: RexNode): Time = {
    interval match {
      case x: RexLiteral if x.getTypeName.getFamily == SqlTypeFamily.INTERVAL_DAY_TIME =>
        Time.milliseconds(x.getValueAs(classOf[JLong]))
      case _ =>
        throw new TableException("Only constant intervals with millisecond resolution " +
          "are supported as time constraints of patterns.")
    }
  }

  @VisibleForTesting
  private[flink] def translatePattern(
      config: TableConfig,
      relBuilder: RelBuilder,
      inputTypeInfo: TypeInformation[BaseRow]
  ): (Pattern[BaseRow, BaseRow], Seq[String]) = {
    val patternVisitor = new PatternVisitor(config, relBuilder, inputTypeInfo, logicalMatch)
    val cepPattern = if (logicalMatch.interval != null) {
      val interval = translateTimeBound(logicalMatch.interval)
      logicalMatch.pattern.accept(patternVisitor).within(interval)
    } else {
      logicalMatch.pattern.accept(patternVisitor)
    }
    (cepPattern, patternVisitor.names.toSeq)
  }

  //~ ExecNode methods -----------------------------------------------------------

  override def getFlinkPhysicalRel: FlinkPhysicalRel = this

  override def translateToPlanInternal(
      tableEnv: StreamTableEnvironment): StreamTransformation[BaseRow] = {

    val inputIsAccRetract = StreamExecRetractionRules.isAccRetract(getInput)

    if (inputIsAccRetract) {
      throw new TableException(
        "Retraction on match recognize is not supported. " +
          "Note: Match recognize should not follow a non-windowed GroupBy aggregation.")
    }

    val config = tableEnv.config
    val relBuilder = tableEnv.getRelBuilder
    val inputTypeInfo =
      inputSchema.typeInfo()

    val inputTransform = getInputNodes.get(0).translateToPlan(tableEnv)
      .asInstanceOf[StreamTransformation[BaseRow]]

    val (timestampedInput, comparator) = translateOrder(
      inputTransform, inputTypeInfo, logicalMatch.orderKeys)

    val (cepPattern, patternNames) = translatePattern(config, relBuilder, inputTypeInfo)

    //TODO remove this once it is supported in CEP library
    if (NFACompiler.canProduceEmptyMatches(cepPattern)) {
      throw new TableException(
        "Patterns that can produce empty matches are not supported. There must be at least one " +
          "non-optional state.")
    }

    //TODO remove this once it is supported in CEP library
    if (cepPattern.getQuantifier.hasProperty(QuantifierProperty.GREEDY)) {
      throw new TableException(
        "Greedy quantifiers are not allowed as the last element of a Pattern yet. Finish your " +
          "pattern with either a simple variable or reluctant quantifier.")
    }

    generateOutputTransformation(
      config, relBuilder, timestampedInput, inputTypeInfo, cepPattern, comparator, patternNames)
  }

  private def translateOrder(
      inputTransform: StreamTransformation[BaseRow],
      inputTypeInfo: BaseRowTypeInfo,
      orderKeys: RelCollation): (StreamTransformation[BaseRow], EventComparator[BaseRow]) = {

    if (orderKeys.getFieldCollations.size() == 0) {
      throw new ValidationException("You must specify either rowtime or proctime for order by.")
    }

    // need to identify time between others order fields. Time needs to be first sort element
    val timeOrderField = SortUtil.getFirstSortField(orderKeys, inputSchema.relDataType)

    if (!FlinkTypeFactory.isTimeIndicatorType(timeOrderField.getType)) {
      throw new ValidationException(
        "You must specify either rowtime or proctime for order by as the first one.")
    }

    // time ordering needs to be ascending
    if (SortUtil.getFirstSortDirection(orderKeys) != Direction.ASCENDING) {
      throw new ValidationException(
        "Primary sort order of a streaming table must be ascending on time.")
    }

    val rowComparator = if (orderKeys.getFieldCollations.size() > 1) {
      if (FlinkTypeFactory.isProctimeIndicatorType(timeOrderField.getType)) {
        MatchUtil createProcTimeSortFunction(orderKeys, inputSchema)
      } else {
        MatchUtil.createRowTimeSortFunction(orderKeys, inputSchema)
      }
    } else {
      null
    }

    val rowtimeFields = inputSchema.relDataType
      .getFieldList.filter(f => FlinkTypeFactory.isRowtimeIndicatorType(f.getType))

    val timestampedInputTransform = if (rowtimeFields.nonEmpty) {
      // copy the rowtime field into the StreamRecord timestamp field
      val timeIdx = rowtimeFields.head.getIndex

      val transformation = new OneInputTransformation(
        inputTransform,
        s"rowtime field: (${rowtimeFields.head})",
        new ProcessOperator(new BaseRowRowtimeProcessFunction(timeIdx, inputTypeInfo)),
        inputTypeInfo,
        inputTransform.getParallelism)
      setTransformationRes(transformation)
      transformation
    } else {
      inputTransform
    }

    (timestampedInputTransform, rowComparator)
  }

  private def generateOutputTransformation(
      config: TableConfig,
      relBuilder: RelBuilder,
      inputTransform: StreamTransformation[BaseRow],
      inputTypeInfo: BaseRowTypeInfo,
      cepPattern: Pattern[BaseRow, BaseRow],
      comparator: EventComparator[BaseRow],
      patternNames: Seq[String]): StreamTransformation[BaseRow] = {
    val rowtimeFields = inputSchema.relDataType
      .getFieldList.filter(f => FlinkTypeFactory.isRowtimeIndicatorType(f.getType))
    val isProcessingTime = rowtimeFields.isEmpty

    val outputTypeInfo = FlinkTypeFactory.toInternalBaseRowTypeInfo(getRowType)

    // define the select function to the detected pattern sequence
    val rowsPerMatchLiteral =
      if (logicalMatch.rowsPerMatch == null) {
        RowsPerMatchOption.ONE_ROW
      } else {
        logicalMatch.rowsPerMatch.asInstanceOf[RexLiteral].getValue
      }

    rowsPerMatchLiteral match {
      case RowsPerMatchOption.ONE_ROW =>
        val patternSelectFunction =
          MatchUtil.generatePatternSelectFunction(
            config,
            relBuilder,
            outputSchema,
            patternNames,
            logicalMatch.partitionKeys,
            logicalMatch.measures,
            inputTypeInfo)

        generateSelectTransformation(
          inputTransform,
          cepPattern,
          comparator,
          patternSelectFunction,
          isProcessingTime,
          inputTypeInfo,
          outputTypeInfo)

      case RowsPerMatchOption.ALL_ROWS =>
        val patternFlatSelectFunction =
          MatchUtil.generatePatternFlatSelectFunction(
            config,
            relBuilder,
            outputSchema,
            patternNames,
            logicalMatch.partitionKeys,
            logicalMatch.orderKeys,
            logicalMatch.measures,
            inputTypeInfo)

        generateFlatSelectTransformation(
          inputTransform,
          cepPattern,
          comparator,
          patternFlatSelectFunction,
          isProcessingTime,
          inputTypeInfo,
          outputTypeInfo)

      case RowsPerMatchOption.ONE_ROW_WITH_TIMEOUT =>
        val patternSelectFunction =
          MatchUtil.generatePatternSelectFunction(
            config,
            relBuilder,
            outputSchema,
            patternNames,
            logicalMatch.partitionKeys,
            logicalMatch.measures,
            inputTypeInfo)

        val patternTimeoutFunction =
          MatchUtil.generatePatternTimeoutFunction(
            config,
            relBuilder,
            outputSchema,
            patternNames,
            logicalMatch.partitionKeys,
            logicalMatch.measures,
            inputTypeInfo)

        generateSelectTimeoutTransformation(
          inputTransform,
          cepPattern,
          comparator,
          patternSelectFunction,
          patternTimeoutFunction,
          isProcessingTime,
          inputTypeInfo,
          outputTypeInfo)

      case RowsPerMatchOption.ALL_ROWS_WITH_TIMEOUT =>
        val patternFlatSelectFunction =
          MatchUtil.generatePatternFlatSelectFunction(
            config,
            relBuilder,
            outputSchema,
            patternNames,
            logicalMatch.partitionKeys,
            logicalMatch.orderKeys,
            logicalMatch.measures,
            inputTypeInfo)

        val patternFlatTimeoutFunction =
          MatchUtil.generatePatternFlatTimeoutFunction(
            config,
            relBuilder,
            outputSchema,
            patternNames,
            logicalMatch.partitionKeys,
            logicalMatch.orderKeys,
            logicalMatch.measures,
            inputTypeInfo)

        generateFlatSelectTimeoutTransformation(
          inputTransform,
          cepPattern,
          comparator,
          patternFlatSelectFunction,
          patternFlatTimeoutFunction,
          isProcessingTime,
          inputTypeInfo,
          outputTypeInfo)

      case _ =>
        throw new TableException(s"Unsupported RowsPerMatchOption: $rowsPerMatchLiteral")
    }
  }

  private def setTransformationRes(streamTransformation: StreamTransformation[_]) {
    streamTransformation.setResources(getResource.getReservedResourceSpec,
      getResource.getPreferResourceSpec)
  }

  private def setKeySelector(
      transform: OneInputTransformation[BaseRow, _],
      inputTypeInfo: BaseRowTypeInfo): Unit = {
    val logicalKeys = logicalMatch.partitionKeys.map {
      case inputRef: RexInputRef => inputRef.getIndex
    }.toArray

    val selector = StreamExecUtil.getKeySelector(logicalKeys, inputTypeInfo)
    transform.setStateKeySelector(selector)
    transform.setStateKeyType(selector.getProducedType)

    if (logicalKeys.isEmpty) {
      transform.setParallelism(1)
      transform.setMaxParallelism(1)
    }
  }

  private def generateSelectTransformation(
      inputTransform: StreamTransformation[BaseRow],
      cepPattern: Pattern[BaseRow, BaseRow],
      comparator: EventComparator[BaseRow],
      patternSelectFunction: PatternSelectFunction[BaseRow, BaseRow],
      isProcessingTime: Boolean,
      inputTypeInfo: BaseRowTypeInfo,
      outputTypeInfo: BaseRowTypeInfo): StreamTransformation[BaseRow] = {
    val inputSerializer = inputTypeInfo.createSerializer(new ExecutionConfig)
    val nfaFactory = NFACompiler.compileFactory(cepPattern, true)
    val timeoutOutputTag = new OutputTag(UUID.randomUUID.toString, outputTypeInfo)

    val patternStreamTransform = new OneInputTransformation(
      inputTransform,
      "SelectCepOperator",
      new SelectCepOperator(
        inputSerializer,
        isProcessingTime,
        nfaFactory,
        comparator,
        cepPattern.getAfterMatchSkipStrategy,
        patternSelectFunction,
        timeoutOutputTag),
      outputTypeInfo,
      inputTransform.getParallelism)
    setTransformationRes(patternStreamTransform)

    patternStreamTransform.setChainingStrategy(ChainingStrategy.ALWAYS)
    setKeySelector(patternStreamTransform, inputTypeInfo)

    patternStreamTransform
  }

  private def generateFlatSelectTransformation(
      inputTransform: StreamTransformation[BaseRow],
      cepPattern: Pattern[BaseRow, BaseRow],
      comparator: EventComparator[BaseRow],
      patternFlatSelectFunction: PatternFlatSelectFunction[BaseRow, BaseRow],
      isProcessingTime: Boolean,
      inputTypeInfo: BaseRowTypeInfo,
      outputTypeInfo: BaseRowTypeInfo): StreamTransformation[BaseRow] = {
    val inputSerializer = inputTypeInfo.createSerializer(new ExecutionConfig)
    val nfaFactory = NFACompiler.compileFactory(cepPattern, true)
    val timeoutOutputTag = new OutputTag(UUID.randomUUID.toString, outputTypeInfo)

    val patternStreamTransform = new OneInputTransformation(
      inputTransform,
      "FlatSelectCepOperator",
      new FlatSelectCepOperator(
        inputSerializer,
        isProcessingTime,
        nfaFactory,
        comparator,
        cepPattern.getAfterMatchSkipStrategy,
        patternFlatSelectFunction,
        timeoutOutputTag),
      outputTypeInfo,
      inputTransform.getParallelism)
    setTransformationRes(patternStreamTransform)

    patternStreamTransform.setChainingStrategy(ChainingStrategy.ALWAYS)
    setKeySelector(patternStreamTransform, inputTypeInfo)

    patternStreamTransform
  }

  private def generateSelectTimeoutTransformation(
      inputTransform: StreamTransformation[BaseRow],
      cepPattern: Pattern[BaseRow, BaseRow],
      comparator: EventComparator[BaseRow],
      patternSelectFunction: PatternSelectFunction[BaseRow, BaseRow],
      patternTimeoutFunction: PatternTimeoutFunction[BaseRow, BaseRow],
      isProcessingTime: Boolean,
      inputTypeInfo: BaseRowTypeInfo,
      outputTypeInfo: BaseRowTypeInfo): StreamTransformation[BaseRow] = {
    val inputSerializer = inputTypeInfo.createSerializer(new ExecutionConfig)
    val nfaFactory = NFACompiler.compileFactory(cepPattern, true)
    val timeoutOutputTag = new OutputTag(UUID.randomUUID.toString, outputTypeInfo)
    val lateDataOutputTag = new OutputTag(UUID.randomUUID.toString, inputTypeInfo)

    val patternStreamTransform = new OneInputTransformation(
      inputTransform,
      "SelectTimeoutCepOperator",
      new SelectTimeoutCepOperator(
        inputSerializer,
        isProcessingTime,
        nfaFactory,
        comparator,
        cepPattern.getAfterMatchSkipStrategy,
        patternSelectFunction,
        patternTimeoutFunction,
        timeoutOutputTag,
        lateDataOutputTag),
      outputTypeInfo,
      inputTransform.getParallelism)
    setTransformationRes(patternStreamTransform)

    patternStreamTransform.setChainingStrategy(ChainingStrategy.ALWAYS)
    setKeySelector(patternStreamTransform, inputTypeInfo)

    val timeoutStreamTransform = new SideOutputTransformation(
      patternStreamTransform,
      timeoutOutputTag)
    setTransformationRes(timeoutStreamTransform)

    val transformation = new TwoInputTransformation(
      patternStreamTransform,
      timeoutStreamTransform,
      "CombineOutputCepOperator",
      new CoStreamMap(new CombineCepOutputCoMapFunction),
      outputTypeInfo,
      inputTransform.getParallelism)
    setTransformationRes(transformation)
    transformation
  }

  private def generateFlatSelectTimeoutTransformation(
      inputTransform: StreamTransformation[BaseRow],
      cepPattern: Pattern[BaseRow, BaseRow],
      comparator: EventComparator[BaseRow],
      patternFlatSelectFunction: PatternFlatSelectFunction[BaseRow, BaseRow],
      patternFlatTimeoutFunction: PatternFlatTimeoutFunction[BaseRow, BaseRow],
      isProcessingTime: Boolean,
      inputTypeInfo: BaseRowTypeInfo,
      outputTypeInfo: BaseRowTypeInfo): StreamTransformation[BaseRow] = {
    val inputSerializer = inputTypeInfo.createSerializer(new ExecutionConfig)
    val nfaFactory = NFACompiler.compileFactory(cepPattern, true)
    val timeoutOutputTag = new OutputTag(UUID.randomUUID.toString, outputTypeInfo)
    val lateDataOutputTag = new OutputTag(UUID.randomUUID.toString, inputTypeInfo)

    val patternStreamTransform = new OneInputTransformation(
      inputTransform,
      "FlatSelectTimeoutCepOperator",
      new FlatSelectTimeoutCepOperator(
        inputSerializer,
        isProcessingTime,
        nfaFactory,
        comparator,
        cepPattern.getAfterMatchSkipStrategy,
        patternFlatSelectFunction,
        patternFlatTimeoutFunction,
        timeoutOutputTag,
        lateDataOutputTag),
      outputTypeInfo,
      inputTransform.getParallelism)
    setTransformationRes(patternStreamTransform)

    patternStreamTransform.setChainingStrategy(ChainingStrategy.ALWAYS)
    setKeySelector(patternStreamTransform, inputTypeInfo)

    val timeoutStreamTransform = new SideOutputTransformation(
      patternStreamTransform,
      timeoutOutputTag)
    setTransformationRes(timeoutStreamTransform)

    val transformation = new TwoInputTransformation(
      patternStreamTransform,
      timeoutStreamTransform,
      "CombineOutputCepOperator",
      new CoStreamMap(new CombineCepOutputCoMapFunction),
      outputTypeInfo,
      inputTransform.getParallelism)
    setTransformationRes(transformation)
    transformation
  }
}

private class PatternVisitor(
    config: TableConfig,
    relBuilder: RelBuilder,
    inputTypeInfo: TypeInformation[BaseRow],
    logicalMatch: MatchRecognize)
  extends RexDefaultVisitor[Pattern[BaseRow, BaseRow]] {

  private var pattern: Pattern[BaseRow, BaseRow] = _
  val names = new collection.mutable.LinkedHashSet[String]()
  private var strictContiguity: Boolean = _

  override def visitLiteral(literal: RexLiteral): Pattern[BaseRow, BaseRow] = {
    val patternName = literal.getValueAs(classOf[String])
    pattern = translateSingleVariable(Option.apply(pattern), patternName)

    val patternDefinition = logicalMatch.patternDefinitions.get(patternName)
    if (patternDefinition != null) {
      val condition = MatchUtil.generateIterativeCondition(
        config,
        relBuilder,
        patternName,
        names.toSeq,
        patternDefinition,
        inputTypeInfo)

      pattern.where(condition)
    } else {
      pattern.where(BooleanConditions.trueFunction())
    }
  }

  override def visitCall(call: RexCall): Pattern[BaseRow, BaseRow] = {
    call.getOperator match {
      case PATTERN_CONCAT =>
        val left = call.operands.get(0)
        val right = call.operands.get(1)

        pattern = left.accept(this)
        strictContiguity = true
        pattern = right.accept(this)
        pattern

      case PATTERN_FOLLOWED_BY =>
        val left = call.operands.get(0)
        val right = call.operands.get(1)

        pattern = left.accept(this)
        strictContiguity = false
        pattern = right.accept(this)
        pattern

      case PATTERN_QUANTIFIER =>
        val name = call.operands.get(0) match {
          case c: RexLiteral => c
          case x => throw new TableException(s"Expression not supported: $x Group patterns are " +
            s"not supported yet.")
        }
        pattern = name.accept(this)
        val startNum = MathUtils.checkedDownCast(call.operands.get(1).asInstanceOf[RexLiteral]
          .getValueAs(classOf[JLong]))
        val endNum = MathUtils.checkedDownCast(call.operands.get(2).asInstanceOf[RexLiteral]
          .getValueAs(classOf[JLong]))
        val isGreedy = !call.operands.get(3).asInstanceOf[RexLiteral]
          .getValueAs(classOf[JBoolean])

        applyQuantifier(pattern, startNum, endNum, isGreedy)

      case PATTERN_ALTER =>
        throw new TableException(
          s"Expression not supported: $call. Currently, CEP doesn't support branching patterns.")

      case PATTERN_PERMUTE =>
        throw new TableException(
          s"Expression not supported: $call. Currently, CEP doesn't support PERMUTE patterns.")

      case PATTERN_EXCLUDE =>
        throw new TableException(
          s"Expression not supported: $call. Currently, CEP doesn't support '{-' '-}' patterns.")
    }
  }

  override def visitNode(rexNode: RexNode): Pattern[BaseRow, BaseRow] = throw new TableException(
    s"Unsupported expression within Pattern: [$rexNode]")

  private def translateSkipStrategy = {
    val getPatternTarget = () => logicalMatch.after.asInstanceOf[RexCall].getOperands.get(0)
      .asInstanceOf[RexLiteral].getValueAs(classOf[String])

    logicalMatch.after.getKind match {
      case SqlKind.LITERAL =>
        logicalMatch.after.asInstanceOf[RexLiteral].getValueAs(classOf[AfterOption]) match {
          case AfterOption.SKIP_PAST_LAST_ROW => AfterMatchSkipStrategy.skipPastLastEvent()
          case AfterOption.SKIP_TO_NEXT_ROW => AfterMatchSkipStrategy.skipToNext()
        }
      case SqlKind.SKIP_TO_FIRST =>
        AfterMatchSkipStrategy.skipToFirst(getPatternTarget()).throwExceptionOnMiss()
      case SqlKind.SKIP_TO_LAST =>
        AfterMatchSkipStrategy.skipToLast(getPatternTarget()).throwExceptionOnMiss()
      case _ => throw new IllegalStateException(s"Corrupted query tree. Unexpected " +
        s"${logicalMatch.after} for after match strategy.")
    }
  }

  private def translateSingleVariable(
      previousPattern: Option[Pattern[BaseRow, BaseRow]],
      patternName: String)
  : Pattern[BaseRow, BaseRow] = {
    if (names.contains(patternName)) {
      throw new TableException("Pattern variables must be unique. That might change in the future.")
    } else {
      names.add(patternName)
    }

    previousPattern match {
      case Some(p) =>
        if (strictContiguity) {
          p.next(patternName)
        } else {
          p.followedBy(patternName)
        }
      case None =>
        Pattern.begin(patternName, translateSkipStrategy)
    }
  }

  private def applyQuantifier(
      pattern: Pattern[BaseRow, BaseRow],
      startNum: Int,
      endNum: Int,
      greedy: Boolean)
  : Pattern[BaseRow, BaseRow] = {
    val isOptional = startNum == 0 && endNum == 1

    val newPattern = if (startNum == 0 && endNum == -1) { // zero or more
      pattern.oneOrMore().optional().consecutive()
    } else if (startNum == 1 && endNum == -1) { // one or more
      pattern.oneOrMore().consecutive()
    } else if (isOptional) { // optional
      pattern.optional()
    } else if (endNum != -1) { // times
      pattern.times(startNum, endNum).consecutive()
    } else { // times or more
      pattern.timesOrMore(startNum).consecutive()
    }

    if (greedy && isOptional) {
      newPattern
    } else if (greedy) {
      newPattern.greedy()
    } else if (isOptional) {
      throw new TableException("Reluctant optional variables are not supported yet.")
    } else {
      newPattern
    }
  }
}
