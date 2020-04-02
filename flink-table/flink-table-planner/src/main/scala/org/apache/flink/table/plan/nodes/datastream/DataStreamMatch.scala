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

import java.lang.{Boolean => JBoolean, Long => JLong}
import java.util
import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.RelFieldCollation.Direction
import org.apache.calcite.rel._
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rex._
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.sql.SqlMatchRecognize.AfterOption
import org.apache.calcite.sql.`type`.SqlTypeFamily
import org.apache.calcite.sql.fun.SqlStdOperatorTable._
import org.apache.flink.annotation.VisibleForTesting
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.runtime.RowComparator
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy
import org.apache.flink.cep.nfa.compiler.NFACompiler
import org.apache.flink.cep.pattern.Pattern
import org.apache.flink.cep.pattern.Quantifier.QuantifierProperty
import org.apache.flink.cep.pattern.conditions.BooleanConditions
import org.apache.flink.cep.{CEP, PatternStream}
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.windowing.time.Time

import scala.collection.JavaConverters._
import scala.collection.JavaConversions._
import org.apache.flink.table.api._
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.codegen.MatchCodeGenerator
import org.apache.flink.table.plan.logical.MatchRecognize
import org.apache.flink.table.plan.nodes.CommonMatchRecognize
import org.apache.flink.table.plan.rules.datastream.DataStreamRetractionRules
import org.apache.flink.table.plan.schema.RowSchema
import org.apache.flink.table.plan.util.PythonUtil.containsPythonCall
import org.apache.flink.table.plan.util.RexDefaultVisitor
import org.apache.flink.table.planner.StreamPlanner
import org.apache.flink.table.runtime.`match`._
import org.apache.flink.table.runtime.aggregate.SortUtil
import org.apache.flink.table.runtime.conversion.CRowToRowMapFunction
import org.apache.flink.table.runtime.types.{CRow, CRowTypeInfo}
import org.apache.flink.table.runtime.{RowKeySelector, RowtimeProcessFunction}
import org.apache.flink.types.Row
import org.apache.flink.util.MathUtils

import org.apache.calcite.util.ImmutableBitSet

/**
  * Flink RelNode which matches along with LogicalMatch.
  */
class DataStreamMatch(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputNode: RelNode,
    logicalMatch: MatchRecognize,
    schema: RowSchema,
    inputSchema: RowSchema)
  extends SingleRel(cluster, traitSet, inputNode)
  with CommonMatchRecognize
  with DataStreamRel {

  if (logicalMatch.measures.values().exists(containsPythonCall(_)) ||
    logicalMatch.patternDefinitions.values().exists(containsPythonCall(_))) {
    throw new TableException("Python Function can not be used in MATCH_RECOGNIZE for now.")
  }

  override def needsUpdatesAsRetraction = true

  override def consumesRetractions = true

  override def deriveRowType(): RelDataType = schema.relDataType

  override def copy(traitSet: RelTraitSet, inputs: util.List[RelNode]): RelNode = {
    new DataStreamMatch(
      cluster,
      traitSet,
      inputs.get(0),
      logicalMatch,
      schema,
      inputSchema)
  }

  override def toString: String = {
    matchToString(logicalMatch, inputSchema.fieldNames, getExpressionString)
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    explainMatch(super.explainTerms(pw), logicalMatch, inputSchema.fieldNames, getExpressionString)
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
    inputTypeInfo: TypeInformation[Row]
  ): (Pattern[Row, Row], Iterable[String]) = {
    val patternVisitor = new PatternVisitor(config, inputTypeInfo, logicalMatch)
    val cepPattern = if (logicalMatch.interval != null) {
      val interval = translateTimeBound(logicalMatch.interval)
      logicalMatch.pattern.accept(patternVisitor).within(interval)
    } else {
      logicalMatch.pattern.accept(patternVisitor)
    }
    (cepPattern, patternVisitor.names)
  }

  override def translateToPlan(
      planner: StreamPlanner,
      queryConfig: StreamQueryConfig)
    : DataStream[CRow] = {

    val inputIsAccRetract = DataStreamRetractionRules.isAccRetract(getInput)

    val config = planner.getConfig
    val inputTypeInfo = inputSchema.typeInfo

    val crowInput: DataStream[CRow] = getInput
      .asInstanceOf[DataStreamRel]
      .translateToPlan(planner, queryConfig)

    if (inputIsAccRetract) {
      throw new TableException(
        "Retraction on match recognize is not supported. " +
          "Note: Match recognize should not follow a non-windowed GroupBy aggregation.")
    }

    val (timestampedInput, rowComparator) = translateOrder(planner,
      crowInput,
      logicalMatch.orderKeys)

    val (cepPattern, patternNames) = translatePattern(config, inputTypeInfo)

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

    val inputDS: DataStream[Row] = timestampedInput
      .map(new CRowToRowMapFunction)
      .setParallelism(timestampedInput.getParallelism)
      .name("ConvertToRow")
      .returns(inputTypeInfo)

    val partitionKeys = logicalMatch.partitionKeys
    val partitionedStream = applyPartitioning(partitionKeys, inputDS)

    val patternStream: PatternStream[Row] = if (rowComparator.isDefined) {
      CEP.pattern[Row](partitionedStream, cepPattern, new EventRowComparator(rowComparator.get))
    } else {
      CEP.pattern[Row](partitionedStream, cepPattern)
    }

    val measures = logicalMatch.measures
    val outTypeInfo = CRowTypeInfo(schema.typeInfo)
    if (logicalMatch.allRows) {
      throw new TableException("All rows per match mode is not supported yet.")
    } else {
      val generator = new MatchCodeGenerator(config, inputTypeInfo, patternNames.toSeq)
      val patternSelectFunction = generator.generateOneRowPerMatchExpression(
          schema,
          partitionKeys,
          measures)
      patternStream.process[CRow](patternSelectFunction, outTypeInfo)
    }
  }

  private def translateOrder(
      planner: StreamPlanner,
      crowInput: DataStream[CRow],
      orderKeys: RelCollation)
    : (DataStream[CRow], Option[RowComparator]) = {

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
      Some(SortUtil
        .createRowComparator(inputSchema.relDataType,
          orderKeys.getFieldCollations.asScala.tail,
          planner.getExecutionEnvironment.getConfig))
    } else {
      None
    }

    timeOrderField.getType match {
      case _ if FlinkTypeFactory.isRowtimeIndicatorType(timeOrderField.getType) =>
        (crowInput.process(
          new RowtimeProcessFunction(timeOrderField.getIndex, CRowTypeInfo(inputSchema.typeInfo))
        ).setParallelism(crowInput.getParallelism),
          rowComparator)
      case _ =>
        (crowInput, rowComparator)
    }
  }

  private def applyPartitioning(partitionKeys: ImmutableBitSet, inputDs: DataStream[Row])
    : DataStream[Row] = {
    if (partitionKeys.size() > 0) {
      val keys = partitionKeys.toArray
      val keySelector = new RowKeySelector(keys, inputSchema.projectedTypeInfo(keys))
      inputDs.keyBy(keySelector)
    } else {
      inputDs
    }
  }
}

private class PatternVisitor(
    config: TableConfig,
    inputTypeInfo: TypeInformation[Row],
    logicalMatch: MatchRecognize)
  extends RexDefaultVisitor[Pattern[Row, Row]] {

  private var pattern: Pattern[Row, Row] = _
  val names = new collection.mutable.LinkedHashSet[String]()

  override def visitLiteral(literal: RexLiteral): Pattern[Row, Row] = {
    val patternName = literal.getValueAs(classOf[String])
    pattern = translateSingleVariable(Option.apply(pattern), patternName)

    val patternDefinition = logicalMatch.patternDefinitions.get(patternName)
    if (patternDefinition != null) {
      val generator = new MatchCodeGenerator(config, inputTypeInfo, names.toSeq, Some(patternName))
      val condition = generator.generateIterativeCondition(patternDefinition)

      pattern.where(condition)
    } else {
      pattern.where(BooleanConditions.trueFunction())
    }
  }

  override def visitCall(call: RexCall): Pattern[Row, Row] = {
    call.getOperator match {
      case PATTERN_CONCAT =>
        val left = call.operands.get(0)
        val right = call.operands.get(1)

        pattern = left.accept(this)
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

  override def visitNode(rexNode: RexNode): Pattern[Row, Row] = throw new TableException(
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
      previousPattern: Option[Pattern[Row, Row]],
      patternName: String)
    : Pattern[Row, Row] = {
    if (names.contains(patternName)) {
      throw new TableException("Pattern variables must be unique. That might change in the future.")
    } else {
      names.add(patternName)
    }

    previousPattern match {
      case Some(p) => p.next(patternName)
      case None =>
        Pattern.begin(patternName, translateSkipStrategy)
    }
  }

  private def applyQuantifier(
      pattern: Pattern[Row, Row],
      startNum: Int,
      endNum: Int,
      greedy: Boolean)
    : Pattern[Row, Row] = {
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

    if (greedy && (isOptional || startNum == endNum)) {
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
