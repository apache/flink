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

package org.apache.flink.table.planner.calcite

import org.apache.flink.table.api.{TableException, ValidationException}
import org.apache.flink.table.planner.calcite.FlinkTypeFactory.{isTimeIndicatorType, _}
import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable
import org.apache.flink.table.planner.plan.metadata.FlinkRelMetadataQuery
import org.apache.flink.table.planner.plan.nodes.calcite._
import org.apache.flink.table.planner.plan.schema.TimeIndicatorRelDataType
import org.apache.flink.table.planner.plan.utils.TemporalJoinUtil
import org.apache.flink.table.planner.plan.utils.WindowUtil.groupingContainsWindowStartEnd
import org.apache.flink.table.types.logical.{LocalZonedTimestampType, TimestampKind, TimestampType}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core._
import org.apache.calcite.rel.hint.RelHint
import org.apache.calcite.rel.logical._
import org.apache.calcite.rel.{RelNode, RelShuttle}
import org.apache.calcite.rex._
import org.apache.calcite.sql.`type`.SqlTypeName
import org.apache.calcite.sql.fun.SqlStdOperatorTable
import org.apache.calcite.sql.fun.SqlStdOperatorTable.FINAL
import org.apache.flink.table.planner.plan.nodes.hive.LogicalDistribution

import java.util.{Collections => JCollections}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable

/**
  * Traverses a [[RelNode]] tree and converts fields with [[TimeIndicatorRelDataType]] type. If a
  * time attribute is accessed for a calculation, it will be materialized. Forwarding is allowed in
  * some cases, but not all.
  */
class RelTimeIndicatorConverter(rexBuilder: RexBuilder) extends RelShuttle {

  val materializerUtils = new RexTimeIndicatorMaterializerUtils(rexBuilder)

  override def visit(intersect: LogicalIntersect): RelNode = visitSetOp(intersect)

  override def visit(union: LogicalUnion): RelNode = visitSetOp(union)

  override def visit(aggregate: LogicalAggregate): RelNode = convertAggregate(aggregate)

  override def visit(minus: LogicalMinus): RelNode = visitSetOp(minus)

  override def visit(sort: LogicalSort): RelNode = {
    val input = sort.getInput.accept(this)
    LogicalSort.create(input, sort.collation, sort.offset, sort.fetch)
  }

  override def visit(matchRel: LogicalMatch): RelNode = {
    // visit children and update inputs
    val input = matchRel.getInput.accept(this)
    val rowType = matchRel.getInput.getRowType

    val materializer = new RexTimeIndicatorMaterializer(
      rexBuilder,
      rowType.getFieldList.map(_.getType))

    // update input expressions
    val patternDefs = matchRel.getPatternDefinitions.mapValues(_.accept(materializer))
    val measures = matchRel.getMeasures
      .mapValues(_.accept(materializer))

    val interval = if (matchRel.getInterval != null) {
      matchRel.getInterval.accept(materializer)
    } else {
      null
    }

    val isNoLongerTimeIndicator : String => Boolean = fieldName =>
      measures.get(fieldName).exists(r => !FlinkTypeFactory.isTimeIndicatorType(r.getType))

    // decide the MATCH_ROWTIME() return type is TIMESTAMP or TIMESTAMP_LTZ, if it is TIMESTAMP_LTZ,
    // we need to materialize the output type of LogicalMatch node to TIMESTAMP_LTZ too.
    val isLtzRowtimeIndicator = measures.exists {
      case (name, node) => FlinkTypeFactory.isTimestampLtzIndicatorType(node.getType) &&
        FlinkTypeFactory.isRowtimeIndicatorType(node.getType)
    }

    // materialize all output types
    val outputType = materializerUtils.getRowTypeWithoutIndicators(
      matchRel.getRowType,
      isLtzRowtimeIndicator,
      isNoLongerTimeIndicator)

    LogicalMatch.create(
      input,
      outputType,
      matchRel.getPattern,
      matchRel.isStrictStart,
      matchRel.isStrictEnd,
      patternDefs,
      measures,
      matchRel.getAfter,
      matchRel.getSubsets.asInstanceOf[java.util.Map[String, java.util.TreeSet[String]]],
      matchRel.isAllRows,
      matchRel.getPartitionKeys,
      matchRel.getOrderKeys,
      interval)
  }

  override def visit(other: RelNode): RelNode = other match {
    case collect: Collect =>
      collect

    case uncollect: Uncollect =>
      // visit children and update inputs
      val input = uncollect.getInput.accept(this)
      Uncollect.create(uncollect.getTraitSet, input, uncollect.withOrdinality,
        JCollections.emptyList())

    case scan: LogicalTableFunctionScan =>
      val inputs = scan.getInputs.map(_.accept(this))
      scan.copy(scan.getTraitSet, inputs)

    case aggregate: LogicalWindowAggregate =>
      val convAggregate = convertAggregate(aggregate)

      LogicalWindowAggregate.create(
        aggregate.getWindow,
        aggregate.getNamedProperties,
        convAggregate)

    case windowTableAggregate: LogicalWindowTableAggregate =>
      val correspondingAggregate = new LogicalWindowAggregate(
        windowTableAggregate.getCluster,
        windowTableAggregate.getTraitSet,
        windowTableAggregate.getInput,
        windowTableAggregate.getGroupSet,
        windowTableAggregate.getAggCallList,
        windowTableAggregate.getWindow,
        windowTableAggregate.getNamedProperties)
      val convAggregate = convertAggregate(correspondingAggregate)
      LogicalWindowTableAggregate.create(
        windowTableAggregate.getWindow,
        windowTableAggregate.getNamedProperties,
        convAggregate)

    case tableAggregate: LogicalTableAggregate =>
      val correspondingAggregate = LogicalAggregate.create(
        tableAggregate.getInput,
        tableAggregate.getGroupSet,
        tableAggregate.getGroupSets,
        tableAggregate.getAggCallList)
      val convAggregate = convertAggregate(correspondingAggregate)
      LogicalTableAggregate.create(convAggregate)

    case watermarkAssigner: LogicalWatermarkAssigner =>
      val input = watermarkAssigner.getInput.accept(this)
      watermarkAssigner.copy(
        watermarkAssigner.getTraitSet,
        input,
        watermarkAssigner.rowtimeFieldIndex,
        watermarkAssigner.watermarkExpr)

    case snapshot: LogicalSnapshot =>
      val input = snapshot.getInput.accept(this)
      snapshot.copy(snapshot.getTraitSet, input, snapshot.getPeriod)

    case rank: LogicalRank =>
      val input = rank.getInput.accept(this)
      rank.copy(rank.getTraitSet, JCollections.singletonList(input))

    case sink: LogicalSink =>
      val newInput = convertSinkInput(sink.getInput)
      new LogicalSink(
        sink.getCluster,
        sink.getTraitSet,
        newInput,
        sink.tableIdentifier,
        sink.catalogTable,
        sink.tableSink,
        sink.staticPartitions,
        sink.abilitySpecs)

    case sink: LogicalLegacySink =>
      val newInput = convertSinkInput(sink.getInput)
      new LogicalLegacySink(
        sink.getCluster,
        sink.getTraitSet,
        newInput,
        sink.sink,
        sink.sinkName,
        sink.catalogTable,
        sink.staticPartitions)

    case distribution: LogicalDistribution =>
      val newInput = distribution.getInput.accept(this)
      distribution.copy(distribution.getTraitSet, JCollections.singletonList(newInput))

    case _ =>
      throw new TableException(s"Unsupported logical operator: ${other.getClass.getSimpleName}")
  }

  override def visit(exchange: LogicalExchange): RelNode =
    throw new TableException("Logical exchange in a stream environment is not supported yet.")

  override def visit(scan: TableScan): RelNode = scan

  override def visit(scan: TableFunctionScan): RelNode =
    throw new TableException("Table function scan in a stream environment is not supported yet.")

  override def visit(values: LogicalValues): RelNode = values

  override def visit(filter: LogicalFilter): RelNode = {
    // visit children and update inputs
    val input = filter.getInput.accept(this)

    // check if input field contains time indicator type
    // materialize field if no time indicator is present anymore
    // if input field is already materialized, change to timestamp type
    val materializer = new RexTimeIndicatorMaterializer(
      rexBuilder,
      input.getRowType.getFieldList.map(_.getType))

    // materialize condition due to filter will validate condition type
    val newCondition = filter.getCondition.accept(materializer)
    LogicalFilter.create(input, newCondition)
  }

  override def visit(project: LogicalProject): RelNode = {
    // visit children and update inputs
    val input = project.getInput.accept(this)

    // check if input field contains time indicator type
    // materialize field if no time indicator is present anymore
    // if input field is already materialized, change to timestamp type
    val materializer = new RexTimeIndicatorMaterializer(
      rexBuilder,
      input.getRowType.getFieldList.map(_.getType))

    val projects = project.getProjects.map(_.accept(materializer))
    val fieldNames = project.getRowType.getFieldNames
    LogicalProject.create(input, JCollections.emptyList[RelHint](), projects, fieldNames)
  }

  override def visit(join: LogicalJoin): RelNode = {
    val left = join.getLeft.accept(this)
    val right = join.getRight.accept(this)

    // temporal table join
    if (TemporalJoinUtil.containsTemporalJoinCondition(join.getCondition)) {
      val rewrittenTemporalJoin = join.copy(join.getTraitSet, List(left, right))

      // Materialize all of the time attributes from the right side of temporal join
      val indicesToMaterialize = (left.getRowType.getFieldCount until
        rewrittenTemporalJoin.getRowType.getFieldCount).toSet

      materializerUtils.projectAndMaterializeFields(rewrittenTemporalJoin, indicesToMaterialize)
    } else {
      val newCondition = join.getCondition.accept(new RexShuttle {
        private val leftFieldCount = left.getRowType.getFieldCount
        private val leftFields = left.getRowType.getFieldList.toList
        private val leftRightFields =
          (left.getRowType.getFieldList ++ right.getRowType.getFieldList).toList

        override def visitInputRef(inputRef: RexInputRef): RexNode = {
          if (isTimeIndicatorType(inputRef.getType)) {
            val fields = if (inputRef.getIndex < leftFieldCount) {
              leftFields
            } else {
              leftRightFields
            }
            RexInputRef.of(inputRef.getIndex, fields)
          } else {
            super.visitInputRef(inputRef)
          }
        }
      })

      LogicalJoin.create(left, right, JCollections.emptyList(),
        newCondition, join.getVariablesSet, join.getJoinType)
    }
  }

  override def visit(correlate: LogicalCorrelate): RelNode = {
    // visit children and update inputs
    val inputs = correlate.getInputs.map(_.accept(this))

    val right = inputs(1) match {
      case scan: LogicalTableFunctionScan =>
        // visit children and update inputs
        val scanInputs = scan.getInputs.map(_.accept(this))

        // check if input field contains time indicator type
        // materialize field if no time indicator is present anymore
        // if input field is already materialized, change to timestamp type
        val materializer = new RexTimeIndicatorMaterializer(
          rexBuilder,
          inputs.head.getRowType.getFieldList.map(_.getType))

        val call = scan.getCall.accept(materializer)
        LogicalTableFunctionScan.create(
          scan.getCluster,
          scanInputs,
          call,
          scan.getElementType,
          scan.getRowType,
          scan.getColumnMappings)

      case _ =>
        inputs(1)
    }

    LogicalCorrelate.create(
      inputs.head,
      right,
      correlate.getCorrelationId,
      correlate.getRequiredColumns,
      correlate.getJoinType)
  }

  def visitSetOp(setOp: SetOp): RelNode = {
    // visit children and update inputs
    val inputs = setOp.getInputs.map(_.accept(this))

    // make sure that time indicator types match
    val inputTypes = inputs.map(_.getRowType)

    val head = inputTypes.head.getFieldList.map(_.getType)

    inputTypes.foreach { t =>
      val fieldTypes = t.getFieldList.map(_.getType)

      fieldTypes.zip(head).foreach{ case (l, r) =>
        validateUnionPair(l, r)
      }
    }

    setOp.copy(setOp.getTraitSet, inputs, setOp.all)
  }

  private def validateUnionPair(l: RelDataType, r: RelDataType): Unit = {
    val exceptionMsg =
      s"Union fields with time attributes requires same types, but the types are %s and %s."
    // check if time indicators match
    val isValid = if (isTimeIndicatorType(l) && isTimeIndicatorType(r)) {
      val leftIsEventTime = l.asInstanceOf[TimeIndicatorRelDataType].isEventTime
      val rightIsEventTime = r.asInstanceOf[TimeIndicatorRelDataType].isEventTime
      if (leftIsEventTime && rightIsEventTime) {
        //rowtime must have same type
        isTimestampLtzIndicatorType(l) == isTimestampLtzIndicatorType(r)
      } else {
        leftIsEventTime == rightIsEventTime
      }
    }
    // one side is not an indicator
    else if (isTimeIndicatorType(l) || isTimeIndicatorType(r)) {
      false
    }
    // uninteresting types
    else {
      true
    }

    if (!isValid) {
      throw new ValidationException(
        String.format(exceptionMsg, l.toString, r.toString))
    }
  }

  private def gatherIndicesToMaterialize(aggregate: Aggregate, input: RelNode): Set[Int] = {
    val fmq = FlinkRelMetadataQuery.reuseOrCreate(aggregate.getCluster.getMetadataQuery)
    val windowProps = fmq.getRelWindowProperties(input)
    val indicesToMaterialize = mutable.Set[Int]()

    // check arguments of agg calls
    aggregate.getAggCallList.foreach { call =>
      call.getArgList.map(_.asInstanceOf[Int]).foreach(indicesToMaterialize.add)
    }

    // check grouping sets
    aggregate.getGroupSets.foreach { grouping =>
      if (windowProps != null && groupingContainsWindowStartEnd(grouping, windowProps)) {
        // for window aggregate we should reserve the time attribute of window_time column
        grouping
          .except(windowProps.getWindowTimeColumns).toArray
          .foreach(indicesToMaterialize.add)
      } else {
        grouping.toArray
          .foreach(indicesToMaterialize.add)
      }
    }

    indicesToMaterialize.toSet
  }

  private def convertSinkInput(sinkInput: RelNode): RelNode = {
    val newInput = sinkInput.accept(this)
    var needsConversion = false

    val projects = newInput.getRowType.getFieldList.map { field =>
      if (isProctimeIndicatorType(field.getType)) {
        needsConversion = true
        rexBuilder.makeCall(FlinkSqlOperatorTable.PROCTIME_MATERIALIZE,
          new RexInputRef(field.getIndex, field.getType))
      } else {
        new RexInputRef(field.getIndex, field.getType)
      }
    }

    // add final conversion if necessary
    if (needsConversion) {
      LogicalProject.create(newInput, JCollections.emptyList[RelHint](),
        projects, newInput.getRowType.getFieldNames)
    } else {
      newInput
    }
  }

  private def convertAggregate(aggregate: Aggregate): LogicalAggregate = {
    // visit children and update inputs
    val input = aggregate.getInput.accept(this)

    // add a project to materialize aggregation arguments/grouping keys

    val refIndices = gatherIndicesToMaterialize(aggregate, input)

    val needsMaterialization = refIndices.exists(idx =>
      isTimeIndicatorType(input.getRowType.getFieldList.get(idx).getType))

    // create project if necessary
    val projectedInput = if (needsMaterialization) {

      // insert or merge with input project if
      // a time attribute is accessed and needs to be materialized
      input match {

        // merge
        case lp: LogicalProject =>
          val projects = lp.getProjects.zipWithIndex.map { case (expr, idx) =>
            if (isTimeIndicatorType(expr.getType) && refIndices.contains(idx)) {
              if (isRowtimeIndicatorType(expr.getType)) {
                // cast rowtime indicator to regular timestamp
                rexBuilder.makeAbstractCast(
                  RelTimeIndicatorConverter.timestamp(
                    expr.getType.isNullable,
                    isTimestampLtzIndicatorType(expr.getType),
                    rexBuilder),
                  expr)
              } else {
                // generate proctime access
                rexBuilder.makeCall(FlinkSqlOperatorTable.PROCTIME_MATERIALIZE, expr)
              }
            } else {
              expr
            }
          }

          LogicalProject.create(
            lp.getInput,
            JCollections.emptyList[RelHint](),
            projects,
            input.getRowType.getFieldNames)

        // new project
        case _ =>
          val projects = input.getRowType.getFieldList.map { field =>
            if (isTimeIndicatorType(field.getType) && refIndices.contains(field.getIndex)) {
              if (isRowtimeIndicatorType(field.getType)) {
                // cast rowtime indicator to regular timestamp
                rexBuilder.makeAbstractCast(
                  RelTimeIndicatorConverter.timestamp(
                    field.getType.isNullable,
                    isTimestampLtzIndicatorType(field.getType),
                    rexBuilder),
                  new RexInputRef(field.getIndex, field.getType))
              } else {
                // generate proctime access
                rexBuilder.makeCall(
                  FlinkSqlOperatorTable.PROCTIME_MATERIALIZE,
                  new RexInputRef(field.getIndex, field.getType))
              }
            } else {
              new RexInputRef(field.getIndex, field.getType)
            }
          }

          LogicalProject.create(
            input,
            JCollections.emptyList[RelHint](),
            projects,
            input.getRowType.getFieldNames)
      }
    } else {
      // no project necessary
      input
    }

    // remove time indicator type as agg call return type
    val updatedAggCalls = aggregate.getAggCallList.map { call =>
      val callType = if (isTimeIndicatorType(call.getType)) {
        RelTimeIndicatorConverter.timestamp(
          call.getType.isNullable,
          isTimestampLtzIndicatorType(call.getType),
          rexBuilder)
      } else {
        call.getType
      }
      AggregateCall.create(
        call.getAggregation,
        call.isDistinct,
        call.getArgList,
        call.filterArg,
        callType,
        call.name)
    }

    LogicalAggregate.create(
      projectedInput,
      aggregate.indicator,
      aggregate.getGroupSet,
      aggregate.getGroupSets,
      updatedAggCalls)
  }

  override def visit(modify: LogicalTableModify): RelNode = {
    val input = modify.getInput.accept(this)
    modify.copy(modify.getTraitSet, JCollections.singletonList(input))
  }

  override def visit(calc: LogicalCalc): RelNode = {
    calc // Do nothing for Calc now.
  }
}

object RelTimeIndicatorConverter {

  def convert(
      rootRel: RelNode,
      rexBuilder: RexBuilder,
      needFinalTimeIndicatorConversion: Boolean): RelNode = {
    val converter = new RelTimeIndicatorConverter(rexBuilder)
    val convertedRoot = rootRel.accept(converter)

    // the LogicalSink is converted in RelTimeIndicatorConverter before
    if (rootRel.isInstanceOf[LogicalLegacySink] || !needFinalTimeIndicatorConversion) {
      return convertedRoot
    }
    var needsConversion = false

    // materialize remaining proctime indicators
    val projects = convertedRoot.getRowType.getFieldList.map(field =>
      if (isProctimeIndicatorType(field.getType)) {
        needsConversion = true
        rexBuilder.makeCall(
          FlinkSqlOperatorTable.PROCTIME_MATERIALIZE,
          new RexInputRef(field.getIndex, field.getType))
      } else {
        new RexInputRef(field.getIndex, field.getType)
      }
    )

    // add final conversion if necessary
    if (needsConversion) {
      LogicalProject.create(
        convertedRoot,
        JCollections.emptyList[RelHint](),
        projects,
        convertedRoot.getRowType.getFieldNames)
    } else {
      convertedRoot
    }
  }

  /**
    * Materializes time indicator accesses in an expression.
    *
    * @param expr The expression in which time indicators are materialized.
    * @param rowType The input schema of the expression.
    * @param rexBuilder A RexBuilder.
    * @return The expression with materialized time indicators.
    */
  def convertExpression(expr: RexNode, rowType: RelDataType, rexBuilder: RexBuilder): RexNode = {
    val materializer = new RexTimeIndicatorMaterializer(
      rexBuilder,
      rowType.getFieldList.map(_.getType))

    expr.accept(materializer)
  }

  /**
    * Checks if the given call is a materialization call for either proctime or rowtime.
    */
  def isMaterializationCall(call: RexCall): Boolean = {
    val isProctimeCall: Boolean = {
      call.getOperator == FlinkSqlOperatorTable.PROCTIME_MATERIALIZE &&
        call.getOperands.size() == 1 &&
        isProctimeIndicatorType(call.getOperands.get(0).getType)
    }

    val isRowtimeCall: Boolean = {
      call.getOperator == SqlStdOperatorTable.CAST &&
        call.getOperands.size() == 1 &&
        isRowtimeIndicatorType(call.getOperands.get(0).getType) &&
        call.getType.getSqlTypeName == SqlTypeName.TIMESTAMP
    }

    isProctimeCall || isRowtimeCall
  }

  def timestamp(
      isNullable: Boolean,
      isTimestampLtz: Boolean,
      rexBuilder: RexBuilder): RelDataType = {
    rexBuilder
      .getTypeFactory
      .asInstanceOf[FlinkTypeFactory]
      .createFieldTypeFromLogicalType(
        if (isTimestampLtz) {
          new LocalZonedTimestampType(isNullable, 3)
        } else {
          new TimestampType(isNullable, 3)
        })
  }

}

/**
  * Takes `newResolvedInput` types of the [[RexNode]] and if those types have changed rewrites
  * the [[RexNode]] to make it consistent with new type.
  */
class RexTimeIndicatorMaterializer(
    private val rexBuilder: RexBuilder,
    private val input: Seq[RelDataType])
  extends RexShuttle {

  private def rowtime(isNullable: Boolean, isTimestampLtz: Boolean = false): RelDataType = {
    rexBuilder.getTypeFactory.asInstanceOf[FlinkTypeFactory]
      .createRowtimeIndicatorType(isNullable, isTimestampLtz)
  }

  override def visitInputRef(inputRef: RexInputRef): RexNode = {
    // reference is interesting
    if (isTimeIndicatorType(inputRef.getType)) {
      val resolvedRefType = input(inputRef.getIndex)
      // input is a valid time indicator
      if (isTimeIndicatorType(resolvedRefType)) {
        rexBuilder.makeInputRef(resolvedRefType, inputRef.getIndex)
      }
      // input has been materialized
      else {
        new RexInputRef(inputRef.getIndex, resolvedRefType)
      }
    }
    // reference is a regular field
    else {
      super.visitInputRef(inputRef)
    }
  }

  private def isMatchTimeIndicator(call: RexNode): Boolean = {
    call match {
      case operand: RexCall if
      operand.getOperator == FlinkSqlOperatorTable.MATCH_PROCTIME ||
        operand.getOperator == FlinkSqlOperatorTable.MATCH_ROWTIME =>
        true
      case _ =>
        false
    }
  }

  override def visitCall(call: RexCall): RexNode = {
    val updatedCall = super.visitCall(call).asInstanceOf[RexCall]

    // materialize operands with time indicators
    val materializedOperands = updatedCall.getOperator match {
      // skip materialization for special operators
      case FlinkSqlOperatorTable.SESSION_OLD |
           FlinkSqlOperatorTable.HOP_OLD |
           FlinkSqlOperatorTable.TUMBLE_OLD =>
          updatedCall.getOperands.toList

      case _ =>
        updatedCall.getOperands.map { o =>
          if (isTimeIndicatorType(o.getType)) {
            if (isRowtimeIndicatorType(o.getType)) {
              // cast rowtime indicator to regular timestamp
              rexBuilder.makeAbstractCast(
                RelTimeIndicatorConverter.timestamp(
                  o.getType.isNullable,
                  isTimestampLtzIndicatorType(o.getType),
                  rexBuilder),
                o)
            } else {
              // generate proctime access
              rexBuilder.makeCall(FlinkSqlOperatorTable.PROCTIME_MATERIALIZE, o)
            }
          } else {
            o
          }
        }
    }

    // remove time indicator return type
    updatedCall.getOperator match {
      // we do not modify AS if operand has not been materialized
      case SqlStdOperatorTable.AS if
      isTimeIndicatorType(updatedCall.getOperands.get(0).getType) =>
        updatedCall

      // All calls in MEASURES and DEFINE are wrapped with FINAL/RUNNING, therefore
      // we should treat FINAL(MATCH_ROWTIME) and FINAL(MATCH_PROCTIME) as a time attribute
      // extraction. The type of FINAL(MATCH_ROWTIME) is inferred by first operand's type,
      // the initial type of MATCH_ROWTIME is TIMESTAMP(3) *ROWTIME*, it may be rewrote,
      // so we re
      case FINAL if updatedCall.getOperands.size() == 1
        && isMatchTimeIndicator(updatedCall.getOperands.get(0)) =>
        val rowtimeType = updatedCall.getOperands.get(0).getType
        rexBuilder.makeCall(rowtimeType, updatedCall.getOperator, updatedCall.getOperands)

      // MATCH_ROWTIME() is a no-args function, it can own two kind of return types based
      // on the rowtime attribute type of its input, we rewrite the return type here
      case FlinkSqlOperatorTable.MATCH_ROWTIME if isTimeIndicatorType(updatedCall.getType) =>
        val rowtimeType = input.filter(isTimeIndicatorType).head
        rexBuilder.makeCall(
          rowtime(
            updatedCall.getType.isNullable,
            isTimestampLtzIndicatorType(rowtimeType)),
          updatedCall.getOperator,
          materializedOperands)

      // do not modify window time attributes
      case FlinkSqlOperatorTable.TUMBLE_ROWTIME |
           FlinkSqlOperatorTable.TUMBLE_PROCTIME |
           FlinkSqlOperatorTable.HOP_ROWTIME |
           FlinkSqlOperatorTable.HOP_PROCTIME |
           FlinkSqlOperatorTable.SESSION_ROWTIME |
           FlinkSqlOperatorTable.SESSION_PROCTIME |
           FlinkSqlOperatorTable.MATCH_PROCTIME
        // since we materialize groupings on time indicators,
        // we cannot check the operands anymore but the return type at least
        if isTimeIndicatorType(updatedCall.getType) =>
        updatedCall

      // materialize function's result and operands
      case _ if isTimeIndicatorType(updatedCall.getType) =>
        if (updatedCall.getOperator == FlinkSqlOperatorTable.PROCTIME) {
          updatedCall
        } else {
          updatedCall.clone(
            RelTimeIndicatorConverter.timestamp(
              updatedCall.getType.isNullable,
              isTimestampLtzIndicatorType(updatedCall.getType),
              rexBuilder),
            materializedOperands)
        }

      // materialize function's operands only
      case _ =>
        updatedCall.clone(updatedCall.getType, materializedOperands)
    }
  }

}


/**
  * Helper class for shared logic of materializing time attributes in [[RelNode]] and [[RexNode]].
  */
class RexTimeIndicatorMaterializerUtils(rexBuilder: RexBuilder) {

  private def timestamp(isNullable: Boolean, isTimestampLtz: Boolean = false): RelDataType =
    rexBuilder
      .getTypeFactory
      .asInstanceOf[FlinkTypeFactory]
      .createFieldTypeFromLogicalType(
        if (isTimestampLtz) {
          new LocalZonedTimestampType(isNullable, 3)
        } else {
          new TimestampType(isNullable, 3)
        })

  def projectAndMaterializeFields(input: RelNode, indicesToMaterialize: Set[Int]): RelNode = {
    val projects = input.getRowType.getFieldList.map { field =>
      materializeIfContains(
        new RexInputRef(field.getIndex, field.getType),
        field.getIndex,
        indicesToMaterialize)
    }

    LogicalProject.create(
      input,
      JCollections.emptyList[RelHint](),
      projects,
      input.getRowType.getFieldNames)
  }

  def getRowTypeWithoutIndicators(
      relType: RelDataType,
      isLtzRowtimeIndicator: Boolean,
      shouldMaterialize: String => Boolean): RelDataType = {
    val outputTypeBuilder = rexBuilder
      .getTypeFactory
      .asInstanceOf[FlinkTypeFactory]
      .builder()

    relType.getFieldList.asScala.zipWithIndex.foreach { case (field, idx) =>
      if (isTimeIndicatorType(field.getType)) {
        val convertedTimeIndicatorFieldType = if (isLtzRowtimeIndicator) {
          rexBuilder
            .getTypeFactory
            .asInstanceOf[FlinkTypeFactory]
            .createFieldTypeFromLogicalType(
              new LocalZonedTimestampType(field.getType.isNullable, TimestampKind.ROWTIME, 3))
        } else {
          field.getType
        }

        if (shouldMaterialize(field.getName)) {
          outputTypeBuilder.add(
            field.getName,
            timestamp(
              convertedTimeIndicatorFieldType.isNullable,
              isTimestampLtzIndicatorType(convertedTimeIndicatorFieldType)))
        } else {
           outputTypeBuilder.add(
             field.getName,
             convertedTimeIndicatorFieldType)
        }
      } else {
        outputTypeBuilder.add(field.getName, field.getType)
      }
    }

    outputTypeBuilder.build()
  }

  def materializeIfContains(expr: RexNode, index: Int, indicesToMaterialize: Set[Int]): RexNode = {
    if (indicesToMaterialize.contains(index)) {
      materialize(expr)
    } else {
      expr
    }
  }

  def materialize(expr: RexNode): RexNode = {
    if (isTimeIndicatorType(expr.getType)) {
      if (isRowtimeIndicatorType(expr.getType)) {
        // cast rowtime indicator to regular timestamp
        rexBuilder.makeAbstractCast(
          timestamp(expr.getType.isNullable, isTimestampLtzIndicatorType(expr.getType)),
          expr)
      } else {
        // generate proctime access
        rexBuilder.makeCall(FlinkSqlOperatorTable.PROCTIME_MATERIALIZE, expr)
      }
    } else {
      expr
    }
  }
}
