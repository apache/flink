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

package org.apache.flink.table.calcite

import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core._
import org.apache.calcite.rel.logical._
import org.apache.calcite.rel.{RelNode, RelShuttle}
import org.apache.calcite.rex._
import org.apache.calcite.sql.`type`.SqlTypeName
import org.apache.calcite.sql.fun.SqlStdOperatorTable
import org.apache.calcite.sql.fun.SqlStdOperatorTable.FINAL
import org.apache.flink.api.common.typeinfo.SqlTimeTypeInfo
import org.apache.flink.table.api.{TableException, ValidationException}
import org.apache.flink.table.calcite.FlinkTypeFactory.{isRowtimeIndicatorType, _}
import org.apache.flink.table.catalog.BasicOperatorTable
import org.apache.flink.table.functions.sql.ProctimeSqlFunction
import org.apache.flink.table.plan.logical.rel._
import org.apache.flink.table.plan.nodes.LogicalSink
import org.apache.flink.table.plan.schema.TimeIndicatorRelDataType

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

  override def visit(intersect: LogicalIntersect): RelNode =
    throw new TableException("Logical intersect in a stream environment is not supported yet.")

  override def visit(union: LogicalUnion): RelNode = {
    // visit children and update inputs
    val inputs = union.getInputs.map(_.accept(this))

    // make sure that time indicator types match
    val inputTypes = inputs.map(_.getRowType)

    val head = inputTypes.head.getFieldList.map(_.getType)

    val isValid = inputTypes.forall { t =>
      val fieldTypes = t.getFieldList.map(_.getType)

      fieldTypes.zip(head).forall { case (l, r) =>
        // check if time indicators match
        if (isTimeIndicatorType(l) && isTimeIndicatorType(r)) {
          val leftTime = l.asInstanceOf[TimeIndicatorRelDataType].isEventTime
          val rightTime = r.asInstanceOf[TimeIndicatorRelDataType].isEventTime
          leftTime == rightTime
        }
        // one side is not an indicator
        else if (isTimeIndicatorType(l) || isTimeIndicatorType(r)) {
          false
        }
        // uninteresting types
        else {
          true
        }
      }
    }

    if (!isValid) {
      throw new ValidationException(
        "Union fields with time attributes have different types.")
    }

    LogicalUnion.create(inputs, union.all)
  }

  override def visit(aggregate: LogicalAggregate): RelNode = convertAggregate(aggregate)

  override def visit(minus: LogicalMinus): RelNode =
    throw new TableException("Logical minus in a stream environment is not supported yet.")

  override def visit(sort: LogicalSort): RelNode = {

    val input = sort.getInput.accept(this)
    LogicalSort.create(input, sort.collation, sort.offset, sort.fetch)
  }

  override def visit(matchRel: LogicalMatch): RelNode = {
    // visit children and update inputs
    val input = matchRel.getInput.accept(this)
    val materializer = createMaterializer(input)

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

    // materialize all output types
    val outputType = materializerUtils.getRowTypeWithoutIndicators(
        matchRel.getRowType,
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

    case uncollect: Uncollect =>
      // visit children and update inputs
      val input = uncollect.getInput.accept(this)
      Uncollect.create(uncollect.getTraitSet, input, uncollect.withOrdinality)

    case scan: LogicalTableFunctionScan =>
      scan

    case aggregate: LogicalWindowAggregate =>
      val convAggregate = convertAggregate(aggregate)

      LogicalWindowAggregate.create(
        aggregate.getWindow,
        aggregate.getNamedProperties,
        convAggregate)

    case windowTableAggregate: LogicalWindowTableAggregate =>
      val convAggregate = convertAggregate(windowTableAggregate.getCorrespondingAggregate)
      LogicalWindowTableAggregate.create(
        windowTableAggregate.getWindow,
        windowTableAggregate.getNamedProperties,
        convAggregate)

    case tableAggregate: LogicalTableAggregate =>
      val convAggregate = convertAggregate(tableAggregate.getCorrespondingAggregate)
      LogicalTableAggregate.create(convAggregate)

    case temporalTableJoin: LogicalTemporalTableJoin =>
      visit(temporalTableJoin)

    case sink: LogicalSink =>
      var newInput = sink.getInput.accept(this)
      var needsConversion = false

      val projects = newInput.getRowType.getFieldList.map { field =>
        if (isProctimeIndicatorType(field.getType)) {
          needsConversion = true
          rexBuilder.makeCall(ProctimeSqlFunction, new RexInputRef(field.getIndex, field.getType))
        } else {
          new RexInputRef(field.getIndex, field.getType)
        }
      }

      // add final conversion if necessary
      if (needsConversion) {
        newInput = LogicalProject.create(newInput, projects, newInput.getRowType.getFieldNames)
      }
      new LogicalSink(
        sink.getCluster,
        sink.getTraitSet,
        newInput,
        sink.sink,
        sink.sinkName)

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
    val materializer = createMaterializer(input)

    val condition = filter.getCondition.accept(materializer)
    LogicalFilter.create(input, condition)
  }

  override def visit(project: LogicalProject): RelNode = {
    // visit children and update inputs
    val input = project.getInput.accept(this)
    val materializer = createMaterializer(input)

    val projects = project.getProjects.map(_.accept(materializer))
    val fieldNames = project.getRowType.getFieldNames
    LogicalProject.create(input, projects, fieldNames)
  }

  override def visit(join: LogicalJoin): RelNode = {
    val left = join.getLeft.accept(this)
    val right = join.getRight.accept(this)
    val materializer = createMaterializer(left, right)

    LogicalJoin.create(
      left,
      right,
      join.getCondition.accept(materializer),
      join.getVariablesSet,
      join.getJoinType)
  }

  def visit(temporalJoin: LogicalTemporalTableJoin): RelNode = {
    val left = temporalJoin.getLeft.accept(this)
    val right = temporalJoin.getRight.accept(this)

    val rewrittenTemporalJoin = temporalJoin.copy(temporalJoin.getTraitSet, List(left, right))

    val indicesToMaterialize = gatherIndicesToMaterialize(rewrittenTemporalJoin, left, right)

    materializerUtils.projectAndMaterializeFields(rewrittenTemporalJoin, indicesToMaterialize)
  }

  override def visit(correlate: LogicalCorrelate): RelNode = {
    // visit children and update inputs
    val inputs = correlate.getInputs.map(_.accept(this))

    val right = inputs(1) match {
      case scan: LogicalTableFunctionScan =>
        // visit children and update inputs
        val scanInputs = scan.getInputs.map(_.accept(this))
        val materializer = createMaterializer(inputs.head)
        LogicalTableFunctionScan.create(
          scan.getCluster,
          scanInputs,
          scan.getCall.accept(materializer),
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

  private def gatherIndicesToMaterialize(
    temporalJoin: Join,
    left: RelNode,
    right: RelNode)
  : Set[Int] = {

    // Materialize all of the time attributes from the right side of temporal join
    var indicesToMaterialize =
      (left.getRowType.getFieldCount until temporalJoin.getRowType.getFieldCount).toSet

    if (!hasRowtimeAttribute(right.getRowType)) {
      // No rowtime on the right side means that this must be a processing time temporal join
      // and that we can not provide watermarks even if there is a rowtime time attribute
      // on the left side (besides processing time attribute used for temporal join).
      for (fieldIndex <- 0 until left.getRowType.getFieldCount) {
        val fieldName = left.getRowType.getFieldNames.get(fieldIndex)
        val fieldType = left.getRowType.getFieldList.get(fieldIndex).getType
        if (isRowtimeIndicatorType(fieldType)) {
          indicesToMaterialize += fieldIndex
        }
      }
    }

    indicesToMaterialize
  }

  private def hasRowtimeAttribute(rowType: RelDataType): Boolean = {
    rowType.getFieldList.exists(field => isRowtimeIndicatorType(field.getType))
  }

  private def convertAggregate(aggregate: Aggregate): LogicalAggregate = {
    // visit children and update inputs
    val input = aggregate.getInput.accept(this)

    // add a project to materialize aggregation arguments/grouping keys

    val indicesToMaterialize = gatherIndicesToMaterialize(aggregate, input)

    val needsMaterialization = indicesToMaterialize.exists(idx =>
      isTimeIndicatorType(input.getRowType.getFieldList.get(idx).getType))

    // create project if necessary
    val projectedInput = if (needsMaterialization) {

      // insert or merge with input project if
      // a time attribute is accessed and needs to be materialized
      input match {

        // merge
        case lp: LogicalProject =>
          val projects = lp.getProjects.zipWithIndex.map { case (expr, idx) =>
            materializerUtils.materializeIfContains(expr, idx, indicesToMaterialize)
          }

          LogicalProject.create(
            lp.getInput,
            projects,
            input.getRowType.getFieldNames)

        // new project
        case _ =>
          materializerUtils.projectAndMaterializeFields(input, indicesToMaterialize)
      }
    } else {
      // no project necessary
      input
    }

    // remove time indicator type as agg call return type
    val updatedAggCalls = aggregate.getAggCallList.map { call =>
      val callType = if (isTimeIndicatorType(call.getType)) {
        materializerUtils.getTimestamp
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

  private def gatherIndicesToMaterialize(aggregate: Aggregate, input: RelNode): Set[Int] = {
    val indicesToMaterialize = mutable.Set[Int]()

    // check arguments of agg calls
    aggregate.getAggCallList.foreach(call => if (call.getArgList.size() == 0) {
      // count(*) has an empty argument list
      (0 until input.getRowType.getFieldCount).foreach(indicesToMaterialize.add)
    } else {
      // for other aggregations
      call.getArgList.map(_.asInstanceOf[Int]).foreach(indicesToMaterialize.add)
    })

    // check grouping sets
    aggregate.getGroupSets.foreach(set =>
      set.asList().map(_.asInstanceOf[Int]).foreach(indicesToMaterialize.add)
    )

    indicesToMaterialize.toSet
  }

  private def createMaterializer(inputs: RelNode*): RexTimeIndicatorMaterializer = {
    // check if input field contains time indicator type
    // materialize field if no time indicator is present anymore
    // if input field is already materialized, change to timestamp type
    new RexTimeIndicatorMaterializer(
      rexBuilder,
      inputs.flatMap(_.getRowType.getFieldList.map(_.getType)))
  }

  override def visit(modify: LogicalTableModify): RelNode = {
    val input = modify.getInput.accept(this)
    modify.copy(modify.getTraitSet, JCollections.singletonList(input))
  }
}

object RelTimeIndicatorConverter {

  def convert(rootRel: RelNode, rexBuilder: RexBuilder): RelNode = {
    val converter = new RelTimeIndicatorConverter(rexBuilder)
    val convertedRoot = rootRel.accept(converter)

    // the LogicalSink is converted in RelTimeIndicatorConverter before
    if (rootRel.isInstanceOf[LogicalSink]) {
      return convertedRoot
    }

    var needsConversion = false

    // materialize remaining proctime indicators
    val projects = convertedRoot.getRowType.getFieldList.map(field =>
      if (isProctimeIndicatorType(field.getType)) {
        needsConversion = true
        rexBuilder.makeCall(
          ProctimeSqlFunction,
          new RexInputRef(field.getIndex, field.getType))
      } else {
        new RexInputRef(field.getIndex, field.getType)
      }
    )

    // add final conversion if necessary
    if (needsConversion) {
      LogicalProject.create(
      convertedRoot,
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
    *
    * @return The expression with materialized time indicators.
    */
  def convertExpression(expr: RexNode, rowType: RelDataType, rexBuilder: RexBuilder): RexNode = {
    // check if input field contains time indicator type
    // materialize field if no time indicator is present anymore
    // if input field is already materialized, change to timestamp type
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
      call.getOperator == ProctimeSqlFunction &&
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
}

/**
  * Takes `newResolvedInput` types of the [[RexNode]] and if those types have changed rewrites
  * the [[RexNode]] to make it consistent with new type.
  */
class RexTimeIndicatorMaterializer(
  private val rexBuilder: RexBuilder,
  private val newResolvedInput: Seq[RelDataType])
  extends RexShuttle {

  private val materializerUtils = new RexTimeIndicatorMaterializerUtils(rexBuilder)

  override def visitInputRef(inputRef: RexInputRef): RexNode = {
    // reference is interesting
    if (isTimeIndicatorType(inputRef.getType)) {
      val resolvedRefType = newResolvedInput(inputRef.getIndex)
      // input is a valid time indicator
      if (isTimeIndicatorType(resolvedRefType)) {
        inputRef
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
      operand.getOperator == BasicOperatorTable.MATCH_PROCTIME ||
      operand.getOperator == BasicOperatorTable.MATCH_ROWTIME =>
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
      case BasicOperatorTable.SESSION |
           BasicOperatorTable.HOP |
           BasicOperatorTable.TUMBLE =>
        updatedCall.getOperands.toList

      case _ =>
        updatedCall.getOperands.map { materializerUtils.materialize }
    }

    // remove time indicator return type
    updatedCall.getOperator match {

      // we do not modify AS if operand has not been materialized
      case SqlStdOperatorTable.AS if
      isTimeIndicatorType(updatedCall.getOperands.get(0).getType) =>
        updatedCall

      // All calls in MEASURES and DEFINE are wrapped with FINAL/RUNNING, therefore
      // we should treat FINAL(MATCH_ROWTIME) and FINAL(MATCH_PROCTIME) as a time attribute
      // extraction
      case FINAL if
      updatedCall.getOperands.size() == 1 && isMatchTimeIndicator(updatedCall.getOperands.get(0)) =>
        updatedCall

      // do not modify window time attributes
      case BasicOperatorTable.TUMBLE_ROWTIME |
          BasicOperatorTable.TUMBLE_PROCTIME |
          BasicOperatorTable.HOP_ROWTIME |
          BasicOperatorTable.HOP_PROCTIME |
          BasicOperatorTable.SESSION_ROWTIME |
          BasicOperatorTable.SESSION_PROCTIME |
          BasicOperatorTable.MATCH_ROWTIME |
          BasicOperatorTable.MATCH_PROCTIME
          // since we materialize groupings on time indicators,
          // we cannot check the operands anymore but the return type at least
          if isTimeIndicatorType(updatedCall.getType) =>
      updatedCall

      // materialize function's result and operands
      case _ if isTimeIndicatorType(updatedCall.getType) =>
        updatedCall.clone(materializerUtils.getTimestamp, materializedOperands)

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

  private val timestamp = rexBuilder
    .getTypeFactory
    .asInstanceOf[FlinkTypeFactory]
    .createTypeFromTypeInfo(SqlTimeTypeInfo.TIMESTAMP, isNullable = false)

  def getTimestamp: RelDataType = {
    timestamp
  }

  def projectAndMaterializeFields(input: RelNode, indicesToMaterialize: Set[Int]) : RelNode = {
    val projects = input.getRowType.getFieldList.map { field =>
      materializeIfContains(
        new RexInputRef(field.getIndex, field.getType),
        field.getIndex,
        indicesToMaterialize)
    }

    LogicalProject.create(
      input,
      projects,
      input.getRowType.getFieldNames)
  }

  def getRowTypeWithoutIndicators(
      relType: RelDataType,
      shouldMaterialize: String => Boolean): RelDataType = {
    val outputTypeBuilder = rexBuilder
      .getTypeFactory
      .asInstanceOf[FlinkTypeFactory]
      .builder()

    relType.getFieldList.asScala.foreach(field =>
      if (FlinkTypeFactory.isTimeIndicatorType(field.getType) && shouldMaterialize(field.getName)) {
        outputTypeBuilder.add(field.getName, timestamp)
      } else {
        outputTypeBuilder.add(field.getName, field.getType)
      }
    )

    outputTypeBuilder.build()
  }

  def materializeIfContains(expr: RexNode, index: Int, indicesToMaterialize: Set[Int]): RexNode = {
    if (indicesToMaterialize.contains(index)) {
      materialize(expr)
    }
    else {
      expr
    }
  }

  def materialize(expr: RexNode): RexNode = {
    if (isTimeIndicatorType(expr.getType)) {
      if (isRowtimeIndicatorType(expr.getType)) {
        // cast rowtime indicator to regular timestamp
        rexBuilder.makeAbstractCast(timestamp, expr)
      } else {
        // generate proctime access
        rexBuilder.makeCall(ProctimeSqlFunction, expr)
      }
    } else {
      expr
    }
  }
}
