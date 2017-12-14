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
import org.apache.calcite.sql.fun.SqlStdOperatorTable
import org.apache.flink.api.common.typeinfo.SqlTimeTypeInfo
import org.apache.flink.table.api.{TableException, ValidationException}
import org.apache.flink.table.calcite.FlinkTypeFactory.{isRowtimeIndicatorType, _}
import org.apache.flink.table.functions.sql.ProctimeSqlFunction
import org.apache.flink.table.plan.logical.rel.LogicalWindowAggregate
import org.apache.flink.table.plan.schema.TimeIndicatorRelDataType
import org.apache.flink.table.validate.BasicOperatorTable

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
  * Traverses a [[RelNode]] tree and converts fields with [[TimeIndicatorRelDataType]] type. If a
  * time attribute is accessed for a calculation, it will be materialized. Forwarding is allowed in
  * some cases, but not all.
  */
class RelTimeIndicatorConverter(rexBuilder: RexBuilder) extends RelShuttle {

  private val timestamp = rexBuilder
      .getTypeFactory
      .asInstanceOf[FlinkTypeFactory]
      .createTypeFromTypeInfo(SqlTimeTypeInfo.TIMESTAMP, isNullable = false)

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

  override def visit(`match`: LogicalMatch): RelNode =
    throw new TableException("Logical match in a stream environment is not supported yet.")

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

    // We do not materialize time indicators in conditions because they can be locally evaluated.
    // Some conditions are evaluated by special operators (e.g., time window joins).
    // Time indicators in remaining conditions are materialized by Calc before the code generation.
    LogicalFilter.create(input, filter.getCondition)
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
    LogicalProject.create(input, projects, fieldNames)
  }

  override def visit(join: LogicalJoin): RelNode = {
    val left = join.getLeft.accept(this)
    val right = join.getRight.accept(this)

    LogicalJoin.create(left, right, join.getCondition, join.getVariablesSet, join.getJoinType)

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

  private def convertAggregate(aggregate: Aggregate): LogicalAggregate = {
    // visit children and update inputs
    val input = aggregate.getInput.accept(this)

    // add a project to materialize aggregation arguments/grouping keys

    val refIndices = mutable.Set[Int]()

    // check arguments of agg calls
    aggregate.getAggCallList.foreach(call => if (call.getArgList.size() == 0) {
        // count(*) has an empty argument list
        (0 until input.getRowType.getFieldCount).foreach(refIndices.add)
      } else {
        // for other aggregations
        call.getArgList.map(_.asInstanceOf[Int]).foreach(refIndices.add)
      })

    // check grouping sets
    aggregate.getGroupSets.foreach(set =>
      set.asList().map(_.asInstanceOf[Int]).foreach(refIndices.add)
    )

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
                rexBuilder.makeAbstractCast(timestamp, expr)
              } else {
                // generate proctime access
                rexBuilder.makeCall(ProctimeSqlFunction, expr)
              }
            } else {
              expr
            }
          }

          LogicalProject.create(
            lp.getInput,
            projects,
            input.getRowType.getFieldNames)

        // new project
        case _ =>
          val projects = input.getRowType.getFieldList.map { field =>
            if (isTimeIndicatorType(field.getType) && refIndices.contains(field.getIndex)) {
              if (isRowtimeIndicatorType(field.getType)) {
                // cast rowtime indicator to regular timestamp
                rexBuilder.makeAbstractCast(
                  timestamp,
                  new RexInputRef(field.getIndex, field.getType))
              } else {
                // generate proctime access
                rexBuilder.makeCall(
                  ProctimeSqlFunction,
                  new RexInputRef(field.getIndex, field.getType))
              }
            } else {
              new RexInputRef(field.getIndex, field.getType)
            }
          }

          LogicalProject.create(
            input,
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
        timestamp
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

}

object RelTimeIndicatorConverter {

  def convert(rootRel: RelNode, rexBuilder: RexBuilder): RelNode = {
    val converter = new RelTimeIndicatorConverter(rexBuilder)
    val convertedRoot = rootRel.accept(converter)

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
    val materializer = new RexTimeIndicatorMaterializer(
          rexBuilder,
          rowType.getFieldList.map(_.getType))

        expr.accept(materializer)
  }
}

class RexTimeIndicatorMaterializer(
  private val rexBuilder: RexBuilder,
  private val input: Seq[RelDataType])
  extends RexShuttle {

  private val timestamp = rexBuilder
    .getTypeFactory
    .asInstanceOf[FlinkTypeFactory]
    .createTypeFromTypeInfo(SqlTimeTypeInfo.TIMESTAMP, isNullable = false)

  override def visitInputRef(inputRef: RexInputRef): RexNode = {
    // reference is interesting
    if (isTimeIndicatorType(inputRef.getType)) {
      val resolvedRefType = input(inputRef.getIndex)
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

  override def visitCall(call: RexCall): RexNode = {
    val updatedCall = super.visitCall(call).asInstanceOf[RexCall]

    // materialize operands with time indicators
    val materializedOperands = updatedCall.getOperator match {

      // skip materialization for special operators
      case BasicOperatorTable.SESSION | BasicOperatorTable.HOP | BasicOperatorTable.TUMBLE =>
        updatedCall.getOperands.toList

      case _ =>
        updatedCall.getOperands.map { o =>
          if (isTimeIndicatorType(o.getType)) {
            if (isRowtimeIndicatorType(o.getType)) {
              // cast rowtime indicator to regular timestamp
              rexBuilder.makeAbstractCast(timestamp, o)
            } else {
              // generate proctime access
              rexBuilder.makeCall(ProctimeSqlFunction, o)
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

      // do not modify window time attributes
      case BasicOperatorTable.TUMBLE_ROWTIME |
          BasicOperatorTable.TUMBLE_PROCTIME |
          BasicOperatorTable.HOP_ROWTIME |
          BasicOperatorTable.HOP_PROCTIME |
          BasicOperatorTable.SESSION_ROWTIME |
          BasicOperatorTable.SESSION_PROCTIME
          // since we materialize groupings on time indicators,
          // we cannot check the operands anymore but the return type at least
          if isTimeIndicatorType(updatedCall.getType) =>
      updatedCall

      // materialize function's result and operands
      case _ if isTimeIndicatorType(updatedCall.getType) =>
        updatedCall.clone(timestamp, materializedOperands)

      // materialize function's operands only
      case _ =>
        updatedCall.clone(updatedCall.getType, materializedOperands)
    }
  }
}
