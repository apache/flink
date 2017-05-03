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

import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeFieldImpl, RelRecordType, StructKind}
import org.apache.calcite.rel.logical._
import org.apache.calcite.rel.{RelNode, RelShuttleImpl}
import org.apache.calcite.rex._
import org.apache.calcite.sql.fun.SqlStdOperatorTable
import org.apache.flink.api.common.typeinfo.SqlTimeTypeInfo
import org.apache.flink.table.api.ValidationException
import org.apache.flink.table.calcite.FlinkTypeFactory.isTimeIndicatorType
import org.apache.flink.table.functions.TimeMaterializationSqlFunction
import org.apache.flink.table.plan.schema.TimeIndicatorRelDataType

import scala.collection.JavaConversions._

/**
  * Traverses a [[RelNode]] tree and converts fields with [[TimeIndicatorRelDataType]] type. If a
  * time attribute is accessed for a calculation, it will be materialized. Forwarding is allowed in
  * some cases, but not all.
  */
class RelTimeIndicatorConverter(rexBuilder: RexBuilder) extends RelShuttleImpl {

  override def visit(project: LogicalProject): RelNode = {
    // visit children and update inputs
    val updatedProject = super.visit(project).asInstanceOf[LogicalProject]

    // check if input field contains time indicator type
    // materialize field if no time indicator is present anymore
    // if input field is already materialized, change to timestamp type
    val materializer = new RexTimeIndicatorMaterializer(
      rexBuilder,
      updatedProject.getInput.getRowType.getFieldList.map(_.getType))
    val newProjects = updatedProject.getProjects.map(_.accept(materializer))

    // copy project
    updatedProject.copy(
      updatedProject.getTraitSet,
      updatedProject.getInput,
      newProjects,
      buildRowType(updatedProject.getRowType.getFieldNames, newProjects.map(_.getType))
    )
  }

  override def visit(filter: LogicalFilter): RelNode = {
    // visit children and update inputs
    val updatedFilter = super.visit(filter).asInstanceOf[LogicalFilter]

    // check if input field contains time indicator type
    // materialize field if no time indicator is present anymore
    // if input field is already materialized, change to timestamp type
    val materializer = new RexTimeIndicatorMaterializer(
      rexBuilder,
      updatedFilter.getInput.getRowType.getFieldList.map(_.getType))
    val newCondition = updatedFilter.getCondition.accept(materializer)

    // copy filter
    updatedFilter.copy(
      updatedFilter.getTraitSet,
      updatedFilter.getInput,
      newCondition
    )
  }

  override def visit(union: LogicalUnion): RelNode = {
    // visit children and update inputs
    val updatedUnion = super.visit(union).asInstanceOf[LogicalUnion]

    // make sure that time indicator types match
    val inputTypes = updatedUnion.getInputs.map(_.getRowType)

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

    updatedUnion
  }

  override def visit(other: RelNode): RelNode = other match {
    case scan: LogicalTableFunctionScan if
        stack.size() > 0 && stack.peek().isInstanceOf[LogicalCorrelate] =>
      // visit children and update inputs
      val updatedScan = super.visit(scan).asInstanceOf[LogicalTableFunctionScan]

      val correlate = stack.peek().asInstanceOf[LogicalCorrelate]

      // check if input field contains time indicator type
      // materialize field if no time indicator is present anymore
      // if input field is already materialized, change to timestamp type
      val materializer = new RexTimeIndicatorMaterializer(
        rexBuilder,
        correlate.getInputs.get(0).getRowType.getFieldList.map(_.getType))
      val newCall = updatedScan.getCall.accept(materializer)

      // copy scan
      updatedScan.copy(
        updatedScan.getTraitSet,
        updatedScan.getInputs,
        newCall,
        updatedScan.getElementType,
        updatedScan.getRowType,
        updatedScan.getColumnMappings
      )

    case _ =>
      super.visit(other)
  }

  private def buildRowType(names: Seq[String], types: Seq[RelDataType]): RelDataType = {
    val fields = names.zipWithIndex.map { case (name, idx) =>
      new RelDataTypeFieldImpl(name, idx, types(idx))
    }
    new RelRecordType(StructKind.FULLY_QUALIFIED, fields)
  }
}

class RexTimeIndicatorMaterializer(
    private val rexBuilder: RexBuilder,
    private val input: Seq[RelDataType])
  extends RexShuttle {

  val timestamp = rexBuilder
    .getTypeFactory
    .asInstanceOf[FlinkTypeFactory]
    .createTypeFromTypeInfo(SqlTimeTypeInfo.TIMESTAMP)

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

    // skip materialization for special operators
    updatedCall.getOperator match {
      case SqlStdOperatorTable.SESSION | SqlStdOperatorTable.HOP | SqlStdOperatorTable.TUMBLE =>
        return updatedCall

      case _ => // do nothing
    }

    // materialize operands with time indicators
    val materializedOperands = updatedCall.getOperands.map { o =>
      if (isTimeIndicatorType(o.getType)) {
        rexBuilder.makeCall(TimeMaterializationSqlFunction, o)
      } else {
        o
      }
    }

    // remove time indicator return type
    if (isTimeIndicatorType(updatedCall.getType)) {
      updatedCall.clone(timestamp, materializedOperands)
    } else {
      updatedCall.clone(updatedCall.getType, materializedOperands)
    }
  }
}

object RelTimeIndicatorConverter {

  def convert(rootRel: RelNode, rexBuilder: RexBuilder): RelNode = {
    val converter = new RelTimeIndicatorConverter(rexBuilder)
    rootRel.accept(converter)
  }
}
