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
package org.apache.flink.table.plan.rules.dataSet

import java.math.BigDecimal

import com.google.common.collect.ImmutableList
import org.apache.calcite.plan._
import org.apache.calcite.plan.hep.HepRelVertex
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.logical.{LogicalAggregate, LogicalProject}
import org.apache.calcite.rex.{RexCall, RexInputRef, RexLiteral, RexNode}
import org.apache.calcite.sql.fun.SqlStdOperatorTable
import org.apache.calcite.util.ImmutableBitSet
import org.apache.flink.table.api.scala.{Session, Slide, Tumble}
import org.apache.flink.table.api._
import org.apache.flink.table.calcite.FlinkRelBuilder.NamedWindowProperty
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.expressions._
import org.apache.flink.table.plan.logical.rel.LogicalWindowAggregate
import org.apache.flink.table.typeutils.TimeIntervalTypeInfo

import _root_.scala.collection.JavaConversions._

class LogicalWindowAggregateRule
  extends RelOptRule(
    LogicalWindowAggregateRule.LOGICAL_WINDOW_PREDICATE,
    "LogicalWindowAggregateRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val agg = call.rel(0).asInstanceOf[LogicalAggregate]

    val distinctAggs = agg.getAggCallList.exists(_.isDistinct)
    val groupSets = agg.getGroupSets.size() != 1 || agg.getGroupSets.get(0) != agg.getGroupSet

    val windowClause = recognizeWindow(agg)
    !distinctAggs && !groupSets && !agg.indicator && windowClause.isDefined
  }

  /**
    * Transform LogicalAggregate with windowing expression to LogicalProject
    * + LogicalWindowAggregate + LogicalProject.
    *
    * The transformation adds an additional LogicalProject at the top to ensure
    * that the types are equivalent.
    */
  override def onMatch(call: RelOptRuleCall): Unit = {
    val agg = call.rel[LogicalAggregate](0)
    val project = agg.getInput.asInstanceOf[HepRelVertex].getCurrentRel.asInstanceOf[LogicalProject]
    val (windowExprIdx, window) = recognizeWindow(agg).get
    val newGroupSet = agg.getGroupSet.except(ImmutableBitSet.of(windowExprIdx))

    val builder = call.builder()
    val rexBuilder = builder.getRexBuilder

    // replace window group function by expression on which it is called
    val windowOperand =
      project.getProjects.get(windowExprIdx).asInstanceOf[RexCall].getOperands.get(0)

    val newAgg = builder
      .push(project.getInput)
      .project(project.getChildExps.updated(windowExprIdx, windowOperand))
      .aggregate(builder.groupKey(
        newGroupSet,
        agg.indicator, ImmutableList.of(newGroupSet)), agg.getAggCallList)
      .build().asInstanceOf[LogicalAggregate]

    // we need to logically forward the grouping field
    val groupingField = rexBuilder.makeZeroLiteral(windowOperand.getType)

    // Create an additional project to conform with types
    val transformed = call.builder()
    transformed.push(LogicalWindowAggregate.create(
      window.toLogicalWindow,
      Seq[NamedWindowProperty](),
      newAgg))
      .project(transformed.fields().patch(windowExprIdx, Seq(groupingField), 0))
    call.transformTo(transformed.build())
  }

  private def recognizeWindow(agg: LogicalAggregate): Option[(Int, Window)] = {
    val project = agg.getInput.asInstanceOf[HepRelVertex].getCurrentRel.asInstanceOf[LogicalProject]
    val inRowType = project.getInput.getRowType
    val groupKeys = agg.getGroupSet

    // filter expressions on which is grouped
    val groupExpr = project.getProjects.zipWithIndex.filter(p => groupKeys.get(p._2))

    // check for window expressions in group expressions
    val windowExpr = groupExpr
      .map(g => (g._2, identifyWindow(g._1, inRowType)) )
      .filter(_._2.isDefined)
      .map(g => (g._1, g._2.get.as("w$")) )

    windowExpr.size match {
      case 0 => None
      case 1 => Some(windowExpr.head)
      case _ => throw new TableException("Multiple windows are not supported")
    }
  }

  private def identifyWindow(field: RexNode, rowType: RelDataType): Option[Window] = {
    field match {
      case call: RexCall =>
        call.getOperator match {
          case SqlStdOperatorTable.TUMBLE => TumbleWindowTranslator(call, rowType).toWindow
          case SqlStdOperatorTable.HOP => SlidingWindowTranslator(call, rowType).toWindow
          case SqlStdOperatorTable.SESSION => SessionWindowTranslator(call, rowType).toWindow
          case _ => None
        }
      case _ => None
    }
  }
}

private abstract class WindowTranslator {
  val call: RexCall

  protected def unwrapLiteral[T](node: RexNode): T =
    node.asInstanceOf[RexLiteral].getValue.asInstanceOf[T]

  protected def getOperandAsLong(idx: Int): Long =
    call.getOperands.get(idx) match {
      case v : RexLiteral => v.getValue.asInstanceOf[BigDecimal].longValue()
      case _ => throw new TableException("Only constant window descriptors are supported")
    }

  def toWindow: Option[Window]
}

private case class TumbleWindowTranslator(
    call: RexCall,
    rowType: RelDataType) extends WindowTranslator {

  override def toWindow: Option[Window] = {
    val interval = getOperandAsLong(1)
    val w = Tumble.over(Literal(interval, TimeIntervalTypeInfo.INTERVAL_MILLIS))

    if (call.getOperands.size() != 2) {
      throw new TableException("TUMBLE with alignment is not supported yet.")
    }
    call.getOperands.get(0) match {
      case ref: RexInputRef =>
        // resolve field name of window attribute
        val fieldName = rowType.getFieldList.get(ref.getIndex).getName
        val fieldType = rowType.getFieldList.get(ref.getIndex).getType
        Some(w.on(ResolvedFieldReference(fieldName, FlinkTypeFactory.toTypeInfo(fieldType))))
      case _ => None
    }
  }
}

private case class SlidingWindowTranslator(
    call: RexCall,
    rowType: RelDataType) extends WindowTranslator {

  override def toWindow: Option[Window] = {
    val (slide, size) = (getOperandAsLong(1), getOperandAsLong(2))
    val w = Slide
      .over(Literal(size, TimeIntervalTypeInfo.INTERVAL_MILLIS))
      .every(Literal(slide, TimeIntervalTypeInfo.INTERVAL_MILLIS))

    if (call.getOperands.size() != 3) {
      throw new TableException("HOP with alignment is not supported yet.")
    }
    call.getOperands.get(0) match {
      case ref: RexInputRef =>
        // resolve field name of window attribute
        val fieldName = rowType.getFieldList.get(ref.getIndex).getName
        val fieldType = rowType.getFieldList.get(ref.getIndex).getType
        Some(w.on(ResolvedFieldReference(fieldName, FlinkTypeFactory.toTypeInfo(fieldType))))
      case _ => None
    }
  }
}

private case class SessionWindowTranslator(
    call: RexCall,
    rowType: RelDataType) extends WindowTranslator {

  override def toWindow: Option[Window] = {
    val gap = getOperandAsLong(1)
    val w = Session.withGap(Literal(gap, TimeIntervalTypeInfo.INTERVAL_MILLIS))

    if (call.getOperands.size() != 2) {
      throw new TableException("SESSION with alignment is not supported yet.")
    }
    call.getOperands.get(0) match {
      case ref: RexInputRef =>
        // resolve field name of window attribute
        val fieldName = rowType.getFieldList.get(ref.getIndex).getName
        val fieldType = rowType.getFieldList.get(ref.getIndex).getType
        Some(w.on(ResolvedFieldReference(fieldName, FlinkTypeFactory.toTypeInfo(fieldType))))
      case _ => None
    }
  }
}

object LogicalWindowAggregateRule {
  private[flink] val LOGICAL_WINDOW_PREDICATE = RelOptRule.operand(classOf[LogicalAggregate],
    RelOptRule.operand(classOf[LogicalProject], RelOptRule.none()))

  private[flink] val INSTANCE = new LogicalWindowAggregateRule
}
