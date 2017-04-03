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
package org.apache.flink.table.plan.rules.datastream

import java.math.BigDecimal

import com.google.common.collect.ImmutableList
import org.apache.calcite.avatica.util.TimeUnitRange
import org.apache.calcite.plan._
import org.apache.calcite.plan.hep.HepRelVertex
import org.apache.calcite.rel.logical.{LogicalAggregate, LogicalProject}
import org.apache.calcite.rex.{RexCall, RexLiteral, RexNode}
import org.apache.calcite.sql.fun.{SqlFloorFunction, SqlStdOperatorTable}
import org.apache.calcite.util.ImmutableBitSet
import org.apache.flink.table.api._
import org.apache.flink.table.api.scala.{Session, Slide, Tumble}
import org.apache.flink.table.calcite.FlinkRelBuilder.NamedWindowProperty
import org.apache.flink.table.expressions._
import org.apache.flink.table.functions.TimeModeTypes
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

    // build dummy literal with type depending on time semantics
    val zero = window match {
      case _: EventTimeWindow =>
        rexBuilder.makeAbstractCast(
          TimeModeTypes.ROWTIME,
          rexBuilder.makeLiteral(0L, TimeModeTypes.ROWTIME, true))
      case _ =>
        rexBuilder.makeAbstractCast(
          TimeModeTypes.PROCTIME,
          rexBuilder.makeLiteral(0L, TimeModeTypes.PROCTIME, true))
    }

    val newAgg = builder
      .push(project.getInput)
      .project(project.getChildExps.updated(windowExprIdx, zero))
      .aggregate(builder.groupKey(
        newGroupSet,
        agg.indicator, ImmutableList.of(newGroupSet)), agg.getAggCallList)
      .build().asInstanceOf[LogicalAggregate]

    // Create an additional project to conform with types
    val transformed = call.builder()
    transformed.push(LogicalWindowAggregate.create(
      window.toLogicalWindow,
      Seq[NamedWindowProperty](),
      newAgg))
      .project(transformed.fields().patch(windowExprIdx, Seq(zero), 0))
    call.transformTo(transformed.build())
  }

  private def recognizeWindow(agg: LogicalAggregate) : Option[(Int, Window)] = {
    val project = agg.getInput.asInstanceOf[HepRelVertex].getCurrentRel.asInstanceOf[LogicalProject]
    val groupKeys = agg.getGroupSet

    // filter expressions on which is grouped
    val groupExpr = project.getProjects.zipWithIndex.filter(p => groupKeys.get(p._2))

    // check for window expressions in group expressions
    val windowExpr = groupExpr
      .map(g => (g._2, identifyWindow(g._1)) )
      .filter(_._2.isDefined)
      .map(g => (g._1, g._2.get.as("w$")) )

    windowExpr.size match {
      case 0 => None
      case 1 => Some(windowExpr.head)
      case _ => throw new TableException("Multiple windows are not supported")
    }
  }

  private def identifyWindow(field: RexNode): Option[Window] = {
    field match {
      case call: RexCall =>
        call.getOperator match {
          case _: SqlFloorFunction => FloorWindowTranslator(call).toWindow
          case SqlStdOperatorTable.TUMBLE => TumbleWindowTranslator(call).toWindow
          case SqlStdOperatorTable.HOP => SlidingWindowTranslator(call).toWindow
          case SqlStdOperatorTable.SESSION => SessionWindowTranslator(call).toWindow
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

private case class FloorWindowTranslator(call: RexCall) extends WindowTranslator {
  override def toWindow: Option[Window] = {
    val range = unwrapLiteral[TimeUnitRange](call.getOperands.get(1))
    val w = Tumble.over(Literal(range.startUnit.multiplier.longValue(),
      TimeIntervalTypeInfo.INTERVAL_MILLIS))
    call.getType match {
      case TimeModeTypes.PROCTIME => Some(w)
      case TimeModeTypes.ROWTIME => Some(w.on("rowtime"))
      case _ => None
    }
  }
}

private case class TumbleWindowTranslator(call: RexCall) extends WindowTranslator {
  override def toWindow: Option[Window] = {

    if (call.getOperands.size() != 2) {
      throw new TableException("TUMBLE with alignment is not supported yet.")
    }

    val interval = getOperandAsLong(1)
    val w = Tumble.over(Literal(interval, TimeIntervalTypeInfo.INTERVAL_MILLIS))
    call.getType match {
      case TimeModeTypes.PROCTIME => Some(w)
      case TimeModeTypes.ROWTIME => Some(w.on("rowtime"))
      case _ => None
    }
  }
}

private case class SlidingWindowTranslator(call: RexCall) extends WindowTranslator {
  override def toWindow: Option[Window] = {

    if (call.getOperands.size() != 3) {
      throw new TableException("HOP with alignment is not supported yet.")
    }

    val (slide, size) = (getOperandAsLong(1), getOperandAsLong(2))
    val w = Slide
      .over(Literal(size, TimeIntervalTypeInfo.INTERVAL_MILLIS))
      .every(Literal(slide, TimeIntervalTypeInfo.INTERVAL_MILLIS))
    call.getType match {
      case TimeModeTypes.PROCTIME => Some(w)
      case TimeModeTypes.ROWTIME => Some(w.on("rowtime"))
      case _ => None
    }
  }
}

private case class SessionWindowTranslator(call: RexCall) extends WindowTranslator {
  override def toWindow: Option[Window] = {

    if (call.getOperands.size() != 2) {
      throw new TableException("SESSION with alignment is not supported yet")
    }

    val gap = getOperandAsLong(1)
    val w = Session.withGap(Literal(gap, TimeIntervalTypeInfo.INTERVAL_MILLIS))
    call.getType match {
      case TimeModeTypes.PROCTIME => Some(w)
      case TimeModeTypes.ROWTIME => Some(w.on("rowtime"))
      case _ => None
    }
  }
}

object LogicalWindowAggregateRule {
  private[flink] val LOGICAL_WINDOW_PREDICATE = RelOptRule.operand(classOf[LogicalAggregate],
    RelOptRule.operand(classOf[LogicalProject], RelOptRule.none()))

  private[flink] val INSTANCE = new LogicalWindowAggregateRule
}
