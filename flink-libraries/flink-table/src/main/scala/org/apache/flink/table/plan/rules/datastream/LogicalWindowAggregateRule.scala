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

import java.util.Calendar

import com.google.common.collect.ImmutableList
import org.apache.calcite.avatica.util.TimeUnitRange
import org.apache.calcite.plan._
import org.apache.calcite.plan.hep.HepRelVertex
import org.apache.calcite.rel.logical.{LogicalAggregate, LogicalProject}
import org.apache.calcite.rex.{RexCall, RexLiteral, RexNode}
import org.apache.calcite.sql.fun.SqlFloorFunction
import org.apache.calcite.util.ImmutableBitSet
import org.apache.flink.table.api.scala.Tumble
import org.apache.flink.table.api.{TableException, TumblingWindow, Window}
import org.apache.flink.table.calcite.FlinkRelBuilder.NamedWindowProperty
import org.apache.flink.table.expressions._
import org.apache.flink.table.functions.EventTimeExtractor
import org.apache.flink.table.plan.logical.rel.LogicalWindowAggregate

import scala.collection.JavaConversions._

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
    val zero = rexBuilder.makeTimestampLiteral(LogicalWindowAggregateRule.TIMESTAMP_ZERO, 3)

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
    val key = agg.getGroupSet.asList()
    val fields = key.flatMap(x => nodeToMaybeWindow(project.getProjects.get(x)) match {
      case Some(w) => Some(x.toInt, w)
      case _ => None
    })
    fields.size match {
      case 0 => None
      case 1 => Some(fields.head)
      case _ => throw new TableException("Multiple windows are not supported")
    }
  }

  private def nodeToMaybeWindow(field: RexNode): Option[Window] = {
    field match {
      case call: RexCall =>
        call.getOperator match {
          case _: SqlFloorFunction => call.getOperands.get(0) match {
            case c: RexCall => if (c.getOperator == EventTimeExtractor) {
              val unit = call.getOperands.get(1)
                .asInstanceOf[RexLiteral].getValue.asInstanceOf[TimeUnitRange]
              return Some(LogicalWindowAggregateRule.timeUnitRangeToWindow(unit)
                .on("rowtime"))
            }
            case _ =>
          }
          case _ =>
        }
      case _ =>
    }
    None
  }
}

object LogicalWindowAggregateRule {
  private[flink] val TIMESTAMP_ZERO = Calendar.getInstance()
  TIMESTAMP_ZERO.setTimeInMillis(0)

  private[flink] val LOGICAL_WINDOW_PREDICATE = RelOptRule.operand(classOf[LogicalAggregate],
    RelOptRule.operand(classOf[LogicalProject], RelOptRule.none()))

  private[flink] val INSTANCE = new LogicalWindowAggregateRule

  private val EXPR_ONE = ExpressionParser.parseExpression("1")

  def timeUnitRangeToWindow(range: TimeUnitRange): TumblingWindow = {
    Tumble over ExpressionUtils.toMilliInterval(EXPR_ONE, range.startUnit.multiplier.longValue())
  }
}

