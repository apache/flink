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

package org.apache.flink.table.planner.plan.rules.logical

import org.apache.flink.table.api.{TableException, ValidationException}
import org.apache.flink.table.planner.expressions.{PlannerNamedWindowProperty, PlannerProctimeAttribute, PlannerRowtimeAttribute, PlannerWindowEnd, PlannerWindowReference, PlannerWindowStart}
import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable
import org.apache.flink.table.planner.plan.logical.LogicalWindow
import org.apache.flink.table.planner.plan.nodes.calcite.LogicalWindowAggregate
import org.apache.flink.table.planner.plan.utils.AggregateUtil
import org.apache.flink.table.types.logical.LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks.hasRoot

import org.apache.calcite.plan.RelOptRule._
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.logical.{LogicalFilter, LogicalProject}
import org.apache.calcite.rex.{RexCall, RexNode}
import org.apache.calcite.tools.RelBuilder

import scala.collection.JavaConversions._

class WindowPropertiesRule extends RelOptRule(
  operand(classOf[LogicalProject],
    operand(classOf[LogicalProject],
      operand(classOf[LogicalWindowAggregate], none()))),
  "WindowPropertiesRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val project: LogicalProject = call.rel(0)
    // project includes at least one group auxiliary function
    project.getProjects.exists(WindowPropertiesRules.hasGroupAuxiliaries)
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val project: LogicalProject = call.rel(0)
    val innerProject: LogicalProject = call.rel(1)
    val agg: LogicalWindowAggregate = call.rel(2)

    val converted = WindowPropertiesRules.convertWindowNodes(
      call.builder(), project, None, innerProject, agg)

    call.transformTo(converted)
  }
}

class WindowPropertiesHavingRule extends RelOptRule(
  RelOptRule.operand(classOf[LogicalProject],
    RelOptRule.operand(classOf[LogicalFilter],
      RelOptRule.operand(classOf[LogicalProject],
        RelOptRule.operand(classOf[LogicalWindowAggregate], RelOptRule.none())))),
  "WindowPropertiesHavingRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val project: LogicalProject = call.rel(0)
    val filter: LogicalFilter = call.rel(1)

    project.getProjects.exists(WindowPropertiesRules.hasGroupAuxiliaries) ||
      WindowPropertiesRules.hasGroupAuxiliaries(filter.getCondition)
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val project: LogicalProject = call.rel(0)
    val filter: LogicalFilter = call.rel(1)
    val innerProject: LogicalProject = call.rel(2)
    val agg: LogicalWindowAggregate = call.rel(3)

    val converted = WindowPropertiesRules.convertWindowNodes(
      call.builder(), project, Some(filter), innerProject, agg)

    call.transformTo(converted)
  }
}

object WindowPropertiesRules {

  val WINDOW_PROPERTIES_HAVING_RULE = new WindowPropertiesHavingRule

  val WINDOW_PROPERTIES_RULE = new WindowPropertiesRule

  def convertWindowNodes(
      builder: RelBuilder,
      project: LogicalProject,
      filter: Option[LogicalFilter],
      innerProject: LogicalProject,
      agg: LogicalWindowAggregate): RelNode = {
    val w = agg.getWindow
    val windowType = getWindowType(w)

    val startEndProperties = Seq(
     new PlannerNamedWindowProperty(
        propertyName(w, "start"), new PlannerWindowStart(w.aliasAttribute)),
      new PlannerNamedWindowProperty(
        propertyName(w, "end"), new PlannerWindowEnd(w.aliasAttribute)))

    // allow rowtime/proctime for rowtime windows and proctime for proctime windows
    val timeProperties = windowType match {
      case 'streamRowtime =>
        Seq(
          new PlannerNamedWindowProperty(propertyName(w, "rowtime"),
            new PlannerRowtimeAttribute(w.aliasAttribute)),
          new PlannerNamedWindowProperty(propertyName(w, "proctime"),
            new PlannerProctimeAttribute(w.aliasAttribute)))
      case 'streamProctime =>
        Seq(new PlannerNamedWindowProperty(propertyName(w, "proctime"),
          new PlannerProctimeAttribute(w.aliasAttribute)))
      case 'batchRowtime =>
        Seq(new PlannerNamedWindowProperty(propertyName(w, "rowtime"),
          new PlannerRowtimeAttribute(w.aliasAttribute)))
      case _ =>
        throw new TableException("Unknown window type encountered. Please report this bug.")
    }

    val properties = startEndProperties ++ timeProperties

    // retrieve window start and end properties
    builder.push(agg.copy(properties))

    // forward window start and end properties
    builder.project(innerProject.getProjects ++ properties.map(np => builder.field(np.getName)))

    // replace window auxiliary function in filter by access to window properties
    filter.foreach { f =>
      builder.filter(replaceGroupAuxiliaries(f.getCondition, w, builder))
    }

    // replace window auxiliary unctions in projection by access to window properties
    builder.project(
      project.getProjects.map(expr => replaceGroupAuxiliaries(expr, w, builder)),
      project.getRowType.getFieldNames
    )

    builder.build()
  }

  private def getWindowType(window: LogicalWindow): Symbol = {
    if (AggregateUtil.isRowtimeAttribute(window.timeAttribute)) {
      'streamRowtime
    } else if (AggregateUtil.isProctimeAttribute(window.timeAttribute)) {
      'streamProctime
    } else if (hasRoot(
          window.timeAttribute.getOutputDataType.getLogicalType,
          TIMESTAMP_WITHOUT_TIME_ZONE)) {
      'batchRowtime
    } else {
      throw new TableException("Unknown window type encountered. Please report this bug.")
    }
  }

  /** Generates a property name for a window. */
  private def propertyName(window: LogicalWindow, name: String): String =
    window.aliasAttribute.asInstanceOf[PlannerWindowReference].getName + name

  /** Replace group auxiliaries with field references. */
  def replaceGroupAuxiliaries(
      node: RexNode,
      window: LogicalWindow,
      builder: RelBuilder): RexNode = {
    val rexBuilder = builder.getRexBuilder
    val windowType = getWindowType(window)

    node match {
      case c: RexCall if isWindowStart(c) =>
        // replace expression by access to window start
        rexBuilder.makeCast(c.getType, builder.field(propertyName(window, "start")), false)

      case c: RexCall if isWindowEnd(c) =>
        // replace expression by access to window end
        rexBuilder.makeCast(c.getType, builder.field(propertyName(window, "end")), false)

      case c: RexCall if isWindowRowtime(c) =>
        windowType match {
          case 'streamRowtime | 'batchRowtime =>
            // replace expression by access to window rowtime
            rexBuilder.makeCast(c.getType, builder.field(propertyName(window, "rowtime")), false)
          case 'streamProctime =>
            throw new ValidationException("A proctime window cannot provide a rowtime attribute.")
          case _ =>
            throw new TableException("Unknown window type encountered. Please report this bug.")
        }

      case c: RexCall if isWindowProctime(c) =>
        windowType match {
          case 'streamProctime | 'streamRowtime =>
            // replace expression by access to window proctime
            rexBuilder.makeCast(c.getType, builder.field(propertyName(window, "proctime")), false)
          case 'batchRowtime =>
            throw new ValidationException(
              "PROCTIME window property is not supported in batch queries.")
          case _ =>
            throw new TableException("Unknown window type encountered. Please report this bug.")
        }

      case c: RexCall =>
        // replace expressions in children
        val newOps = c.getOperands.map(replaceGroupAuxiliaries(_, window, builder))
        c.clone(c.getType, newOps)

      case x =>
        // preserve expression
        x
    }
  }

  /** Checks if a RexNode is a window start auxiliary function. */
  private def isWindowStart(node: RexNode): Boolean = {
    node match {
      case n: RexCall if n.getOperator.isGroupAuxiliary =>
        n.getOperator match {
          case FlinkSqlOperatorTable.TUMBLE_START |
               FlinkSqlOperatorTable.HOP_START |
               FlinkSqlOperatorTable.SESSION_START
          => true
          case _ => false
        }
      case _ => false
    }
  }

  /** Checks if a RexNode is a window end auxiliary function. */
  private def isWindowEnd(node: RexNode): Boolean = {
    node match {
      case n: RexCall if n.getOperator.isGroupAuxiliary =>
        n.getOperator match {
          case FlinkSqlOperatorTable.TUMBLE_END |
               FlinkSqlOperatorTable.HOP_END |
               FlinkSqlOperatorTable.SESSION_END => true
          case _ => false
        }
      case _ => false
    }
  }

  /** Checks if a RexNode is a window rowtime auxiliary function. */
  private def isWindowRowtime(node: RexNode): Boolean = {
    node match {
      case n: RexCall if n.getOperator.isGroupAuxiliary =>
        n.getOperator match {
          case FlinkSqlOperatorTable.TUMBLE_ROWTIME |
               FlinkSqlOperatorTable.HOP_ROWTIME |
               FlinkSqlOperatorTable.SESSION_ROWTIME => true
          case _ => false
        }
      case _ => false
    }
  }

  /** Checks if a RexNode is a window proctime auxiliary function. */
  private def isWindowProctime(node: RexNode): Boolean = {
    node match {
      case n: RexCall if n.getOperator.isGroupAuxiliary =>
        n.getOperator match {
          case FlinkSqlOperatorTable.TUMBLE_PROCTIME |
               FlinkSqlOperatorTable.HOP_PROCTIME |
               FlinkSqlOperatorTable.SESSION_PROCTIME => true
          case _ => false
        }
      case _ => false
    }
  }

  def hasGroupAuxiliaries(node: RexNode): Boolean = {
    node match {
      case c: RexCall if c.getOperator.isGroupAuxiliary => true
      case c: RexCall =>
        c.operands.exists(hasGroupAuxiliaries)
      case _ => false
    }
  }

  def hasGroupFunction(node: RexNode): Boolean = {
    node match {
      case c: RexCall if c.getOperator.isGroup => true
      case c: RexCall => c.operands.exists(hasGroupFunction)
      case _ => false
    }
  }

}
