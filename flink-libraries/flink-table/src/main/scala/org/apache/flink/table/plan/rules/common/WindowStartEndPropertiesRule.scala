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

package org.apache.flink.table.plan.rules.common

import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall, RelOptRuleOperand}
import org.apache.calcite.rel.logical.{LogicalFilter, LogicalProject}
import org.apache.calcite.rex.{RexCall, RexNode}
import org.apache.calcite.sql.fun.SqlStdOperatorTable
import org.apache.calcite.tools.RelBuilder
import org.apache.flink.table.calcite.FlinkRelBuilder.NamedWindowProperty
import org.apache.flink.table.expressions.{WindowEnd, WindowStart}
import org.apache.flink.table.plan.logical.rel.LogicalWindowAggregate

import scala.collection.JavaConversions._

abstract class WindowStartEndPropertiesBaseRule(rulePredicate: RelOptRuleOperand, ruleName: String)
  extends RelOptRule(rulePredicate, ruleName) {

  override def matches(call: RelOptRuleCall): Boolean = {
    val project = call.rel(0).asInstanceOf[LogicalProject]
    // project includes at least on group auxiliary function

    def hasGroupAuxiliaries(node: RexNode): Boolean = {
      node match {
        case c: RexCall if c.getOperator.isGroupAuxiliary => true
        case c: RexCall =>
          c.operands.exists(hasGroupAuxiliaries)
        case _ => false
      }
    }

    project.getProjects.exists(hasGroupAuxiliaries)
  }

  def replaceGroupAuxiliaries(node: RexNode, relBuilder: RelBuilder): RexNode = {
    val rexBuilder = relBuilder.getRexBuilder
    node match {
      case c: RexCall if isWindowStart(c) =>
        // replace expression by access to window start
        rexBuilder.makeCast(c.getType, relBuilder.field("w$start"), false)
      case c: RexCall if isWindowEnd(c) =>
        // replace expression by access to window end
        rexBuilder.makeCast(c.getType, relBuilder.field("w$end"), false)
      case c: RexCall =>
        // replace expressions in children
        val newOps = c.getOperands.map(x => replaceGroupAuxiliaries(x, relBuilder))
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
          case SqlStdOperatorTable.TUMBLE_START |
               SqlStdOperatorTable.HOP_START |
               SqlStdOperatorTable.SESSION_START
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
          case SqlStdOperatorTable.TUMBLE_END |
               SqlStdOperatorTable.HOP_END |
               SqlStdOperatorTable.SESSION_END
          => true
          case _ => false
        }
      case _ => false
    }
  }
}

object WindowStartEndPropertiesRule {

  val INSTANCE = new WindowStartEndPropertiesBaseRule(
    RelOptRule.operand(classOf[LogicalProject],
      RelOptRule.operand(classOf[LogicalProject],
        RelOptRule.operand(classOf[LogicalWindowAggregate], RelOptRule.none()))),
    "WindowStartEndPropertiesRule") {

    override def onMatch(call: RelOptRuleCall): Unit = {

      val project = call.rel(0).asInstanceOf[LogicalProject]
      val innerProject = call.rel(1).asInstanceOf[LogicalProject]
      val agg = call.rel(2).asInstanceOf[LogicalWindowAggregate]

      // Retrieve window start and end properties
      val builder = call.builder()
      builder.push(LogicalWindowAggregate.create(
        agg.getWindow,
        Seq(
          NamedWindowProperty("w$start", WindowStart(agg.getWindow.aliasAttribute)),
          NamedWindowProperty("w$end", WindowEnd(agg.getWindow.aliasAttribute))),
        agg)
      )

      // forward window start and end properties
      builder.project(
        innerProject.getProjects ++ Seq(builder.field("w$start"), builder.field("w$end")))

      // replace window auxiliary function by access to window properties
      builder.project(
        project.getProjects.map(expr => replaceGroupAuxiliaries(expr, builder))
      )
      val res = builder.build()
      call.transformTo(res)
    }
  }
}

object WindowStartEndPropertiesHavingRule {

  val INSTANCE = new WindowStartEndPropertiesBaseRule(
    RelOptRule.operand(classOf[LogicalProject],
      RelOptRule.operand(classOf[LogicalFilter],
        RelOptRule.operand(classOf[LogicalProject],
          RelOptRule.operand(classOf[LogicalWindowAggregate], RelOptRule.none())))),
    "WindowStartEndPropertiesHavingRule") {

    override def onMatch(call: RelOptRuleCall): Unit = {

      val project = call.rel(0).asInstanceOf[LogicalProject]
      val filter = call.rel(1).asInstanceOf[LogicalFilter]
      val innerProject = call.rel(2).asInstanceOf[LogicalProject]
      val agg = call.rel(3).asInstanceOf[LogicalWindowAggregate]

      // Retrieve window start and end properties
      val builder = call.builder()
      builder.push(LogicalWindowAggregate.create(
        agg.getWindow,
        Seq(
          NamedWindowProperty("w$start", WindowStart(agg.getWindow.aliasAttribute)),
          NamedWindowProperty("w$end", WindowEnd(agg.getWindow.aliasAttribute))),
        agg)
      )

      // forward window start and end properties
      builder.project(
        innerProject.getProjects ++ Seq(builder.field("w$start"), builder.field("w$end")))

      // replace window auxiliary function by access to window properties
      builder.filter(
        filter.getChildExps.map(expr => replaceGroupAuxiliaries(expr, builder))
      )

      // replace window auxiliary function by access to window properties
      builder.project(
        project.getProjects.map(expr => replaceGroupAuxiliaries(expr, builder))
      )

      val res = builder.build()
      call.transformTo(res)
    }
  }
}

