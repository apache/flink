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

import org.apache.calcite.plan.hep.HepRelVertex
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.logical.LogicalProject
import org.apache.calcite.rex.{RexCall, RexNode}
import org.apache.calcite.sql.fun.SqlStdOperatorTable
import org.apache.flink.table.calcite.FlinkRelBuilder.NamedWindowProperty
import org.apache.flink.table.expressions.{WindowEnd, WindowStart}
import org.apache.flink.table.plan.logical._
import org.apache.flink.table.plan.logical.rel.LogicalWindowAggregate

import scala.collection.JavaConversions._

class WindowExpressionRule
  extends RelOptRule(
    WindowExpressionRule.WINDOW_EXPRESSION_RULE_PREDICATE,
    "WindowExpressionRule") {
  override def matches(call: RelOptRuleCall): Boolean = {
    val project = call.rel(0).asInstanceOf[LogicalProject]
    val found = project.getProjects.find {
      case c: RexCall => c.getOperator.isGroupAuxiliary
      case _ => false
    }
    found.isDefined
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val project = call.rel(0).asInstanceOf[LogicalProject]
    val innerProject = WindowExpressionRule.getInput[LogicalProject](project.getInput)
    val agg = WindowExpressionRule.getInput[LogicalWindowAggregate](innerProject.getInput)

    // Create an additional project to conform with types
    val transformed = call.builder()
    val rexBuilder = transformed.getRexBuilder
    transformed.push(LogicalWindowAggregate.create(
      agg.getWindow,
      Seq(
        NamedWindowProperty("w$start", WindowStart(agg.getWindow.alias.get)),
        NamedWindowProperty("w$end", WindowEnd(agg.getWindow.alias.get))
      ), agg)
    )
    val aggSize = transformed.fields().size()

    transformed
      .project(innerProject.getProjects ++ Seq(
        rexBuilder.makeInputRef(transformed.peek(), aggSize - 2),
        rexBuilder.makeInputRef(transformed.peek(), aggSize - 1)
      ))

    val n = transformed.fields().size()
    val (windowStartExprIdx, windowEndExprIdx) = (n - 2, n - 1)

    val windowStart = rexBuilder.makeInputRef(transformed.peek(), windowStartExprIdx)
    val windowEnd = rexBuilder.makeInputRef(transformed.peek(), windowEndExprIdx)

    transformed.project(
      project.getProjects.map(x =>
        WindowExpressionRule.identifyAuxiliaryExpr(x, agg.getWindow) match {
          case Some(WindowStart(_)) => rexBuilder.makeCast(x.getType, windowStart, false)
          case Some(WindowEnd(_)) => rexBuilder.makeCast(x.getType, windowEnd, false)
          case _ => x
      })
    )
    val res = transformed.build()
    call.transformTo(res)
  }
}

object WindowExpressionRule {
  private val WINDOW_EXPRESSION_RULE_PREDICATE =
    RelOptRule.operand(classOf[LogicalProject],
      RelOptRule.operand(classOf[LogicalProject],
        RelOptRule.operand(classOf[LogicalWindowAggregate], RelOptRule.none())))

  val INSTANCE = new WindowExpressionRule

  private def getInput[T](node: RelNode) : T = {
    node.asInstanceOf[HepRelVertex].getCurrentRel.asInstanceOf[T]
  }

  private def identifyAuxiliaryExpr(node: RexNode, window: LogicalWindow) = {
    node match {
      case n: RexCall if n.getOperator.isGroupAuxiliary =>
        n.getOperator match {
          case SqlStdOperatorTable.TUMBLE_START
               | SqlStdOperatorTable.HOP_START
               | SqlStdOperatorTable.SESSION_START
            => Some(WindowStart(window.alias.get))
          case SqlStdOperatorTable.TUMBLE_END
               | SqlStdOperatorTable.HOP_END
               | SqlStdOperatorTable.SESSION_END
            => Some(WindowEnd(window.alias.get))
        }
      case _ => None
    }
  }
}
