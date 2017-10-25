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
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.logical.{LogicalFilter, LogicalProject}
import org.apache.calcite.rex.{RexCall, RexNode}
import org.apache.calcite.tools.RelBuilder
import org.apache.flink.table.api.{TableException, ValidationException}
import org.apache.flink.table.calcite.FlinkRelBuilder.NamedWindowProperty
import org.apache.flink.table.expressions._
import org.apache.flink.table.plan.logical.rel.LogicalWindowAggregate
import org.apache.flink.table.validate.BasicOperatorTable

import scala.collection.JavaConversions._

abstract class WindowPropertiesBaseRule(rulePredicate: RelOptRuleOperand, ruleName: String)
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

  def convertWindowNodes(
      builder: RelBuilder,
      project: LogicalProject,
      filter: Option[LogicalFilter],
      innerProject: LogicalProject,
      agg: LogicalWindowAggregate)
    : RelNode = {

    val rexBuilder = builder.getRexBuilder

    val window = agg.getWindow

    val isRowtime = ExpressionUtils.isRowtimeAttribute(window.timeAttribute)
    val isProctime = ExpressionUtils.isProctimeAttribute(window.timeAttribute)

    def propertyName(name: String): String =
      window.aliasAttribute.asInstanceOf[WindowReference].name + name

    val startEndProperties = Seq(
      NamedWindowProperty(propertyName("start"), WindowStart(window.aliasAttribute)),
      NamedWindowProperty(propertyName("end"), WindowEnd(window.aliasAttribute)))

    // allow rowtime/proctime for rowtime windows and proctime for proctime windows
    val timeProperties = if (isRowtime) {
      Seq(
        NamedWindowProperty(propertyName("rowtime"), RowtimeAttribute(window.aliasAttribute)),
        NamedWindowProperty(propertyName("proctime"), ProctimeAttribute(window.aliasAttribute)))
    } else if (isProctime) {
      Seq(NamedWindowProperty(propertyName("proctime"), ProctimeAttribute(window.aliasAttribute)))
    } else {
      Seq()
    }

    val properties = startEndProperties ++ timeProperties

    // retrieve window start and end properties
    builder.push(agg.copy(properties))

    // forward window start and end properties
    builder.project(
      innerProject.getProjects ++ properties.map(np => builder.field(np.name)))

    // function to replace the group auxiliaries with field references
    def replaceGroupAuxiliaries(node: RexNode): RexNode = {
      node match {
        case c: RexCall if isWindowStart(c) =>
          // replace expression by access to window start
          rexBuilder.makeCast(c.getType, builder.field(propertyName("start")), false)

        case c: RexCall if isWindowEnd(c) =>
          // replace expression by access to window end
          rexBuilder.makeCast(c.getType, builder.field(propertyName("end")), false)

        case c: RexCall if isWindowRowtime(c) =>
          if (isProctime) {
            throw ValidationException("A proctime window cannot provide a rowtime attribute.")
          } else if (isRowtime) {
            // replace expression by access to window rowtime
            builder.field(propertyName("rowtime"))
          } else {
            throw TableException("Accessing the rowtime attribute of a window is not yet " +
              "supported in a batch environment.")
          }

        case c: RexCall if isWindowProctime(c) =>
          if (isProctime || isRowtime) {
            // replace expression by access to window proctime
            builder.field(propertyName("proctime"))
          } else {
            throw ValidationException("Proctime is not supported in a batch environment.")
          }

        case c: RexCall =>
          // replace expressions in children
          val newOps = c.getOperands.map(replaceGroupAuxiliaries)
          c.clone(c.getType, newOps)

        case x =>
          // preserve expression
          x
      }
    }

    // replace window auxiliary function by access to window properties
    filter.foreach { f =>
      builder.filter(
        f.getChildExps.map(expr => replaceGroupAuxiliaries(expr))
      )
    }

    // replace window auxiliary function by access to window properties
    builder.project(
      project.getProjects.map(expr => replaceGroupAuxiliaries(expr)),
      project.getRowType.getFieldNames
    )

    builder.build()
  }

  /** Checks if a RexNode is a window start auxiliary function. */
  private def isWindowStart(node: RexNode): Boolean = {
    node match {
      case n: RexCall if n.getOperator.isGroupAuxiliary =>
        n.getOperator match {
          case BasicOperatorTable.TUMBLE_START |
               BasicOperatorTable.HOP_START |
               BasicOperatorTable.SESSION_START
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
          case BasicOperatorTable.TUMBLE_END |
               BasicOperatorTable.HOP_END |
               BasicOperatorTable.SESSION_END
          => true
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
          case BasicOperatorTable.TUMBLE_ROWTIME |
               BasicOperatorTable.HOP_ROWTIME |
               BasicOperatorTable.SESSION_ROWTIME
            => true
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
          case BasicOperatorTable.TUMBLE_PROCTIME |
               BasicOperatorTable.HOP_PROCTIME |
               BasicOperatorTable.SESSION_PROCTIME
            => true
          case _ => false
        }
      case _ => false
    }
  }
}

object WindowPropertiesRule {

  val INSTANCE = new WindowPropertiesBaseRule(
    RelOptRule.operand(classOf[LogicalProject],
      RelOptRule.operand(classOf[LogicalProject],
        RelOptRule.operand(classOf[LogicalWindowAggregate], RelOptRule.none()))),
    "WindowPropertiesRule") {

    override def onMatch(call: RelOptRuleCall): Unit = {

      val project = call.rel(0).asInstanceOf[LogicalProject]
      val innerProject = call.rel(1).asInstanceOf[LogicalProject]
      val agg = call.rel(2).asInstanceOf[LogicalWindowAggregate]

      val converted = convertWindowNodes(call.builder(), project, None, innerProject, agg)

      call.transformTo(converted)
    }
  }
}

object WindowPropertiesHavingRule {

  val INSTANCE = new WindowPropertiesBaseRule(
    RelOptRule.operand(classOf[LogicalProject],
      RelOptRule.operand(classOf[LogicalFilter],
        RelOptRule.operand(classOf[LogicalProject],
          RelOptRule.operand(classOf[LogicalWindowAggregate], RelOptRule.none())))),
    "WindowPropertiesHavingRule") {

    override def onMatch(call: RelOptRuleCall): Unit = {

      val project = call.rel(0).asInstanceOf[LogicalProject]
      val filter = call.rel(1).asInstanceOf[LogicalFilter]
      val innerProject = call.rel(2).asInstanceOf[LogicalProject]
      val agg = call.rel(3).asInstanceOf[LogicalWindowAggregate]

      val converted = convertWindowNodes(call.builder(), project, Some(filter), innerProject, agg)

      call.transformTo(converted)
    }
  }
}

