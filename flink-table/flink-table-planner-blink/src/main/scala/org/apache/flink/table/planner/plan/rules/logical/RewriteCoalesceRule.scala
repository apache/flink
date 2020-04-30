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

import com.google.common.collect.ImmutableList
import org.apache.calcite.plan.RelOptRule.{any, operand}
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.{Calc, Filter, Join, Project}
import org.apache.calcite.rel.logical.{LogicalCalc, LogicalFilter, LogicalJoin, LogicalProject}
import org.apache.calcite.rex._
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.sql.fun.SqlStdOperatorTable
import org.apache.calcite.tools.RelBuilder
import org.apache.calcite.util.Util

import scala.collection.JavaConversions._

/**
  * Collection of planner rules that transform `Coalesce` to `Case When` RexNode trees.
  *
  * <p>Currently this is only used for natural join, for explicit Coalesce
  * Calcite already replace it with Case When.
  *
  * <p>There are four transformation contexts:
  * <ul>
  * <li>Project project list
  * <li>Join condition
  * <li>Filter condition
  * <li>Calc expression list
  * </ul>
  */
abstract class RewriteCoalesceRule[T <: RelNode](
    clazz: Class[T],
    description: String)
  extends RelOptRule(
    operand(clazz, any),
    description) {

  private class CoalesceToCaseShuttle(rexBuilder: RexBuilder) extends RexShuttle {
    override def visitCall(call: RexCall): RexNode = {
      call.getKind match {
        case SqlKind.COALESCE =>
          val operands = call.getOperands
          if (operands.size == 1) {
            operands.head
          } else {
            val operandsExceptLast = Util.skipLast(call.getOperands)
            val args: ImmutableList.Builder[RexNode] = ImmutableList.builder()
            operandsExceptLast.foreach { operand =>
              args.add(rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_NULL, operand), operand)
            }
            args.add(call.getOperands.last)
            rexBuilder.makeCall(SqlStdOperatorTable.CASE, args.build)
          }
        case _ => super.visitCall(call)
      }
    }
  }

  protected def replace(input: RexNode,
      rexBuilder: RexBuilder): RexNode = {
    val shuttle = new CoalesceToCaseShuttle(rexBuilder)
    input.accept(shuttle)
  }

  protected def existsCoalesce(rexNode: RexNode): Boolean = {
    class CoalesceFinder extends RexVisitorImpl[Unit](true) {
      var found = false

      override def visitCall(call: RexCall): Unit = {
        call.getKind match {
          case SqlKind.COALESCE => found = true
          case _ => super.visitCall(call)
        }
      }

      def isFound: Boolean = found
    }
    val finder = new CoalesceFinder
    rexNode.accept(finder)
    finder.isFound
  }

}

/**
  * Planner rule that rewrites `Coalesce` in filter condition to `Case When`.
  */
class FilterRewriteCoalesceRule extends
  RewriteCoalesceRule(
    classOf[LogicalFilter],
    "FilterRewriteCoalesceRule") {
  override def matches(call: RelOptRuleCall): Boolean = {
    val filter: Filter = call.rel(0)
    existsCoalesce(filter.getCondition)
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val filter: Filter = call.rel(0)
    val relBuilder: RelBuilder = call.builder()
    val rexBuilder: RexBuilder = relBuilder.getRexBuilder
    // transform the filter recursively, may change all the Coalesce in the filter
    // to Case when, this is ok for us now, cause the filter will never be matched again
    // by this rule after the transformation.
    val newCondition = replace(filter.getCondition, rexBuilder)
    val newFilter = relBuilder
      .push(filter.getInput)
      .filter(newCondition)
      .build()
    call.transformTo(newFilter)
  }
}

/**
  * Planner rule that rewrites `Coalesce` in project list to `Case When`.
  */
class ProjectRewriteCoalesceRule extends
  RewriteCoalesceRule(
    classOf[LogicalProject],
    "ProjectRewriteCoalesceRule") {
  override def matches(call: RelOptRuleCall): Boolean = {
    val prj: Project = call.rel(0)
    prj.getProjects.exists(p => existsCoalesce(p))
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val prj: Project = call.rel(0)
    val relBuilder: RelBuilder = call.builder()
    val rexBuilder: RexBuilder = relBuilder.getRexBuilder
    // transform the project recursively, may change all the Coalesce in the project
    // to Case when, this is ok for us now, cause the project will never be matched again
    // by this rule after the transformation.
    val newProjects = prj.getProjects.map(p => replace(p, rexBuilder))
    val newProject = relBuilder
      .push(prj.getInput)
      .project(newProjects)
      .build()
    call.transformTo(newProject)
  }
}

/**
  * Planner rule that rewrites `Coalesce` in join condition to `Case When`.
  */
class JoinRewriteCoalesceRule extends
  RewriteCoalesceRule(
    classOf[LogicalJoin],
    "JoinRewriteCoalesceRule") {
  override def matches(call: RelOptRuleCall): Boolean = {
    val prj: Join = call.rel(0)
    existsCoalesce(prj.getCondition)
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val join: Join = call.rel(0)
    val relBuilder: RelBuilder = call.builder()
    val rexBuilder: RexBuilder = relBuilder.getRexBuilder
    val newCondition = replace(join.getCondition, rexBuilder)
    // transform the join recursively, may change all the Coalesce in the join
    // to Case when, this is ok for us now, cause the join will never be matched again
    // by this rule after the transformation.
    val newJoin = join.copy(
      join.getTraitSet,
      newCondition,
      join.getLeft,
      join.getRight,
      join.getJoinType,
      join.isSemiJoinDone)
    call.transformTo(newJoin)
  }
}

/**
  * Planner rule that rewrites `Coalesce` in calc expression list to `Case When`.
  */
class CalcRewriteCoalesceRule extends
  RewriteCoalesceRule(
    classOf[LogicalCalc],
    "CalcRewriteCoalesceRule") {
  override def matches(call: RelOptRuleCall): Boolean = {
    val calc: Calc = call.rel(0)
    val program = calc.getProgram
    val exprList = program.getExprList
    exprList.exists(p => existsCoalesce(p))
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val calc: Calc = call.rel(0)
    val relBuilder: RelBuilder = call.builder()
    val rexBuilder: RexBuilder = relBuilder.getRexBuilder
    // transform the Calc recursively, may change all the Coalesce in the Calc
    // to Case when, this is ok for us now, cause the Calc will never be matched again
    // by this rule after the transformation.
    val program = calc.getProgram
    val exprList = program.getExprList.map(expr => replace(expr, rexBuilder))
    val builder: RexProgramBuilder = new RexProgramBuilder(
      calc.getInput.getRowType, calc.getCluster.getRexBuilder)
    val list = exprList.map(expr => builder.registerInput(expr))
    if (program.getCondition != null) {
      val conditionIndex = program.getCondition.getIndex
      builder.addCondition(list.get(conditionIndex))
    }
    program.getProjectList.zipWithIndex.foreach {
      case (projectExpr, idx) =>
        val index = projectExpr.getIndex
        builder.addProject(list.get(index).getIndex,
          program.getOutputRowType.getFieldNames.get(idx))
    }

    val newCalc = calc.copy(calc.getTraitSet, calc.getInput, builder.getProgram)
    call.transformTo(newCalc)
  }
}

object RewriteCoalesceRule {
  val FILTER_INSTANCE = new FilterRewriteCoalesceRule
  val PROJECT_INSTANCE = new ProjectRewriteCoalesceRule
  val JOIN_INSTANCE = new JoinRewriteCoalesceRule
  val CALC_INSTANCE = new CalcRewriteCoalesceRule
}
