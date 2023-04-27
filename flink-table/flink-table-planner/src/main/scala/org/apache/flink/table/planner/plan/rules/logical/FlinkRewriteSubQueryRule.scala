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

import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall, RelOptRuleOperand}
import org.apache.calcite.plan.RelOptRule.{any, operandJ}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.{Aggregate, Filter, RelFactories}
import org.apache.calcite.rex.{RexShuttle, _}
import org.apache.calcite.sql.`type`.SqlTypeFamily
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.sql.fun.SqlCountAggFunction
import org.apache.calcite.tools.RelBuilderFactory

import scala.collection.JavaConversions._

/**
 * Planner rule that rewrites scalar query in filter like: `select * from T1 where (select count(*)
 * from T2) > 0` to `select * from T1 where exists (select * from T2)`, which could be converted to
 * SEMI join by [[FlinkSubQueryRemoveRule]].
 *
 * Without this rule, the original query will be rewritten to a filter on a join on an aggregate by
 * [[org.apache.calcite.rel.rules.SubQueryRemoveRule]]. the full logical plan is
 * {{{
 * LogicalProject(a=[$0], b=[$1], c=[$2])
 * +- LogicalJoin(condition=[$3], joinType=[semi])
 *    :- LogicalTableScan(table=[[x, source: [TestTableSource(a, b, c)]]])
 *    +- LogicalProject($f0=[IS NOT NULL($0)])
 *       +- LogicalAggregate(group=[{}], m=[MIN($0)])
 *          +- LogicalProject(i=[true])
 *             +- LogicalTableScan(table=[[y, source: [TestTableSource(d, e, f)]]])
 * }}}
 */
class FlinkRewriteSubQueryRule(
    operand: RelOptRuleOperand,
    relBuilderFactory: RelBuilderFactory,
    description: String)
  extends RelOptRule(operand, relBuilderFactory, description) {

  override def onMatch(call: RelOptRuleCall): Unit = {
    val filter: Filter = call.rel(0)
    val condition = filter.getCondition
    val newCondition = rewriteScalarQuery(condition)
    if (condition.equals(newCondition)) {
      return
    }

    val newFilter = filter.copy(filter.getTraitSet, filter.getInput, newCondition)
    call.transformTo(newFilter)
  }

  // scalar query like: `(select count(*) from T) > 0` can be converted to `exists(select * from T)`
  def rewriteScalarQuery(condition: RexNode): RexNode = {
    condition.accept(new RexShuttle() {
      override def visitCall(call: RexCall): RexNode = {
        val subQuery = getSupportedScalarQuery(call)
        subQuery match {
          case Some(sq) =>
            val aggInput = sq.rel.getInput(0)
            RexSubQuery.exists(aggInput)
          case _ => super.visitCall(call)
        }
      }
    })
  }

  private def isScalarQuery(n: RexNode): Boolean = n.isA(SqlKind.SCALAR_QUERY)

  private def getSupportedScalarQuery(call: RexCall): Option[RexSubQuery] = {
    // check the RexNode is a RexLiteral which's value is between 0 and 1
    def isBetween0And1(n: RexNode, include0: Boolean, include1: Boolean): Boolean = {
      n match {
        case l: RexLiteral =>
          l.getTypeName.getFamily match {
            case SqlTypeFamily.NUMERIC if l.getValue != null =>
              val v = l.getValue.toString.toDouble
              (0.0 < v && v < 1.0) || (include0 && v == 0.0) || (include1 && v == 1.0)
            case _ => false
          }
        case _ => false
      }
    }

    // check the RelNode is a Aggregate which has only count aggregate call with empty args
    def isCountStarAggWithoutGroupBy(n: RelNode): Boolean = {
      n match {
        case agg: Aggregate =>
          if (agg.getGroupCount == 0 && agg.getAggCallList.size() == 1) {
            val aggCall = agg.getAggCallList.head
            !aggCall.isDistinct &&
            aggCall.filterArg < 0 &&
            aggCall.getArgList.isEmpty &&
            aggCall.getAggregation.isInstanceOf[SqlCountAggFunction]
          } else {
            false
          }
        case _ => false
      }
    }

    call.getKind match {
      // (select count(*) from T) > X (X is between 0 (inclusive) and 1 (exclusive))
      case SqlKind.GREATER_THAN if isScalarQuery(call.operands.head) =>
        val subQuery = call.operands.head.asInstanceOf[RexSubQuery]
        if (
          isCountStarAggWithoutGroupBy(subQuery.rel) &&
          isBetween0And1(call.operands.last, include0 = true, include1 = false)
        ) {
          Some(subQuery)
        } else {
          None
        }
      // (select count(*) from T) >= X (X is between 0 (exclusive) and 1 (inclusive))
      case SqlKind.GREATER_THAN_OR_EQUAL if isScalarQuery(call.operands.head) =>
        val subQuery = call.operands.head.asInstanceOf[RexSubQuery]
        if (
          isCountStarAggWithoutGroupBy(subQuery.rel) &&
          isBetween0And1(call.operands.last, include0 = false, include1 = true)
        ) {
          Some(subQuery)
        } else {
          None
        }
      // X < (select count(*) from T) (X is between 0 (inclusive) and 1 (exclusive))
      case SqlKind.LESS_THAN if isScalarQuery(call.operands.last) =>
        val subQuery = call.operands.last.asInstanceOf[RexSubQuery]
        if (
          isCountStarAggWithoutGroupBy(subQuery.rel) &&
          isBetween0And1(call.operands.head, include0 = true, include1 = false)
        ) {
          Some(subQuery)
        } else {
          None
        }
      // X <= (select count(*) from T) (X is between 0 (exclusive) and 1 (inclusive))
      case SqlKind.LESS_THAN_OR_EQUAL if isScalarQuery(call.operands.last) =>
        val subQuery = call.operands.last.asInstanceOf[RexSubQuery]
        if (
          isCountStarAggWithoutGroupBy(subQuery.rel) &&
          isBetween0And1(call.operands.head, include0 = false, include1 = true)
        ) {
          Some(subQuery)
        } else {
          None
        }
      case _ => None
    }
  }
}

object FlinkRewriteSubQueryRule {

  val FILTER = new FlinkRewriteSubQueryRule(
    operandJ(classOf[Filter], null, RexUtil.SubQueryFinder.FILTER_PREDICATE, any),
    RelFactories.LOGICAL_BUILDER,
    "FlinkRewriteSubQueryRule:Filter")

}
