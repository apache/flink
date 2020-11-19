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

import org.apache.flink.table.planner.calcite.{FlinkRelBuilder, FlinkRelFactories}
import org.apache.flink.table.planner.plan.utils.SortUtil
import org.apache.flink.table.runtime.operators.rank.{ConstantRankRange, RankType}

import org.apache.calcite.plan.RelOptRule.{any, operand}
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.calcite.rel.RelCollations
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.{Aggregate, Correlate, Filter, JoinRelType, Project, Sort}
import org.apache.calcite.rex.{RexCall, RexCorrelVariable, RexFieldAccess, RexInputRef, RexLiteral, RexNode}
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.util.ImmutableBitSet

import java.util

import scala.collection.JavaConversions._

/**
 * Planner rule that rewrites sort correlation to a Rank.
 * Typically, the following plan
 *
 * {{{
 *   LogicalProject(state=[$0], name=[$1])
 *   +- LogicalCorrelate(correlation=[$cor0], joinType=[inner], requiredColumns=[{0}])
 *      :- LogicalAggregate(group=[{0}])
 *      :  +- LogicalProject(state=[$1])
 *      :     +- LogicalTableScan(table=[[default_catalog, default_database, cities]])
 *      +- LogicalSort(sort0=[$1], dir0=[DESC-nulls-last], fetch=[3])
 *         +- LogicalProject(name=[$0], pop=[$2])
 *            +- LogicalFilter(condition=[=($1, $cor0.state)])
 *               +- LogicalTableScan(table=[[default_catalog, default_database, cities]])
 * }}}
 *
 * <p>would be transformed to
 *
 * {{{
 *   LogicalProject(state=[$0], name=[$1])
 *    +- LogicalProject(state=[$1], name=[$0], pop=[$2])
 *       +- LogicalRank(rankType=[ROW_NUMBER], rankRange=[rankStart=1, rankEnd=3],
 *            partitionBy=[$1], orderBy=[$2 DESC], select=[name=$0, state=$1, pop=$2])
 *          +- LogicalTableScan(table=[[default_catalog, default_database, cities]])
 * }}}
 *
 * <p>To match the Correlate, the LHS needs to be a global Aggregate on a scan, the RHS should
 * be a Sort with an equal Filter predicate whose keys are same with the LHS grouping keys.
 *
 * <p>This rule can only be used in HepPlanner.
 */
class CorrelateSortToRankRule extends RelOptRule(
  operand(classOf[Correlate],
    operand(classOf[Aggregate],
      operand(classOf[Project], any())),
    operand(classOf[Sort],
      operand(classOf[Project],
        operand(classOf[Filter], any())))),
  FlinkRelFactories.FLINK_REL_BUILDER,
  "CorrelateSortToRankRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val correlate: Correlate = call.rel(0)
    if (correlate.getJoinType != JoinRelType.INNER) {
      return false
    }
    val agg: Aggregate = call.rel(1)
    if (agg.getAggCallList.size() > 0
      || agg.getGroupSets.size() > 1
      || agg.getGroupSet.cardinality() != 1) {
      // only one group field is supported now
      return false
    }
    val aggInput: Project = call.rel(2)
    if (!aggInput.isMapping) {
      return false
    }
    val sort: Sort = call.rel(3)
    if (sort.offset != null || sort.fetch == null) {
      // 1. we can not describe the offset using rank
      // 2. there is no need to transform to rank if no fetch limit
      return false
    }
    val sortInput: Project = call.rel(4)
    if (!sortInput.isMapping) {
      return false
    }
    val filter: Filter = call.rel(5)
    val condition = filter.getCondition
    if (condition.getKind != SqlKind.EQUALS) {
      return false
    }
    // only support one partition key
    val (inputRef, fieldAccess) = resolveFilterCondition(condition)
    if (inputRef == null) {
      return false
    }
    val variable = fieldAccess.getReferenceExpr.asInstanceOf[RexCorrelVariable]
    if (!variable.id.equals(correlate.getCorrelationId)) {
      return false
    }
    aggInput.getInput.getDigest.equals(filter.getInput.getDigest)
  }

  /**
   * Resolves the filter condition with specific pattern: input ref and field access.
   *
   * @param condition The join condition
   * @return tuple of operands (RexInputRef, RexFieldAccess),
   *         or null if the pattern does not match
   */
  def resolveFilterCondition(condition: RexNode): (RexInputRef, RexFieldAccess) = {
    val condCall = condition.asInstanceOf[RexCall]
    val operand0 = condCall.getOperands.get(0)
    val operand1 = condCall.getOperands.get(1)
    if (operand0.isA(SqlKind.INPUT_REF) && operand1.isA(SqlKind.FIELD_ACCESS)) {
      (operand0.asInstanceOf[RexInputRef], operand1.asInstanceOf[RexFieldAccess])
    } else if (operand0.isA(SqlKind.FIELD_ACCESS) && operand1.isA(SqlKind.INPUT_REF)) {
      (operand1.asInstanceOf[RexInputRef], operand0.asInstanceOf[RexFieldAccess])
    } else {
      (null, null)
    }
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val builder = call.builder()

    val sort: Sort = call.rel(3)
    val sortInput: Project = call.rel(4)
    val filter: Filter = call.rel(5)

    val partitionKey: ImmutableBitSet =
      ImmutableBitSet.of(resolveFilterCondition(filter.getCondition)._1.getIndex)

    val baseType: RelDataType = sortInput.getInput().getRowType
    val projects = new util.ArrayList[RexNode]()
    partitionKey.asList().foreach(k => projects.add(RexInputRef.of(k, baseType)))
    projects.addAll(sortInput.getProjects)

    val oriCollation = sort.getCollation
    val newFieldCollations = oriCollation.getFieldCollations.map { fc =>
      val newFieldIdx = sortInput.getProjects.get(fc.getFieldIndex)
        .asInstanceOf[RexInputRef].getIndex
      fc.withFieldIndex(newFieldIdx)
    }
    val newCollation = RelCollations.of(newFieldCollations)

    val newRel = builder
      .push(filter.getInput()).asInstanceOf[FlinkRelBuilder]
      .rank(
        partitionKey,
        newCollation,
        RankType.ROW_NUMBER,
        new ConstantRankRange(
          1,
          sort.fetch.asInstanceOf[RexLiteral].getValueAs(classOf[java.lang.Long])),
        null,
        outputRankNumber = false)
      .project(projects)
      .build()

    call.transformTo(newRel)
  }
}

object CorrelateSortToRankRule {
  val INSTANCE = new CorrelateSortToRankRule
}
