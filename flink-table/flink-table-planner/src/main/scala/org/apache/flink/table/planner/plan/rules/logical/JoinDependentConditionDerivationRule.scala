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

import org.apache.flink.table.planner.plan.utils.FlinkRexUtil

import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall, RelOptUtil}
import org.apache.calcite.plan.RelOptRule.{any, operand}
import org.apache.calcite.rel.core.JoinRelType
import org.apache.calcite.rel.logical.LogicalJoin
import org.apache.calcite.rex.{RexNode, RexUtil}
import org.apache.calcite.util.ImmutableBitSet

import java.util.function.Predicate

/**
 * Planner Rule that extracts some sub-conditions in the Join OR condition that can be pushed
 * into join inputs by [[FlinkFilterJoinRule]].
 *
 * <p>For example, there is a join query (table A join table B):
 * {{{
 * SELECT * FROM A, B WHERE A.f1 = B.f1 AND ((A.f2 = 'aaa1' AND B.f2 = 'bbb1') OR
 * (A.f2 = 'aaa2' AND B.f2 = 'bbb2'))
 * }}}
 *
 * <p>Hence the query rewards optimizers that can analyze complex join conditions which cannot be
 * pushed below the join, but still derive filters from such join conditions. It could immediately
 * filter the scan(A) with the condition: (A.f2 = 'aaa1' OR A.f2 = 'aaa2').
 *
 * <p>After join condition dependent optimization, the query will be:
 * {{{
 * SELECT * FROM A, B WHERE A.f1 = B.f1 AND
 * ((A.f2 = 'aaa1' AND B.f2 = 'bbb1') OR (A.f2 = 'aaa2' AND B.f2 = 'bbb2'))
 * AND (A.f2 = 'aaa1' OR A.f2 = 'aaa2') AND (B.f2 = 'bbb1' OR B.f2 = 'bbb2')
 * }}}.
 *
 * <p>Note: This class can only be used in HepPlanner with RULE_SEQUENCE.
 */
class JoinDependentConditionDerivationRule
  extends RelOptRule(operand(classOf[LogicalJoin], any()), "JoinDependentConditionDerivationRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val join: LogicalJoin = call.rel(0)
    // TODO supports more join type
    join.getJoinType == JoinRelType.INNER
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val join: LogicalJoin = call.rel(0)
    val builder = call.builder()
    val rexBuilder = builder.getRexBuilder

    // assumed that cond is already simplified by SimplifyJoinConditionRule
    val cond = join.getCondition
    val leftCond = FlinkRexUtil.extract(rexBuilder, cond, isPushable(leftInputs(join)))
    val rightCond = FlinkRexUtil.extract(rexBuilder, cond, isPushable(rightInputs(join)))

    if (!(leftCond.isAlwaysTrue && rightCond.isAlwaysTrue)) {
      val newCondExp = FlinkRexUtil.simplify(
        rexBuilder,
        builder.and(cond, leftCond, rightCond),
        join.getCluster.getPlanner.getExecutor)

      if (!newCondExp.equals(join.getCondition)) {
        val newJoin = join.copy(
          join.getTraitSet,
          newCondExp,
          join.getLeft,
          join.getRight,
          join.getJoinType,
          join.isSemiJoinDone)

        call.transformTo(newJoin)
      }
    }
  }

  private def isPushable(sideInputs: ImmutableBitSet): Predicate[RexNode] = {
    // deterministic rex containing inputs only from particular side(left or right)
    (rex: RexNode) =>
      RexUtil.isDeterministic(rex) && sideInputs.contains(RelOptUtil.InputFinder.bits(rex))
  }

  private def leftInputs(join: LogicalJoin): ImmutableBitSet = {
    require(join.getSystemFieldList.size() == 0)
    ImmutableBitSet.range(0, join.getLeft.getRowType.getFieldCount)
  }

  private def rightInputs(join: LogicalJoin): ImmutableBitSet = {
    require(join.getSystemFieldList.size() == 0)
    ImmutableBitSet.range(join.getLeft.getRowType.getFieldCount, join.getRowType.getFieldCount)
  }
}

object JoinDependentConditionDerivationRule {
  val INSTANCE = new JoinDependentConditionDerivationRule
}
