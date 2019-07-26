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

import org.apache.calcite.plan.RelOptRule.{any, operand}
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall, RelOptUtil}
import org.apache.calcite.rel.core.{Join, JoinRelType}
import org.apache.calcite.rel.logical.LogicalJoin
import org.apache.calcite.rex.RexNode

import scala.collection.JavaConversions._
import scala.collection.mutable

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
  extends RelOptRule(
    operand(classOf[LogicalJoin], any()),
    "JoinDependentConditionDerivationRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val join: LogicalJoin = call.rel(0)
    // TODO supports more join type
    join.getJoinType == JoinRelType.INNER
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val join: LogicalJoin = call.rel(0)
    val conjunctions = RelOptUtil.conjunctions(join.getCondition)

    val builder = call.builder()
    val additionalConditions = new mutable.ArrayBuffer[RexNode]

    // and
    conjunctions.foreach { conjunctionRex =>
      val disjunctions = RelOptUtil.disjunctions(conjunctionRex)

      // Apart or: (A.f2 = 'aaa1' and B.f2 = 'bbb1') or (A.f2 = 'aaa2' and B.f2 = 'bbb2')
      if (disjunctions.size() > 1) {

        val leftDisjunctions = new mutable.ArrayBuffer[RexNode]
        val rightDisjunctions = new mutable.ArrayBuffer[RexNode]
        disjunctions.foreach { disjunctionRex =>

          val leftConjunctions = new mutable.ArrayBuffer[RexNode]
          val rightConjunctions = new mutable.ArrayBuffer[RexNode]

          // Apart and: A.f2 = 'aaa1' and B.f2 = 'bbb1'
          RelOptUtil.conjunctions(disjunctionRex).foreach { cond =>
            val rCols = RelOptUtil.InputFinder.bits(cond).map(_.intValue())

            // May have multi conditions, eg: A.f2 = 'aaa1' and A.f3 = 'aaa3' and B.f2 = 'bbb1'
            if (rCols.forall(fromJoinLeft(join, _))) {
              leftConjunctions += cond
            } else if (rCols.forall(!fromJoinLeft(join, _))) {
              rightConjunctions += cond
            }
          }

          // It is true if conjunction conditions is empty.
          leftDisjunctions += builder.and(leftConjunctions)
          rightDisjunctions += builder.and(rightConjunctions)
        }

        // TODO Consider whether it is worth doing a filter if we have histogram.
        if (leftDisjunctions.nonEmpty) {
          additionalConditions += builder.or(leftDisjunctions)
        }
        if (rightDisjunctions.nonEmpty) {
          additionalConditions += builder.or(rightDisjunctions)
        }
      }
    }

    if (additionalConditions.nonEmpty) {
      val newCondExp = FlinkRexUtil.simplify(
        builder.getRexBuilder,
        builder.and(conjunctions ++ additionalConditions))

      if (!newCondExp.toString.equals(join.getCondition.toString)) {
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

  /**
    * Returns true if the given index is from join left, else false.
    */
  private def fromJoinLeft(join: Join, index: Int): Boolean = {
    require(join.getSystemFieldList.size() == 0)
    index < join.getLeft.getRowType.getFieldCount
  }

}

object JoinDependentConditionDerivationRule {
  val INSTANCE = new JoinDependentConditionDerivationRule
}
