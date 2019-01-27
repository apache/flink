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

package org.apache.flink.table.plan.rules.logical

import org.apache.flink.table.plan.util.FlinkRexUtil

import org.apache.calcite.plan.RelOptRule.{any, operand}
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall, RelOptUtil}
import org.apache.calcite.rel.logical.LogicalJoin
import org.apache.calcite.rex.RexNode

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
  * This Rule extracts some subconditions in the Join OR condition that can be used individually
  * to the join side of input, then other rules pushdown these subconditions, to optimize
  * the execution efficiency.
  *
  * <p>For example, there is a join query that unites table A and table B:
  * "select * from A, B where A.f1 = B.f1 and ((A.f2 = 'aaa1' and B.f2 = 'bbb1') or
  * (A.f2 = 'aaa2' and B.f2 = 'bbb2'))".
  *
  * <p>Hence the query rewards optimizers that can analyze complex join conditions which cannot be
  * pushed below the join, but still derive filters from such join conditions. It could immediate
  * filter the scan(A) with the condition: (A.f2 = 'aaa1' or A.f2 = 'aaa2').
  *
  * <p>After join dependent optimization, sql will be:
  * "select * from A, B where A.f1 = B.f1 and
  * ((A.f2 = 'aaa1' and B.f2 = 'bbb1') or (A.f2 = 'aaa2' and B.f2 = 'bbb2'))
  * and (A.f2 = 'aaa1' or A.f2 = 'aaa2') and (B.f2 = 'bbb1' or B.f2 = 'bbb2')".
  *
  * <p>Note: This class can only be used in RULE_SEQUENCE.
  */
class JoinDependentFilterPushdownRule extends RelOptRule(
  operand(classOf[LogicalJoin], any()),
  "JoinDependentFilterPushdownRule") {

  override def onMatch(call: RelOptRuleCall): Unit = {
    val join: LogicalJoin = call.rel(0)
    val conjunctions = RelOptUtil.conjunctions(join.getCondition)

    val builder = call.builder()

    def fromLeft(i: Int) = {
      require(join.getSystemFieldList.size() == 0)
      i < join.getLeft.getRowType.getFieldCount
    }

    val additionalConds = new mutable.ArrayBuffer[RexNode]

    // and
    conjunctions.foreach { conjunctionRex =>
      val disjunctions = RelOptUtil.disjunctions(conjunctionRex)

      // Apart or: (A.f2 = 'aaa1' and B.f2 = 'bbb1') or (A.f2 = 'aaa2' and B.f2 = 'bbb2')
      if (disjunctions.size() > 1) {

        val leftDisjuncConds = new mutable.ArrayBuffer[RexNode]
        val rightDisjuncConds = new mutable.ArrayBuffer[RexNode]
        disjunctions.foreach { disjunctionRex =>

          val leftConjuncConds = new mutable.ArrayBuffer[RexNode]
          val rightConjuncConds = new mutable.ArrayBuffer[RexNode]

          // Apart and: A.f2 = 'aaa1' and B.f2 = 'bbb1'
          RelOptUtil.conjunctions(disjunctionRex).foreach { cond =>
            val rCols = RelOptUtil.InputFinder.bits(cond).map(_.intValue())

            // May have multi conds, eg: A.f2 = 'aaa1' and A.f3 = 'aaa3' and B.f2 = 'bbb1'
            if (rCols.forall(fromLeft)) {
              leftConjuncConds += cond
            } else if (rCols.forall(!fromLeft(_))) {
              rightConjuncConds += cond
            }
          }

          // It is true if conjuncConds is empty.
          leftDisjuncConds += builder.and(leftConjuncConds)
          rightDisjuncConds += builder.and(rightConjuncConds)
        }

        // TODO Consider whether it is worth doing a filter if we have histogram.
        if (leftDisjuncConds.nonEmpty) {
          additionalConds += builder.or(leftDisjuncConds)
        }
        if (rightDisjuncConds.nonEmpty) {
          additionalConds += builder.or(rightDisjuncConds)
        }
      }
    }

    if (additionalConds.nonEmpty) {
      val newCondExp = FlinkRexUtil.simplify(
        builder.getRexBuilder,
        builder.and(conjunctions ++ additionalConds))

      if (!newCondExp.toString.equals(join.getCondition.toString)) {
        val newJoinRel = join.copy(
          join.getTraitSet,
          newCondExp,
          join.getLeft,
          join.getRight,
          join.getJoinType,
          join.isSemiJoinDone)

        call.transformTo(newJoinRel)
      }
    }
  }
}

object JoinDependentFilterPushdownRule {
  val INSTANCE = new JoinDependentFilterPushdownRule
}
