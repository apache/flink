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

import org.apache.calcite.plan.RelOptRule._
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall, RelOptUtil}
import org.apache.calcite.rel.core.JoinRelType
import org.apache.calcite.rel.rules.MultiJoin
import org.apache.calcite.rex.{RexCall, RexNode}
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.sql.fun.SqlStdOperatorTable._

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
  * Planner rule to apply transitive closure on [[MultiJoin]] for equi-join predicates.
  *
  * <p>e.g.
  * MJ(A, B, C) ON A.a1=B.b1 AND B.b1=C.c1 &rarr;
  * MJ(A, B, C) ON A.a1=B.b1 AND B.b1=C.c1 AND A.a1=C.c1
  *
  * The advantage of applying this rule is that it increases the choice of join reorder;
  * at the same time, the disadvantage is that it will use more CPU for additional join predicates.
  */
class RewriteMultiJoinConditionRule extends RelOptRule(
  operand(classOf[MultiJoin], any),
  "RewriteMultiJoinConditionRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val multiJoin: MultiJoin = call.rel(0)
    // currently only supports all join types are INNER join
    val isAllInnerJoin = multiJoin.getJoinTypes.forall(_ eq JoinRelType.INNER)
    val (equiJoinFilters, _) = partitionJoinFilters(multiJoin)
    !multiJoin.isFullOuterJoin && isAllInnerJoin && equiJoinFilters.size > 1
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val multiJoin: MultiJoin = call.rel(0)
    val (equiJoinFilters, nonEquiJoinFilters) = partitionJoinFilters(multiJoin)
    // there is no `equals` method in RexCall, so the key of this map should be String
    val equiJoinFilterMap = mutable.HashMap[RexNode, mutable.ListBuffer[RexNode]]()
    equiJoinFilters.foreach {
      case c: RexCall =>
        require(c.isA(SqlKind.EQUALS))
        val left = c.operands.head
        val right = c.operands(1)
        equiJoinFilterMap.getOrElseUpdate(left, mutable.ListBuffer[RexNode]()) += right
        equiJoinFilterMap.getOrElseUpdate(right, mutable.ListBuffer[RexNode]()) += left
    }

    val candidateJoinFilters = equiJoinFilterMap.values.filter(_.size > 1)
    if (candidateJoinFilters.isEmpty) {
      // no transitive closure predicates
      return
    }

    val newEquiJoinFilters = mutable.ListBuffer[RexNode](equiJoinFilters: _*)
    def containEquiJoinFilter(joinFilter: RexNode): Boolean = {
      newEquiJoinFilters.exists { f => f.equals(joinFilter) }
    }

    val rexBuilder = multiJoin.getCluster.getRexBuilder
    candidateJoinFilters.foreach {
      candidate => candidate.indices.foreach {
        startIndex =>
          val op1 = candidate(startIndex)
          candidate.subList(startIndex + 1, candidate.size).foreach {
            op2 =>
              val newFilter = rexBuilder.makeCall(EQUALS, op1, op2)
              if (!containEquiJoinFilter(newFilter)) {
                newEquiJoinFilters += newFilter
              }
          }
      }
    }

    if (newEquiJoinFilters.size == equiJoinFilters.size) {
      // no new join filters added
      return
    }

    val newJoinFilter = call.builder().and(newEquiJoinFilters.toList ::: nonEquiJoinFilters.toList)
    val newMultiJoin =
      new MultiJoin(
        multiJoin.getCluster,
        multiJoin.getInputs,
        newJoinFilter,
        multiJoin.getRowType,
        multiJoin.isFullOuterJoin,
        multiJoin.getOuterJoinConditions,
        multiJoin.getJoinTypes,
        multiJoin.getProjFields,
        multiJoin.getJoinFieldRefCountsMap,
        multiJoin.getPostJoinFilter)

    call.transformTo(newMultiJoin)
  }

  /**
    * Partitions MultiJoin condition in equi join filters and non-equi join filters.
    */
  private def partitionJoinFilters(multiJoin: MultiJoin): (Seq[RexNode], Seq[RexNode]) = {
    val joinFilters = RelOptUtil.conjunctions(multiJoin.getJoinFilter)
    joinFilters.partition(f => f.isA(SqlKind.EQUALS))
  }

}

object RewriteMultiJoinConditionRule {
  val INSTANCE = new RewriteMultiJoinConditionRule
}
