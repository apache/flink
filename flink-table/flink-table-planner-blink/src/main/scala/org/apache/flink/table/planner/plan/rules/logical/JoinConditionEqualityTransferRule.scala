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
import org.apache.calcite.rex.{RexBuilder, RexCall, RexInputRef, RexNode}
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.sql.fun.SqlStdOperatorTable.EQUALS

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
  * Planner rule that converts Join's conditions to the left or right table's own
  * independent filter as much as possible, so that the rules of filter-push-down can push down
  * the filter to below.
  *
  * <p>e.g. join condition: l_a = r_b and l_a = r_c.
  * The l_a is a field from left input, both r_b and r_c are fields from the right input.
  * After rewrite, condition will be: l_a = r_b and r_b = r_c.
  * r_b = r_c can be pushed down to the right input.
  */
class JoinConditionEqualityTransferRule extends RelOptRule(
  operand(classOf[Join], any),
  "JoinConditionEqualityTransferRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val join: Join = call.rel(0)
    val joinType = join.getJoinType
    if (joinType != JoinRelType.INNER && joinType != JoinRelType.SEMI) {
      return false
    }

    val (optimizableFilters, _) = partitionJoinFilters(join)
    val groups = getEquiFilterRelationshipGroup(optimizableFilters)
    groups.exists(_.size > 2)
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val join: Join = call.rel(0)
    val (optimizableFilters, remainFilters) = partitionJoinFilters(join)
    val (equiFiltersToOpt, equiFiltersNotOpt) =
      getEquiFilterRelationshipGroup(optimizableFilters).partition(_.size > 2)

    val builder = call.builder()
    val rexBuilder = builder.getRexBuilder
    val newEquiJoinFilters = mutable.ListBuffer[RexNode]()

    // add equiFiltersNotOpt.
    equiFiltersNotOpt.foreach { refs =>
      require(refs.size == 2)
      newEquiJoinFilters += rexBuilder.makeCall(EQUALS, refs.head, refs.last)
    }

    // new opt filters.
    equiFiltersToOpt.foreach { refs =>
      // partition to InputRef to left and right.
      val (leftRefs, rightRefs) = refs.partition(fromJoinLeft(join, _))
      val rexCalls = new mutable.ArrayBuffer[RexNode]()

      // equals for each other.
      rexCalls ++= makeCalls(rexBuilder, leftRefs)
      rexCalls ++= makeCalls(rexBuilder, rightRefs)

      // equals for left and right.
      if (leftRefs.nonEmpty && rightRefs.nonEmpty) {
        rexCalls += rexBuilder.makeCall(EQUALS, leftRefs.head, rightRefs.head)
      }

      // add to newEquiJoinFilters with deduplication.
      rexCalls.foreach(call => newEquiJoinFilters += call)
    }

    val newJoinFilter = builder.and(remainFilters :+
      FlinkRexUtil.simplify(rexBuilder, builder.and(newEquiJoinFilters)))
    val newJoin = join.copy(
      join.getTraitSet,
      newJoinFilter,
      join.getLeft,
      join.getRight,
      join.getJoinType,
      join.isSemiJoinDone)

    call.transformTo(newJoin)
  }

  /**
    * Returns true if the given input ref is from join left, else false.
    */
  private def fromJoinLeft(join: Join, ref: RexInputRef): Boolean = {
    require(join.getSystemFieldList.size() == 0)
    ref.getIndex < join.getLeft.getRowType.getFieldCount
  }

  /**
    * Partition join condition to leftRef-rightRef equals and others.
    */
  def partitionJoinFilters(join: Join): (Seq[RexNode], Seq[RexNode]) = {
    val conjunctions = RelOptUtil.conjunctions(join.getCondition)
    conjunctions.partition {
      case call: RexCall if call.isA(SqlKind.EQUALS) =>
        (call.operands.head, call.operands.last) match {
          case (ref1: RexInputRef, ref2: RexInputRef) =>
            val isLeft1 = fromJoinLeft(join, ref1)
            val isLeft2 = fromJoinLeft(join, ref2)
            isLeft1 != isLeft2
          case _ => false
        }
      case _ => false
    }
  }

  /**
    * Put fields to a group that have equivalence relationships.
    */
  def getEquiFilterRelationshipGroup(equiJoinFilters: Seq[RexNode]): Seq[Seq[RexInputRef]] = {
    val filterSets = mutable.ArrayBuffer[mutable.HashSet[RexInputRef]]()
    equiJoinFilters.foreach {
      case call: RexCall =>
        require(call.isA(SqlKind.EQUALS))
        val left = call.operands.head.asInstanceOf[RexInputRef]
        val right = call.operands.last.asInstanceOf[RexInputRef]
        val set = filterSets.find(set => set.contains(left) || set.contains(right)) match {
          case Some(s) => s
          case None =>
            val s = new mutable.HashSet[RexInputRef]()
            filterSets += s
            s
        }
        set += left
        set += right
    }

    filterSets.map(_.toSeq)
  }

  /**
    * Make calls to a number of inputRefs, make sure that they both have a relationship.
    */
  def makeCalls(rexBuilder: RexBuilder, nodes: Seq[RexInputRef]): Seq[RexNode] = {
    val calls = new mutable.ArrayBuffer[RexNode]()
    if (nodes.length > 1) {
      val rex = nodes.head
      nodes.drop(1).foreach(calls += rexBuilder.makeCall(EQUALS, rex, _))
    }
    calls
  }
}

object JoinConditionEqualityTransferRule {
  val INSTANCE = new JoinConditionEqualityTransferRule
}
