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
import org.apache.calcite.rel.core.{Join, JoinRelType}
import org.apache.calcite.rex.{RexBuilder, RexCall, RexInputRef, RexNode}
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.sql.fun.SqlStdOperatorTable.EQUALS

import java.util
import java.util.Collections

import scala.collection.{breakOut, mutable}
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.util.control.Breaks.{break, breakable}

/**
 * Planner rule that converts Join's conditions to the left or right table's own independent filter
 * as much as possible, so that the rules of filter-push-down can push down the filter to below.
 *
 * <p>e.g. join condition: l_a = r_b and l_a = r_c. The l_a is a field from left input, both r_b and
 * r_c are fields from the right input. After rewrite, condition will be: l_a = r_b and r_b = r_c.
 * r_b = r_c can be pushed down to the right input.
 */
class JoinConditionEqualityTransferRule
  extends RelOptRule(operand(classOf[Join], any), "JoinConditionEqualityTransferRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val join: Join = call.rel(0)
    val joinType = join.getJoinType
    if (joinType != JoinRelType.INNER && joinType != JoinRelType.SEMI) {
      return false
    }

    val optimizableFilters = partitionJoinFilters(join)
    val groups = getEquiFilterRelationshipGroup(optimizableFilters.f0)
    groups.exists(_.size > 2)
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val join: Join = call.rel(0)
    val optimizableAndRemainFilters =
      partitionJoinFilters(join);
    val optimizableFilters = optimizableAndRemainFilters.f0
    val remainFilters = optimizableAndRemainFilters.f1

    /*    val partitioned =
      getEquiFilterRelationshipGroup(optimizableFilters).stream()
        .collect(java.util.stream.Collectors.partitioningBy(new java.util.function.Predicate[java.util.Set[RexInputRef]] {
          override def test(t: java.util.Set[RexInputRef]): Boolean = t.size() > 2
        } ))*/

    val (equiFiltersToOpt, equiFiltersNotOpt) =
      getEquiFilterRelationshipGroup(optimizableFilters).partition(_.size > 2)
    /*val equiFiltersToOpt = partitioned.get(true)
    val equiFiltersNotOpt = partitioned.get(false)
     */
    val builder = call.builder()
    val rexBuilder = builder.getRexBuilder
    val newEquiJoinFilters = new java.util.ArrayList[RexNode]()

    // add equiFiltersNotOpt.
    equiFiltersNotOpt.foreach {
      refs =>
        require(refs.size == 2)
        newEquiJoinFilters.add(rexBuilder.makeCall(EQUALS, refs.head, refs.last))
    }

    // new opt filters.
    equiFiltersToOpt.foreach {
      refs =>
        // partition to InputRef to left and right.
        val leftAndRightRefs =
          refs
            .stream()
            // .collect(java.util.stream.Collectors.partitioningBy(t => fromJoinLeft(join, t)));
            .collect(java.util.stream.Collectors
              .partitioningBy(new java.util.function.Predicate[RexInputRef] {
                override def test(t: RexInputRef): Boolean = fromJoinLeft(join, t)
              }))
        val leftRefs = leftAndRightRefs.get(true)
        val rightRefs = leftAndRightRefs.get(false)
        val rexCalls = new java.util.ArrayList[RexNode]()

        // equals for each other.
        rexCalls.addAll(makeCalls(rexBuilder, leftRefs))
        rexCalls.addAll(makeCalls(rexBuilder, rightRefs))

        // equals for left and right.
        if (leftRefs.nonEmpty && rightRefs.nonEmpty) {
          rexCalls.add(rexBuilder.makeCall(EQUALS, leftRefs.head, rightRefs.head))
        }

        // add to newEquiJoinFilters with deduplication.
        newEquiJoinFilters.addAll(rexCalls)
    }

    remainFilters.add(
      FlinkRexUtil.simplify(
        rexBuilder,
        builder.and(newEquiJoinFilters),
        join.getCluster.getPlanner.getExecutor))
    val newJoinFilter = builder.and(remainFilters)
    val newJoin = join.copy(
      join.getTraitSet,
      newJoinFilter,
      join.getLeft,
      join.getRight,
      join.getJoinType,
      join.isSemiJoinDone)

    call.transformTo(newJoin)
  }

  /** Returns true if the given input ref is from join left, else false. */
  private def fromJoinLeft(join: Join, ref: RexInputRef): Boolean = {
    require(join.getSystemFieldList.size() == 0)
    ref.getIndex < join.getLeft.getRowType.getFieldCount
  }

  /** Partition join condition to leftRef-rightRef equals and others. */
  /*  def partitionJoinFilters(join: Join): (Seq[RexNode], Seq[RexNode]) = {
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
   */
  def partitionJoinFilters(join: Join)
      : org.apache.flink.api.java.tuple.Tuple2[java.util.List[RexNode], java.util.List[RexNode]] = {
    val left = new java.util.ArrayList[RexNode]()
    val right = new java.util.ArrayList[RexNode]()
    val conjunctions = RelOptUtil.conjunctions(join.getCondition)

    for (rexNode <- conjunctions) {
      if (rexNode.isInstanceOf[RexCall]) {
        val call = rexNode.asInstanceOf[RexCall]
        if (call.isA(SqlKind.EQUALS)) {
          if (
            call.operands.get(0).isInstanceOf[RexInputRef] && call.operands
              .get(1)
              .isInstanceOf[RexInputRef]
          ) {
            val ref1 = call.operands.get(0).asInstanceOf[RexInputRef]
            val ref2 = call.operands.get(1).asInstanceOf[RexInputRef]
            val isLeft1 = fromJoinLeft(join, ref1)
            val isLeft2 = fromJoinLeft(join, ref2)
            if (isLeft1 != isLeft2) {
              left.add(rexNode)
            } else {
              right.add(rexNode)
            }
          } else {
            right.add(rexNode)
          }
        } else {
          right.add(rexNode)
        }
      } else {
        right.add(rexNode)
      }
    }

    /*   conjunctions.partition {
      case call: RexCall if call.isA(SqlKind.EQUALS) =>
        (call.operands.head, call.operands.last) match {
          case (ref1: RexInputRef, ref2: RexInputRef) =>
            val isLeft1 = fromJoinLeft(join, ref1)
            val isLeft2 = fromJoinLeft(join, ref2)
            isLeft1 != isLeft2
          case _ => false
        }
      case _ => false
    }*/

    org.apache.flink.api.java.tuple.Tuple2.of(left, right)
  }

  /** Put fields to a group that have equivalence relationships. */
  def getEquiFilterRelationshipGroup(
      equiJoinFilters: java.util.List[RexNode]): Seq[Seq[RexInputRef]] = {
    val filterSets = new java.util.ArrayList[util.HashSet[RexInputRef]]()
    equiJoinFilters.foreach {
      case call: RexCall =>
        require(call.isA(SqlKind.EQUALS))
        val left = call.operands.head.asInstanceOf[RexInputRef]
        val right = call.operands.last.asInstanceOf[RexInputRef]
        val set = filterSets
          .stream()
          .filter(set => set.contains(left) || set.contains(right))
          .findFirst()
          .orElseGet(
            () => {
              val s = new util.HashSet[RexInputRef]()
              filterSets.add(s)
              s
            })
        /*val set = filterSets.find(set => set.contains(left) || set.contains(right)) match {
          case Some(s) => s
          case None =>
            val s = new mutable.HashSet[RexInputRef]()
            filterSets.add(s)
            s
        }*/
        set.add(left)
        set.add(right)
    }

    filterSets.map(_.toSeq)
  }

  /*  private def getEquiFilterRelationshipGroup(equiJoinFilters: java.util.List[RexNode]): util.ArrayList[util.Set[RexInputRef]] = {
    val res = new java.util.ArrayList[java.util.Set[RexInputRef]]
    for (rexNode <- equiJoinFilters) {
      if (rexNode.isInstanceOf[RexCall]) {
        val call = rexNode.asInstanceOf[RexCall]
        require (call.isA(SqlKind.EQUALS))
          val left = call.operands.head.asInstanceOf[RexInputRef]
          val right = call.operands.last.asInstanceOf[RexInputRef]
          var found = false
          /*breakable {
            for (refs <- res) {
              if (refs.contains(left) || refs.contains(right)) {
                refs.add(left)
                refs.add(right)
                found = true
                break;
              }
            }
          }*/
          if (!found) {
            val set = new util.HashSet[RexInputRef]()
            set.add(left)
            set.add(right)
            res.add(set)
          }
      }
    }
    res
  }*/

  /** Make calls to a number of inputRefs, make sure that they both have a relationship. */
  def makeCalls(rexBuilder: RexBuilder, nodes: Seq[RexInputRef]): java.util.List[RexNode] = {
    val calls = new java.util.ArrayList[RexNode]()
    if (nodes.length > 1) {
      val rex = nodes.head
      nodes.drop(1).foreach(t => calls.add(rexBuilder.makeCall(EQUALS, rex, t)))
    }
    calls
  }
}

object JoinConditionEqualityTransferRule {
  val INSTANCE = new JoinConditionEqualityTransferRule
}
