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

import org.apache.flink.table.plan.FlinkJoinRelType
import org.apache.flink.table.plan.util.FlinkRexUtil

import org.apache.calcite.plan.RelOptRule.{any, operand}
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall, RelOptUtil}
import org.apache.calcite.rel.core.Join
import org.apache.calcite.rex.{RexBuilder, RexCall, RexInputRef, RexNode}
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.sql.fun.SqlStdOperatorTable.EQUALS

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
  * The role of this Rule is to convert Join's conditions to the left or right table's own
  * independent filter as much as possible, so that the FPD can push down the filter to below.
  *
  * <p>Join condition: a = b and a = c.
  * The a is a field of left table, both b and c are fields in the right table.
  * After optimization of this Rule, condition will be: a = b and b = c.
  * So we can push down b = c to right table later.
  */
class JoinCondEqualityTransferRule extends RelOptRule(
  operand(classOf[Join], any),
  "JoinCondEqualityTransferRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val join: Join = call.rel(0)
    val joinType = FlinkJoinRelType.getFlinkJoinRelType(join)
    val (optimizableFilters, _) = partitionJoinFilters(join)
    (joinType == FlinkJoinRelType.INNER || joinType == FlinkJoinRelType.SEMI) &&
        getEquiFilterRelationshipGroup(optimizableFilters).exists(_.size > 2)
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val join: Join = call.rel(0)
    val (optimizableFilters, remainFilters) = partitionJoinFilters(join)
    val (candidateJoinFilters, notOptFilters) =
      getEquiFilterRelationshipGroup(optimizableFilters).partition(_.size > 2)

    val builder = call.builder()
    val rexBuilder = join.getCluster.getRexBuilder
    val newEquiJoinFilters = mutable.ListBuffer[RexNode]()

    // add notOptFilters.
    notOptFilters.foreach { refs =>
      require(refs.size == 2)
      newEquiJoinFilters += rexBuilder.makeCall(EQUALS, refs.head, refs.last)
    }

    // new opt filters.
    candidateJoinFilters.foreach { refs =>
      // partition to InputRef to left and right.
      val (leftRefs, rightRefs) = refs.partition(fromLeft(join, _))
      val calls = new mutable.ArrayBuffer[RexNode]()

      // equals for each other.
      calls ++= makeCalls(rexBuilder, leftRefs)
      calls ++= makeCalls(rexBuilder, rightRefs)

      // equals for left and right.
      if (leftRefs.nonEmpty && rightRefs.nonEmpty) {
        calls += rexBuilder.makeCall(EQUALS, leftRefs.head, rightRefs.head)
      }

      // add to newEquiJoinFilters with deduplication.
      calls.foreach((call) => newEquiJoinFilters += call)
    }

    val newJoinFilter = builder.and(remainFilters :+
        FlinkRexUtil.simplify(rexBuilder, builder.and(newEquiJoinFilters)))
    call.transformTo(join.copy(
      join.getTraitSet,
      newJoinFilter,
      join.getLeft,
      join.getRight,
      join.getJoinType,
      join.isSemiJoinDone))
  }

  def fromLeft(join: Join, ref: RexInputRef): Boolean = {
    require(join.getSystemFieldList.size() == 0)
    ref.getIndex < join.getLeft.getRowType.getFieldCount
  }

  /**
    * Partition join filters to inputRef equals and others.
    */
  def partitionJoinFilters(join: Join): (Seq[RexNode], Seq[RexNode]) = {
    val joinFilters = RelOptUtil.conjunctions(join.getCondition)
    joinFilters.partition {
      case c: RexCall if c.isA(SqlKind.EQUALS) =>
        (c.operands.head, c.operands(1)) match {
          case (ref1: RexInputRef, ref2: RexInputRef) =>
            val isLeft1 = fromLeft(join, ref1)
            val isLeft2 = fromLeft(join, ref2)
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
    val filterSets = mutable.ArrayBuffer[mutable.HashSet[RexNode]]()
    equiJoinFilters.foreach {
      case c: RexCall =>
        require(c.isA(SqlKind.EQUALS))
        val left = c.operands.head
        val right = c.operands(1)
        val set = filterSets.find((set) => set.contains(left) || set.contains(right)) match {
          case Some(s) => s
          case None =>
            val s = new mutable.HashSet[RexNode]()
            filterSets += s
            s
        }
        set += left
        set += right
    }

    filterSets.map(_.toSeq.asInstanceOf[Seq[RexInputRef]])
  }

  def getHeadRefFromCall(call: RexNode): RexInputRef = {
    call.asInstanceOf[RexCall].getOperands.head.asInstanceOf[RexInputRef]
  }

  def getSecondRefFromCall(call: RexNode): RexInputRef = {
    call.asInstanceOf[RexCall].getOperands()(1).asInstanceOf[RexInputRef]
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

object JoinCondEqualityTransferRule {
  val INSTANCE = new JoinCondEqualityTransferRule
}
