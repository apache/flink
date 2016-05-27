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

package org.apache.flink.api.table.plan.rules.dataSet

import org.apache.calcite.plan.RelOptRule.{any, operand}
import org.apache.calcite.plan.{RelOptUtil, RelOptRule, RelOptRuleCall}
import org.apache.calcite.plan.volcano.RelSubset
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.{JoinRelType, Join}
import org.apache.calcite.rel.logical.LogicalJoin
import org.apache.calcite.rex.{RexUtil, RexNode, RexPermuteInputsShuttle}
import org.apache.calcite.util.ImmutableBitSet
import org.apache.calcite.util.mapping.Mappings

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
  * Based on Apache Calcite's JoinAssociateRule.
  * JoinAssociateRule could not be extended because it does not provide a public constructor.
  *
  * - onMatch() is the same as in JoinAssociateRule (except for porting Java to Scala code)
  * - matches() was added to restrict the rule to only switch if at least one join is a
  *     Cartesian product.
  *
  */
class JoinCrossAssociateRule extends RelOptRule(
  operand(classOf[Join], operand(classOf[Join], any), operand(classOf[RelSubset], any))) {

  override def matches(call: RelOptRuleCall): Boolean = {

    val left = call.rel(0).asInstanceOf[LogicalJoin]
    val right = call.rel(1).asInstanceOf[LogicalJoin]

    // check if either Join is a Cartesian product
    left.getCondition.isAlwaysTrue || right.getCondition.isAlwaysTrue
  }

  def onMatch(call: RelOptRuleCall) {

    val topJoin = call.rel(0).asInstanceOf[Join]
    val bottomJoin = call.rel(1).asInstanceOf[Join]
    val relA: RelNode = bottomJoin.getLeft
    val relB: RelNode = bottomJoin.getRight
    val relC = call.rel(2).asInstanceOf[RelSubset]
    val cluster = topJoin.getCluster
    val rexBuilder = cluster.getRexBuilder

    if (relC.getConvention != relA.getConvention) {
      // relC could have any trait-set. But if we're matching say
      // EnumerableConvention, we're only interested in enumerable subsets.
      return
    }

    val aCount = relA.getRowType.getFieldCount
    val bCount = relB.getRowType.getFieldCount
    val cCount = relC.getRowType.getFieldCount
    val aBitSet: ImmutableBitSet = ImmutableBitSet.range(0, aCount)
    val bBitSet: ImmutableBitSet = ImmutableBitSet.range(aCount, aCount + bCount)
    if (!topJoin.getSystemFieldList.isEmpty) {
      // FIXME Enable this rule for joins with system fields
      return
    }

    // If either join is not inner, we cannot proceed.
    // (Is this too strict?)
    if (topJoin.getJoinType != JoinRelType.INNER || bottomJoin.getJoinType != JoinRelType.INNER) {
      return
    }

    // Goal is to transform to
    //
    //       newTopJoin
    //        /     \
    //       A   newBottomJoin
    //               /    \
    //              B      C

    // Split the condition of topJoin and bottomJoin into a conjunctions. A
    // condition can be pushed down if it does not use columns from A.
    val top = new mutable.ArrayBuffer[RexNode]().asJava
    val bottom = new mutable.ArrayBuffer[RexNode]().asJava
    split(topJoin.getCondition, aBitSet, top, bottom)
    split(bottomJoin.getCondition, aBitSet, top, bottom)

    // Mapping for moving conditions from topJoin or bottomJoin to
    // newBottomJoin.
    // target: | B | C      |
    // source: | A       | B | C      |
    val bottomMapping: Mappings.TargetMapping = Mappings.createShiftMapping(
      aCount + bCount + cCount, 0, aCount, bCount, bCount, aCount + bCount, cCount)
    val newBottomList = new mutable.ArrayBuffer[RexNode]().asJava
    new RexPermuteInputsShuttle(bottomMapping, relB, relC)
      .visitList(bottom, newBottomList)
    val newBottomCondition: RexNode =
      RexUtil.composeConjunction(rexBuilder, newBottomList, false)

    val newBottomJoin: Join = bottomJoin.copy(
      bottomJoin.getTraitSet, newBottomCondition, relB, relC, JoinRelType.INNER, false)

    // Condition for newTopJoin consists of pieces from bottomJoin and topJoin.
    // Field ordinals do not need to be changed.
    val newTopCondition: RexNode = RexUtil.composeConjunction(rexBuilder, top, false)
    val newTopJoin: Join = topJoin.copy(
      topJoin.getTraitSet, newTopCondition, relA, newBottomJoin, JoinRelType.INNER, false)

    call.transformTo(newTopJoin)
  }

  /**
    * Copied from Calcite's JoinPushThroughJoinRule
    *
    * Splits a condition into conjunctions that do or do not intersect with
    * a given bit set.
    */
  private def split(
      condition: RexNode,
      bitSet: ImmutableBitSet,
      intersecting: java.util.List[RexNode],
      nonIntersecting: java.util.List[RexNode]) {

    import scala.collection.JavaConversions._
    for (node <- RelOptUtil.conjunctions(condition)) {
      val inputBitSet: ImmutableBitSet = RelOptUtil.InputFinder.bits(node)
      if (bitSet.intersects(inputBitSet)) {
        intersecting.add(node)
      }
      else {
        nonIntersecting.add(node)
      }
    }
  }

}

object JoinCrossAssociateRule {
  val INSTANCE = new JoinCrossAssociateRule()
}
