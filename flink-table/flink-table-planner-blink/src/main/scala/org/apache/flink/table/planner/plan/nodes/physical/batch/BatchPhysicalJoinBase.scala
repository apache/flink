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
package org.apache.flink.table.planner.plan.nodes.physical.batch

import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.planner.codegen.{CodeGeneratorContext, ExprCodeGenerator, FunctionCodeGenerator}
import org.apache.flink.table.planner.plan.`trait`.{FlinkRelDistribution, FlinkRelDistributionTraitDef}
import org.apache.flink.table.planner.plan.nodes.physical.common.CommonPhysicalJoin
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition
import org.apache.flink.table.types.logical.RowType

import org.apache.calcite.plan.{RelOptCluster, RelOptRule, RelTraitSet}
import org.apache.calcite.rel.RelDistribution.Type.{HASH_DISTRIBUTED, RANGE_DISTRIBUTED}
import org.apache.calcite.rel.core.{Join, JoinRelType}
import org.apache.calcite.rel.{RelCollations, RelNode}
import org.apache.calcite.rex.RexNode
import org.apache.calcite.util.ImmutableIntList

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
  * Batch physical RelNode for [[Join]]
  */
abstract class BatchPhysicalJoinBase(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    leftRel: RelNode,
    rightRel: RelNode,
    condition: RexNode,
    joinType: JoinRelType)
  extends CommonPhysicalJoin(cluster, traitSet, leftRel, rightRel, condition, joinType)
  with BatchPhysicalRel {

  private[flink] def generateCondition(
      config: TableConfig,
      leftType: RowType,
      rightType: RowType): GeneratedJoinCondition = {
    val ctx = CodeGeneratorContext(config)
    val exprGenerator = new ExprCodeGenerator(ctx, false)
        .bindInput(leftType)
        .bindSecondInput(rightType)

    val body = if (joinInfo.isEqui) {
      // only equality condition
      "return true;"
    } else {
      val nonEquiPredicates = joinInfo.getRemaining(getCluster.getRexBuilder)
      val condition = exprGenerator.generateExpression(nonEquiPredicates)
      s"""
         |${condition.code}
         |return ${condition.resultTerm};
         |""".stripMargin
    }

    FunctionCodeGenerator.generateJoinCondition(
      ctx,
      "JoinConditionFunction",
      body)
  }

  /**
    * Try to satisfy hash distribution on Non-BroadcastJoin (including SortMergeJoin and
    * Non-Broadcast HashJoin).
    *
    * @param requiredDistribution distribution requirement
    * @return a Tuple including 3 element.
    *         The first element is a flag which indicates whether the requirement can be satisfied.
    *         The second element is the distribution requirement of left child if the requirement
    *         can be push down into join.
    *         The third element is the distribution requirement of right child if the requirement
    *         can be push down into join.
    */
  def satisfyHashDistributionOnNonBroadcastJoin(
      requiredDistribution: FlinkRelDistribution
  ): (Boolean, FlinkRelDistribution, FlinkRelDistribution) = {
    // Only Non-broadcast HashJoin could provide HashDistribution
    if (requiredDistribution.getType != HASH_DISTRIBUTED) {
      return (false, null, null)
    }
    // Full outer join cannot provide Hash distribute because it will generate null for left/right
    // side if there is no match row.
    if (joinType == JoinRelType.FULL) {
      return (false, null, null)
    }

    val leftKeys = joinInfo.leftKeys
    val rightKeys = joinInfo.rightKeys
    val leftKeysToRightKeys = leftKeys.zip(rightKeys).toMap
    val rightKeysToLeftKeys = rightKeys.zip(leftKeys).toMap
    val leftFieldCnt = getLeft.getRowType.getFieldCount
    val requiredShuffleKeys = requiredDistribution.getKeys
    val requiredLeftShuffleKeys = mutable.ArrayBuffer[Int]()
    val requiredRightShuffleKeys = mutable.ArrayBuffer[Int]()
    requiredShuffleKeys.foreach { key =>
      if (key < leftFieldCnt && joinType != JoinRelType.RIGHT) {
        leftKeysToRightKeys.get(key) match {
          case Some(rk) =>
            requiredLeftShuffleKeys += key
            requiredRightShuffleKeys += rk
          case None if requiredDistribution.requireStrict =>
            // Cannot satisfy required hash distribution due to required distribution is restrict
            // however the key is not found in right
            return (false, null, null)
          case _ => // do nothing
        }
      } else if (key >= leftFieldCnt &&
        (joinType == JoinRelType.RIGHT || joinType == JoinRelType.INNER)) {
        val keysOnRightChild = key - leftFieldCnt
        rightKeysToLeftKeys.get(keysOnRightChild) match {
          case Some(lk) =>
            requiredLeftShuffleKeys += lk
            requiredRightShuffleKeys += keysOnRightChild
          case None if requiredDistribution.requireStrict =>
            // Cannot satisfy required hash distribution due to required distribution is restrict
            // however the key is not found in left
            return (false, null, null)
          case _ => // do nothing
        }
      } else {
        // cannot satisfy required hash distribution if requirement shuffle keys are not come from
        // left side when Join is LOJ or are not come from right side when Join is ROJ.
        return (false, null, null)
      }
    }
    if (requiredLeftShuffleKeys.isEmpty) {
      // the join can not satisfy the required hash distribution
      // due to the required input shuffle keys are empty
      return (false, null, null)
    }

    val (leftShuffleKeys, rightShuffleKeys) = if (joinType == JoinRelType.INNER &&
        !requiredDistribution.requireStrict) {
      (requiredLeftShuffleKeys.distinct, requiredRightShuffleKeys.distinct)
    } else {
      (requiredLeftShuffleKeys, requiredRightShuffleKeys)
    }
    (true,
      FlinkRelDistribution.hash(ImmutableIntList.of(leftShuffleKeys: _*), requireStrict = true),
      FlinkRelDistribution.hash(ImmutableIntList.of(rightShuffleKeys: _*), requireStrict = true))
  }

  /**
    * Try to satisfy the given required traits on BroadcastJoin (including Broadcast-HashJoin and
    * NestedLoopJoin).
    *
    * @param requiredTraitSet requirement traitSets
    * @return Equivalent Join which satisfies required traitSet, return null if
    *         requirement cannot be satisfied.
    */
  protected def satisfyTraitsOnBroadcastJoin(
      requiredTraitSet: RelTraitSet,
      leftIsBroadcast: Boolean): Option[RelNode] = {
    val requiredDistribution = requiredTraitSet.getTrait(FlinkRelDistributionTraitDef.INSTANCE)
    val keys = requiredDistribution.getKeys
    val left = getLeft
    val right = getRight
    val leftFieldCnt = left.getRowType.getFieldCount
    val canSatisfy = requiredDistribution.getType match {
      case HASH_DISTRIBUTED | RANGE_DISTRIBUTED =>
        // required distribution can be satisfied only if distribution keys all from
        // non-broadcast side of BroadcastJoin
        if (leftIsBroadcast) {
          // all distribution keys must come from right child
          keys.forall(_ >= leftFieldCnt)
        } else {
          // all distribution keys must come from left child
          keys.forall(_ < leftFieldCnt)
        }
      // SINGLETON, BROADCAST_DISTRIBUTED, ANY, RANDOM_DISTRIBUTED, ROUND_ROBIN_DISTRIBUTED
      // distribution cannot be pushed down.
      case _ => false
    }
    if (!canSatisfy) {
      return None
    }

    val keysInProbeSide = if (leftIsBroadcast) {
      ImmutableIntList.of(keys.map(_ - leftFieldCnt): _ *)
    } else {
      keys
    }

    val inputRequiredDistribution = requiredDistribution.getType match {
      case HASH_DISTRIBUTED =>
        FlinkRelDistribution.hash(keysInProbeSide, requiredDistribution.requireStrict)
      case RANGE_DISTRIBUTED =>
        FlinkRelDistribution.range(keysInProbeSide)
    }
    // remove collation traits from input traits and provided traits
    val (newLeft, newRight) = if (leftIsBroadcast) {
      val rightRequiredTraitSet = right.getTraitSet
        .replace(inputRequiredDistribution)
        .replace(RelCollations.EMPTY)
      val newRight = RelOptRule.convert(right, rightRequiredTraitSet)
      (left, newRight)
    } else {
      val leftRequiredTraitSet = left.getTraitSet
        .replace(inputRequiredDistribution)
        .replace(RelCollations.EMPTY)
      val newLeft = RelOptRule.convert(left, leftRequiredTraitSet)
      (newLeft, right)
    }
    val providedTraitSet = requiredTraitSet.replace(RelCollations.EMPTY)
    Some(copy(providedTraitSet, Seq(newLeft, newRight)))
  }
}
