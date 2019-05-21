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
package org.apache.flink.table.plan.nodes.physical.batch

import org.apache.flink.table.`type`.RowType
import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.codegen.{CodeGeneratorContext, ExprCodeGenerator, FunctionCodeGenerator}
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.generated.GeneratedJoinCondition
import org.apache.flink.table.plan.`trait`.{FlinkRelDistribution, FlinkRelDistributionTraitDef}
import org.apache.flink.table.plan.nodes.common.CommonPhysicalJoin
import org.apache.flink.table.plan.nodes.exec.BatchExecNode

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
abstract class BatchExecJoinBase(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    leftRel: RelNode,
    rightRel: RelNode,
    condition: RexNode,
    joinType: JoinRelType)
  extends CommonPhysicalJoin(cluster, traitSet, leftRel, rightRel, condition, joinType)
  with BatchPhysicalRel
  with BatchExecNode[BaseRow] {

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
    * Try to push down hash distribution into Non-BroadcastJoin (including SortMergeJoin and
    * Non-Broadcast HashJoin).
    *
    * @param requiredDistribution distribution requirement
    * @return a Tuple including 3 element.
    *         The first element is a flag which indicate whether the requirement can be push down
    *         into Join.
    *         The second element is the distribution requirement of left child if the requirement
    *         can be push down into join.
    *         The third elememt is the distribution requirement of right child if the requirement
    *         can be push down into join.
    */
  def pushDownHashDistributionIntoNonBroadcastJoin(
      requiredDistribution: FlinkRelDistribution)
  : (Boolean, FlinkRelDistribution, FlinkRelDistribution) = {
    // Only HashDistribution can be push down into Non-broadcast HashJoin
    if (requiredDistribution.getType != HASH_DISTRIBUTED) {
      return (false, null, null)
    }
    // Full outer join cannot provide Hash distribute because it will generate null for left/ right
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
            // Cannot partial push down distribution if required hash distribution is restrict
            return (false, null, null)
          case _ =>
        }
      } else if (key >= leftFieldCnt &&
        (joinType == JoinRelType.RIGHT ||
          joinType == JoinRelType.INNER)) {
        val keysOnRightChild = key - leftFieldCnt
        rightKeysToLeftKeys.get(keysOnRightChild) match {
          case Some(lk) =>
            requiredLeftShuffleKeys += lk
            requiredRightShuffleKeys += keysOnRightChild
          case None if requiredDistribution.requireStrict =>
            return (false, null, null)
          case _ =>
        }
      } else {
        // cannot push down hash distribute if requirement shuffle keys are not come from left side
        // when Join is LOJ or are not come from right side when Join is ROJ.
        return (false, null, null)
      }
    }
    if (requiredLeftShuffleKeys.isEmpty) {
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
    * Try to push down trait into BroadcastJoin (including Broadcast-HashJoin and
    * NestedLoopJoin).
    *
    * @param requiredTraitSet requirement traitSets
    * @return Equivalent Join which is push down required traitSet into input, return null if
    *         requirement cannot push down.
    */
  protected def pushDownTraitsIntoBroadcastJoin(
      requiredTraitSet: RelTraitSet,
      leftIsBroadcast: Boolean): RelNode = {
    val requiredDistribution = requiredTraitSet.getTrait(FlinkRelDistributionTraitDef.INSTANCE)
    requiredDistribution.getType match {
      case HASH_DISTRIBUTED | RANGE_DISTRIBUTED =>
        // Distribution can be pushed down only if distribution keys all from non-broadcast side of
        // BroadcastJoin
        val keys = requiredDistribution.getKeys
        val leftFieldCnt = getLeft.getRowType.getFieldCount
        var isKeysAllFromProbe = true
        val mappingKeys = if (leftIsBroadcast) {
          // all distribution keys must come from right child
          val keysInProbeSide = mutable.ArrayBuffer[Int]()
          keys.foreach { key =>
            if (key < leftFieldCnt) {
              isKeysAllFromProbe = false
            }
            keysInProbeSide += key - leftFieldCnt
          }
          ImmutableIntList.of(keysInProbeSide: _*)
        } else {
          keys.foreach { key =>
            if (key >= leftFieldCnt) {
              isKeysAllFromProbe = false
            }
          }
          keys
        }
        if (!isKeysAllFromProbe) {
          null
        } else {
          val pushDownDistribution = requiredDistribution.getType match {
            case HASH_DISTRIBUTED => FlinkRelDistribution.hash(mappingKeys,
              requiredDistribution.requireStrict)
            case RANGE_DISTRIBUTED => FlinkRelDistribution.range(mappingKeys)
          }
          val providedTraitSet = requiredTraitSet.replace(RelCollations.EMPTY)
          val (newLeft, newRight) = if (leftIsBroadcast) {
            // remove collation traits from push down traits and provided traits
            val pushDownTraitSet = getRight.getTraitSet.replace(pushDownDistribution)
              .replace(RelCollations.EMPTY)
            (getLeft, RelOptRule.convert(getRight, pushDownTraitSet))
          } else {
            val pushDownTraitSet = getLeft.getTraitSet.replace(pushDownDistribution)
              .replace(RelCollations.EMPTY)
            (RelOptRule.convert(getLeft, pushDownTraitSet), getRight)
          }
          copy(providedTraitSet, Seq(newLeft, newRight))
        }
      // SINGLETON, BROADCAST_DISTRIBUTED, ANY, RANDOM_DISTRIBUTED, ROUND_ROBIN_DISTRIBUTED
      // distribution cannot be pushed down.
      case _ => null
    }
  }
}
