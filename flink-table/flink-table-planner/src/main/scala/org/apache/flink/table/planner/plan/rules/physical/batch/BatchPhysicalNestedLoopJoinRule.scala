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
package org.apache.flink.table.planner.plan.rules.physical.batch

import org.apache.flink.table.api.TableException
import org.apache.flink.table.planner.hint.JoinStrategy
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalJoin
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalNestedLoopJoin
import org.apache.flink.table.planner.utils.ShortcutUtils.unwrapTableConfig

import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.calcite.plan.RelOptRule.{any, operand}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.{Join, JoinRelType}

import scala.collection.JavaConversions._

/**
 * Rule that converts [[FlinkLogicalJoin]] to [[BatchPhysicalNestedLoopJoin]] if NestedLoopJoin is
 * enabled.
 */
class BatchPhysicalNestedLoopJoinRule
  extends RelOptRule(
    operand(classOf[FlinkLogicalJoin], operand(classOf[RelNode], any)),
    "BatchPhysicalNestedLoopJoinRule")
  with BatchPhysicalJoinRuleBase
  with BatchPhysicalNestedLoopJoinRuleBase {

  override def matches(call: RelOptRuleCall): Boolean = {
    val join: Join = call.rel(0)
    val tableConfig = unwrapTableConfig(join)
    canUseJoinStrategy(join, tableConfig, JoinStrategy.NEST_LOOP)
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val join: Join = call.rel(0)
    val tableConfig = unwrapTableConfig(join)
    val left = join.getLeft
    val right = join.getJoinType match {
      case JoinRelType.SEMI | JoinRelType.ANTI =>
        // We can do a distinct to buildSide(right) when semi join.
        val distinctKeys = 0 until join.getRight.getRowType.getFieldCount
        val useBuildDistinct = chooseSemiBuildDistinct(join.getRight, distinctKeys)
        if (useBuildDistinct) {
          addLocalDistinctAgg(join.getRight, distinctKeys)
        } else {
          join.getRight
        }
      case _ => join.getRight
    }

    val firstValidJoinHintOp = getFirstValidJoinHint(join, tableConfig)

    val temJoin = join.copy(join.getTraitSet, List(left, right))

    val isLeftToBuild = firstValidJoinHintOp match {
      case Some(firstValidJoinHint) =>
        firstValidJoinHint match {
          case JoinStrategy.NEST_LOOP =>
            val (_, isLeft) = checkNestLoopJoin(temJoin, tableConfig, withNestLoopHint = true)
            isLeft
          case _ =>
            // this should not happen
            throw new TableException(String.format(
              "The planner is trying to convert the " +
                "`FlinkLogicalJoin` using NEST_LOOP, but the valid join hint is not NEST_LOOP: %s",
              firstValidJoinHint
            ))
        }
      case None =>
        // treat as non-join-hints
        val (_, isLeft) = checkNestLoopJoin(temJoin, tableConfig, withNestLoopHint = false)
        isLeft
    }

    val newJoin = createNestedLoopJoin(join, left, right, isLeftToBuild, singleRowJoin = false)
    call.transformTo(newJoin)
  }
}

object BatchPhysicalNestedLoopJoinRule {
  val INSTANCE: RelOptRule = new BatchPhysicalNestedLoopJoinRule
}
