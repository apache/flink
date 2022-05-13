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

import org.apache.flink.table.planner.plan.`trait`.FlinkRelDistribution
import org.apache.flink.table.planner.plan.`trait`.FlinkRelDistribution.BROADCAST_DISTRIBUTED
import org.apache.flink.table.planner.plan.nodes.FlinkConventions.BATCH_PHYSICAL
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalNestedLoopJoin

import org.apache.calcite.plan.RelOptRule
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.{Join, JoinRelType}

trait BatchPhysicalNestedLoopJoinRuleBase {

  def createNestedLoopJoin(
      join: Join,
      left: RelNode,
      right: RelNode,
      leftIsBuild: Boolean,
      singleRowJoin: Boolean): RelNode = {
    var leftRequiredTrait = join.getTraitSet.replace(BATCH_PHYSICAL)
    var rightRequiredTrait = join.getTraitSet.replace(BATCH_PHYSICAL)

    if (join.getJoinType == JoinRelType.FULL) {
      leftRequiredTrait = leftRequiredTrait.replace(FlinkRelDistribution.SINGLETON)
      rightRequiredTrait = rightRequiredTrait.replace(FlinkRelDistribution.SINGLETON)
    } else {
      if (leftIsBuild) {
        leftRequiredTrait = leftRequiredTrait.replace(BROADCAST_DISTRIBUTED)
      } else {
        rightRequiredTrait = rightRequiredTrait.replace(BROADCAST_DISTRIBUTED)
      }
    }

    val newLeft = RelOptRule.convert(left, leftRequiredTrait)
    val newRight = RelOptRule.convert(right, rightRequiredTrait)
    val providedTraitSet = join.getTraitSet.replace(BATCH_PHYSICAL)

    new BatchPhysicalNestedLoopJoin(
      join.getCluster,
      providedTraitSet,
      newLeft,
      newRight,
      join.getCondition,
      join.getJoinType,
      leftIsBuild,
      singleRowJoin)
  }
}
