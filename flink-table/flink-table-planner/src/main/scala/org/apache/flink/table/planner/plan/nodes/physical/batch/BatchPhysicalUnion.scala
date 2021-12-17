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

import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.plan.`trait`.{FlinkRelDistribution, FlinkRelDistributionTraitDef}
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecUnion
import org.apache.flink.table.planner.plan.nodes.exec.{InputProperty, ExecNode}

import org.apache.calcite.plan.{RelOptCluster, RelOptRule, RelTraitSet}
import org.apache.calcite.rel.RelDistribution.Type.{ANY, BROADCAST_DISTRIBUTED, HASH_DISTRIBUTED, RANDOM_DISTRIBUTED, RANGE_DISTRIBUTED, ROUND_ROBIN_DISTRIBUTED, SINGLETON}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.{SetOp, Union}
import org.apache.calcite.rel.{RelNode, RelWriter}

import java.util

import scala.collection.JavaConversions._

/**
  * Batch physical RelNode for [[Union]].
  */
class BatchPhysicalUnion(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputRels: util.List[RelNode],
    all: Boolean,
    outputRowType: RelDataType)
  extends Union(cluster, traitSet, inputRels, all)
  with BatchPhysicalRel {

  require(all, "Only support union all now")

  override def deriveRowType(): RelDataType = outputRowType

  override def copy(traitSet: RelTraitSet, inputs: util.List[RelNode], all: Boolean): SetOp = {
    new BatchPhysicalUnion(cluster, traitSet, inputs, all, outputRowType)
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    super.explainTerms(pw)
      .item("union", getRowType.getFieldNames.mkString(", "))
  }

  override def satisfyTraits(requiredTraitSet: RelTraitSet): Option[RelNode] = {
    // union will destroy collation trait. So does not handle collation requirement.
    val requiredDistribution = requiredTraitSet.getTrait(FlinkRelDistributionTraitDef.INSTANCE)
    val canSatisfy = requiredDistribution.getType match {
      case RANDOM_DISTRIBUTED |
           ROUND_ROBIN_DISTRIBUTED |
           BROADCAST_DISTRIBUTED |
           HASH_DISTRIBUTED => true
      // range distribution cannot be satisfied because partition's [lower, upper] of each union
      // child may be different.
      case RANGE_DISTRIBUTED => false
      // singleton cannot cannot be satisfied because singleton exchange limits the parallelism of
      // exchange output RelNode to 1.
      // Push down Singleton into input of union will destroy the limitation.
      case SINGLETON => false
      // there is no need to satisfy Any distribution
      case ANY => false
    }
    if (!canSatisfy) {
      return None
    }

    val inputRequiredDistribution = requiredDistribution.getType match {
      case RANDOM_DISTRIBUTED | ROUND_ROBIN_DISTRIBUTED | BROADCAST_DISTRIBUTED =>
        requiredDistribution
      case HASH_DISTRIBUTED =>
        // apply strict hash distribution of each child
        // to avoid inconsistent of shuffle of each child
        FlinkRelDistribution.hash(requiredDistribution.getKeys)
    }
    val newInputs = getInputs.map(RelOptRule.convert(_, inputRequiredDistribution))
    val providedTraitSet = getTraitSet.replace(inputRequiredDistribution)
    Some(copy(providedTraitSet, newInputs))
  }

  override def translateToExecNode(): ExecNode[_] = {
    new BatchExecUnion(
      getInputs.map(_ => InputProperty.DEFAULT),
      FlinkTypeFactory.toLogicalRowType(getRowType),
      getRelDetailedDescription
    )
  }
}
