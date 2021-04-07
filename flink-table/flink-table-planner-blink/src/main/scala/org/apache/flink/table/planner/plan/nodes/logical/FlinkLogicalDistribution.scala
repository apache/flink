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

package org.apache.flink.table.planner.plan.nodes.logical

import org.apache.calcite.plan.{Convention, RelOptCluster, RelOptRule, RelTraitSet}
import org.apache.calcite.rel.{RelCollation, RelCollationTraitDef, RelNode, SingleRel}

import java.util.{List => JList}
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.flink.table.planner.plan.`trait`.FlinkRelDistribution
import org.apache.flink.table.planner.plan.nodes.FlinkConventions
import org.apache.flink.table.planner.plan.nodes.hive.LogicalDistribution

/**
 * A FlinkLogicalRel to represent Hive's SORT BY, DISTRIBUTE BY, and CLUSTER BY semantics.
 */
class FlinkLogicalDistribution(
  cluster: RelOptCluster,
  traits: RelTraitSet,
  child: RelNode,
  val collation: RelCollation,
  val distKeys: JList[Integer])
  extends SingleRel(cluster, traits, child)
    with FlinkLogicalRel {

  override def copy(traitSet: RelTraitSet, inputs: JList[RelNode]): RelNode =
    new FlinkLogicalDistribution(getCluster, traitSet, inputs.get(0), collation, distKeys)
}

class FlinkLogicalDistributionBatchConverter extends ConverterRule(
  classOf[LogicalDistribution],
  Convention.NONE,
  FlinkConventions.LOGICAL,
  "FlinkLogicalDistributionBatchConverter") {

  override def convert(rel: RelNode): RelNode = {
    val distribution = rel.asInstanceOf[LogicalDistribution]
    val newInput = RelOptRule.convert(distribution.getInput, FlinkConventions.LOGICAL)
    FlinkLogicalDistribution.create(
      newInput, distribution.getCollation, distribution.getDistKeys)
  }
}

object FlinkLogicalDistribution {
  val BATCH_CONVERTER: RelOptRule = new FlinkLogicalDistributionBatchConverter

  def create(
    input: RelNode,
    collation: RelCollation,
    distKeys: JList[Integer]): FlinkLogicalDistribution = {
    val cluster = input.getCluster
    val collationTrait = RelCollationTraitDef.INSTANCE.canonize(collation)
    val traitSet = if (distKeys.isEmpty) {
      cluster.traitSetOf(FlinkConventions.LOGICAL)
        .replace(collationTrait)
        .replace(FlinkRelDistribution.ANY)
    } else {
      cluster.traitSetOf(FlinkConventions.LOGICAL)
        .replace(collationTrait)
        .replace(FlinkRelDistribution.hash(distKeys))
    }
    new FlinkLogicalDistribution(cluster, traitSet, input, collation, distKeys)
  }
}
