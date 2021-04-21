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

import org.apache.calcite.plan.RelOptRule
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.flink.table.planner.plan.`trait`.FlinkRelDistributionTraitDef
import org.apache.flink.table.planner.plan.nodes.FlinkConventions
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecSort
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalDistribution
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalSort

/**
 * Rule that matches [[FlinkLogicalDistribution]].
 */
class BatchPhysicalDistributionRule extends ConverterRule(
  classOf[FlinkLogicalDistribution],
  FlinkConventions.LOGICAL,
  FlinkConventions.BATCH_PHYSICAL,
  "BatchExecDistributionRule") {

  override def convert(rel: RelNode): RelNode = {
    val logicalDistribution = rel.asInstanceOf[FlinkLogicalDistribution]
    val distribution = logicalDistribution.getTraitSet.getTrait(
      FlinkRelDistributionTraitDef.INSTANCE)

    val input = logicalDistribution.getInput
    val requiredTraitSet = input.getTraitSet
      .replace(distribution)
      .replace(FlinkConventions.BATCH_PHYSICAL)
    val newInput = RelOptRule.convert(input, requiredTraitSet)
    if (logicalDistribution.collation.getFieldCollations.isEmpty) {
      newInput
    } else {
      val providedTraitSet = logicalDistribution.getTraitSet.replace(
        FlinkConventions.BATCH_PHYSICAL)
      new BatchPhysicalSort(
        logicalDistribution.getCluster,
        providedTraitSet,
        newInput,
        logicalDistribution.collation)
    }
  }

}

object BatchPhysicalDistributionRule {
  val INSTANCE: RelOptRule = new BatchPhysicalDistributionRule
}
