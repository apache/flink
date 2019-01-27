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

package org.apache.flink.table.plan.rules.physical.batch

import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.flink.table.plan.`trait`.FlinkRelDistribution
import org.apache.flink.table.plan.nodes.FlinkConventions
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecSortLimit
import org.apache.flink.table.plan.nodes.logical.FlinkLogicalSort

class BatchExecSortLimitRule
  extends ConverterRule(
    classOf[FlinkLogicalSort],
    FlinkConventions.LOGICAL,
    FlinkConventions.BATCH_PHYSICAL,
    "BatchExecSortLimitRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val sort = call.rel(0).asInstanceOf[FlinkLogicalSort]
    !sort.getCollation.getFieldCollations.isEmpty && (sort.fetch != null)
  }

  override def convert(rel: RelNode): RelNode = {
    val sort = rel.asInstanceOf[FlinkLogicalSort]
    val input = sort.getInput
    //convert localSortLimit --> globalSortLimit if limit is not null
    val localRequiredTrait = input.getTraitSet.replace(FlinkConventions.BATCH_PHYSICAL)
    val localInput = RelOptRule.convert(sort.getInput, localRequiredTrait)
    val providedLocalTraitSet = localRequiredTrait.replace(sort.getCollation)
    val localSortLimit = new BatchExecSortLimit(
      rel.getCluster,
      providedLocalTraitSet,
      localInput,
      sort.getCollation,
      sort.offset,
      sort.fetch,
      false,
      description
    )

    //global
    val requiredTrait = rel.getCluster.getPlanner.emptyTraitSet()
      .replace(FlinkConventions.BATCH_PHYSICAL)
      .replace(FlinkRelDistribution.SINGLETON)

    val newInput = RelOptRule.convert(localSortLimit, requiredTrait)
    val providedGlobalTraitSet = requiredTrait.replace(sort.getCollation)
    val globalSortLimit = new BatchExecSortLimit(
      rel.getCluster,
      providedGlobalTraitSet,
      newInput,
      sort.getCollation,
      sort.offset,
      sort.fetch,
      true,
      description
    )
    globalSortLimit
  }
}

object BatchExecSortLimitRule {
  val INSTANCE: RelOptRule = new BatchExecSortLimitRule
}
