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
package org.apache.flink.table.planner.plan.rules.physical.stream

import org.apache.flink.table.planner.plan.`trait`.FlinkRelDistribution
import org.apache.flink.table.planner.plan.nodes.FlinkConventions
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalSort
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalSortLimit
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.flink.table.planner.plan.utils.RankProcessStrategy

/**
  * Rule that matches [[FlinkLogicalSort]] with non-empty sort fields and non-null fetch or offset,
  * and converts it to [[StreamPhysicalSortLimit]].
  */
class StreamPhysicalSortLimitRule
  extends ConverterRule(
    classOf[FlinkLogicalSort],
    FlinkConventions.LOGICAL,
    FlinkConventions.STREAM_PHYSICAL,
    "StreamPhysicalSortLimitRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val sort: FlinkLogicalSort = call.rel(0)
    // only matches Sort with non-empty sort fields and non-null fetch
    !sort.getCollation.getFieldCollations.isEmpty && (sort.fetch != null || sort.offset != null)
  }

  override def convert(rel: RelNode): RelNode = {
    val sort: FlinkLogicalSort = rel.asInstanceOf[FlinkLogicalSort]
    val input = sort.getInput
    val requiredTraitSet = input.getTraitSet
      .replace(FlinkRelDistribution.SINGLETON)
      .replace(FlinkConventions.STREAM_PHYSICAL)
    val providedTraitSet = sort.getTraitSet
      .replace(FlinkRelDistribution.SINGLETON)
      .replace(FlinkConventions.STREAM_PHYSICAL)

    val newInput: RelNode = RelOptRule.convert(input, requiredTraitSet)
    new StreamPhysicalSortLimit(
      rel.getCluster,
      providedTraitSet,
      newInput,
      sort.collation,
      sort.offset,
      sort.fetch,
      RankProcessStrategy.UNDEFINED_STRATEGY)
  }
}

object StreamPhysicalSortLimitRule {
  val INSTANCE: RelOptRule = new StreamPhysicalSortLimitRule
}
