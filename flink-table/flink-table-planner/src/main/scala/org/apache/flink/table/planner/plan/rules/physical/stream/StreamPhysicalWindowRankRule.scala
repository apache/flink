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
import org.apache.flink.table.planner.plan.logical.WindowAttachedWindowingStrategy
import org.apache.flink.table.planner.plan.metadata.FlinkRelMetadataQuery
import org.apache.flink.table.planner.plan.nodes.FlinkConventions
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalRank
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalWindowRank
import org.apache.flink.table.planner.plan.utils.{RankUtil, WindowUtil}

import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rel.convert.ConverterRule.Config

/** Rule to convert a [[FlinkLogicalRank]] into a [[StreamPhysicalWindowRank]]. */
class StreamPhysicalWindowRankRule(config: Config) extends ConverterRule(config) {

  override def matches(call: RelOptRuleCall): Boolean = {
    val rank: FlinkLogicalRank = call.rel(0)

    val fmq = FlinkRelMetadataQuery.reuseOrCreate(call.getMetadataQuery)
    val windowProperties = fmq.getRelWindowProperties(rank.getInput)
    val partitionKey = rank.partitionKey
    WindowUtil.groupingContainsWindowStartEnd(partitionKey, windowProperties) &&
    !RankUtil.canConvertToDeduplicate(rank)
  }

  override def convert(rel: RelNode): RelNode = {
    val rank: FlinkLogicalRank = rel.asInstanceOf[FlinkLogicalRank]
    val fmq = FlinkRelMetadataQuery.reuseOrCreate(rel.getCluster.getMetadataQuery)
    val relWindowProperties = fmq.getRelWindowProperties(rank.getInput)
    val partitionKey = rank.partitionKey
    val (startColumns, endColumns, _, newPartitionKey) =
      WindowUtil.groupingExcludeWindowStartEndTimeColumns(partitionKey, relWindowProperties)
    val requiredDistribution = if (!newPartitionKey.isEmpty) {
      FlinkRelDistribution.hash(newPartitionKey.toArray, requireStrict = true)
    } else {
      FlinkRelDistribution.SINGLETON
    }

    val requiredTraitSet = rank.getCluster.getPlanner
      .emptyTraitSet()
      .replace(requiredDistribution)
      .replace(FlinkConventions.STREAM_PHYSICAL)
    val providedTraitSet = rank.getTraitSet.replace(FlinkConventions.STREAM_PHYSICAL)
    val newInput: RelNode = RelOptRule.convert(rank.getInput, requiredTraitSet)

    val windowingStrategy = new WindowAttachedWindowingStrategy(
      relWindowProperties.getWindowSpec,
      relWindowProperties.getTimeAttributeType,
      startColumns.toArray.head,
      endColumns.toArray.head)

    new StreamPhysicalWindowRank(
      rank.getCluster,
      providedTraitSet,
      newInput,
      newPartitionKey,
      rank.orderKey,
      rank.rankType,
      rank.rankRange,
      rank.rankNumberType,
      rank.outputRankNumber,
      windowingStrategy)
  }

}

object StreamPhysicalWindowRankRule {
  val INSTANCE = new StreamPhysicalWindowRankRule(
    Config.INSTANCE.withConversion(
      classOf[FlinkLogicalRank],
      FlinkConventions.LOGICAL,
      FlinkConventions.STREAM_PHYSICAL,
      "StreamPhysicalWindowRankRule"))

}
