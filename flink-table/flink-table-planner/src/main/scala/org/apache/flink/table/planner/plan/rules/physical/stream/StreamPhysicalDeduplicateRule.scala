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

import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.plan.`trait`.FlinkRelDistribution
import org.apache.flink.table.planner.plan.nodes.FlinkConventions
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalRank
import org.apache.flink.table.planner.plan.nodes.physical.stream.{StreamPhysicalDeduplicate, StreamPhysicalRank}
import org.apache.flink.table.planner.plan.utils.RankUtil

import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rel.convert.ConverterRule.Config

/**
 * Rule that matches [[FlinkLogicalRank]] which is sorted by time attribute and limits 1 and its
 * rank type is ROW_NUMBER, and converts it to [[StreamPhysicalDeduplicate]].
 *
 * NOTES: Queries that can be converted to [[StreamPhysicalDeduplicate]] could be converted to
 * [[StreamPhysicalRank]] too. [[StreamPhysicalDeduplicate]] is more efficient than
 * [[StreamPhysicalRank]] due to mini-batch and less state access.
 *
 * e.g.
 *   1. {{{ SELECT a, b, c FROM ( SELECT a, b, c, proctime, ROW_NUMBER() OVER (PARTITION BY a ORDER
 *      BY proctime ASC) as row_num FROM MyTable ) WHERE row_num <= 1 }}} will be converted to
 *      StreamExecDeduplicate which keeps first row in proctime.
 *
 * 2. {{{ SELECT a, b, c FROM ( SELECT a, b, c, rowtime, ROW_NUMBER() OVER (PARTITION BY a ORDER BY
 * rowtime DESC) as row_num FROM MyTable ) WHERE row_num <= 1 }}} will be converted to
 * StreamExecDeduplicate which keeps last row in rowtime.
 */
class StreamPhysicalDeduplicateRule(config: Config) extends ConverterRule(config) {

  override def matches(call: RelOptRuleCall): Boolean = {
    val rank: FlinkLogicalRank = call.rel(0)
    RankUtil.canConvertToDeduplicate(rank)
  }

  override def convert(rel: RelNode): RelNode = {
    val rank = rel.asInstanceOf[FlinkLogicalRank]
    val requiredDistribution = if (rank.partitionKey.isEmpty) {
      FlinkRelDistribution.SINGLETON
    } else {
      FlinkRelDistribution.hash(rank.partitionKey.toList)
    }
    val requiredTraitSet = rel.getCluster.getPlanner
      .emptyTraitSet()
      .replace(FlinkConventions.STREAM_PHYSICAL)
      .replace(requiredDistribution)
    val convInput: RelNode = RelOptRule.convert(rank.getInput, requiredTraitSet)

    // order by timeIndicator desc ==> lastRow, otherwise is firstRow
    val fieldCollation = rank.orderKey.getFieldCollations.get(0)
    val isLastRow = fieldCollation.direction.isDescending

    val fieldType = rank
      .getInput()
      .getRowType
      .getFieldList
      .get(fieldCollation.getFieldIndex)
      .getType
    val isRowtime = FlinkTypeFactory.isRowtimeIndicatorType(fieldType)

    val providedTraitSet = rel.getTraitSet.replace(FlinkConventions.STREAM_PHYSICAL)
    new StreamPhysicalDeduplicate(
      rel.getCluster,
      providedTraitSet,
      convInput,
      rank.partitionKey.toArray,
      isRowtime,
      isLastRow)
  }
}

object StreamPhysicalDeduplicateRule {
  val INSTANCE = new StreamPhysicalDeduplicateRule(
    Config.INSTANCE.withConversion(
      classOf[FlinkLogicalRank],
      FlinkConventions.LOGICAL,
      FlinkConventions.STREAM_PHYSICAL,
      "StreamPhysicalDeduplicateRule"))
}
