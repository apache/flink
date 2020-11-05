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
import org.apache.flink.table.planner.plan.nodes.physical.stream.{StreamExecDeduplicate, StreamExecRank}
import org.apache.flink.table.runtime.operators.rank.{ConstantRankRange, RankType}

import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rel.{RelCollation, RelNode}

/**
  * Rule that matches [[FlinkLogicalRank]] which is sorted by time attribute and
  * limits 1 and its rank type is ROW_NUMBER, and converts it to [[StreamExecDeduplicate]].
  *
  * NOTES: Queries that can be converted to [[StreamExecDeduplicate]] could be converted to
  * [[StreamExecRank]] too. [[StreamExecDeduplicate]] is more efficient than [[StreamExecRank]]
  * due to mini-batch and less state access.
  *
  * e.g.
  * 1. {{{
  * SELECT a, b, c FROM (
  *   SELECT a, b, c, proctime,
  *          ROW_NUMBER() OVER (PARTITION BY a ORDER BY proctime ASC) as row_num
  *   FROM MyTable
  * ) WHERE row_num <= 1
  * }}} will be converted to StreamExecDeduplicate which keeps first row in proctime.
  *
  * 2. {{{
  * SELECT a, b, c FROM (
  *   SELECT a, b, c, rowtime,
  *          ROW_NUMBER() OVER (PARTITION BY a ORDER BY rowtime DESC) as row_num
  *   FROM MyTable
  * ) WHERE row_num <= 1
  * }}} will be converted to StreamExecDeduplicate which keeps last row in rowtime.
  */
class StreamExecDeduplicateRule
  extends ConverterRule(
    classOf[FlinkLogicalRank],
    FlinkConventions.LOGICAL,
    FlinkConventions.STREAM_PHYSICAL,
    "StreamExecDeduplicateRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val rank: FlinkLogicalRank = call.rel(0)
    StreamExecDeduplicateRule.canConvertToDeduplicate(rank)
  }

  override def convert(rel: RelNode): RelNode = {
    val rank = rel.asInstanceOf[FlinkLogicalRank]
    val requiredDistribution = if (rank.partitionKey.isEmpty) {
      FlinkRelDistribution.SINGLETON
    } else {
      FlinkRelDistribution.hash(rank.partitionKey.toList)
    }
    val requiredTraitSet = rel.getCluster.getPlanner.emptyTraitSet()
      .replace(FlinkConventions.STREAM_PHYSICAL)
      .replace(requiredDistribution)
    val convInput: RelNode = RelOptRule.convert(rank.getInput, requiredTraitSet)

    // order by timeIndicator desc ==> lastRow, otherwise is firstRow
    val fieldCollation = rank.orderKey.getFieldCollations.get(0)
    val isLastRow = fieldCollation.direction.isDescending

    val fieldType = rank.getInput().getRowType.getFieldList
      .get(fieldCollation.getFieldIndex).getType
    val isRowtime = FlinkTypeFactory.isRowtimeIndicatorType(fieldType)

    val providedTraitSet = rel.getTraitSet.replace(FlinkConventions.STREAM_PHYSICAL)
    new StreamExecDeduplicate(
      rel.getCluster,
      providedTraitSet,
      convInput,
      rank.partitionKey.toArray,
      isRowtime,
      isLastRow)
  }
}

object StreamExecDeduplicateRule {

  val RANK_INSTANCE = new StreamExecDeduplicateRule

  /**
    * Whether the given rank could be converted to [[StreamExecDeduplicate]].
    *
    * Returns true if the given rank is sorted by time attribute and limits 1
    * and its RankFunction is ROW_NUMBER, else false.
    *
    * @param rank The [[FlinkLogicalRank]] node
    * @return True if the input rank could be converted to [[StreamExecDeduplicate]]
    */
  def canConvertToDeduplicate(rank: FlinkLogicalRank): Boolean = {
    val sortCollation = rank.orderKey
    val rankRange = rank.rankRange

    val isRowNumberType = rank.rankType == RankType.ROW_NUMBER

    val isLimit1 = rankRange match {
      case rankRange: ConstantRankRange =>
        rankRange.getRankStart() == 1 && rankRange.getRankEnd() == 1
      case _ => false
    }

    val inputRowType = rank.getInput.getRowType
    val isSortOnProctime = sortOnTimeAttribute(sortCollation, inputRowType)

    !rank.outputRankNumber && isLimit1 && isSortOnProctime && isRowNumberType
  }

  private def sortOnTimeAttribute(
      sortCollation: RelCollation,
      inputRowType: RelDataType): Boolean = {
    if (sortCollation.getFieldCollations.size() != 1) {
      false
    } else {
      val firstSortField = sortCollation.getFieldCollations.get(0)
      val fieldType = inputRowType.getFieldList.get(firstSortField.getFieldIndex).getType
      FlinkTypeFactory.isProctimeIndicatorType(fieldType) ||
        FlinkTypeFactory.isRowtimeIndicatorType(fieldType)
    }
  }
}
