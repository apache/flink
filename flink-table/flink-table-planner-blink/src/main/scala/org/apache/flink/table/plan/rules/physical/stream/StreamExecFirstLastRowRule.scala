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

package org.apache.flink.table.plan.rules.physical.stream

import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.plan.`trait`.FlinkRelDistribution
import org.apache.flink.table.plan.nodes.FlinkConventions
import org.apache.flink.table.plan.nodes.calcite.{ConstantRankRange, RankType}
import org.apache.flink.table.plan.nodes.logical.{FlinkLogicalRank, FlinkLogicalSort}
import org.apache.flink.table.plan.nodes.physical.stream.{StreamExecFirstLastRow, StreamExecRank, StreamExecSortLimit}
import org.apache.flink.table.plan.util.FlinkRelOptUtil

import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rel.core.Sort
import org.apache.calcite.rel.{RelCollation, RelNode}

/**
  * Rule that matches [[FlinkLogicalSort]] which is sorted by proc-time attribute and
  * fetches only one record started from 0, and converts it to [[StreamExecFirstLastRow]].
  *
  * NOTES: Queries that can be converted to [[StreamExecFirstLastRow]] could be converted to
  * [[StreamExecSortLimit]] too. [[StreamExecFirstLastRow]] is more efficient than
  * [[StreamExecSortLimit]] due to mini-batch and less state access.
  *
  * e.g.
  * 1. ''SELECT a FROM MyTable ORDER BY proctime LIMIT 1'' will be converted to FirstRow
  * 2. ''SELECT a FROM MyTable ORDER BY proctime desc LIMIT 1'' will be converted to LastRow
  */
class StreamExecFirstLastRowFromSortRule
  extends ConverterRule(
    classOf[FlinkLogicalSort],
    FlinkConventions.LOGICAL,
    FlinkConventions.STREAM_PHYSICAL,
    "StreamExecFirstLastRowFromSortRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val sort: FlinkLogicalSort = call.rel(0)
    StreamExecFirstLastRowRule.canConvertToFirstLastRow(sort)
  }

  override def convert(rel: RelNode): RelNode = {
    val sort = rel.asInstanceOf[FlinkLogicalSort]
    val input = sort.getInput
    val requiredDistribution = FlinkRelDistribution.SINGLETON
    val requiredTraitSet = sort.getInput.getTraitSet
      .replace(FlinkConventions.STREAM_PHYSICAL)
      .replace(requiredDistribution)

    val convInput: RelNode = RelOptRule.convert(input, requiredTraitSet)

    val fieldCollation = sort.collation.getFieldCollations.get(0)
    val fieldType = input.getRowType.getFieldList.get(fieldCollation.getFieldIndex).getType
    val isRowtime = FlinkTypeFactory.isRowtimeIndicatorType(fieldType)
    val providedTraitSet = rel.getTraitSet.replace(FlinkConventions.STREAM_PHYSICAL)
    // order by timeIndicator desc ==> lastRow, otherwise is firstRow
    val isLastRow = fieldCollation.direction.isDescending
    new StreamExecFirstLastRow(
      rel.getCluster,
      providedTraitSet,
      convInput,
      Array(),
      isRowtime,
      isLastRow)
  }
}

/**
  * Rule that matches [[FlinkLogicalRank]] which is sorted by proc-time attribute and
  * limits 1 and its rank type is ROW_NUMBER, and converts it to [[StreamExecFirstLastRow]].
  *
  * NOTES: Queries that can be converted to [[StreamExecFirstLastRow]] could be converted to
  * [[StreamExecRank]] too. [[StreamExecFirstLastRow]] is more efficient than [[StreamExecRank]]
  * due to mini-batch and less state access.
  *
  * e.g.
  * 1. {{{
  * SELECT a, b, c FROM (
  *   SELECT a, b, c, proctime,
  *          ROW_NUMBER() OVER (PARTITION BY a ORDER BY proctime ASC) as row_num
  *   FROM MyTable
  * ) WHERE row_num <= 1
  * }}} will be converted to FirstRow
  * 2. {{{
  * SELECT a, b, c FROM (
  *   SELECT a, b, c, proctime,
  *          ROW_NUMBER() OVER (PARTITION BY a ORDER BY proctime DESC) as row_num
  *   FROM MyTable
  * ) WHERE row_num <= 1
  * }}} will be converted to LastRow
  */
class StreamExecFirstLastRowFromRankRule
  extends ConverterRule(
    classOf[FlinkLogicalRank],
    FlinkConventions.LOGICAL,
    FlinkConventions.STREAM_PHYSICAL,
    "StreamExecFirstLastRowFromRankRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val rank: FlinkLogicalRank = call.rel(0)
    StreamExecFirstLastRowRule.canConvertToFirstLastRow(rank)
  }

  override def convert(rel: RelNode): RelNode = {
    val rank = rel.asInstanceOf[FlinkLogicalRank]
    val fieldCollation = rank.orderKey.getFieldCollations.get(0)
    val fieldType = rank.getInput.getRowType.getFieldList.get(fieldCollation.getFieldIndex).getType
    val isRowtime = FlinkTypeFactory.isRowtimeIndicatorType(fieldType)

    val requiredDistribution = FlinkRelDistribution.hash(rank.partitionKey.toList)
    val requiredTraitSet = rel.getCluster.getPlanner.emptyTraitSet()
      .replace(FlinkConventions.STREAM_PHYSICAL)
      .replace(requiredDistribution)
    val convInput: RelNode = RelOptRule.convert(rank.getInput, requiredTraitSet)

    // order by timeIndicator desc ==> lastRow, otherwise is firstRow
    val isLastRow = fieldCollation.direction.isDescending
    val providedTraitSet = rel.getTraitSet.replace(FlinkConventions.STREAM_PHYSICAL)
    new StreamExecFirstLastRow(
      rel.getCluster,
      providedTraitSet,
      convInput,
      rank.partitionKey.toArray,
      isRowtime,
      isLastRow)
  }
}

object StreamExecFirstLastRowRule {

  val SORT_INSTANCE = new StreamExecFirstLastRowFromSortRule
  val RANK_INSTANCE = new StreamExecFirstLastRowFromRankRule

  /**
    * Whether the given rank could be converted to [[StreamExecFirstLastRow]].
    *
    * Returns true if the given rank is sorted by proc-time attribute and limits 1
    * and its RankFunction is ROW_NUMBER, else false.
    *
    * @param rank The [[FlinkLogicalRank]] node
    * @return True if the input rank could be converted to [[StreamExecFirstLastRow]]
    */
  def canConvertToFirstLastRow(rank: FlinkLogicalRank): Boolean = {
    val sortCollation = rank.orderKey
    val rankRange = rank.rankRange

    val isRowNumberType = rank.rankType == RankType.ROW_NUMBER

    val isLimit1 = rankRange match {
      case ConstantRankRange(rankStart, rankEnd) => rankStart == 1 && rankEnd == 1
      case _ => false
    }

    val inputRowType = rank.getInput.getRowType
    val isSortOnProctime = sortOnProcTimeAttribute(sortCollation, inputRowType)

    !rank.outputRankNumber && isLimit1 && isSortOnProctime && isRowNumberType
  }

  /**
    * Whether the given sort could be converted to [[StreamExecFirstLastRow]].
    *
    * Return true if the given sort is sorted by proc-time attribute and
    * fetches only one record started from 0, else false.
    *
    * @param sort the [[Sort]] node
    * @return True if the input sort could be converted to [[StreamExecFirstLastRow]]
    */
  def canConvertToFirstLastRow(sort: FlinkLogicalSort): Boolean = {
    val sortCollation = sort.collation
    val inputRowType = sort.getInput.getRowType
    val isSortOnProctime = sortOnProcTimeAttribute(sortCollation, inputRowType)
    val isStart0 = FlinkRelOptUtil.getLimitStart(sort.offset) == 0
    val isLimit1 = FlinkRelOptUtil.getLimitEnd(sort.offset, sort.fetch) == 1

    isSortOnProctime && isStart0 && isLimit1
  }

  private def sortOnProcTimeAttribute(
      sortCollation: RelCollation,
      inputRowType: RelDataType): Boolean = {
    if (sortCollation.getFieldCollations.size() != 1) {
      false
    } else {
      val firstSortField = sortCollation.getFieldCollations.get(0)
      val fieldType = inputRowType.getFieldList.get(firstSortField.getFieldIndex).getType
      FlinkTypeFactory.isProctimeIndicatorType(fieldType)
    }
  }
}
