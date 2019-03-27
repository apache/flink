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

import org.apache.flink.table.api.TableException
import org.apache.flink.table.plan.`trait`.FlinkRelDistribution
import org.apache.flink.table.plan.nodes.FlinkConventions
import org.apache.flink.table.plan.nodes.calcite.ConstantRankRange
import org.apache.flink.table.plan.nodes.logical.{FlinkLogicalRank, FlinkLogicalSort}
import org.apache.flink.table.plan.nodes.physical.stream.{StreamExecFirstLastRow, StreamExecRank}
import org.apache.flink.table.plan.util.FlinkRelOptUtil

import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rex.RexLiteral
import org.apache.calcite.sql.fun.SqlStdOperatorTable
import org.apache.calcite.util.ImmutableBitSet

/**
  * Rule that converts [[FlinkLogicalSort]] with non-null `fetch` to [[StreamExecRank]].
  * NOTES: the sort can not be converted to [[StreamExecFirstLastRow]].
  */
class StreamExecRankFromSortRule
  extends ConverterRule(
    classOf[FlinkLogicalSort],
    FlinkConventions.LOGICAL,
    FlinkConventions.STREAM_PHYSICAL,
    "StreamExecRankFromSortRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val sort: FlinkLogicalSort = call.rel(0)
    StreamExecRankRule.canConvertToRank(sort) &&
      !StreamExecFirstLastRowRule.canConvertToFirstLastRow(sort)
  }

  override def convert(rel: RelNode): RelNode = {
    val sort = rel.asInstanceOf[FlinkLogicalSort]
    val input = sort.getInput
    val requiredDistribution = FlinkRelDistribution.SINGLETON

    val requiredTraitSet = input.getTraitSet
      .replace(FlinkConventions.STREAM_PHYSICAL)
      .replace(requiredDistribution)

    val newInput: RelNode = RelOptRule.convert(input, requiredTraitSet)

    val rankStart = FlinkRelOptUtil.getLimitStart(sort.offset) + 1 // rank start always start from 1

    val rankEnd = if (sort.fetch != null) {
      FlinkRelOptUtil.getLimitEnd(sort.offset, sort.fetch)
    } else {
      // we have checked in matches method that fetch is not null
      throw new TableException("This should never happen, please file an issue.")
    }

    val providedTraitSet = rel.getTraitSet.replace(FlinkConventions.STREAM_PHYSICAL)
    new StreamExecRank(
      rel.getCluster,
      providedTraitSet,
      newInput,
      SqlStdOperatorTable.ROW_NUMBER,
      ImmutableBitSet.of(),
      sort.collation,
      ConstantRankRange(rankStart, rankEnd),
      outputRankFunColumn = false)
  }
}

/**
  * Rule that converts [[FlinkLogicalRank]] with fetch to [[StreamExecRank]].
  * NOTES: the rank can not be converted to [[StreamExecFirstLastRow]].
  */
class StreamExecRankFromRankRule
  extends ConverterRule(
    classOf[FlinkLogicalRank],
    FlinkConventions.LOGICAL,
    FlinkConventions.STREAM_PHYSICAL,
    "StreamExecRankFromRankRule") {


  override def matches(call: RelOptRuleCall): Boolean = {
    val rank: FlinkLogicalRank = call.rel(0)
    !StreamExecFirstLastRowRule.canConvertToFirstLastRow(rank)
  }

  override def convert(rel: RelNode): RelNode = {
    val rank = rel.asInstanceOf[FlinkLogicalRank]
    val input = rank.getInput
    val requiredDistribution = if (!rank.partitionKey.isEmpty) {
      FlinkRelDistribution.hash(rank.partitionKey.asList())
    } else {
      FlinkRelDistribution.SINGLETON
    }
    val requiredTraitSet = input.getTraitSet
      .replace(FlinkConventions.STREAM_PHYSICAL)
      .replace(requiredDistribution)
    val providedTraitSet = rank.getTraitSet.replace(FlinkConventions.STREAM_PHYSICAL)
    val newInput: RelNode = RelOptRule.convert(input, requiredTraitSet)

    new StreamExecRank(
      rank.getCluster,
      providedTraitSet,
      newInput,
      rank.rankFunction,
      rank.partitionKey,
      rank.sortCollation,
      rank.rankRange,
      rank.outputRankFunColumn)
  }
}

object StreamExecRankRule {
  val SORT_INSTANCE: RelOptRule = new StreamExecRankFromSortRule
  val RANK_INSTANCE: RelOptRule = new StreamExecRankFromRankRule

  /**
    * Whether the given sort could be converted to [[StreamExecRank]].
    *
    * Return true if `fetch` is not null and is greater than 0, else false.
    *
    * @param sort the [[FlinkLogicalSort]] node
    * @return True if the input sort could be converted to [[StreamExecRank]]
    */
  def canConvertToRank(sort: FlinkLogicalSort): Boolean = {
    sort.fetch != null && RexLiteral.intValue(sort.fetch) > 0
  }
}
