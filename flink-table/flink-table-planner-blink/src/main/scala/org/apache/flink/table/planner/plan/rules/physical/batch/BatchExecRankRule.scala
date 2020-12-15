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

import org.apache.flink.table.api.TableException
import org.apache.flink.table.planner.plan.`trait`.FlinkRelDistribution
import org.apache.flink.table.planner.plan.nodes.FlinkConventions
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalRank
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchExecRank
import org.apache.flink.table.planner.plan.utils.FlinkRelOptUtil
import org.apache.flink.table.runtime.operators.rank.{ConstantRankRange, RankType}

import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rel.{RelCollations, RelNode}

import scala.collection.JavaConversions._

/**
  * Rule that matches [[FlinkLogicalRank]] with rank function and constant rank range,
  * and converts it to
  * {{{
  * BatchExecRank (global)
  * +- BatchPhysicalExchange (singleton if partition keys is empty, else hash)
  *    +- BatchExecRank (local)
  *       +- input of rank
  * }}}
  */
class BatchExecRankRule
  extends ConverterRule(
    classOf[FlinkLogicalRank],
    FlinkConventions.LOGICAL,
    FlinkConventions.BATCH_PHYSICAL,
    "BatchExecRankRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val rank: FlinkLogicalRank = call.rel(0)
    // Only support rank() now
    rank.rankType == RankType.RANK && rank.rankRange.isInstanceOf[ConstantRankRange]
  }

  def convert(rel: RelNode): RelNode = {
    val rank = rel.asInstanceOf[FlinkLogicalRank]
    val (_, rankEnd) = rank.rankRange match {
      case r: ConstantRankRange => (r.getRankStart, r.getRankEnd)
      case o => throw new TableException(s"$o is not supported now")
    }

    val cluster = rel.getCluster
    val emptyTraits = cluster.getPlanner.emptyTraitSet().replace(FlinkConventions.BATCH_PHYSICAL)
    val sortFieldCollations = rank.partitionKey.asList()
      .map(FlinkRelOptUtil.ofRelFieldCollation(_)) ++ rank.orderKey.getFieldCollations
    val sortCollation = RelCollations.of(sortFieldCollations: _*)
    val localRequiredTraitSet = emptyTraits.replace(sortCollation)
    val newLocalInput = RelOptRule.convert(rank.getInput, localRequiredTraitSet)

    // create local BatchExecRank
    val localRankRange = new ConstantRankRange(1, rankEnd) // local rank always start from 1
    val localRank = new BatchExecRank(
      cluster,
      emptyTraits,
      newLocalInput,
      rank.partitionKey,
      rank.orderKey,
      rank.rankType,
      localRankRange,
      rank.rankNumberType,
      outputRankNumber = false,
      isGlobal = false
    )

    // create local BatchExecRank
    val globalRequiredDistribution = if (rank.partitionKey.isEmpty) {
      FlinkRelDistribution.SINGLETON
    } else {
      FlinkRelDistribution.hash(rank.partitionKey.toList, requireStrict = false)
    }

    val globalRequiredTraitSet = emptyTraits
      .replace(globalRequiredDistribution)
      .replace(sortCollation)

    // require SINGLETON or HASH exchange
    val newGlobalInput = RelOptRule.convert(localRank, globalRequiredTraitSet)
    val globalRank = new BatchExecRank(
      cluster,
      emptyTraits,
      newGlobalInput,
      rank.partitionKey,
      rank.orderKey,
      rank.rankType,
      rank.rankRange,
      rank.rankNumberType,
      rank.outputRankNumber,
      isGlobal = true
    )
    globalRank
  }
}

object BatchExecRankRule {
  val INSTANCE: RelOptRule = new BatchExecRankRule
}
