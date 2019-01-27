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
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rel.{RelCollations, RelNode}
import org.apache.calcite.sql.SqlKind
import org.apache.flink.table.api.TableException
import org.apache.flink.table.plan.`trait`.FlinkRelDistribution
import org.apache.flink.table.plan.nodes.FlinkConventions
import org.apache.flink.table.plan.nodes.logical.FlinkLogicalRank
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecRank
import org.apache.flink.table.plan.util.ConstantRankRange
import org.apache.flink.table.runtime.aggregate.RelFieldCollations

import scala.collection.JavaConversions._

class BatchExecRankRule
  extends ConverterRule(
    classOf[FlinkLogicalRank],
    FlinkConventions.LOGICAL,
    FlinkConventions.BATCH_PHYSICAL,
    "BatchExecRankRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val rank: FlinkLogicalRank = call.rel(0)
    // Only support rank() now
    rank.rankFunction.kind == SqlKind.RANK && rank.rankRange.isInstanceOf[ConstantRankRange]
  }

  def convert(rel: RelNode): RelNode = {
    val rank = rel.asInstanceOf[FlinkLogicalRank]
    val (_, rankEnd) = rank.rankRange match {
      case r: ConstantRankRange => (r.rankStart, r.rankEnd)
      case o => throw new TableException(s"$o is not supported now")
    }

    val cluster = rel.getCluster
    val emptyBatchExecTraits = cluster.getPlanner.emptyTraitSet()
      .replace(FlinkConventions.BATCH_PHYSICAL)
    val sortFieldCollations = rank.partitionKey.asList().map(RelFieldCollations.of(_)) ++
      rank.sortCollation.getFieldCollations
    val sortCollation = RelCollations.of(sortFieldCollations: _*)
    val localRequiredTraitSet = emptyBatchExecTraits.replace(sortCollation)
    val newLocalInput = RelOptRule.convert(rank.getInput, localRequiredTraitSet)

    // local rank
    val localRankRange = ConstantRankRange(1, rankEnd) // always start from 1
    val localRank = new BatchExecRank(
      cluster,
      emptyBatchExecTraits,
      newLocalInput,
      rank.rankFunction,
      rank.partitionKey,
      rank.sortCollation,
      localRankRange,
      outputRankFunColumn = false,
      isGlobal = false
    )

    // global
    val globalRequiredDistribution = if (rank.partitionKey.isEmpty) {
      FlinkRelDistribution.SINGLETON
    } else {
      FlinkRelDistribution.hash(rank.partitionKey.toList, requireStrict = false)
    }

    val globalRequiredTraitSet = emptyBatchExecTraits
      .replace(globalRequiredDistribution)
      .replace(sortCollation)

    val newGlobalInput = RelOptRule.convert(localRank, globalRequiredTraitSet)
    val globalRank = new BatchExecRank(
      cluster,
      emptyBatchExecTraits,
      newGlobalInput,
      rank.rankFunction,
      rank.partitionKey,
      rank.sortCollation,
      rank.rankRange,
      rank.outputRankFunColumn,
      isGlobal = true
    )
    globalRank
  }
}

object BatchExecRankRule {
  val INSTANCE: RelOptRule = new BatchExecRankRule
}
