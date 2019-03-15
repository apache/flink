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

package org.apache.flink.table.plan.nodes.physical.batch

import org.apache.flink.table.api.TableException
import org.apache.flink.table.plan.cost.{FlinkCost, FlinkCostFactory}
import org.apache.flink.table.plan.nodes.calcite.{ConstantRankRange, Rank, RankRange}
import org.apache.flink.table.plan.util.RelExplainUtil

import org.apache.calcite.plan._
import org.apache.calcite.rel._
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.sql.{SqlKind, SqlRankFunction}
import org.apache.calcite.util.ImmutableBitSet

import java.util

import scala.collection.JavaConversions._

/**
  * Batch physical RelNode for [[Rank]].
  */
class BatchExecRank(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputRel: RelNode,
    rankFunction: SqlRankFunction,
    partitionKey: ImmutableBitSet,
    sortCollation: RelCollation,
    rankRange: RankRange,
    val outputRankFunColumn: Boolean,
    val isGlobal: Boolean)
  extends Rank(
    cluster,
    traitSet,
    inputRel,
    rankFunction,
    partitionKey,
    sortCollation,
    rankRange)
  with BatchPhysicalRel {

  require(rankFunction.kind == SqlKind.RANK, "Only RANK function is supported now")
  val (rankStart, rankEnd) = rankRange match {
    case r: ConstantRankRange => (r.rankStart, r.rankEnd)
    case o => throw new TableException(s"$o is not supported now")
  }

  override def deriveRowType(): RelDataType = {
    if (outputRankFunColumn) {
      super.deriveRowType()
    } else {
      inputRel.getRowType
    }
  }

  override def copy(traitSet: RelTraitSet, inputs: util.List[RelNode]): RelNode = {
    new BatchExecRank(
      cluster,
      traitSet,
      inputs.get(0),
      rankFunction,
      partitionKey,
      sortCollation,
      rankRange,
      outputRankFunColumn,
      isGlobal
    )
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    val inputRowType = inputRel.getRowType
    pw.item("input", getInput)
      .item("rankFunction", rankFunction)
      .item("rankRange", rankRange.toString(inputRowType.getFieldNames))
      .item("partitionBy", RelExplainUtil.fieldToString(partitionKey.toArray, inputRowType))
      .item("orderBy", RelExplainUtil.collationToString(sortCollation, inputRowType))
      .item("global", isGlobal)
      .item("select", getRowType.getFieldNames.mkString(", "))
  }

  override def computeSelfCost(planner: RelOptPlanner, mq: RelMetadataQuery): RelOptCost = {
    // sort is done in the last sort operator, only need to compare between agg column.
    val inputRowCnt = mq.getRowCount(getInput())
    val cpuCost = FlinkCost.FUNC_CPU_COST * inputRowCnt
    val memCost: Double = mq.getAverageRowSize(this)
    val rowCount = mq.getRowCount(this)
    val costFactory = planner.getCostFactory.asInstanceOf[FlinkCostFactory]
    costFactory.makeCost(rowCount, cpuCost, 0, 0, memCost)
  }
}
