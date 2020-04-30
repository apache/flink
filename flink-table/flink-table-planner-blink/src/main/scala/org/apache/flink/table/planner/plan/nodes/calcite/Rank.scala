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

package org.apache.flink.table.planner.plan.nodes.calcite

import org.apache.flink.table.api.TableException
import org.apache.flink.table.planner.plan.utils._
import org.apache.flink.table.runtime.operators.rank.{ConstantRankRange, RankRange, RankType, VariableRankRange}

import org.apache.calcite.plan.{RelOptCluster, RelOptCost, RelOptPlanner, RelTraitSet}
import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeField}
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rel.{RelCollation, RelNode, RelWriter, SingleRel}
import org.apache.calcite.util.{ImmutableBitSet, NumberUtil}

import scala.collection.JavaConversions._

/**
  * Relational expression that returns the rows in which the rank number of each row
  * is in the given range.
  *
  * The node is an optimization of `OVER` for some special cases,
  * e.g.
  * {{{
  * SELECT * FROM (
  *  SELECT a, b, RANK() OVER (PARTITION BY b ORDER BY c) rk FROM MyTable) t
  * WHERE rk < 10
  * }}}
  * can be converted to this node.
  *
  * @param cluster          cluster that this relational expression belongs to
  * @param traitSet         the traits of this rel
  * @param input            input relational expression
  * @param partitionKey     partition keys (may be empty)
  * @param orderKey         order keys (should not be empty)
  * @param rankType         rank type to define how exactly generate rank number
  * @param rankRange        the expected range of rank number value
  * @param rankNumberType   the field type of rank number
  * @param outputRankNumber whether output rank number
  */
abstract class Rank(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    input: RelNode,
    val partitionKey: ImmutableBitSet,
    val orderKey: RelCollation,
    val rankType: RankType,
    val rankRange: RankRange,
    val rankNumberType: RelDataTypeField,
    val outputRankNumber: Boolean)
  extends SingleRel(cluster, traitSet, input) {

  if (orderKey.getFieldCollations.isEmpty) {
    throw new TableException("orderKey should not be empty.")
  }

  rankRange match {
    case r: ConstantRankRange =>
      if (r.getRankEnd <= 0) {
        throw new TableException(
          s"Rank end can't smaller than zero. The rank end is ${r.getRankEnd}")
      }
      if (r.getRankStart > r.getRankEnd) {
        throw new TableException(
          s"Rank start '${r.getRankStart}' can't greater than rank end '${r.getRankEnd}'.")
      }
    case v: VariableRankRange =>
      if (v.getRankEndIndex < 0) {
        throw new TableException(s"Rank end index can't smaller than zero.")
      }
      if (v.getRankEndIndex >= input.getRowType.getFieldCount) {
        throw new TableException(s"Rank end index can't greater than input field count.")
      }
  }

  override def deriveRowType(): RelDataType = {
    if (!outputRankNumber) {
      return input.getRowType
    }
    // output row type = input row type + rank number type
    val typeFactory = cluster.getRexBuilder.getTypeFactory
    val typeBuilder = typeFactory.builder()
    input.getRowType.getFieldList.foreach(typeBuilder.add)
    typeBuilder.add(rankNumberType)
    typeBuilder.build()
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    val select = getRowType.getFieldNames.zipWithIndex.map {
      case (name, idx) => s"$name=$$$idx"
    }.mkString(", ")
    super.explainTerms(pw)
      .item("rankType", rankType)
      .item("rankRange", rankRange)
      .item("partitionBy", partitionKey.map(i => s"$$$i").mkString(","))
      .item("orderBy", RelExplainUtil.collationToString(orderKey))
      .item("select", select)
  }

  override def estimateRowCount(mq: RelMetadataQuery): Double = {
    val countPerGroup = FlinkRelMdUtil.getRankRangeNdv(rankRange)
    if (partitionKey.isEmpty) {
      // only one group
      countPerGroup
    } else {
      val inputRowCount = mq.getRowCount(input)
      val numOfGroup = mq.getDistinctRowCount(input, partitionKey, null)
      if (numOfGroup != null) {
        NumberUtil.min(numOfGroup * countPerGroup, inputRowCount)
      } else {
        NumberUtil.min(mq.getRowCount(input) * 0.1, inputRowCount)
      }
    }
  }

  override def computeSelfCost(planner: RelOptPlanner, mq: RelMetadataQuery): RelOptCost = {
    val rowCount = mq.getRowCount(input)
    val cpuCost = rowCount
    planner.getCostFactory.makeCost(rowCount, cpuCost, 0)
  }

}
