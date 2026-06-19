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
package org.apache.flink.table.planner.plan.nodes.logical

import org.apache.flink.table.planner.plan.nodes.FlinkConventions
import org.apache.flink.table.planner.plan.nodes.calcite.{LogicalRank, Rank}
import org.apache.flink.table.planner.plan.utils.RelExplainUtil
import org.apache.flink.table.runtime.operators.rank.{RankRange, RankType}

import org.apache.calcite.plan._
import org.apache.calcite.rel.`type`.RelDataTypeField
import org.apache.calcite.rel.{RelCollation, RelNode, RelWriter}
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rel.convert.ConverterRule.Config
import org.apache.calcite.util.ImmutableBitSet

import java.util

import scala.collection.JavaConversions._

/**
 * Sub-class of [[Rank]] that is a relational expression which returns the rows in which the rank
 * function value of each row is in the given range.
 */
class FlinkLogicalRank(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    input: RelNode,
    partitionKey: ImmutableBitSet,
    orderKey: RelCollation,
    rankType: RankType,
    rankRange: RankRange,
    rankNumberType: RelDataTypeField,
    outputRankNumber: Boolean)
  extends Rank(
    cluster,
    traitSet,
    input,
    partitionKey,
    orderKey,
    rankType,
    rankRange,
    rankNumberType,
    outputRankNumber)
  with FlinkLogicalRel {

  override def explainTerms(pw: RelWriter): RelWriter = {
    val inputFieldNames = input.getRowType.getFieldNames
    pw.item("input", getInput)
      .item("rankType", rankType)
      .item("rankRange", rankRange.toString(inputFieldNames))
      .item("partitionBy", partitionKey.map(inputFieldNames.get(_)).mkString(","))
      .item("orderBy", RelExplainUtil.collationToString(orderKey, input.getRowType))
      .item("select", getRowType.getFieldNames.mkString(", "))
  }

  override def copy(traitSet: RelTraitSet, inputs: util.List[RelNode]): RelNode = {
    new FlinkLogicalRank(
      cluster,
      traitSet,
      inputs.get(0),
      partitionKey,
      orderKey,
      rankType,
      rankRange,
      rankNumberType,
      outputRankNumber)
  }

}

private class FlinkLogicalRankConverter(config: Config) extends ConverterRule(config) {
  override def convert(rel: RelNode): RelNode = {
    val rank = rel.asInstanceOf[LogicalRank]
    val newInput = RelOptRule.convert(rank.getInput, FlinkConventions.LOGICAL)
    FlinkLogicalRank.create(
      newInput,
      rank.partitionKey,
      rank.orderKey,
      rank.rankType,
      rank.rankRange,
      rank.rankNumberType,
      rank.outputRankNumber
    )
  }
}

object FlinkLogicalRank {
  val CONVERTER: ConverterRule = new FlinkLogicalRankConverter(
    Config.INSTANCE.withConversion(
      classOf[LogicalRank],
      Convention.NONE,
      FlinkConventions.LOGICAL,
      "FlinkLogicalRankConverter"))

  def create(
      input: RelNode,
      partitionKey: ImmutableBitSet,
      orderKey: RelCollation,
      rankType: RankType,
      rankRange: RankRange,
      rankNumberType: RelDataTypeField,
      outputRankNumber: Boolean): FlinkLogicalRank = {
    val cluster = input.getCluster
    val traits = cluster.traitSetOf(FlinkConventions.LOGICAL).simplify()
    new FlinkLogicalRank(
      cluster,
      traits,
      input,
      partitionKey,
      orderKey,
      rankType,
      rankRange,
      rankNumberType,
      outputRankNumber)
  }

}
