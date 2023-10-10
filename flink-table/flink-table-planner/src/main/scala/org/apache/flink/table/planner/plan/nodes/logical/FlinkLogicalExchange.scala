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

import org.apache.calcite.plan.{Convention, RelOptCluster, RelOptCost, RelOptPlanner, RelOptRule, RelTraitSet}
import org.apache.calcite.rel.{RelDistribution, RelNode}
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rel.convert.ConverterRule.Config
import org.apache.calcite.rel.core.Exchange
import org.apache.calcite.rel.logical.LogicalExchange
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.util.Util

/**
 * Sub-class of [[Exchange]] that is a relational expression which imposes a particular distribution
 * on its input without otherwise changing its content in Flink.
 *
 * Note that, currently only "Session Window TVF" could generate an LogicalExchange node by Calcite.
 *
 * Mainly copy from [[LogicalExchange]] except implementing [[FlinkLogicalRel]].
 */
class FlinkLogicalExchange(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    input: RelNode,
    distribution: RelDistribution)
  extends Exchange(cluster, traitSet, input, distribution)
  with FlinkLogicalRel {

  override def copy(
      traitSet: RelTraitSet,
      newInput: RelNode,
      newDistribution: RelDistribution): Exchange = {
    new FlinkLogicalExchange(cluster, traitSet, newInput, newDistribution)
  }

  override def computeSelfCost(planner: RelOptPlanner, mq: RelMetadataQuery): RelOptCost = {
    // Higher cost if rows are wider discourages pushing a project through an
    // exchange.
    val rowCount = mq.getRowCount(this)
    val bytesPerRow = getRowType.getFieldCount * 4
    planner.getCostFactory.makeCost(Util.nLogN(rowCount) * bytesPerRow, rowCount, 0)
  }

}

private class FlinkLogicalExchangeConverter(config: Config) extends ConverterRule(config) {

  override def convert(rel: RelNode): RelNode = {
    val exchange = rel.asInstanceOf[LogicalExchange]
    val newInput = RelOptRule.convert(exchange.getInput(), FlinkConventions.LOGICAL)
    FlinkLogicalExchange.create(newInput, exchange.getDistribution)
  }
}

object FlinkLogicalExchange {

  val CONVERTER: ConverterRule = new FlinkLogicalExchangeConverter(
    Config.INSTANCE.withConversion(
      classOf[LogicalExchange],
      Convention.NONE,
      FlinkConventions.LOGICAL,
      "FlinkLogicalExchangeConverter"))

  def create(input: RelNode, distribution: RelDistribution): FlinkLogicalExchange = {
    val cluster = input.getCluster
    val traitSet =
      cluster.traitSetOf(FlinkConventions.LOGICAL).simplify()
    new FlinkLogicalExchange(cluster, traitSet, input, distribution)
  }
}
