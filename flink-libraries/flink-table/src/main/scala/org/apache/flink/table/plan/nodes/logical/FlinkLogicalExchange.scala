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

package org.apache.flink.table.plan.nodes.logical

import org.apache.calcite.plan._
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rel.core.Exchange
import org.apache.calcite.rel.logical.LogicalExchange
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rel.{RelDistribution, RelNode}
import org.apache.calcite.util.Util
import org.apache.flink.table.plan.nodes.FlinkConventions

class FlinkLogicalExchange(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputNode: RelNode,
    distribution: RelDistribution)
  extends Exchange(cluster, traitSet, inputNode, distribution)
  with FlinkLogicalRel {

  override def copy(traitSet: RelTraitSet, input: RelNode,
    distribution: RelDistribution): Exchange = {
    new FlinkLogicalExchange(cluster,traitSet,input,distribution)
  }

  override def computeSelfCost(planner: RelOptPlanner, mq: RelMetadataQuery): RelOptCost = {
    val rowCount = mq.getRowCount(this)
    val bytesPerRow = getRowType().getFieldCount() * 4
    planner.getCostFactory().makeCost(
      Util.nLogN(rowCount) * bytesPerRow, rowCount, 0)
  }
}

private class FlinkLogicalExchangeConverter
  extends ConverterRule(
    classOf[LogicalExchange],
    Convention.NONE,
    FlinkConventions.LOGICAL,
    "FlinkLogicalExchangeConverter") {

  override def convert(rel: RelNode): RelNode = {
    val exchange = rel.asInstanceOf[LogicalExchange]
    val traitSet = rel.getTraitSet.replace(FlinkConventions.LOGICAL)
    val input = RelOptRule.convert(exchange.getInput, FlinkConventions.LOGICAL)
    new FlinkLogicalExchange(rel.getCluster,traitSet,input,exchange.distribution)
  }

}

object FlinkLogicalExchange {
  val CONVERTER: ConverterRule = new FlinkLogicalExchangeConverter
}
