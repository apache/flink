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

import org.apache.flink.table.planner.plan.logical.LogicalWindow
import org.apache.flink.table.planner.plan.nodes.FlinkConventions
import org.apache.flink.table.planner.plan.nodes.calcite.{LogicalWindowTableAggregate, WindowTableAggregate}
import org.apache.flink.table.runtime.groupwindow.NamedWindowProperty

import org.apache.calcite.plan._
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rel.convert.ConverterRule.Config
import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.util.ImmutableBitSet

import java.util

/**
 * Sub-class of [[WindowTableAggregate]] that is a relational expression which performs window
 * aggregations but outputs 0 or more records for a group.
 */
class FlinkLogicalWindowTableAggregate(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    child: RelNode,
    groupSet: ImmutableBitSet,
    groupSets: util.List[ImmutableBitSet],
    aggCalls: util.List[AggregateCall],
    window: LogicalWindow,
    namedProperties: util.List[NamedWindowProperty])
  extends WindowTableAggregate(
    cluster,
    traitSet,
    child,
    groupSet,
    groupSets,
    aggCalls,
    window,
    namedProperties)
  with FlinkLogicalRel {

  override def copy(traitSet: RelTraitSet, inputs: util.List[RelNode]): RelNode = {
    new FlinkLogicalWindowTableAggregate(
      cluster,
      traitSet,
      inputs.get(0),
      groupSet,
      groupSets,
      aggCalls,
      window,
      namedProperties)
  }

  override def computeSelfCost(planner: RelOptPlanner, mq: RelMetadataQuery): RelOptCost = {
    val child = this.getInput
    val rowCnt = mq.getRowCount(child)
    val rowSize = mq.getAverageRowSize(child)
    val aggCnt = this.getAggCallList.size
    // group by CPU cost(multiple by 1.1 to encourage less group keys) + agg call CPU cost
    val cpuCost: Double = rowCnt * getGroupSet.cardinality() * 1.1 + rowCnt * aggCnt
    planner.getCostFactory.makeCost(rowCnt, cpuCost, rowCnt * rowSize)
  }
}

class FlinkLogicalWindowTableAggregateConverter(config: Config) extends ConverterRule(config) {

  override def convert(rel: RelNode): RelNode = {
    val agg = rel.asInstanceOf[LogicalWindowTableAggregate]
    val newInput = RelOptRule.convert(agg.getInput, FlinkConventions.LOGICAL)
    val traitSet = newInput.getCluster.traitSet().replace(FlinkConventions.LOGICAL).simplify()

    new FlinkLogicalWindowTableAggregate(
      rel.getCluster,
      traitSet,
      newInput,
      agg.getGroupSet,
      agg.getGroupSets,
      agg.getAggCallList,
      agg.getWindow,
      agg.getNamedProperties)
  }
}

object FlinkLogicalWindowTableAggregate {
  val CONVERTER = new FlinkLogicalWindowTableAggregateConverter(
    Config.INSTANCE.withConversion(
      classOf[LogicalWindowTableAggregate],
      Convention.NONE,
      FlinkConventions.LOGICAL,
      "FlinkLogicalWindowTableAggregateConverter"))
}
