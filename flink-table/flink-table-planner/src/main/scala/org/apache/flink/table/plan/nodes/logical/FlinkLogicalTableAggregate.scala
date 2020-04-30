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

import java.util
import java.util.{List => JList}

import org.apache.calcite.plan._
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.util.ImmutableBitSet
import org.apache.flink.table.plan.logical.rel.{LogicalTableAggregate, TableAggregate}
import org.apache.flink.table.plan.nodes.FlinkConventions

class FlinkLogicalTableAggregate(
  cluster: RelOptCluster,
  traitSet: RelTraitSet,
  input: RelNode,
  indicator: Boolean,
  groupSet: ImmutableBitSet,
  groupSets: util.List[ImmutableBitSet],
  aggCalls: util.List[AggregateCall])
  extends TableAggregate(cluster, traitSet, input, indicator, groupSet, groupSets, aggCalls)
    with FlinkLogicalRel {

  override def copy(traitSet: RelTraitSet, inputs: JList[RelNode]): RelNode = {
    new FlinkLogicalTableAggregate(
      cluster,
      traitSet,
      inputs.get(0),
      indicator,
      groupSet,
      groupSets,
      aggCalls
    )
  }
}

private class FlinkLogicalTableAggregateConverter
  extends ConverterRule(
    classOf[LogicalTableAggregate],
    Convention.NONE,
    FlinkConventions.LOGICAL,
    "FlinkLogicalTableAggregateConverter") {

  override def convert(rel: RelNode): RelNode = {
    val agg = rel.asInstanceOf[LogicalTableAggregate]
    val traitSet = rel.getTraitSet.replace(FlinkConventions.LOGICAL)
    val newInput = RelOptRule.convert(agg.getInput, FlinkConventions.LOGICAL)

    new FlinkLogicalTableAggregate(
      rel.getCluster,
      traitSet,
      newInput,
      agg.getIndicator,
      agg.getGroupSet,
      agg.getGroupSets,
      agg.aggCalls)
  }
}

object FlinkLogicalTableAggregate {
  val CONVERTER: ConverterRule = new FlinkLogicalTableAggregateConverter()
}
