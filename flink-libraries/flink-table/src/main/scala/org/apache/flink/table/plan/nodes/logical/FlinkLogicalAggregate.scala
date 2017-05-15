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

import java.util.{List => JList}

import org.apache.calcite.plan._
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rel.core.{Aggregate, AggregateCall}
import org.apache.calcite.rel.logical.LogicalAggregate
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.util.ImmutableBitSet
import org.apache.flink.table.plan.nodes.FlinkConventions

import scala.collection.JavaConversions._

class FlinkLogicalAggregate(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    child: RelNode,
    indicator: Boolean,
    groupSet: ImmutableBitSet,
    groupSets: JList[ImmutableBitSet],
    aggCalls: JList[AggregateCall])
  extends Aggregate(cluster, traitSet, child, indicator, groupSet, groupSets, aggCalls)
  with FlinkLogicalRel {

  override def copy(
      traitSet: RelTraitSet,
      input: RelNode,
      indicator: Boolean,
      groupSet: ImmutableBitSet,
      groupSets: JList[ImmutableBitSet],
      aggCalls: JList[AggregateCall]): Aggregate = {
    new FlinkLogicalAggregate(cluster, traitSet, input, indicator, groupSet, groupSets, aggCalls)
  }

  override def computeSelfCost(planner: RelOptPlanner, metadata: RelMetadataQuery): RelOptCost = {
    val child = this.getInput
    val rowCnt = metadata.getRowCount(child)
    val rowSize = this.estimateRowSize(child.getRowType)
    val aggCnt = this.getAggCallList.size
    planner.getCostFactory.makeCost(rowCnt, rowCnt * aggCnt, rowCnt * rowSize)
  }
}

private class FlinkLogicalAggregateConverter
  extends ConverterRule(
    classOf[LogicalAggregate],
    Convention.NONE,
    FlinkConventions.LOGICAL,
    "FlinkLogicalAggregateConverter") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val agg = call.rel(0).asInstanceOf[LogicalAggregate]

    // we do not support these functions natively
    // they have to be converted using the AggregateReduceFunctionsRule
    val supported = agg.getAggCallList.map(_.getAggregation.getKind).forall {
      case SqlKind.STDDEV_POP | SqlKind.STDDEV_SAMP | SqlKind.VAR_POP | SqlKind.VAR_SAMP => false
      case _ => true
    }

    !agg.containsDistinctCall() && supported
  }

  override def convert(rel: RelNode): RelNode = {
    val agg = rel.asInstanceOf[LogicalAggregate]
    val traitSet = rel.getTraitSet.replace(FlinkConventions.LOGICAL)
    val newInput = RelOptRule.convert(agg.getInput, FlinkConventions.LOGICAL)

    new FlinkLogicalAggregate(
      rel.getCluster,
      traitSet,
      newInput,
      agg.indicator,
      agg.getGroupSet,
      agg.getGroupSets,
      agg.getAggCallList)
  }
}

object FlinkLogicalAggregate {
  val CONVERTER: ConverterRule = new FlinkLogicalAggregateConverter()
}
