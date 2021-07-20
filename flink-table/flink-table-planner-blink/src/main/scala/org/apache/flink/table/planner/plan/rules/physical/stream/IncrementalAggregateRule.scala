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
package org.apache.flink.table.planner.plan.rules.physical.stream

import org.apache.flink.annotation.Experimental
import org.apache.flink.configuration.ConfigOption
import org.apache.flink.configuration.ConfigOptions.key
import org.apache.flink.table.planner.calcite.{FlinkContext, FlinkTypeFactory}
import org.apache.flink.table.planner.plan.PartialFinalType
import org.apache.flink.table.planner.plan.nodes.physical.stream.{StreamPhysicalExchange, StreamPhysicalGlobalGroupAggregate, StreamPhysicalIncrementalGroupAggregate, StreamPhysicalLocalGroupAggregate}
import org.apache.flink.table.planner.plan.utils.AggregateUtil
import org.apache.flink.util.Preconditions

import org.apache.calcite.plan.RelOptRule.{any, operand}
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall, RelOptUtil}

import java.lang.{Boolean => JBoolean}
import java.util.Collections

/**
 * Rule that matches final [[StreamPhysicalGlobalGroupAggregate]] on [[StreamPhysicalExchange]]
 * on final [[StreamPhysicalLocalGroupAggregate]] on partial [[StreamPhysicalGlobalGroupAggregate]],
 * and combines the final [[StreamPhysicalLocalGroupAggregate]] and
 * the partial [[StreamPhysicalGlobalGroupAggregate]] into a
 * [[StreamPhysicalIncrementalGroupAggregate]].
 */
class IncrementalAggregateRule
  extends RelOptRule(
    operand(classOf[StreamPhysicalGlobalGroupAggregate], // final global agg
      operand(classOf[StreamPhysicalExchange], // key by
        operand(classOf[StreamPhysicalLocalGroupAggregate], // final local agg
          operand(classOf[StreamPhysicalGlobalGroupAggregate], any())))), // partial global agg
    "IncrementalAggregateRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val finalGlobalAgg: StreamPhysicalGlobalGroupAggregate = call.rel(0)
    val finalLocalAgg: StreamPhysicalLocalGroupAggregate = call.rel(2)
    val partialGlobalAgg: StreamPhysicalGlobalGroupAggregate = call.rel(3)

    val tableConfig = call.getPlanner.getContext.unwrap(classOf[FlinkContext]).getTableConfig

    // whether incremental aggregate is enabled
    val incrementalAggEnabled = tableConfig.getConfiguration.getBoolean(
      IncrementalAggregateRule.TABLE_OPTIMIZER_INCREMENTAL_AGG_ENABLED)

    partialGlobalAgg.partialFinalType == PartialFinalType.PARTIAL &&
      finalLocalAgg.partialFinalType == PartialFinalType.FINAL &&
      finalGlobalAgg.partialFinalType == PartialFinalType.FINAL &&
      incrementalAggEnabled
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val finalGlobalAgg: StreamPhysicalGlobalGroupAggregate = call.rel(0)
    val exchange: StreamPhysicalExchange = call.rel(1)
    val finalLocalAgg: StreamPhysicalLocalGroupAggregate = call.rel(2)
    val partialGlobalAgg: StreamPhysicalGlobalGroupAggregate = call.rel(3)
    val partialLocalAggInputRowType = partialGlobalAgg.localAggInputRowType

    val partialOriginalAggCalls = partialGlobalAgg.aggCalls.toArray
    val partialRealAggCalls = partialGlobalAgg.localAggInfoList.getActualAggregateCalls
    val finalRealAggCalls = finalGlobalAgg.globalAggInfoList.getActualAggregateCalls

    val incrAgg = new StreamPhysicalIncrementalGroupAggregate(
      partialGlobalAgg.getCluster,
      finalLocalAgg.getTraitSet, // extends final local agg traits (ACC trait)
      partialGlobalAgg.getInput,
      partialGlobalAgg.grouping,
      partialRealAggCalls,
      finalLocalAgg.grouping,
      finalRealAggCalls,
      partialOriginalAggCalls,
      partialGlobalAgg.aggCallNeedRetractions,
      partialGlobalAgg.needRetraction,
      partialLocalAggInputRowType,
      partialGlobalAgg.getRowType)
    val incrAggOutputRowType = incrAgg.getRowType

    val newExchange = exchange.copy(exchange.getTraitSet, incrAgg, exchange.distribution)

    val partialAggCountStarInserted = partialGlobalAgg.globalAggInfoList.countStarInserted

    val globalAgg = if (partialAggCountStarInserted) {
      val globalAggInputAccType = finalLocalAgg.getRowType
      Preconditions.checkState(RelOptUtil.areRowTypesEqual(
        incrAggOutputRowType,
        globalAggInputAccType,
        false))
      finalGlobalAgg.copy(finalGlobalAgg.getTraitSet, Collections.singletonList(newExchange))
    } else {
      // adapt the needRetract of final global agg to be same as that of partial agg
      val localAggInfoList = AggregateUtil.transformToStreamAggregateInfoList(
        // the final agg input is partial agg
        FlinkTypeFactory.toLogicalRowType(partialGlobalAgg.getRowType),
        finalRealAggCalls,
        // use partial global agg's aggCallNeedRetractions
        partialGlobalAgg.aggCallNeedRetractions,
        partialGlobalAgg.needRetraction,
        partialGlobalAgg.globalAggInfoList.indexOfCountStar,
        // the local agg is not works on state
        isStateBackendDataViews = false,
        needDistinctInfo = true)

      // check whether the global agg required input row type equals the incr agg output row type
      val globalAggInputAccType = AggregateUtil.inferLocalAggRowType(
        localAggInfoList,
        incrAgg.getRowType,
        finalGlobalAgg.grouping,
        finalGlobalAgg.getCluster.getTypeFactory.asInstanceOf[FlinkTypeFactory])

      Preconditions.checkState(RelOptUtil.areRowTypesEqual(
        incrAggOutputRowType,
        globalAggInputAccType,
        false))

      new StreamPhysicalGlobalGroupAggregate(
        finalGlobalAgg.getCluster,
        finalGlobalAgg.getTraitSet,
        newExchange,
        finalGlobalAgg.getRowType,
        finalGlobalAgg.grouping,
        finalRealAggCalls,
        partialGlobalAgg.aggCallNeedRetractions,
        finalGlobalAgg.localAggInputRowType,
        partialGlobalAgg.needRetraction,
        finalGlobalAgg.partialFinalType,
        partialGlobalAgg.globalAggInfoList.indexOfCountStar)
    }

    call.transformTo(globalAgg)
  }
}

object IncrementalAggregateRule {
  val INSTANCE = new IncrementalAggregateRule

  // It is a experimental config, will may be removed later.
  @Experimental
  val TABLE_OPTIMIZER_INCREMENTAL_AGG_ENABLED: ConfigOption[JBoolean] =
  key("table.optimizer.incremental-agg-enabled")
      .defaultValue(JBoolean.valueOf(true))
      .withDescription("When both local aggregation and distinct aggregation splitting " +
          "are enabled, a distinct aggregation will be optimized into four aggregations, " +
          "i.e., local-agg1, global-agg1, local-agg2 and global-Agg2. We can combine global-agg1" +
          " and local-agg2 into a single operator (we call it incremental agg because " +
          "it receives incremental accumulators and output incremental results). " +
          "In this way, we can reduce some state overhead and resources. Default is enabled.")
}
