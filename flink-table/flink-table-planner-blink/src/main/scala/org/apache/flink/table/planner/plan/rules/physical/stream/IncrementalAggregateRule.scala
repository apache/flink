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
import org.apache.flink.table.planner.plan.nodes.physical.stream.{StreamExecGlobalGroupAggregate, StreamExecIncrementalGroupAggregate, StreamExecLocalGroupAggregate, StreamPhysicalExchange}
import org.apache.flink.table.planner.plan.utils.{AggregateInfoList, AggregateUtil, DistinctInfo}
import org.apache.flink.util.Preconditions

import org.apache.calcite.plan.RelOptRule.{any, operand}
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall, RelOptUtil}

import java.lang.{Boolean => JBoolean}
import java.util.Collections

/**
  * Rule that matches final [[StreamExecGlobalGroupAggregate]] on [[StreamPhysicalExchange]]
  * on final [[StreamExecLocalGroupAggregate]] on partial [[StreamExecGlobalGroupAggregate]],
  * and combines the final [[StreamExecLocalGroupAggregate]] and
  * the partial [[StreamExecGlobalGroupAggregate]] into a [[StreamExecIncrementalGroupAggregate]].
  */
class IncrementalAggregateRule
  extends RelOptRule(
    operand(classOf[StreamExecGlobalGroupAggregate], // final global agg
      operand(classOf[StreamPhysicalExchange], // key by
        operand(classOf[StreamExecLocalGroupAggregate], // final local agg
          operand(classOf[StreamExecGlobalGroupAggregate], any())))), // partial global agg
    "IncrementalAggregateRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val finalGlobalAgg: StreamExecGlobalGroupAggregate = call.rel(0)
    val finalLocalAgg: StreamExecLocalGroupAggregate = call.rel(2)
    val partialGlobalAgg: StreamExecGlobalGroupAggregate = call.rel(3)

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
    val finalGlobalAgg: StreamExecGlobalGroupAggregate = call.rel(0)
    val exchange: StreamPhysicalExchange = call.rel(1)
    val finalLocalAgg: StreamExecLocalGroupAggregate = call.rel(2)
    val partialGlobalAgg: StreamExecGlobalGroupAggregate = call.rel(3)
    val aggInputRowType = partialGlobalAgg.inputRowType

    val partialLocalAggInfoList = partialGlobalAgg.localAggInfoList
    val partialGlobalAggInfoList = partialGlobalAgg.globalAggInfoList
    val finalGlobalAggInfoList = finalGlobalAgg.globalAggInfoList
    val aggCalls = finalGlobalAggInfoList.getActualAggregateCalls

    val typeFactory = finalGlobalAgg.getCluster.getTypeFactory.asInstanceOf[FlinkTypeFactory]

    // pick distinct info from global which is on state, and modify excludeAcc parameter
    val incrDistinctInfo = partialGlobalAggInfoList.distinctInfos.map { info =>
      DistinctInfo(
        info.argIndexes,
        info.keyType,
        info.accType,
        // exclude distinct acc from the aggregate accumulator,
        // because the output acc only need to contain the count
        excludeAcc = true,
        info.dataViewSpec,
        info.consumeRetraction,
        info.filterArgs,
        info.aggIndexes
      )
    }

    val incrAggInfoList = AggregateInfoList(
      // pick local aggs info from local which is on heap
      partialLocalAggInfoList.aggInfos,
      partialGlobalAggInfoList.indexOfCountStar,
      partialGlobalAggInfoList.countStarInserted,
      incrDistinctInfo)

    val incrAggOutputRowType = AggregateUtil.inferLocalAggRowType(
      incrAggInfoList,
      partialGlobalAgg.getRowType,
      finalGlobalAgg.grouping,
      typeFactory)

    val incrAgg = new StreamExecIncrementalGroupAggregate(
      partialGlobalAgg.getCluster,
      finalLocalAgg.getTraitSet, // extends final local agg traits (ACC trait)
      partialGlobalAgg.getInput,
      aggInputRowType,
      incrAggOutputRowType,
      partialLocalAggInfoList,
      incrAggInfoList,
      aggCalls,
      finalLocalAgg.grouping,
      partialGlobalAgg.grouping)

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
      // an additional count1 is inserted, need to adapt the global agg
      val localAggInfoList = AggregateUtil.transformToStreamAggregateInfoList(
        aggCalls,
        // the final agg input is partial agg
        partialGlobalAgg.getRowType,
        // all the aggs do not need retraction
        Array.fill(aggCalls.length)(false),
        // also do not need count*
        needInputCount = false,
        // the local agg is not works on state
        isStateBackendDataViews = false)
      val globalAggInfoList = AggregateUtil.transformToStreamAggregateInfoList(
        aggCalls,
        // the final agg input is partial agg
        partialGlobalAgg.getRowType,
        // all the aggs do not need retraction
        Array.fill(aggCalls.length)(false),
        // also do not need count*
        needInputCount = false,
        // the global agg is works on state
        isStateBackendDataViews = true)

      // check whether the global agg required input row type equals the incr agg output row type
      val globalAggInputAccType = AggregateUtil.inferLocalAggRowType(
        localAggInfoList,
        incrAgg.getRowType,
        finalGlobalAgg.grouping,
        typeFactory)

      Preconditions.checkState(RelOptUtil.areRowTypesEqual(
        incrAggOutputRowType,
        globalAggInputAccType,
        false))

      new StreamExecGlobalGroupAggregate(
        finalGlobalAgg.getCluster,
        finalGlobalAgg.getTraitSet,
        newExchange,
        finalGlobalAgg.inputRowType,
        finalGlobalAgg.getRowType,
        finalGlobalAgg.grouping,
        localAggInfoList, // the agg info list is changed
        globalAggInfoList, // the agg info list is changed
        finalGlobalAgg.partialFinalType)
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
