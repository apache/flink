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
package org.apache.flink.table.plan.rules.physical.stream

import java.util.Collections

import org.apache.flink.table.api.{TableConfig, TableConfigOptions}
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.plan.PartialFinalType
import org.apache.flink.table.plan.nodes.physical.stream.{StreamExecExchange, StreamExecGlobalGroupAggregate, StreamExecIncrementalGroupAggregate, StreamExecLocalGroupAggregate}
import org.apache.flink.table.plan.util.AggregateUtil.{inferLocalAggRowType, transformToStreamAggregateInfoList}
import org.apache.flink.table.plan.util.{AggregateInfoList, DistinctInfo}
import org.apache.flink.util.Preconditions

import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall, RelOptUtil}
import org.apache.calcite.plan.RelOptRule.{any, operand}

class IncrementalAggregateRule
  extends RelOptRule(
    operand(classOf[StreamExecGlobalGroupAggregate],                  // final global agg
      operand(classOf[StreamExecExchange],                            // keyby
        operand(classOf[StreamExecLocalGroupAggregate],               // final local
          operand(classOf[StreamExecGlobalGroupAggregate], any())))), // partial global agg
    "IncrementalAggregateRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val finalGlobalAgg = call.rel[StreamExecGlobalGroupAggregate](0)
    val finalLocalAgg = call.rel[StreamExecLocalGroupAggregate](2)
    val partialAgg = call.rel[StreamExecGlobalGroupAggregate](3)

    val config = partialAgg.getCluster.getPlanner.getContext.unwrap(classOf[TableConfig])

    // whether incremental aggregate is enabled
    val incrAggEnabled = config.getConf.getBoolean(
      TableConfigOptions.SQL_EXEC_INCREMENTAL_AGG_ENABLED)

    partialAgg.partialFinal == PartialFinalType.PARTIAL &&
      finalLocalAgg.partialFinal == PartialFinalType.FINAL &&
      finalGlobalAgg.partialFinal == PartialFinalType.FINAL &&
      incrAggEnabled
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val finalGlobalAgg = call.rel[StreamExecGlobalGroupAggregate](0)
    val exchange = call.rel[StreamExecExchange](1)
    val finalLocalAgg = call.rel[StreamExecLocalGroupAggregate](2)
    val partialAgg = call.rel[StreamExecGlobalGroupAggregate](3)
    val aggInputRowType = partialAgg.aggInputRowType

    val partialLocalAggInfoList = partialAgg.localAggInfoList
    val partialGlobalAggInfoList = partialAgg.globalAggInfoList
    val finalGlobalAggInfoList = finalGlobalAgg.globalAggInfoList
    val aggCalls = finalGlobalAggInfoList.getActualAggregateCalls

    val typeFactory = finalGlobalAgg.getCluster.getTypeFactory.asInstanceOf[FlinkTypeFactory]

    // pick distinct info from global which is on state, and modify excludeAcc parameter
    val incrDistinctInfo = partialGlobalAggInfoList.distinctInfos.map(info =>
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
    )

    val incrAggInfoList = AggregateInfoList(
      // pick local aggs info from local which is on heap
      partialLocalAggInfoList.aggInfos,
      partialGlobalAggInfoList.count1AggIndex,
      partialGlobalAggInfoList.count1AggInserted,
      incrDistinctInfo)

    val incrAggOutputRowType = inferLocalAggRowType(
      incrAggInfoList,
      partialAgg.getRowType,
      finalGlobalAgg.groupings,
      typeFactory)

    val incrAgg = new StreamExecIncrementalGroupAggregate(
      partialAgg.getCluster,
      finalLocalAgg.getTraitSet, // extends final local agg traits (ACC trait)
      partialAgg.getInput,
      aggInputRowType,
      incrAggOutputRowType,
      partialLocalAggInfoList,
      incrAggInfoList,
      aggCalls,
      partialAgg.groupings,
      finalLocalAgg.groupings)

    val newExchange = exchange.copy(exchange.getTraitSet, incrAgg, exchange.distribution)

    val partialAggCount1Inserted = partialAgg.globalAggInfoList.count1AggInserted

    val globalAgg = if (partialAggCount1Inserted) {
      val globalAggInputAccType = finalLocalAgg.getRowType
      Preconditions.checkState(RelOptUtil.areRowTypesEqual(
        incrAggOutputRowType,
        globalAggInputAccType,
        false))
      finalGlobalAgg.copy(finalGlobalAgg.getTraitSet, Collections.singletonList(newExchange))
    } else {
      // an additional count1 is inserted, need to adapt the global agg
      val localAggInfoList = transformToStreamAggregateInfoList(
        aggCalls,
        // the final agg input is partial agg
        partialAgg.getRowType,
        // all the aggs do not need retraction
        Array.fill(aggCalls.length)(false),
        // also do not need count*
        needInputCount = false,
        // the local agg is not works on state
        isStateBackendDataViews = false)
      val globalAggInfoList = transformToStreamAggregateInfoList(
        aggCalls,
        // the final agg input is partial agg
        partialAgg.getRowType,
        // all the aggs do not need retraction
        Array.fill(aggCalls.length)(false),
        // also do not need count*
        needInputCount = false,
        // the global agg is works on state
        isStateBackendDataViews = true)

      // check whether the global agg required input row type equals the incr agg output row type
      val globalAggInputAccType = inferLocalAggRowType(
        localAggInfoList,
        incrAgg.getRowType,
        finalGlobalAgg.groupings,
        typeFactory)

      Preconditions.checkState(RelOptUtil.areRowTypesEqual(
        incrAggOutputRowType,
        globalAggInputAccType,
        false))

      new StreamExecGlobalGroupAggregate(
        finalGlobalAgg.getCluster,
        finalGlobalAgg.getTraitSet,
        newExchange,
        localAggInfoList, // the agg info list is changed
        globalAggInfoList, // the agg info list is changed
        finalGlobalAgg.aggInputRowType,
        finalGlobalAgg.getRowType,
        finalGlobalAgg.groupings,
        finalGlobalAgg.partialFinal)
    }

    call.transformTo(globalAgg)
  }
}

object IncrementalAggregateRule {
  val INSTANCE = new IncrementalAggregateRule
}
