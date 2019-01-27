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

import org.apache.flink.table.api.{AggPhaseEnforcer, TableConfig, TableConfigOptions}
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.plan.`trait`.{AccMode, AccModeTrait, FlinkRelDistribution, FlinkRelDistributionTraitDef}
import org.apache.flink.table.plan.metadata.FlinkRelMetadataQuery
import org.apache.flink.table.plan.nodes.FlinkConventions
import org.apache.flink.table.plan.nodes.physical.stream._
import org.apache.flink.table.plan.rules.physical.FlinkExpandConversionRule._
import org.apache.flink.table.plan.util.AggregateUtil._
import org.apache.flink.table.plan.util.{AggregateInfoList, AggregateUtil}

import org.apache.calcite.plan.RelOptRule.{any, operand}
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.calcite.rel.RelNode

import java.util.{ArrayList => JArrayList}

// use the two stage agg optimize the plan
class TwoStageOptimizedAggregateRule extends RelOptRule(
  operand(
    classOf[StreamExecGroupAggregate],
    operand(
      classOf[StreamExecExchange],
      operand(classOf[RelNode], any))), "TwoStageOptimizedAggregateRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val tableConfig = call.getPlanner.getContext.unwrap(classOf[TableConfig])
    val agg = call.rels(0).asInstanceOf[StreamExecGroupAggregate]
    val realInput = call.rels(2)

    val needRetraction = StreamExecRetractionRules.isAccRetract(realInput)

    val modifiedMono = call.getMetadataQuery.asInstanceOf[FlinkRelMetadataQuery]
      .getRelModifiedMonotonicity(agg)
    val needRetractionArray = AggregateUtil.getNeedRetractions(
      agg.getGroupings.length, needRetraction, modifiedMono, agg.aggCalls)

    val aggInfoList = transformToStreamAggregateInfoList(
      agg.aggCalls,
      agg.inputRelDataType,
      needRetractionArray,
      needRetraction,
      isStateBackendDataViews = true)

    tableConfig.getConf.contains(TableConfigOptions.SQL_EXEC_MINIBATCH_ALLOW_LATENCY) &&
      !tableConfig.getConf.getString(TableConfigOptions.SQL_OPTIMIZER_AGG_PHASE_ENFORCER)
        .equalsIgnoreCase(AggPhaseEnforcer.ONE_PHASE.toString) &&
      doAllSupportPartialMerge(aggInfoList.aggInfos) &&
      !satisfyRequiredDistribution(realInput, agg.getGroupings)
  }

  override def onMatch(call: RelOptRuleCall): Unit = {

    val agg = call.rels(0).asInstanceOf[StreamExecGroupAggregate]
    val realInput = call.rels(2)
    val needRetraction = StreamExecRetractionRules.isAccRetract(realInput)

    val modifiedMono = call.getMetadataQuery.asInstanceOf[FlinkRelMetadataQuery]
      .getRelModifiedMonotonicity(agg)
    val needRetractionArray = AggregateUtil.getNeedRetractions(
      agg.getGroupings.length, needRetraction, modifiedMono, agg.aggCalls)

    val localAggInfoList = transformToStreamAggregateInfoList(
      agg.aggCalls,
      realInput.getRowType,
      needRetractionArray,
      needRetraction,
      isStateBackendDataViews = false)

    val globalAggInfoList = transformToStreamAggregateInfoList(
      agg.aggCalls,
      realInput.getRowType,
      needRetractionArray,
      needRetraction,
      isStateBackendDataViews = true)

    transformToTwoStageAgg(
      call,
      realInput,
      localAggInfoList,
      globalAggInfoList,
      agg)
  }

  // the difference between localAggInfos and aggInfos is local agg use heap dataview,
  // but global agg use state dataview
  private[flink] def transformToTwoStageAgg(
      call: RelOptRuleCall,
      input: RelNode,
      localAggInfoList: AggregateInfoList,
      globalAggInfoList: AggregateInfoList,
      agg: StreamExecGroupAggregate): Unit = {

    // localAgg
    val localAggRowType = inferLocalAggRowType(
      localAggInfoList,
      input.getRowType,
      agg.getGroupings,
      input.getCluster.getTypeFactory.asInstanceOf[FlinkTypeFactory])

    // local agg shouldn't produce AccRetract Message
    val localAggTraitSet = input.getTraitSet.plus(new AccModeTrait(AccMode.Acc))
    val localHashAgg = new StreamExecLocalGroupAggregate(
      agg.getCluster,
      localAggTraitSet,
      input,
      localAggInfoList,
      localAggRowType,
      agg.getGroupings,
      agg.aggCalls,
      agg.partialFinal)

    // globalHashAgg
    val globalDistribution = if (agg.getGroupings.nonEmpty) {
      val fields = new JArrayList[Integer]()
      // grouping keys is forwarded by local agg, use indices instead of groupings
      agg.getGroupings.indices.foreach(fields.add(_))
      FlinkRelDistribution.hash(fields)
    } else {
      FlinkRelDistribution.SINGLETON
    }

    val newInput = satisfyDistribution(
      FlinkConventions.STREAM_PHYSICAL, localHashAgg, globalDistribution)
    val globalAggProvidedTraitSet = agg.getTraitSet

    val globalHashAgg = new StreamExecGlobalGroupAggregate(
      agg.getCluster,
      globalAggProvidedTraitSet,
      newInput,
      localAggInfoList,
      globalAggInfoList,
      input.getRowType,
      agg.getRowType,
      // grouping keys is forwarded by local agg, use indices instead of groupings
      agg.getGroupings.indices.toArray,
      agg.partialFinal)

    call.transformTo(globalHashAgg)
  }

  private[flink] def satisfyRequiredDistribution(input: RelNode, keys: Array[Int]): Boolean = {
    val requiredDistribution = if (keys.nonEmpty) {
      val fields = new JArrayList[Integer]()
      keys.foreach(fields.add(_))
      FlinkRelDistribution.hash(fields)
    } else {
      FlinkRelDistribution.SINGLETON
    }
    input.getTraitSet.getTrait(
      FlinkRelDistributionTraitDef.INSTANCE).satisfies(requiredDistribution)
  }
}

object TwoStageOptimizedAggregateRule {
  val INSTANCE: RelOptRule = new TwoStageOptimizedAggregateRule
}


