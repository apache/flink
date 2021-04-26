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

import org.apache.flink.table.api.config.ExecutionConfigOptions
import org.apache.flink.table.planner.calcite.{FlinkContext, FlinkTypeFactory}
import org.apache.flink.table.planner.plan.`trait`.{FlinkRelDistribution, FlinkRelDistributionTraitDef, ModifyKindSetTrait, UpdateKindTrait}
import org.apache.flink.table.planner.plan.metadata.FlinkRelMetadataQuery
import org.apache.flink.table.planner.plan.nodes.FlinkConventions
import org.apache.flink.table.planner.plan.nodes.physical.stream._
import org.apache.flink.table.planner.plan.rules.physical.FlinkExpandConversionRule._
import org.apache.flink.table.planner.plan.utils.{AggregateInfoList, AggregateUtil, ChangelogPlanUtils}
import org.apache.flink.table.planner.utils.AggregatePhaseStrategy
import org.apache.flink.table.planner.utils.TableConfigUtils.getAggPhaseStrategy

import org.apache.calcite.plan.RelOptRule.{any, operand}
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.AggregateCall

import java.util

/**
  * Rule that matches [[StreamPhysicalGroupAggregate]] on [[StreamPhysicalExchange]]
  * with the following condition:
  * 1. mini-batch is enabled in given TableConfig,
  * 2. two-phase aggregation is enabled in given TableConfig,
  * 3. all aggregate functions are mergeable,
  * 4. the input of exchange does not satisfy the shuffle distribution,
  *
  * and converts them to
  * {{{
  *   StreamExecGlobalGroupAggregate
  *   +- StreamPhysicalExchange
  *      +- StreamPhysicalLocalGroupAggregate
  *         +- input of exchange
  * }}}
  */
class TwoStageOptimizedAggregateRule extends RelOptRule(
  operand(classOf[StreamPhysicalGroupAggregate],
    operand(classOf[StreamPhysicalExchange],
      operand(classOf[RelNode], any))),
  "TwoStageOptimizedAggregateRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val tableConfig = call.getPlanner.getContext.unwrap(classOf[FlinkContext]).getTableConfig
    val agg: StreamPhysicalGroupAggregate = call.rel(0)
    val realInput: RelNode = call.rel(2)

    val needRetraction = !ChangelogPlanUtils.isInsertOnly(
      realInput.asInstanceOf[StreamPhysicalRel])
    val fmq = FlinkRelMetadataQuery.reuseOrCreate(call.getMetadataQuery)
    val monotonicity = fmq.getRelModifiedMonotonicity(agg)
    val needRetractionArray = AggregateUtil.deriveAggCallNeedRetractions(
      agg.grouping.length, agg.aggCalls, needRetraction, monotonicity)

    val aggInfoList = AggregateUtil.transformToStreamAggregateInfoList(
      FlinkTypeFactory.toLogicalRowType( agg.getInput.getRowType),
      agg.aggCalls,
      needRetractionArray,
      needRetraction,
      isStateBackendDataViews = true)

    val isMiniBatchEnabled = tableConfig.getConfiguration.getBoolean(
      ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED)
    val isTwoPhaseEnabled = getAggPhaseStrategy(tableConfig) != AggregatePhaseStrategy.ONE_PHASE

    isMiniBatchEnabled && isTwoPhaseEnabled &&
      AggregateUtil.doAllSupportPartialMerge(aggInfoList.aggInfos) &&
      !isInputSatisfyRequiredDistribution(realInput, agg.grouping)
  }

  private def isInputSatisfyRequiredDistribution(input: RelNode, keys: Array[Int]): Boolean = {
    val requiredDistribution = createDistribution(keys)
    val inputDistribution = input.getTraitSet.getTrait(FlinkRelDistributionTraitDef.INSTANCE)
    inputDistribution.satisfies(requiredDistribution)
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val agg: StreamPhysicalGroupAggregate = call.rel(0)
    val realInput: RelNode = call.rel(2)
    val needRetraction = !ChangelogPlanUtils.isInsertOnly(
      realInput.asInstanceOf[StreamPhysicalRel])
    val fmq = FlinkRelMetadataQuery.reuseOrCreate(call.getMetadataQuery)
    val monotonicity = fmq.getRelModifiedMonotonicity(agg)
    val aggCallNeedRetractions = AggregateUtil.deriveAggCallNeedRetractions(
      agg.grouping.length, agg.aggCalls, needRetraction, monotonicity)

    val globalAggInfoList = AggregateUtil.transformToStreamAggregateInfoList(
      FlinkTypeFactory.toLogicalRowType(realInput.getRowType),
      agg.aggCalls,
      aggCallNeedRetractions,
      needRetraction,
      isStateBackendDataViews = true)

    val globalHashAgg = createTwoStageAgg(
      realInput, agg.aggCalls, aggCallNeedRetractions, needRetraction, globalAggInfoList, agg)
    call.transformTo(globalHashAgg)
  }

  // the difference between localAggInfos and aggInfos is local agg use heap dataview,
  // but global agg use state dataview
  private def createTwoStageAgg(
      realInput: RelNode,
      aggCalls: Seq[AggregateCall],
      aggCallNeedRetractions: Array[Boolean],
      needRetraction: Boolean,
      globalAggInfoList: AggregateInfoList,
      agg: StreamPhysicalGroupAggregate): StreamExecGlobalGroupAggregate = {

    // local agg shouldn't produce insert only messages
    val localAggTraitSet = realInput.getTraitSet
      .plus(ModifyKindSetTrait.INSERT_ONLY)
      .plus(UpdateKindTrait.NONE)
    val localHashAgg = new StreamPhysicalLocalGroupAggregate(
      agg.getCluster,
      localAggTraitSet,
      realInput,
      agg.grouping,
      agg.aggCalls,
      aggCallNeedRetractions,
      needRetraction,
      agg.partialFinalType)

    // grouping keys is forwarded by local agg, use indices instead of groupings
    val globalGrouping = agg.grouping.indices.toArray
    val globalDistribution = createDistribution(globalGrouping)
    // create exchange if needed
    val newInput = satisfyDistribution(
      FlinkConventions.STREAM_PHYSICAL, localHashAgg, globalDistribution)
    val globalAggProvidedTraitSet = agg.getTraitSet

    // TODO Temporary solution, remove it later
    val localAggInfoList = AggregateUtil.transformToStreamAggregateInfoList(
      FlinkTypeFactory.toLogicalRowType(realInput.getRowType),
      aggCalls,
      aggCallNeedRetractions,
      needRetraction,
      isStateBackendDataViews = false)
    new StreamExecGlobalGroupAggregate(
      agg.getCluster,
      globalAggProvidedTraitSet,
      newInput,
      realInput.getRowType,
      agg.getRowType,
      globalGrouping,
      localAggInfoList,
      globalAggInfoList,
      agg.partialFinalType)
  }

  private def createDistribution(keys: Array[Int]): FlinkRelDistribution = {
    if (keys.nonEmpty) {
      val fields = new util.ArrayList[Integer]()
      keys.foreach(fields.add(_))
      FlinkRelDistribution.hash(fields)
    } else {
      FlinkRelDistribution.SINGLETON
    }
  }

}

object TwoStageOptimizedAggregateRule {
  val INSTANCE: RelOptRule = new TwoStageOptimizedAggregateRule
}
