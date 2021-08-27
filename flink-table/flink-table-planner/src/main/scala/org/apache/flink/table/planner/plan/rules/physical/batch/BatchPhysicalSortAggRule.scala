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
package org.apache.flink.table.planner.plan.rules.physical.batch

import org.apache.flink.table.api.config.OptimizerConfigOptions
import org.apache.flink.table.planner.calcite.{FlinkContext, FlinkTypeFactory}
import org.apache.flink.table.planner.plan.`trait`.FlinkRelDistribution
import org.apache.flink.table.planner.plan.nodes.FlinkConventions
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalAggregate
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalSortAggregate
import org.apache.flink.table.planner.plan.utils.PythonUtil.isPythonAggregate
import org.apache.flink.table.planner.plan.utils.{AggregateUtil, OperatorType}
import org.apache.flink.table.planner.utils.TableConfigUtils.isOperatorDisabled

import org.apache.calcite.plan.RelOptRule.{any, operand}
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.calcite.rel._

import scala.collection.JavaConversions._

/**
 * Rule that converts [[FlinkLogicalAggregate]] to
 * {{{
 *   BatchPhysicalSortAggregate (global)
 *   +- Sort (exists if group keys are not empty)
 *      +- BatchPhysicalExchange (hash by group keys if group keys is not empty, else singleton)
 *         +- BatchPhysicalLocalSortAggregate (local)
 *           +- Sort (exists if group keys are not empty)
 *              +- input of agg
 * }}}
 * when all aggregate functions are mergeable
 * and [[OptimizerConfigOptions.TABLE_OPTIMIZER_AGG_PHASE_STRATEGY]] is TWO_PHASE, or
 * {{{
 *   BatchPhysicalSortAggregate
 *   +- Sort (exists if group keys are not empty)
 *      +- BatchPhysicalExchange (hash by group keys if group keys is not empty, else singleton)
 *         +- input of agg
 * }}}
 * when some aggregate functions are not mergeable
 * or [[OptimizerConfigOptions.TABLE_OPTIMIZER_AGG_PHASE_STRATEGY]] is ONE_PHASE.
 *
 * Notes: if [[OptimizerConfigOptions.TABLE_OPTIMIZER_AGG_PHASE_STRATEGY]] is NONE,
 * this rule will try to create two possibilities above, and chooses the best one based on cost.
 */
class BatchPhysicalSortAggRule
  extends RelOptRule(
    operand(classOf[FlinkLogicalAggregate],
      operand(classOf[RelNode], any)),
    "BatchPhysicalSortAggRule")
  with BatchPhysicalAggRuleBase {

  override def matches(call: RelOptRuleCall): Boolean = {
    val tableConfig = call.getPlanner.getContext.unwrap(classOf[FlinkContext]).getTableConfig
    val agg: FlinkLogicalAggregate = call.rel(0)
    !isOperatorDisabled(tableConfig, OperatorType.SortAgg) &&
      !agg.getAggCallList.exists(isPythonAggregate(_))
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val tableConfig = call.getPlanner.getContext.unwrap(classOf[FlinkContext]).getTableConfig
    val agg: FlinkLogicalAggregate = call.rel(0)
    val input: RelNode = call.rel(1)
    val inputRowType = input.getRowType

    val (auxGroupSet, aggCallsWithoutAuxGroupCalls) = AggregateUtil.checkAndSplitAggCalls(agg)

    val (_, aggBufferTypes, aggFunctions) = AggregateUtil.transformToBatchAggregateFunctions(
      FlinkTypeFactory.toLogicalRowType(inputRowType), aggCallsWithoutAuxGroupCalls)
    val groupSet = agg.getGroupSet.toArray
    val aggCallToAggFunction = aggCallsWithoutAuxGroupCalls.zip(aggFunctions)
    // TODO aggregate include projection now, so do not provide new trait will be safe
    val aggProvidedTraitSet = agg.getTraitSet.replace(FlinkConventions.BATCH_PHYSICAL)

    // create two-phase agg if possible
    if (isTwoPhaseAggWorkable(aggFunctions, tableConfig)) {
      // create BatchPhysicalLocalSortAggregate
      var localRequiredTraitSet = input.getTraitSet.replace(FlinkConventions.BATCH_PHYSICAL)
      if (agg.getGroupCount != 0) {
        val sortCollation = createRelCollation(groupSet)
        localRequiredTraitSet = localRequiredTraitSet.replace(sortCollation)
      }
      val newLocalInput = RelOptRule.convert(input, localRequiredTraitSet)
      val providedLocalTraitSet = localRequiredTraitSet

      val localSortAgg = createLocalAgg(
        agg.getCluster,
        providedLocalTraitSet,
        newLocalInput,
        agg.getRowType,
        groupSet,
        auxGroupSet,
        aggBufferTypes,
        aggCallToAggFunction,
        isLocalHashAgg = false)

      // create global BatchPhysicalSortAggregate
      val (globalGroupSet, globalAuxGroupSet) = getGlobalAggGroupSetPair(groupSet, auxGroupSet)
      val (globalDistributions, globalCollation) = if (agg.getGroupCount != 0) {
        // global agg should use groupSet's indices as distribution fields
        val distributionFields = globalGroupSet.map(Integer.valueOf).toList
        (
          Seq(
            FlinkRelDistribution.hash(distributionFields),
            FlinkRelDistribution.hash(distributionFields, requireStrict = false)),
          createRelCollation(globalGroupSet)
        )
      } else {
        (Seq(FlinkRelDistribution.SINGLETON), RelCollations.EMPTY)
      }
      // Remove the global agg call filters because the
      // filter is already done by local aggregation.
      val aggCallsWithoutFilter = aggCallsWithoutAuxGroupCalls.map {
        aggCall =>
          if (aggCall.filterArg > 0) {
            aggCall.copy(aggCall.getArgList, -1, aggCall.getCollation)
          } else {
            aggCall
          }
      }
      val globalAggCallToAggFunction = aggCallsWithoutFilter.zip(aggFunctions)
      globalDistributions.foreach { globalDistribution =>
        val requiredTraitSet = localSortAgg.getTraitSet
          .replace(globalDistribution)
          .replace(globalCollation)

        val newInputForFinalAgg = RelOptRule.convert(localSortAgg, requiredTraitSet)
        val globalSortAgg = new BatchPhysicalSortAggregate(
          agg.getCluster,
          aggProvidedTraitSet,
          newInputForFinalAgg,
          agg.getRowType,
          newInputForFinalAgg.getRowType,
          newLocalInput.getRowType,
          globalGroupSet,
          globalAuxGroupSet,
          globalAggCallToAggFunction,
          isMerge = true)
        call.transformTo(globalSortAgg)
      }
    }

    // create one-phase agg if possible
    if (isOnePhaseAggWorkable(agg, aggFunctions, tableConfig)) {
      val requiredDistributions = if (agg.getGroupCount != 0) {
        val distributionFields = groupSet.map(Integer.valueOf).toList
        Seq(
          FlinkRelDistribution.hash(distributionFields),
          FlinkRelDistribution.hash(distributionFields, requireStrict = false))
      } else {
        Seq(FlinkRelDistribution.SINGLETON)
      }
      requiredDistributions.foreach { requiredDistribution =>
        var requiredTraitSet = input.getTraitSet
          .replace(FlinkConventions.BATCH_PHYSICAL)
          .replace(requiredDistribution)
        if (agg.getGroupCount != 0) {
          val sortCollation = createRelCollation(groupSet)
          requiredTraitSet = requiredTraitSet.replace(sortCollation)
        }
        val newInput = RelOptRule.convert(input, requiredTraitSet)
        val sortAgg = new BatchPhysicalSortAggregate(
          agg.getCluster,
          aggProvidedTraitSet,
          newInput,
          agg.getRowType,
          newInput.getRowType,
          newInput.getRowType,
          groupSet,
          auxGroupSet,
          aggCallToAggFunction,
          isMerge = false
        )
        call.transformTo(sortAgg)
      }
    }
  }
}

object BatchPhysicalSortAggRule {
  val INSTANCE = new BatchPhysicalSortAggRule
}
