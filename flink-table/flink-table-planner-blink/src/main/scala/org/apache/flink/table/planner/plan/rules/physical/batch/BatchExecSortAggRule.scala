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
import org.apache.flink.table.planner.calcite.FlinkContext
import org.apache.flink.table.planner.plan.`trait`.FlinkRelDistribution
import org.apache.flink.table.planner.plan.nodes.FlinkConventions
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalAggregate
import org.apache.flink.table.planner.plan.nodes.physical.batch.{BatchExecLocalSortAggregate, BatchExecSortAggregate}
import org.apache.flink.table.planner.plan.utils.{AggregateUtil, OperatorType}
import org.apache.flink.table.planner.utils.TableConfigUtils.isOperatorDisabled
import org.apache.flink.table.runtime.types.LogicalTypeDataTypeConverter.fromDataTypeToLogicalType

import org.apache.calcite.plan.RelOptRule.{any, operand}
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.calcite.rel._

import scala.collection.JavaConversions._

/**
  * Rule that converts [[FlinkLogicalAggregate]] to
  * {{{
  *   BatchExecSortAggregate (global)
  *   +- Sort (exists if group keys are not empty)
  *      +- BatchExecExchange (hash by group keys if group keys is not empty, else singleton)
  *         +- BatchExecLocalSortAggregate (local)
  *           +- Sort (exists if group keys are not empty)
  *              +- input of agg
  * }}}
  * when all aggregate functions are mergeable
  * and [[OptimizerConfigOptions.TABLE_OPTIMIZER_AGG_PHASE_STRATEGY]] is TWO_PHASE, or
  * {{{
  *   BatchExecSortAggregate
  *   +- Sort (exists if group keys are not empty)
  *      +- BatchExecExchange (hash by group keys if group keys is not empty, else singleton)
  *         +- input of agg
  * }}}
  * when some aggregate functions are not mergeable
  * or [[OptimizerConfigOptions.TABLE_OPTIMIZER_AGG_PHASE_STRATEGY]] is ONE_PHASE.
  *
  * Notes: if [[OptimizerConfigOptions.TABLE_OPTIMIZER_AGG_PHASE_STRATEGY]] is NONE,
  * this rule will try to create two possibilities above, and chooses the best one based on cost.
  */
class BatchExecSortAggRule
  extends RelOptRule(
    operand(classOf[FlinkLogicalAggregate],
      operand(classOf[RelNode], any)),
    "BatchExecSortAggRule")
  with BatchExecAggRuleBase {

  override def matches(call: RelOptRuleCall): Boolean = {
    val tableConfig = call.getPlanner.getContext.asInstanceOf[FlinkContext].getTableConfig
    !isOperatorDisabled(tableConfig, OperatorType.SortAgg)
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val tableConfig = call.getPlanner.getContext.asInstanceOf[FlinkContext].getTableConfig
    val agg: FlinkLogicalAggregate = call.rel(0)
    val input: RelNode = call.rel(1)
    val inputRowType = input.getRowType

    if (agg.indicator) {
      throw new UnsupportedOperationException("Not support group sets aggregate now.")
    }

    val (auxGroupSet, aggCallsWithoutAuxGroupCalls) = AggregateUtil.checkAndSplitAggCalls(agg)

    val (_, aggBufferTypes, aggFunctions) = AggregateUtil.transformToBatchAggregateFunctions(
      aggCallsWithoutAuxGroupCalls, inputRowType)
    val groupSet = agg.getGroupSet.toArray
    val aggCallToAggFunction = aggCallsWithoutAuxGroupCalls.zip(aggFunctions)
    // TODO aggregate include projection now, so do not provide new trait will be safe
    val aggProvidedTraitSet = agg.getTraitSet.replace(FlinkConventions.BATCH_PHYSICAL)

    // create two-phase agg if possible
    if (isTwoPhaseAggWorkable(aggFunctions, tableConfig)) {
      val localAggRelType = inferLocalAggType(
        inputRowType,
        agg,
        groupSet,
        auxGroupSet,
        aggFunctions,
        aggBufferTypes.map(_.map(fromDataTypeToLogicalType)))
      // create BatchExecLocalSortAggregate
      var localRequiredTraitSet = input.getTraitSet.replace(FlinkConventions.BATCH_PHYSICAL)
      if (agg.getGroupCount != 0) {
        val sortCollation = createRelCollation(groupSet)
        localRequiredTraitSet = localRequiredTraitSet.replace(sortCollation)
      }
      val newLocalInput = RelOptRule.convert(input, localRequiredTraitSet)
      val providedLocalTraitSet = localRequiredTraitSet
      val localSortAgg = new BatchExecLocalSortAggregate(
        agg.getCluster,
        call.builder(),
        providedLocalTraitSet,
        newLocalInput,
        localAggRelType,
        newLocalInput.getRowType,
        groupSet,
        auxGroupSet,
        aggCallToAggFunction)

      // create global BatchExecSortAggregate
      val globalGroupSet = groupSet.indices.toArray
      val globalAuxGroupSet = (groupSet.length until groupSet.length + auxGroupSet.length).toArray
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
      globalDistributions.foreach { globalDistribution =>
        val requiredTraitSet = localSortAgg.getTraitSet
          .replace(globalDistribution)
          .replace(globalCollation)

        val newInputForFinalAgg = RelOptRule.convert(localSortAgg, requiredTraitSet)
        val globalSortAgg = new BatchExecSortAggregate(
          agg.getCluster,
          call.builder(),
          aggProvidedTraitSet,
          newInputForFinalAgg,
          agg.getRowType,
          newInputForFinalAgg.getRowType,
          newLocalInput.getRowType,
          globalGroupSet,
          globalAuxGroupSet,
          aggCallToAggFunction,
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
        val sortAgg = new BatchExecSortAggregate(
          agg.getCluster,
          call.builder(),
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

object BatchExecSortAggRule {
  val INSTANCE = new BatchExecSortAggRule
}
