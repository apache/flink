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
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchExecHashAggregate
import org.apache.flink.table.planner.plan.utils.PythonUtil.isPythonAggregate
import org.apache.flink.table.planner.plan.utils.{AggregateUtil, OperatorType}
import org.apache.flink.table.planner.utils.TableConfigUtils.isOperatorDisabled

import org.apache.calcite.plan.RelOptRule.{any, operand}
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.calcite.rel.RelNode

import scala.collection.JavaConversions._

/**
  * Rule that matches [[FlinkLogicalAggregate]] which all aggregate function buffer are fix length,
  * and converts it to
  * {{{
  *   BatchExecHashAggregate (global)
  *   +- BatchExecExchange (hash by group keys if group keys is not empty, else singleton)
  *      +- BatchExecLocalHashAggregate (local)
  *         +- input of agg
  * }}}
  * when all aggregate functions are mergeable
  * and [[OptimizerConfigOptions.TABLE_OPTIMIZER_AGG_PHASE_STRATEGY]] is TWO_PHASE, or
  * {{{
  *   BatchExecHashAggregate
  *   +- BatchExecExchange (hash by group keys if group keys is not empty, else singleton)
  *      +- input of agg
  * }}}
  * when some aggregate functions are not mergeable
  * or [[OptimizerConfigOptions.TABLE_OPTIMIZER_AGG_PHASE_STRATEGY]] is ONE_PHASE.
  *
  * Notes: if [[OptimizerConfigOptions.TABLE_OPTIMIZER_AGG_PHASE_STRATEGY]] is NONE,
  * this rule will try to create two possibilities above, and chooses the best one based on cost.
  */
class BatchExecHashAggRule
  extends RelOptRule(
    operand(classOf[FlinkLogicalAggregate],
      operand(classOf[RelNode], any)),
    "BatchExecHashAggRule")
  with BatchExecAggRuleBase {

  override def matches(call: RelOptRuleCall): Boolean = {
    val tableConfig = call.getPlanner.getContext.unwrap(classOf[FlinkContext]).getTableConfig
    if (isOperatorDisabled(tableConfig, OperatorType.HashAgg)) {
      return false
    }
    val agg: FlinkLogicalAggregate = call.rel(0)
    // HashAgg cannot process aggregate whose agg buffer is not fix length
    isAggBufferFixedLength(agg) &&
      !agg.getAggCallList.exists(isPythonAggregate(_))
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val tableConfig = call.getPlanner.getContext.unwrap(classOf[FlinkContext]).getTableConfig
    val agg: FlinkLogicalAggregate = call.rel(0)
    val input: RelNode = call.rel(1)
    val inputRowType = input.getRowType

    if (agg.indicator) {
      throw new UnsupportedOperationException("Not support group sets aggregate now.")
    }

    val groupSet = agg.getGroupSet.toArray
    val (auxGroupSet, aggCallsWithoutAuxGroupCalls) = AggregateUtil.checkAndSplitAggCalls(agg)

    val (_, aggBufferTypes, aggFunctions) = AggregateUtil.transformToBatchAggregateFunctions(
      aggCallsWithoutAuxGroupCalls, inputRowType)

    val aggCallToAggFunction = aggCallsWithoutAuxGroupCalls.zip(aggFunctions)
    val aggProvidedTraitSet = agg.getTraitSet.replace(FlinkConventions.BATCH_PHYSICAL)

    // create two-phase agg if possible
    if (isTwoPhaseAggWorkable(aggFunctions, tableConfig)) {
      // create BatchExecLocalHashAggregate
      val localRequiredTraitSet = input.getTraitSet.replace(FlinkConventions.BATCH_PHYSICAL)
      val newInput = RelOptRule.convert(input, localRequiredTraitSet)
      val providedTraitSet = localRequiredTraitSet
      val localHashAgg = createLocalAgg(
        agg.getCluster,
        call.builder(),
        providedTraitSet,
        newInput,
        agg.getRowType,
        groupSet,
        auxGroupSet,
        aggBufferTypes,
        aggCallToAggFunction,
        isLocalHashAgg = true)

      // create global BatchExecHashAggregate
      val (globalGroupSet, globalAuxGroupSet) = getGlobalAggGroupSetPair(groupSet, auxGroupSet)
      val globalDistributions = if (agg.getGroupCount != 0) {
        val distributionFields = globalGroupSet.map(Integer.valueOf).toList
        Seq(
          FlinkRelDistribution.hash(distributionFields),
          FlinkRelDistribution.hash(distributionFields, requireStrict = false))
      } else {
        Seq(FlinkRelDistribution.SINGLETON)
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
        val requiredTraitSet = localHashAgg.getTraitSet.replace(globalDistribution)
        val newLocalHashAgg = RelOptRule.convert(localHashAgg, requiredTraitSet)
        val globalHashAgg = new BatchExecHashAggregate(
          agg.getCluster,
          call.builder(),
          aggProvidedTraitSet,
          newLocalHashAgg,
          agg.getRowType,
          newLocalHashAgg.getRowType,
          inputRowType,
          globalGroupSet,
          globalAuxGroupSet,
          globalAggCallToAggFunction,
          isMerge = true)
        call.transformTo(globalHashAgg)
      }
    }

    // create one-phase agg if possible
    if (isOnePhaseAggWorkable(agg, aggFunctions, tableConfig)) {
      val requiredDistributions = if (agg.getGroupCount != 0) {
        val distributionFields = groupSet.map(Integer.valueOf).toList
        Seq(
          FlinkRelDistribution.hash(distributionFields, requireStrict = false),
          FlinkRelDistribution.hash(distributionFields))
      } else {
        Seq(FlinkRelDistribution.SINGLETON)
      }
      requiredDistributions.foreach { requiredDistribution =>
        val requiredTraitSet = input.getTraitSet
          .replace(FlinkConventions.BATCH_PHYSICAL)
          .replace(requiredDistribution)
        val newInput = RelOptRule.convert(input, requiredTraitSet)
        val hashAgg = new BatchExecHashAggregate(
          agg.getCluster,
          call.builder(),
          aggProvidedTraitSet,
          newInput,
          agg.getRowType,
          input.getRowType,
          input.getRowType,
          groupSet,
          auxGroupSet,
          aggCallToAggFunction,
          isMerge = false)
        call.transformTo(hashAgg)
      }
    }
  }
}

object BatchExecHashAggRule {
  val INSTANCE = new BatchExecHashAggRule
}
