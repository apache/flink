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
package org.apache.flink.table.plan.rules.physical.batch

import org.apache.flink.table.api.{OperatorType, TableConfig}
import org.apache.flink.table.plan.`trait`.FlinkRelDistribution
import org.apache.flink.table.plan.nodes.FlinkConventions
import org.apache.flink.table.plan.nodes.physical.batch.{BatchExecLocalSortAggregate, BatchExecSortAggregate}
import org.apache.flink.table.plan.nodes.logical.FlinkLogicalAggregate
import org.apache.flink.table.plan.util.{AggregateUtil, FlinkRelOptUtil}

import org.apache.calcite.plan.RelOptRule.{any, operand}
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.calcite.rel._

import scala.collection.JavaConversions._

class BatchExecSortAggRule
  extends RelOptRule(
    operand(classOf[FlinkLogicalAggregate],
      operand(classOf[RelNode], any)),
    "BatchExecSortAggRule")
  with BaseBatchExecAggRule {

  override def matches(call: RelOptRuleCall): Boolean = {
    val tableConfig = call.getPlanner.getContext.unwrap(classOf[TableConfig])
    tableConfig.enabledGivenOpType(OperatorType.SortAgg)
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val agg = call.rels(0).asInstanceOf[FlinkLogicalAggregate]
    val input = call.rels(1)

    if (agg.indicator) {
      throw new UnsupportedOperationException("Not support group sets aggregate now.")
    }

    val (auxGroupSet, aggCallsWithoutAuxGroupCalls) = FlinkRelOptUtil.checkAndSplitAggCalls(agg)

    val (_, aggBufferTypes, aggregates) = AggregateUtil.transformToBatchAggregateFunctions(
      aggCallsWithoutAuxGroupCalls, input.getRowType)
    val groupSet = agg.getGroupSet.toArray
    val aggCallToAggFunction = aggCallsWithoutAuxGroupCalls.zip(aggregates)
    // TODO aggregate include projection now, so do not provide new trait will be safe
    val aggProvidedTraitSet = agg.getTraitSet.replace(FlinkConventions.BATCH_PHYSICAL)

    if (isTwoPhaseAggWorkable(aggregates, call)) {
      val localAggRelType = inferLocalAggType(
        input.getRowType, agg, groupSet, auxGroupSet, aggregates,
        aggBufferTypes.map(_.map(_.toInternalType)))
      //localSortAgg
      var localRequiredTraitSet = input.getTraitSet.replace(FlinkConventions.BATCH_PHYSICAL)
      if (agg.getGroupCount != 0) {
        localRequiredTraitSet = localRequiredTraitSet.replace(createRelCollation(groupSet))
      }
      val newLocalInput = RelOptRule.convert(input, localRequiredTraitSet)
      val providedLocalTraitSet = localRequiredTraitSet
      val localSortAgg = new BatchExecLocalSortAggregate(
        agg.getCluster,
        call.builder(),
        providedLocalTraitSet,
        newLocalInput,
        aggCallToAggFunction,
        localAggRelType,
        newLocalInput.getRowType,
        groupSet,
        auxGroupSet)

      //globalSortAgg
      val globalDistributions = if (agg.getGroupCount != 0) {
        // global agg should use groupSet's indices as distribution fields
        val globalGroupSet = groupSet.indices
        val distributionFields = globalGroupSet.map(Integer.valueOf)
        Seq(
          FlinkRelDistribution.hash(distributionFields),
          FlinkRelDistribution.hash(distributionFields, requireStrict = false))
      } else {
        Seq(FlinkRelDistribution.SINGLETON)
      }
      globalDistributions.foreach { globalDistribution =>
        //replace the RelCollation with EMPTY
        var requiredTraitSet = localSortAgg.getTraitSet
            .replace(globalDistribution).replace(RelCollations.EMPTY)
        if (agg.getGroupCount != 0) {
          requiredTraitSet = requiredTraitSet.replace(createRelCollation(groupSet.indices.toArray))
        }
        val newInputForFinalAgg = RelOptRule.convert(localSortAgg, requiredTraitSet)
        val globalSortAgg = new BatchExecSortAggregate(
          agg.getCluster,
          call.builder(),
          aggProvidedTraitSet,
          newInputForFinalAgg,
          aggCallToAggFunction,
          agg.getRowType,
          newInputForFinalAgg.getRowType,
          groupSet.indices.toArray,
          (groupSet.length until groupSet.length + auxGroupSet.length).toArray,
          isMerge = true)
        call.transformTo(globalSortAgg)
      }
    }
    if (isOnePhaseAggWorkable(agg, aggregates, call)) {
      val requiredDistributions = if (agg.getGroupCount != 0) {
        val distributionFields = groupSet.map(Integer.valueOf).toList
        Seq(
          FlinkRelDistribution.hash(distributionFields),
          FlinkRelDistribution.hash(distributionFields, requireStrict = false))
      } else {
        Seq(FlinkRelDistribution.SINGLETON)
      }
      requiredDistributions.foreach { requiredDistribution =>
        var requiredTraitSet = input.getTraitSet.replace(FlinkConventions.BATCH_PHYSICAL)
            .replace(requiredDistribution)
        if (agg.getGroupCount != 0) {
          requiredTraitSet = requiredTraitSet.replace(createRelCollation(groupSet))
        }
        val newInput = RelOptRule.convert(input, requiredTraitSet)
        //transform aggregate physical node
        val sortAgg = new BatchExecSortAggregate(
          agg.getCluster,
          call.builder(),
          aggProvidedTraitSet,
          newInput,
          aggCallToAggFunction,
          agg.getRowType,
          newInput.getRowType,
          groupSet,
          auxGroupSet,
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
