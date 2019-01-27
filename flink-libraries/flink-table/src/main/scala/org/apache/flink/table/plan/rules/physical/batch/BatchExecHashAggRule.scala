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
import org.apache.flink.table.plan.nodes.logical.FlinkLogicalAggregate
import org.apache.flink.table.plan.nodes.physical.batch.{BatchExecHashAggregate, BatchExecLocalHashAggregate}
import org.apache.flink.table.plan.util.{AggregateUtil, FlinkRelOptUtil}

import org.apache.calcite.plan.RelOptRule.{any, operand}
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.calcite.rel.RelNode

import scala.collection.JavaConversions._

class BatchExecHashAggRule
  extends RelOptRule(
    operand(classOf[FlinkLogicalAggregate],
      operand(classOf[RelNode], any)),
    "BatchExecHashAggRule")
  with BaseBatchExecAggRule {

  override def matches(call: RelOptRuleCall): Boolean = {
    val tableConfig = call.getPlanner.getContext.unwrap(classOf[TableConfig])
    if (!tableConfig.enabledGivenOpType(OperatorType.HashAgg)) {
      return false
    }
    // HashAgg cannot process aggregate whose agg buffer is not fix length
    isAggBufferFixedLength(call)
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val agg = call.rels(0).asInstanceOf[FlinkLogicalAggregate]
    val input = call.rels(1)

    val (auxGroupSet, aggCallsWithoutAuxGroupCalls) = FlinkRelOptUtil.checkAndSplitAggCalls(agg)

    val (_, aggBufferTypes, aggregates) = AggregateUtil.transformToBatchAggregateFunctions(
      aggCallsWithoutAuxGroupCalls, input.getRowType)

    val aggCallToAggFunction = aggCallsWithoutAuxGroupCalls.zip(aggregates)
    val groupSet = agg.getGroupSet.toArray
    val aggProvidedTraitSet = agg.getTraitSet.replace(FlinkConventions.BATCH_PHYSICAL)
    if (isTwoPhaseAggWorkable(aggregates, call)) {
      //localHashAgg
      val localAggRelType = inferLocalAggType(
        input.getRowType, agg, groupSet, auxGroupSet, aggregates,
        aggBufferTypes.map(_.map(_.toInternalType)))
      val localRequiredTraitSet = input.getTraitSet.replace(FlinkConventions.BATCH_PHYSICAL)
      val newInput = RelOptRule.convert(input, localRequiredTraitSet)
      val providedTraitSet = localRequiredTraitSet

      val localHashAgg = new BatchExecLocalHashAggregate(
        agg.getCluster,
        call.builder(),
        providedTraitSet,
        newInput,
        aggCallToAggFunction,
        localAggRelType,
        newInput.getRowType,
        groupSet,
        auxGroupSet)

      //globalHashAgg
      val globalDistributions = if (agg.getGroupCount != 0) {
        val globalGroupSet = groupSet.indices
        val distributionFields = globalGroupSet.map(Integer.valueOf).toList
        Seq(
          FlinkRelDistribution.hash(distributionFields),
          FlinkRelDistribution.hash(distributionFields, requireStrict = false))
      } else {
        Seq(FlinkRelDistribution.SINGLETON)
      }
      globalDistributions.foreach { globalDistribution =>
        val requiredTraitSet = localHashAgg.getTraitSet.replace(globalDistribution)
        val newLocalHashAgg = RelOptRule.convert(localHashAgg, requiredTraitSet)
        val globalHashAgg = new BatchExecHashAggregate(
          agg.getCluster,
          call.builder(),
          aggProvidedTraitSet,
          newLocalHashAgg,
          aggCallToAggFunction,
          agg.getRowType,
          newLocalHashAgg.getRowType,
          groupSet.indices.toArray,
          (groupSet.length until groupSet.length + auxGroupSet.length).toArray,
          true)
        call.transformTo(globalHashAgg)
      }
    }
    if (isOnePhaseAggWorkable(agg, aggregates, call)) {
      val requiredDistributions = if (agg.getGroupCount != 0) {
        val distributionFields = groupSet.map(Integer.valueOf).toList
        Seq(
          FlinkRelDistribution.hash(distributionFields, requireStrict = false),
          FlinkRelDistribution.hash(distributionFields))
      } else {
        Seq(FlinkRelDistribution.SINGLETON)
      }
      requiredDistributions.foreach { requiredDistribution =>
        val newInput = RelOptRule.convert(input,
          input.getTraitSet.replace(FlinkConventions.BATCH_PHYSICAL).replace(requiredDistribution))
        val hashAgg = new BatchExecHashAggregate(
          agg.getCluster,
          call.builder(),
          aggProvidedTraitSet,
          newInput,
          aggCallToAggFunction,
          agg.getRowType,
          input.getRowType,
          groupSet,
          auxGroupSet,
          false)
        call.transformTo(hashAgg)
      }
    }
  }
}

object BatchExecHashAggRule {
  val INSTANCE = new BatchExecHashAggRule
}
