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

import org.apache.flink.table.api.TableException
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.plan.`trait`.FlinkRelDistribution
import org.apache.flink.table.planner.plan.nodes.FlinkConventions
import org.apache.flink.table.planner.plan.nodes.physical.batch.{BatchExecExchange, BatchExecExpand, BatchExecGroupAggregateBase, BatchExecHashAggregate, BatchExecLocalHashAggregate, BatchExecLocalSortAggregate, BatchExecSortAggregate}
import org.apache.flink.table.planner.plan.utils.{AggregateUtil, FlinkRelOptUtil}
import org.apache.flink.table.runtime.types.LogicalTypeDataTypeConverter.fromDataTypeToLogicalType

import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall, RelOptRuleOperand}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rex.RexUtil
import org.apache.calcite.tools.RelBuilder
import org.apache.calcite.util.Util

import scala.collection.JavaConversions._

/**
  * Planner rule that writes one phase aggregate to two phase aggregate,
  * when the following conditions are met:
  * 1. there is no local aggregate,
  * 2. the aggregate has non-empty grouping and two phase aggregate strategy is enabled,
  * 3. the input is [[BatchExecExpand]] and there is at least one expand row
  * which the columns for grouping are all constant.
  */
abstract class EnforceLocalAggRuleBase(
    operand: RelOptRuleOperand,
    description: String)
  extends RelOptRule(operand, description)
  with BatchExecAggRuleBase {

  protected def getBatchExecExpand(call: RelOptRuleCall): BatchExecExpand

  override def matches(call: RelOptRuleCall): Boolean = {
    val agg: BatchExecGroupAggregateBase = call.rel(0)
    val expand = getBatchExecExpand(call)

    val tableConfig = FlinkRelOptUtil.getTableConfigFromContext(agg)
    val aggFunctions = agg.getAggCallToAggFunction.map(_._2).toArray
    val enableTwoPhaseAgg = isTwoPhaseAggWorkable(aggFunctions, tableConfig)

    val grouping = agg.getGrouping
    // if all group columns in a expand row are constant, this row will be shuffled to
    // a single node. (shuffle keys are grouping)
    // add local aggregate to greatly reduce the output data
    val hasConstantRow = expand.projects.exists {
      project =>
        val groupingColumns = grouping.map(i => project.get(i))
        groupingColumns.forall(RexUtil.isConstant)
    }

    grouping.nonEmpty && enableTwoPhaseAgg && hasConstantRow
  }

  protected def createLocalAgg(
      completeAgg: BatchExecGroupAggregateBase,
      input: RelNode,
      relBuilder: RelBuilder): BatchExecGroupAggregateBase = {
    val cluster = completeAgg.getCluster
    val inputRowType = input.getRowType

    val grouping = completeAgg.getGrouping
    val auxGrouping = completeAgg.getAuxGrouping
    val aggCalls = completeAgg.getAggCallList
    val aggCallToAggFunction = completeAgg.getAggCallToAggFunction

    val (_, aggBufferTypes, aggFunctions) = AggregateUtil.transformToBatchAggregateFunctions(
      aggCalls, inputRowType)

    val typeFactory = completeAgg.getCluster.getTypeFactory.asInstanceOf[FlinkTypeFactory]
    val aggCallNames = Util.skip(
      completeAgg.getRowType.getFieldNames, grouping.length + auxGrouping.length).toList.toArray

    val localAggRowType = inferLocalAggType(
      inputRowType,
      typeFactory,
      aggCallNames,
      grouping,
      auxGrouping,
      aggFunctions,
      aggBufferTypes.map(_.map(fromDataTypeToLogicalType)))

    val traitSet = cluster.getPlanner
      .emptyTraitSet
      .replace(FlinkConventions.BATCH_PHYSICAL)

    completeAgg match {
      case _: BatchExecHashAggregate =>
        new BatchExecLocalHashAggregate(
          cluster,
          relBuilder,
          traitSet,
          input,
          localAggRowType,
          inputRowType,
          grouping,
          auxGrouping,
          aggCallToAggFunction)
      case _: BatchExecSortAggregate =>
        new BatchExecLocalSortAggregate(
          cluster,
          relBuilder,
          traitSet,
          input,
          localAggRowType,
          inputRowType,
          grouping,
          auxGrouping,
          aggCallToAggFunction)
      case _ =>
        throw new TableException(s"Unsupported aggregate: ${completeAgg.getClass.getSimpleName}")
    }
  }

  protected def createExchange(
      completeAgg: BatchExecGroupAggregateBase,
      input: RelNode): BatchExecExchange = {
    val cluster = completeAgg.getCluster
    val grouping = completeAgg.getGrouping

    // local aggregate outputs group fields first, and then agg calls
    val distributionFields = grouping.indices.map(Integer.valueOf)
    val newDistribution = FlinkRelDistribution.hash(distributionFields, requireStrict = true)
    val newTraitSet = completeAgg.getCluster.getPlanner
      .emptyTraitSet
      .replace(FlinkConventions.BATCH_PHYSICAL)
      .replace(newDistribution)

    new BatchExecExchange(cluster, newTraitSet, input, newDistribution)
  }

  protected def createGlobalAgg(
      completeAgg: BatchExecGroupAggregateBase,
      input: RelNode,
      relBuilder: RelBuilder): BatchExecGroupAggregateBase = {
    val grouping = completeAgg.getGrouping
    val auxGrouping = completeAgg.getAuxGrouping
    val aggCallToAggFunction = completeAgg.getAggCallToAggFunction

    val newGrouping = grouping.indices.toArray
    val newAuxGrouping = (grouping.length until grouping.length + auxGrouping.length).toArray

    val aggRowType = completeAgg.getRowType
    val inputRowType = input.getRowType
    val aggInputRowType = completeAgg.getInput.getRowType

    completeAgg match {
      case _: BatchExecHashAggregate =>
        new BatchExecHashAggregate(
          completeAgg.getCluster,
          relBuilder,
          completeAgg.getTraitSet,
          input,
          aggRowType,
          inputRowType,
          aggInputRowType,
          newGrouping,
          newAuxGrouping,
          aggCallToAggFunction,
          isMerge = true)
      case _: BatchExecSortAggregate =>
        new BatchExecSortAggregate(
          completeAgg.getCluster,
          relBuilder,
          completeAgg.getTraitSet,
          input,
          aggRowType,
          inputRowType,
          aggInputRowType,
          newGrouping,
          newAuxGrouping,
          aggCallToAggFunction,
          isMerge = true)
      case _ =>
        throw new TableException(s"Unsupported aggregate: ${completeAgg.getClass.getSimpleName}")
    }
  }
}
