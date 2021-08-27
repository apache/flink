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
import org.apache.flink.table.planner.plan.nodes.physical.batch.{BatchPhysicalSortAggregate, BatchPhysicalExchange, BatchPhysicalExpand, BatchPhysicalGroupAggregateBase, BatchPhysicalHashAggregate}
import org.apache.flink.table.planner.plan.utils.{AggregateUtil, FlinkRelOptUtil}

import org.apache.calcite.plan.{RelOptRule, RelOptRuleOperand}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rex.RexUtil

import scala.collection.JavaConversions._

/**
 * Planner rule that writes one phase aggregate to two phase aggregate,
 * when the following conditions are met:
 * 1. there is no local aggregate,
 * 2. the aggregate has non-empty grouping and two phase aggregate strategy is enabled,
 * 3. the input is [[BatchPhysicalExpand]] and there is at least one expand row
 * which the columns for grouping are all constant.
 */
abstract class EnforceLocalAggRuleBase(
    operand: RelOptRuleOperand,
    description: String)
  extends RelOptRule(operand, description)
  with BatchPhysicalAggRuleBase {

  protected def isTwoPhaseAggEnabled(agg: BatchPhysicalGroupAggregateBase): Boolean = {
    val tableConfig = FlinkRelOptUtil.getTableConfigFromContext(agg)
    val aggFunctions = agg.getAggCallToAggFunction.map(_._2).toArray
    isTwoPhaseAggWorkable(aggFunctions, tableConfig)
  }

  protected def hasConstantShuffleKey(
      shuffleKey: Array[Int],
      expand: BatchPhysicalExpand): Boolean = {
    // if all shuffle-key columns in a expand row are constant, this row will be shuffled to
    // a single node.
    // add local aggregate to greatly reduce the output data
    expand.projects.exists {
      project => shuffleKey.map(i => project.get(i)).forall(RexUtil.isConstant)
    }
  }

  protected def createLocalAgg(
      completeAgg: BatchPhysicalGroupAggregateBase,
      input: RelNode): BatchPhysicalGroupAggregateBase = {
    val cluster = completeAgg.getCluster
    val inputRowType = input.getRowType

    val grouping = completeAgg.grouping
    val auxGrouping = completeAgg.auxGrouping
    val aggCalls = completeAgg.getAggCallList
    val aggCallToAggFunction = completeAgg.getAggCallToAggFunction

    val (_, aggBufferTypes, _) = AggregateUtil.transformToBatchAggregateFunctions(
      FlinkTypeFactory.toLogicalRowType(inputRowType), aggCalls)

    val traitSet = cluster.getPlanner
      .emptyTraitSet
      .replace(FlinkConventions.BATCH_PHYSICAL)

    val isLocalHashAgg = completeAgg match {
      case _: BatchPhysicalHashAggregate => true
      case _: BatchPhysicalSortAggregate => false
      case _ =>
        throw new TableException(s"Unsupported aggregate: ${completeAgg.getClass.getSimpleName}")
    }

    createLocalAgg(
      cluster,
      traitSet,
      input,
      completeAgg.getRowType,
      grouping,
      auxGrouping,
      aggBufferTypes,
      aggCallToAggFunction,
      isLocalHashAgg
    )
  }

  protected def createExchange(
      completeAgg: BatchPhysicalGroupAggregateBase,
      input: RelNode): BatchPhysicalExchange = {
    val cluster = completeAgg.getCluster
    val grouping = completeAgg.grouping

    // local aggregate outputs group fields first, and then agg calls
    val distributionFields = grouping.indices.map(Integer.valueOf)
    val newDistribution = FlinkRelDistribution.hash(distributionFields, requireStrict = true)
    val newTraitSet = completeAgg.getCluster.getPlanner
      .emptyTraitSet
      .replace(FlinkConventions.BATCH_PHYSICAL)
      .replace(newDistribution)

    new BatchPhysicalExchange(cluster, newTraitSet, input, newDistribution)
  }

  protected def createGlobalAgg(
      completeAgg: BatchPhysicalGroupAggregateBase,
      input: RelNode): BatchPhysicalGroupAggregateBase = {
    val grouping = completeAgg.grouping
    val auxGrouping = completeAgg.auxGrouping
    val aggCallToAggFunction = completeAgg.getAggCallToAggFunction

    val (newGrouping, newAuxGrouping) = getGlobalAggGroupSetPair(grouping, auxGrouping)

    val aggRowType = completeAgg.getRowType
    val inputRowType = input.getRowType
    val aggInputRowType = completeAgg.getInput.getRowType

    completeAgg match {
      case _: BatchPhysicalHashAggregate =>
        new BatchPhysicalHashAggregate(
          completeAgg.getCluster,
          completeAgg.getTraitSet,
          input,
          aggRowType,
          inputRowType,
          aggInputRowType,
          newGrouping,
          newAuxGrouping,
          aggCallToAggFunction,
          isMerge = true)
      case _: BatchPhysicalSortAggregate =>
        new BatchPhysicalSortAggregate(
          completeAgg.getCluster,
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
