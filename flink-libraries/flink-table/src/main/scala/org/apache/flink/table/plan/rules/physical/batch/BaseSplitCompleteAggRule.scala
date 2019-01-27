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

import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.plan.metadata.FlinkRelMetadataQuery
import org.apache.flink.table.plan.nodes.FlinkConventions
import org.apache.flink.table.plan.nodes.physical.batch._
import org.apache.flink.table.plan.`trait`.FlinkRelDistribution
import org.apache.flink.table.plan.util.AggregateUtil

import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall, RelOptRuleOperand}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.tools.RelBuilder
import org.apache.calcite.util.Util

import scala.collection.JavaConversions._

/**
  *
  * Split Complete Aggregate into two phase Aggregate if its input data is skew on group by keys.
  */
abstract class BaseSplitCompleteAggRule(
    operand: RelOptRuleOperand,
    description: String)
  extends RelOptRule(operand, description)
  with BaseBatchExecAggRule {

  override def matches(call: RelOptRuleCall): Boolean = {
    val agg = call.rels(0).asInstanceOf[BatchExecGroupAggregateBase]
    isCompleteAgg(agg) &&
      isTwoPhaseAggWorkable(agg.aggregates, call) &&
      isSkewOnGroupKeys(agg)
  }

  /**
    * Check whether agg is a CompleteAggregate.
    *
    * @param agg aggregate.
    * @return True agg is a CompleteAggregate.
    */
  protected def isCompleteAgg(agg: BatchExecGroupAggregateBase): Boolean =
    agg.isFinal && !agg.isMerge

  /**
    * Check whether input data of agg is skew on group keys.
    *
    * @param agg aggregate.
    * @return True if input data of agg is skew on group keys, else false.
    */
  protected def isSkewOnGroupKeys(agg: BatchExecGroupAggregateBase): Boolean = {
    val grouping = agg.getGrouping
    if (grouping.isEmpty) {
      return false
    }
    val mq = agg.getCluster.getMetadataQuery
    val fmq = FlinkRelMetadataQuery.reuseOrCreate(mq)
    val skewInfo = fmq.getSkewInfo(agg.getInput)
    if (skewInfo == null) {
      return false
    }
    val skewMap = skewInfo.skewInfo
    // TODO introduce skewInfo on a specified set of columns later,
    // skew on column [a] or skew on column [b] does not mean skew on [a, b]
    grouping exists { k =>
      skewMap.get(k) match {
        case Some(skewValues) => skewValues.nonEmpty
        case _ => false
      }
    }
  }

  protected def createExchange(
    completeAgg: BatchExecGroupAggregateBase,
    input: RelNode): BatchExecExchange = {
    val grouping = completeAgg.getGrouping
    // it's not possible to arrive here if there is no grouping key
    require(grouping.length != 0)

    val distributionFields = grouping.indices.map(Integer.valueOf)
    val distributionOfExchange = FlinkRelDistribution.hash(
      distributionFields, requireStrict = true)
    val traitSetOfExchange = completeAgg.getCluster.getPlanner.emptyTraitSet
      .replace(FlinkConventions.BATCH_PHYSICAL)
      .replace(distributionOfExchange)
    new BatchExecExchange(
      completeAgg.getCluster, traitSetOfExchange, input, distributionOfExchange)
  }

  protected def createLocalAgg(
    completeAgg: BatchExecGroupAggregateBase,
    input: RelNode,
    relBuilder: RelBuilder): BatchExecGroupAggregateBase = {
    val grouping = completeAgg.getGrouping
    val auxGrouping = completeAgg.getAuxGrouping
    val aggCallsWithoutAuxGroupCalls = completeAgg.aggregateCalls
    val aggCallToAggFunction = completeAgg.getAggCallToAggFunction
    val (_, aggBufferTypes, aggregates) = AggregateUtil.transformToBatchAggregateFunctions(
      aggCallsWithoutAuxGroupCalls,
      input.getRowType)
    val typeFactory = completeAgg.getCluster.getTypeFactory.asInstanceOf[FlinkTypeFactory]
    val aggCallNames = Util.skip(
      completeAgg.getRowType.getFieldNames, grouping.length + auxGrouping.length).toList
    val localAggRelType = inferLocalAggType(
      input.getRowType,
      typeFactory,
      aggCallNames.toArray[String],
      grouping,
      auxGrouping,
      aggregates,
      aggBufferTypes.map(_.map(_.toInternalType)))
    val traitSet = completeAgg.getCluster.getPlanner.emptyTraitSet
      .replace(FlinkConventions.BATCH_PHYSICAL)
    completeAgg match {
      case _: BatchExecHashAggregate =>
        new BatchExecLocalHashAggregate(
          completeAgg.getCluster,
          relBuilder,
          traitSet,
          input,
          aggCallToAggFunction,
          localAggRelType,
          input.getRowType,
          grouping,
          auxGrouping)
      case _: BatchExecSortAggregate =>
        new BatchExecLocalSortAggregate(
          completeAgg.getCluster,
          relBuilder,
          traitSet,
          input,
          aggCallToAggFunction,
          localAggRelType,
          input.getRowType,
          grouping,
          auxGrouping)
    }
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
    completeAgg match {
      case _: BatchExecHashAggregate =>
        new BatchExecHashAggregate(
          completeAgg.getCluster,
          relBuilder,
          completeAgg.getTraitSet,
          input,
          aggCallToAggFunction,
          completeAgg.getRowType,
          input.getRowType,
          newGrouping,
          newAuxGrouping,
          isMerge = true)
      case _: BatchExecSortAggregate =>
        new BatchExecSortAggregate(
          completeAgg.getCluster,
          relBuilder,
          completeAgg.getTraitSet,
          input,
          aggCallToAggFunction,
          completeAgg.getRowType,
          input.getRowType,
          newGrouping,
          newAuxGrouping,
          isMerge = true)
    }
  }

}
