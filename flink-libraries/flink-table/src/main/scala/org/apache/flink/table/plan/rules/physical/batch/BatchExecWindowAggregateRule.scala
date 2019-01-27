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

import org.apache.calcite.plan.RelOptRule._
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.calcite.rel.{RelCollations, RelNode}
import org.apache.calcite.rel.core.Aggregate.Group
import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.sql.`type`.SqlTypeName
import org.apache.commons.math3.util.ArithmeticUtils
import org.apache.flink.table.api.functions.UserDefinedFunction
import org.apache.flink.table.api.types.{DataTypes, IntType, InternalType, LongType}
import org.apache.flink.table.api.{TableConfig, TableException}
import org.apache.flink.table.expressions.ExpressionUtils._
import org.apache.flink.table.plan.`trait`.FlinkRelDistribution
import org.apache.flink.table.plan.logical.{LogicalWindow, SlidingGroupWindow, TumblingGroupWindow}
import org.apache.flink.table.plan.nodes.FlinkConventions
import org.apache.flink.table.plan.nodes.physical.batch.{BatchExecHashWindowAggregate, BatchExecLocalHashWindowAggregate, BatchExecLocalSortWindowAggregate, BatchExecSortWindowAggregate}
import org.apache.flink.table.plan.nodes.logical.FlinkLogicalWindowAggregate
import org.apache.flink.table.plan.util.{AggregateUtil, FlinkRelOptUtil}
import org.apache.flink.table.plan.util.AggregateUtil._

import scala.collection.JavaConversions._

class BatchExecWindowAggregateRule
  extends RelOptRule(
    operand(classOf[FlinkLogicalWindowAggregate],
      operand(classOf[RelNode], any)),
    "BatchExecWindowAggregateRule")
  with BaseBatchExecAggRule {

  override def matches(call: RelOptRuleCall): Boolean = {
    val agg: FlinkLogicalWindowAggregate = call.rel(0).asInstanceOf[FlinkLogicalWindowAggregate]

    // check if we have distinct aggregates
    val distinctAggs = agg.containsDistinctCall()
    if (distinctAggs) {
      throw new TableException("DISTINCT aggregates are currently not supported.")
    }

    // check if we have grouping sets
    val groupSets = agg.getGroupType != Group.SIMPLE
    if (groupSets || agg.indicator) {
      throw new TableException("GROUPING SETS are currently not supported.")
    }

    !distinctAggs && !groupSets && !agg.indicator
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val tableConfig = call.getPlanner.getContext.unwrap(classOf[TableConfig])
    val input = call.rels(1)
    val agg = call.rels(0).asInstanceOf[FlinkLogicalWindowAggregate]
    val window = agg.getWindow

    val (auxGroupSet, aggCallsWithoutAuxGroupCalls) = FlinkRelOptUtil.checkAndSplitAggCalls(agg)

    val (_, aggBufferTypes, aggregates) = AggregateUtil.transformToBatchAggregateFunctions(
      aggCallsWithoutAuxGroupCalls, input.getRowType)
    val aggCallToAggFunction = aggCallsWithoutAuxGroupCalls.zip(aggregates)
    val internalAggBufferTypes = aggBufferTypes.map(_.map(_.toInternalType))

    window match {
      case TumblingGroupWindow(_, _, size) if isTimeIntervalLiteral(size) =>
        val sizeInLong = asLong(size)
        transformTimeSlidingWindow(
          call,
          input,
          agg,
          window.asInstanceOf[TumblingGroupWindow],
          auxGroupSet,
          aggCallToAggFunction,
          internalAggBufferTypes,
          useHashExec(call),
          enableAssignPane = false,
          supportLocalWindowAgg(call, tableConfig, aggregates, sizeInLong, sizeInLong))

      case SlidingGroupWindow(_, _, size, slide) if isTimeIntervalLiteral(size) =>
        val (sizeInLong, slideInLong) = (asLong(size), asLong(slide))
        transformTimeSlidingWindow(
          call,
          input,
          agg,
          window.asInstanceOf[SlidingGroupWindow],
          auxGroupSet,
          aggCallToAggFunction,
          internalAggBufferTypes,
          useHashExec(call),
          useAssignPane(aggregates, sizeInLong, slideInLong),
          supportLocalWindowAgg(call, tableConfig, aggregates, sizeInLong, slideInLong))

      case _ => // sliding & tumbling count window and session window not supported
        throw new UnsupportedOperationException(s"Window $window is not supported right now.")
    }
  }

  private def transformTimeSlidingWindow(
      call: RelOptRuleCall,
      input: RelNode,
      agg: FlinkLogicalWindowAggregate,
      window: LogicalWindow,
      auxGroupSet: Array[Int],
      aggCallToAggFunction: Seq[(AggregateCall, UserDefinedFunction)],
      aggBufferTypes: Array[Array[InternalType]],
      preferHashExec: Boolean,
      enableAssignPane: Boolean,
      supportLocalAgg: Boolean): Unit = {
    val groupSet = agg.getGroupSet.toArray
    val aggregates = aggCallToAggFunction.map(_._2).toArray

    // TODO aggregate include projection now, so do not provide new trait will be safe
    val aggProvidedTraitSet = input.getTraitSet.replace(FlinkConventions.BATCH_PHYSICAL)
    val inputTimestampIndex = timeFieldIndex(input.getRowType, call.builder(), window.timeAttribute)
    val inputTimestampType = agg.getInput.getRowType.getFieldList.get(inputTimestampIndex).getType
    val isDate = inputTimestampType.getSqlTypeName == SqlTypeName.DATE
    // local-agg output order: groupset | assignTs | aucGroupSet | aggCalls
    val newInputTimestampIndexFromLocal = groupSet.length

    if (!isEnforceOnePhaseAgg(call) && supportLocalAgg) {
      val windowType = if (isDate) IntType.INSTANCE else LongType.INSTANCE
      // local
      var localRequiredTraitSet = input.getTraitSet.replace(FlinkConventions.BATCH_PHYSICAL)
      // local win-agg output order: groupSet + assignTs + auxGroupSet + aggCalls
      val localAggRelType = inferLocalWindowAggType(enableAssignPane, input.getRowType, agg,
        groupSet, auxGroupSet, windowType, aggregates, aggBufferTypes)

      val localAgg = if (preferHashExec) {
        // hash
        val newLocalInput = RelOptRule.convert(input, localRequiredTraitSet)
        val localProvidedTraitSet = localRequiredTraitSet

        new BatchExecLocalHashWindowAggregate(
          window,
          inputTimestampIndex,
          inputTimestampType,
          agg.getNamedProperties,
          agg.getCluster,
          call.builder(),
          localProvidedTraitSet,
          newLocalInput,
          aggCallToAggFunction,
          localAggRelType,
          newLocalInput.getRowType,
          groupSet,
          auxGroupSet,
          enableAssignPane)
      } else {
        // sort
        localRequiredTraitSet = localRequiredTraitSet.replace(createRelCollation(
          groupSet :+ inputTimestampIndex))

        val newLocalInput = RelOptRule.convert(input, localRequiredTraitSet)
        val localProvidedTraitSet = localRequiredTraitSet

        new BatchExecLocalSortWindowAggregate(
          window,
          inputTimestampIndex,
          inputTimestampType,
          agg.getNamedProperties,
          agg.getCluster,
          call.builder(),
          localProvidedTraitSet,
          newLocalInput,
          aggCallToAggFunction,
          localAggRelType,
          newLocalInput.getRowType,
          groupSet,
          auxGroupSet,
          enableAssignPane)
      }

      // global
      var globalRequiredTraitSet = localAgg.getTraitSet
      // distribute by grouping keys or single plus assigned pane
      val distributionFields = if (agg.getGroupCount != 0) {
        // global agg should use groupSet's indices as distribution fields
        val globalGroupSet = groupSet.indices
        FlinkRelDistribution.hash(globalGroupSet.map(Integer.valueOf), requireStrict = false)
      } else {
        FlinkRelDistribution.SINGLETON
      }
      globalRequiredTraitSet = globalRequiredTraitSet.replace(distributionFields)

      val globalAgg = if (preferHashExec) {
        // hash
        val newGlobalAggInput = RelOptRule.convert(localAgg, globalRequiredTraitSet)

        new BatchExecHashWindowAggregate(
          window,
          newInputTimestampIndexFromLocal,
          inputTimestampType,
          agg.getNamedProperties,
          agg.getCluster,
          call.builder(),
          aggProvidedTraitSet,
          newGlobalAggInput,
          aggCallToAggFunction,
          agg.getRowType,
          newGlobalAggInput.getRowType,
          groupSet.indices.toArray,
          // auxGroupSet starts from `size of groupSet + 1(assignTs)`
          (groupSet.length + 1 until groupSet.length + 1 + auxGroupSet.length).toArray,
          enableAssignPane,
          isMerge = true)
      } else {
        // sort
        globalRequiredTraitSet = globalRequiredTraitSet
            .replace(RelCollations.EMPTY)
            .replace(createRelCollation(groupSet.indices.toArray :+ groupSet.length))
        val newGlobalAggInput = RelOptRule.convert(localAgg, globalRequiredTraitSet)

        new BatchExecSortWindowAggregate(
          window,
          newInputTimestampIndexFromLocal,
          inputTimestampType,
          agg.getNamedProperties,
          agg.getCluster,
          call.builder(),
          aggProvidedTraitSet,
          newGlobalAggInput,
          aggCallToAggFunction,
          agg.getRowType,
          newGlobalAggInput.getRowType,
          groupSet.indices.toArray,
          // auxGroupSet starts from `size of groupSet + 1(assignTs)`
          (groupSet.length + 1 until groupSet.length + 1 + auxGroupSet.length).toArray,
          enableAssignPane,
          isMerge = true)
      }

      call.transformTo(globalAgg)
    }
    // disable one-phase agg if prefer two-phase agg
    if (!isEnforceTwoPhaseAgg(call) || !supportLocalAgg) {
      var requiredTraitSet = agg.getTraitSet.replace(FlinkConventions.BATCH_PHYSICAL)
      // distribute by grouping keys
      requiredTraitSet = if (agg.getGroupCount != 0) {
        requiredTraitSet.replace(
          FlinkRelDistribution.hash(groupSet.map(Integer.valueOf).toList, requireStrict = false))
      } else {
        requiredTraitSet.replace(FlinkRelDistribution.SINGLETON)
      }

      val windowAgg = if (preferHashExec && !enableAssignPane) {
        // case 1: Tumbling window, Sliding window windowSize >= slideSize
        // case 2: Sliding window without pane optimization
        val newInput = RelOptRule.convert(input, requiredTraitSet)

        new BatchExecHashWindowAggregate(
          window,
          inputTimestampIndex,
          inputTimestampType,
          agg.getNamedProperties,
          agg.getCluster,
          call.builder(),
          aggProvidedTraitSet,
          newInput,
          aggCallToAggFunction,
          agg.getRowType,
          newInput.getRowType,
          groupSet,
          auxGroupSet,
          enableAssignPane,
          isMerge = false)
      } else {
        // sort by grouping keys and time field in ascending direction
        requiredTraitSet = requiredTraitSet.replace(createRelCollation(
          groupSet :+ inputTimestampIndex))
        val newInput = RelOptRule.convert(input, requiredTraitSet)

        new BatchExecSortWindowAggregate(
          window,
          inputTimestampIndex,
          inputTimestampType,
          agg.getNamedProperties,
          agg.getCluster,
          call.builder(),
          aggProvidedTraitSet,
          newInput,
          aggCallToAggFunction,
          agg.getRowType,
          newInput.getRowType,
          groupSet,
          auxGroupSet,
          enableAssignPane,
          isMerge = false)
      }

      call.transformTo(windowAgg)
    }
  }

  /**
    * Return true when sliding window with slideSize < windowSize && gcd(windowSize, slideSize) > 1.
    * Otherwise return false, including the cases of tumbling window,
    * sliding window with slideSize >= windowSize and
    * sliding window with slideSize < windowSize but gcd(windowSize, slideSize) == 1.
    */
  private def useAssignPane(
      aggregateList: Array[UserDefinedFunction],
      windowSize: Long,
      slideSize: Long): Boolean = {
    doAllSupportMerge(aggregateList) &&
        slideSize < windowSize && isEffectiveAssigningPane(windowSize, slideSize)
  }

  /**
    * In the case of sliding window without the optimization of assigning pane which means
    * slideSize < windowSize && ArithmeticUtils.gcd(windowSize, slideSize) == 1, we will disable the
    * local aggregate.
    * Otherwise, we use the same way as the group aggregate to make the decision whether
    * to use a local aggregate or not.
    */
  private def supportLocalWindowAgg(
      call: RelOptRuleCall,
      tableConfig: TableConfig,
      aggregateList: Array[UserDefinedFunction],
      windowSize: Long,
      slideSize: Long): Boolean = {
    if (slideSize < windowSize && !isEffectiveAssigningPane(windowSize, slideSize)) {
      false
    } else {
      doAllSupportMerge(aggregateList)
    }
  }

  private def useHashExec(call: RelOptRuleCall): Boolean = {
    isAggBufferFixedLength(call)
  }

  private def isEffectiveAssigningPane(windowSize: Long, slideSize: Long) =
    ArithmeticUtils.gcd(windowSize, slideSize) > 1
}

object BatchExecWindowAggregateRule {
  val INSTANCE: RelOptRule = new BatchExecWindowAggregateRule
}
