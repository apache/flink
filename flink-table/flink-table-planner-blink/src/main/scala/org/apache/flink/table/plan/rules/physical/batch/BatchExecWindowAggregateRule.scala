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

import org.apache.flink.table.`type`.TypeConverters.createInternalTypeFromTypeInfo
import org.apache.flink.table.`type`.{InternalType, InternalTypes}
import org.apache.flink.table.api.{TableConfig, TableException}
import org.apache.flink.table.calcite.{FlinkContext, FlinkTypeFactory}
import org.apache.flink.table.functions.aggfunctions.DeclarativeAggregateFunction
import org.apache.flink.table.functions.{AggregateFunction, UserDefinedFunction}
import org.apache.flink.table.plan.`trait`.FlinkRelDistribution
import org.apache.flink.table.plan.logical.{LogicalWindow, SlidingGroupWindow, TumblingGroupWindow}
import org.apache.flink.table.plan.nodes.FlinkConventions
import org.apache.flink.table.plan.nodes.logical.FlinkLogicalWindowAggregate
import org.apache.flink.table.plan.nodes.physical.batch.{BatchExecHashWindowAggregate, BatchExecLocalHashWindowAggregate, BatchExecLocalSortWindowAggregate, BatchExecSortWindowAggregate}
import org.apache.flink.table.plan.util.AggregateUtil
import org.apache.flink.table.typeutils.TimeIntervalTypeInfo.INTERVAL_MILLIS

import org.apache.calcite.plan.RelOptRule._
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.Aggregate.Group
import org.apache.calcite.rel.core.{Aggregate, AggregateCall}
import org.apache.calcite.rel.{RelCollations, RelNode}
import org.apache.calcite.sql.`type`.SqlTypeName
import org.apache.commons.math3.util.ArithmeticUtils

import scala.collection.JavaConversions._

class BatchExecWindowAggregateRule
    extends RelOptRule(
      operand(classOf[FlinkLogicalWindowAggregate],
        operand(classOf[RelNode], any)),
      "BatchExecWindowAggregateRule")
        with BatchExecAggRuleBase {

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

    val (auxGroupSet, aggCallsWithoutAuxGroupCalls) = AggregateUtil.checkAndSplitAggCalls(agg)

    val (_, aggBufferTypes, aggregates) = AggregateUtil.transformToBatchAggregateFunctions(
      aggCallsWithoutAuxGroupCalls, input.getRowType)
    val aggCallToAggFunction = aggCallsWithoutAuxGroupCalls.zip(aggregates)
    val internalAggBufferTypes = aggBufferTypes.map(_.map(createInternalTypeFromTypeInfo))

    window match {
      case TumblingGroupWindow(_, _, size) if size.getType == INTERVAL_MILLIS =>
        val sizeInLong = size.getValue.asInstanceOf[java.lang.Long]
        transformTimeSlidingWindow(
          call,
          input,
          agg,
          window.asInstanceOf[TumblingGroupWindow],
          auxGroupSet,
          aggCallToAggFunction,
          internalAggBufferTypes,
          useHashExec(agg),
          enableAssignPane = false,
          supportLocalWindowAgg(call, tableConfig, aggregates, sizeInLong, sizeInLong))

      case SlidingGroupWindow(_, _, size, slide) if size.getType == INTERVAL_MILLIS =>
        val (sizeInLong, slideInLong) = (
            size.getValue.asInstanceOf[java.lang.Long],
            slide.getValue.asInstanceOf[java.lang.Long])
        transformTimeSlidingWindow(
          call,
          input,
          agg,
          window.asInstanceOf[SlidingGroupWindow],
          auxGroupSet,
          aggCallToAggFunction,
          internalAggBufferTypes,
          useHashExec(agg),
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
    val inputTimeFieldIndex = AggregateUtil.timeFieldIndex(
      input.getRowType, call.builder(), window.timeAttribute)
    val inputTimeFieldType = agg.getInput.getRowType.getFieldList.get(inputTimeFieldIndex).getType
    val inputTimeIsDate = inputTimeFieldType.getSqlTypeName == SqlTypeName.DATE
    // local-agg output order: groupset | assignTs | aucGroupSet | aggCalls
    val newInputTimeFieldIndexFromLocal = groupSet.length

    val config = input.getCluster.getPlanner.getContext.asInstanceOf[FlinkContext].getTableConfig
    if (!isEnforceOnePhaseAgg(config) && supportLocalAgg) {
      val windowType = if (inputTimeIsDate) InternalTypes.INT else InternalTypes.LONG
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
          inputTimeFieldIndex,
          inputTimeIsDate,
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
          groupSet :+ inputTimeFieldIndex))

        val newLocalInput = RelOptRule.convert(input, localRequiredTraitSet)
        val localProvidedTraitSet = localRequiredTraitSet

        new BatchExecLocalSortWindowAggregate(
          window,
          inputTimeFieldIndex,
          inputTimeIsDate,
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
          newInputTimeFieldIndexFromLocal,
          inputTimeIsDate,
          agg.getNamedProperties,
          agg.getCluster,
          call.builder(),
          aggProvidedTraitSet,
          newGlobalAggInput,
          aggCallToAggFunction,
          agg.getRowType,
          newGlobalAggInput.getRowType,
          input.getRowType,
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
          newInputTimeFieldIndexFromLocal,
          inputTimeIsDate,
          agg.getNamedProperties,
          agg.getCluster,
          call.builder(),
          aggProvidedTraitSet,
          newGlobalAggInput,
          aggCallToAggFunction,
          agg.getRowType,
          newGlobalAggInput.getRowType,
          input.getRowType,
          groupSet.indices.toArray,
          // auxGroupSet starts from `size of groupSet + 1(assignTs)`
          (groupSet.length + 1 until groupSet.length + 1 + auxGroupSet.length).toArray,
          enableAssignPane,
          isMerge = true)
      }

      call.transformTo(globalAgg)
    }
    // disable one-phase agg if prefer two-phase agg
    if (!isEnforceTwoPhaseAgg(config) || !supportLocalAgg) {
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
          inputTimeFieldIndex,
          inputTimeIsDate,
          agg.getNamedProperties,
          agg.getCluster,
          call.builder(),
          aggProvidedTraitSet,
          newInput,
          aggCallToAggFunction,
          agg.getRowType,
          newInput.getRowType,
          newInput.getRowType,
          groupSet,
          auxGroupSet,
          enableAssignPane,
          isMerge = false)
      } else {
        // sort by grouping keys and time field in ascending direction
        requiredTraitSet = requiredTraitSet.replace(createRelCollation(
          groupSet :+ inputTimeFieldIndex))
        val newInput = RelOptRule.convert(input, requiredTraitSet)

        new BatchExecSortWindowAggregate(
          window,
          inputTimeFieldIndex,
          inputTimeIsDate,
          agg.getNamedProperties,
          agg.getCluster,
          call.builder(),
          aggProvidedTraitSet,
          newInput,
          aggCallToAggFunction,
          agg.getRowType,
          newInput.getRowType,
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

  private def useHashExec(agg: Aggregate): Boolean = {
    isAggBufferFixedLength(agg)
  }

  private def isEffectiveAssigningPane(windowSize: Long, slideSize: Long) =
    ArithmeticUtils.gcd(windowSize, slideSize) > 1

  def inferLocalWindowAggType(
      enableAssignPane: Boolean,
      inputType: RelDataType,
      agg: Aggregate,
      groupSet: Array[Int],
      auxGroupSet: Array[Int],
      windowType: InternalType,
      aggregates: Array[UserDefinedFunction],
      aggBufferTypes: Array[Array[InternalType]]): RelDataType = {
    val aggNames = agg.getNamedAggCalls.map(_.right)

    val aggBufferFieldNames = new Array[Array[String]](aggregates.length)
    var index = -1
    aggregates.zipWithIndex.foreach{ case (udf, aggIndex) =>
      aggBufferFieldNames(aggIndex) = udf match {
        case _: AggregateFunction[_, _] =>
          Array(aggNames(aggIndex))
        case agf: DeclarativeAggregateFunction =>
          agf.aggBufferAttributes.map { attr =>
            index += 1
            s"${attr.getName}$$$index"}
        case _: UserDefinedFunction =>
          throw new TableException(s"Don't get localAgg merge name")
      }
    }

    // local win-agg output order: groupSet + assignTs + auxGroupSet + aggCalls
    val typeFactory = agg.getCluster.getTypeFactory.asInstanceOf[FlinkTypeFactory]
    val aggBufferSqlTypes = aggBufferTypes.flatten.map { t =>
      val nullable = !FlinkTypeFactory.isTimeIndicatorType(t)
      typeFactory.createTypeFromInternalType(t, nullable)
    }

    val localAggFieldTypes = (
        groupSet.map(inputType.getFieldList.get(_).getType) ++ // groupSet
            // assignTs
            Array(typeFactory.createTypeFromInternalType(windowType, isNullable = true)) ++
            auxGroupSet.map(inputType.getFieldList.get(_).getType) ++ // auxGroupSet
            aggBufferSqlTypes // aggCalls
        ).toList

    val assignTsFieldName = if (enableAssignPane) "assignedPane$" else "assignedWindow$"
    val localAggFieldNames = (
        groupSet.map(inputType.getFieldList.get(_).getName) ++ // groupSet
            Array(assignTsFieldName) ++ // assignTs
            auxGroupSet.map(inputType.getFieldList.get(_).getName) ++ // auxGroupSet
            aggBufferFieldNames.flatten.toArray[String] // aggCalls
        ).toList

    typeFactory.createStructType(localAggFieldTypes, localAggFieldNames)
  }
}

object BatchExecWindowAggregateRule {
  val INSTANCE: RelOptRule = new BatchExecWindowAggregateRule
}
