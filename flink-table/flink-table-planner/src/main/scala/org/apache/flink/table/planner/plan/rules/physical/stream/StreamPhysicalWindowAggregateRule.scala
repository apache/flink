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

package org.apache.flink.table.planner.plan.rules.physical.stream

import org.apache.flink.table.api.TableException
import org.apache.flink.table.planner.expressions._
import org.apache.flink.table.planner.plan.`trait`.{FlinkRelDistribution, RelWindowProperties}
import org.apache.flink.table.planner.plan.logical.{WindowAttachedWindowingStrategy, WindowingStrategy}
import org.apache.flink.table.planner.plan.metadata.FlinkRelMetadataQuery
import org.apache.flink.table.planner.plan.nodes.FlinkConventions
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalAggregate
import org.apache.flink.table.planner.plan.nodes.physical.stream.{StreamPhysicalCalc, StreamPhysicalWindowAggregate}
import org.apache.flink.table.planner.plan.rules.physical.stream.StreamPhysicalWindowAggregateRule.{WINDOW_END, WINDOW_START, WINDOW_TIME}
import org.apache.flink.table.planner.plan.utils.PythonUtil.isPythonAggregate
import org.apache.flink.table.planner.plan.utils.WindowUtil

import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rel.core.Aggregate.Group
import org.apache.calcite.rex.{RexInputRef, RexProgram}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

/**
 * Rule to convert a [[FlinkLogicalAggregate]] into a [[StreamPhysicalWindowAggregate]].
 */
class StreamPhysicalWindowAggregateRule
  extends ConverterRule(
    classOf[FlinkLogicalAggregate],
    FlinkConventions.LOGICAL,
    FlinkConventions.STREAM_PHYSICAL,
    "StreamPhysicalWindowAggregateRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val agg: FlinkLogicalAggregate = call.rel(0)

    // check if we have grouping sets
    if (agg.getGroupType != Group.SIMPLE || agg.indicator) {
      throw new TableException("GROUPING SETS are currently not supported.")
    }

    // the aggregate calls shouldn't contain python aggregates
    if (agg.getAggCallList.asScala.exists(isPythonAggregate(_))) {
      return false
    }

    val fmq = FlinkRelMetadataQuery.reuseOrCreate(call.getMetadataQuery)
    val windowProperties = fmq.getRelWindowProperties(agg.getInput)
    val grouping = agg.getGroupSet
    WindowUtil.groupingContainsWindowStartEnd(grouping, windowProperties)
  }

  override def convert(rel: RelNode): RelNode = {
    val agg: FlinkLogicalAggregate = rel.asInstanceOf[FlinkLogicalAggregate]
    val fmq = FlinkRelMetadataQuery.reuseOrCreate(rel.getCluster.getMetadataQuery)
    val relWindowProperties = fmq.getRelWindowProperties(agg.getInput)
    val grouping = agg.getGroupSet
    // we have check there is only one start and end in groupingContainsWindowStartEnd()
    val startColumns = relWindowProperties.getWindowStartColumns.intersect(grouping)
    val endColumns = relWindowProperties.getWindowEndColumns.intersect(grouping)
    val timeColumns = relWindowProperties.getWindowTimeColumns.intersect(grouping)
    val newGrouping = grouping.except(startColumns).except(endColumns).except(timeColumns)

    // step-1: build window aggregate node
    val windowAgg = buildWindowAggregateNode(
      agg,
      newGrouping.toArray,
      startColumns.toArray,
      endColumns.toArray,
      timeColumns.toArray,
      relWindowProperties)

    // step-2: build projection on window aggregate to fix the fields mapping
    buildCalcProjection(
      grouping.toArray,
      newGrouping.toArray,
      startColumns.toArray,
      endColumns.toArray,
      timeColumns.toArray,
      agg,
      windowAgg
    )
  }

  private def buildWindowAggregateNode(
      agg: FlinkLogicalAggregate,
      newGrouping: Array[Int],
      startColumns: Array[Int],
      endColumns: Array[Int],
      timeColumns: Array[Int],
      relWindowProperties: RelWindowProperties): StreamPhysicalWindowAggregate = {
    val requiredDistribution = if (!newGrouping.isEmpty) {
      FlinkRelDistribution.hash(newGrouping, requireStrict = true)
    } else {
      FlinkRelDistribution.SINGLETON
    }

    val requiredTraitSet = agg.getCluster.getPlanner.emptyTraitSet()
      .replace(requiredDistribution)
      .replace(FlinkConventions.STREAM_PHYSICAL)
    val providedTraitSet = agg.getTraitSet.replace(FlinkConventions.STREAM_PHYSICAL)
    val newInput: RelNode = RelOptRule.convert(agg.getInput, requiredTraitSet)

    val windowingStrategy = new WindowAttachedWindowingStrategy(
      relWindowProperties.getWindowSpec,
      relWindowProperties.getTimeAttributeType,
      startColumns.head,
      endColumns.head)

    val windowProperties = createPlannerNamedWindowProperties(
      windowingStrategy,
      startColumns,
      endColumns,
      timeColumns)

    new StreamPhysicalWindowAggregate(
      agg.getCluster,
      providedTraitSet,
      newInput,
      newGrouping,
      agg.getAggCallList.asScala,
      windowingStrategy,
      windowProperties)
  }

  private def buildCalcProjection(
      grouping: Array[Int],
      newGrouping: Array[Int],
      startColumns: Array[Int],
      endColumns: Array[Int],
      timeColumns: Array[Int],
      agg: FlinkLogicalAggregate,
      windowAgg: StreamPhysicalWindowAggregate): StreamPhysicalCalc = {
    val projectionMapping = getProjectionMapping(
      grouping,
      newGrouping,
      startColumns,
      endColumns,
      timeColumns,
      windowAgg.namedWindowProperties,
      agg.getAggCallList.size())
    val projectExprs = projectionMapping.map(RexInputRef.of(_, windowAgg.getRowType))
    val calcProgram = RexProgram.create(
      windowAgg.getRowType,
      projectExprs.toList.asJava,
      null, // no filter
      agg.getRowType,
      agg.getCluster.getRexBuilder
    )
    val traitSet: RelTraitSet = agg.getTraitSet.replace(FlinkConventions.STREAM_PHYSICAL)
    val newInput: RelNode = RelOptRule.convert(windowAgg, FlinkConventions.STREAM_PHYSICAL)

    new StreamPhysicalCalc(
      agg.getCluster,
      traitSet,
      newInput,
      calcProgram,
      calcProgram.getOutputRowType)
  }

  private def createPlannerNamedWindowProperties(
      windowingStrategy: WindowingStrategy,
      startColumns: Array[Int],
      endColumns: Array[Int],
      timeColumns: Array[Int]): Seq[PlannerNamedWindowProperty] = {
    val windowProperties = ArrayBuffer[PlannerNamedWindowProperty]()
    val windowRef = new PlannerWindowReference("w$", windowingStrategy.getTimeAttributeType)
    if (!startColumns.isEmpty) {
      windowProperties +=
        new PlannerNamedWindowProperty(WINDOW_START, new PlannerWindowStart(windowRef))
    }
    if (!endColumns.isEmpty) {
      windowProperties +=
        new PlannerNamedWindowProperty(WINDOW_END, new PlannerWindowEnd(windowRef))
    }
    if (!timeColumns.isEmpty) {
      val property = if (windowingStrategy.isRowtime) {
        new PlannerRowtimeAttribute(windowRef)
      } else {
        new PlannerProctimeAttribute(windowRef)
      }
      windowProperties += new PlannerNamedWindowProperty(WINDOW_TIME, property)
    }
    windowProperties
  }

  private def getProjectionMapping(
      grouping: Array[Int],
      newGrouping: Array[Int],
      startColumns: Array[Int],
      endColumns: Array[Int],
      timeColumns: Array[Int],
      windowProperties: Seq[PlannerNamedWindowProperty],
      aggCount: Int): Array[Int] = {
    val (startPos, endPos, timePos) = windowPropertyPositions(
      windowProperties,
      newGrouping,
      aggCount)
    val keyMapping = grouping.map { key =>
      if (newGrouping.contains(key)) {
        newGrouping.indexOf(key)
      } else if (startColumns.contains(key)) {
        startPos
      } else if (endColumns.contains(key)) {
        endPos
      } else if (timeColumns.contains(key)) {
        timePos
      } else {
        throw new IllegalArgumentException(
          s"Can't find grouping key $$$key, this should never happen.")
      }
    }
    val aggMapping = (0 until aggCount).map { aggIndex =>
      newGrouping.length + aggIndex
    }
    keyMapping ++ aggMapping
  }

  private def windowPropertyPositions(
      windowProperties: Seq[PlannerNamedWindowProperty],
      newGrouping: Array[Int],
      aggCount: Int): (Int, Int, Int) = {
    val windowPropsIndexOffset = newGrouping.length + aggCount
    var startPos = -1
    var endPos = -1
    var timePos = -1
    windowProperties.zipWithIndex.foreach { case (p, idx) =>
      if (WINDOW_START.equals(p.getName)) {
        startPos = windowPropsIndexOffset + idx
      } else if (WINDOW_END.equals(p.getName)) {
        endPos = windowPropsIndexOffset + idx
      } else if (WINDOW_TIME.equals(p.getName)) {
        timePos = windowPropsIndexOffset + idx
      }
    }
    (startPos, endPos, timePos)
  }
}

object StreamPhysicalWindowAggregateRule {
  val INSTANCE = new StreamPhysicalWindowAggregateRule

  private val WINDOW_START: String = "window_start"
  private val WINDOW_END: String = "window_end"
  private val WINDOW_TIME: String = "window_time"
}
