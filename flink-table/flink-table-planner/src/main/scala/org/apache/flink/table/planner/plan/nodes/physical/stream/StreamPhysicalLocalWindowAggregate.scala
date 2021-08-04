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

package org.apache.flink.table.planner.plan.nodes.physical.stream

import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.expressions.{PlannerNamedWindowProperty, PlannerSliceEnd, PlannerWindowReference}
import org.apache.flink.table.planner.plan.logical.{TimeAttributeWindowingStrategy, WindowAttachedWindowingStrategy, WindowingStrategy}
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecLocalWindowAggregate
import org.apache.flink.table.planner.plan.nodes.exec.{ExecNode, InputProperty}
import org.apache.flink.table.planner.plan.rules.physical.stream.TwoStageOptimizedWindowAggregateRule
import org.apache.flink.table.planner.plan.utils.WindowUtil.checkEmitConfiguration
import org.apache.flink.table.planner.plan.utils.{AggregateUtil, FlinkRelOptUtil, RelExplainUtil, WindowUtil}

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.rel.{RelNode, RelWriter, SingleRel}
import org.apache.calcite.util.Litmus

import java.util

import scala.collection.JavaConverters._

/**
 * Streaming local window aggregate physical node.
 *
 * <p>This is a local-aggregation node optimized from [[StreamPhysicalWindowAggregate]] after
 * [[TwoStageOptimizedWindowAggregateRule]] optimization.
 *
 * @see [[TwoStageOptimizedWindowAggregateRule]]
 * @see [[StreamPhysicalWindowAggregate]]
 */
class StreamPhysicalLocalWindowAggregate(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputRel: RelNode,
    val grouping: Array[Int],
    val aggCalls: Seq[AggregateCall],
    val windowing: WindowingStrategy)
  extends SingleRel(cluster, traitSet, inputRel)
  with StreamPhysicalRel {

  private lazy val aggInfoList = AggregateUtil.deriveStreamWindowAggregateInfoList(
    FlinkTypeFactory.toLogicalRowType(inputRel.getRowType),
    aggCalls,
    windowing.getWindow,
    isStateBackendDataViews = false)

  private lazy val endPropertyName = windowing match {
    case _: WindowAttachedWindowingStrategy => "window_end"
    case _: TimeAttributeWindowingStrategy => "slice_end"
  }

  override def isValid(litmus: Litmus, context: RelNode.Context): Boolean = {
    windowing match {
      case _: WindowAttachedWindowingStrategy | _: TimeAttributeWindowingStrategy =>
        // pass
      case _ =>
        return litmus.fail("StreamPhysicalLocalWindowAggregate should only accepts " +
          "WindowAttachedWindowingStrategy and TimeAttributeWindowingStrategy, " +
          s"but got ${windowing.getClass.getSimpleName}. " +
          "This should never happen, please open an issue.")
    }
    super.isValid(litmus, context)
  }

  override def requireWatermark: Boolean = windowing.isRowtime

  override def deriveRowType(): RelDataType = {
    WindowUtil.deriveLocalWindowAggregateRowType(
      aggInfoList,
      grouping,
      endPropertyName,
      inputRel.getRowType,
      getCluster.getTypeFactory.asInstanceOf[FlinkTypeFactory])
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    val inputRowType = getInput.getRowType
    val inputFieldNames = inputRowType.getFieldNames.asScala.toArray
    val windowRef = new PlannerWindowReference("w$", windowing.getTimeAttributeType)
    val namedProperties = Seq(
      new PlannerNamedWindowProperty(endPropertyName, new PlannerSliceEnd(windowRef)))
    super.explainTerms(pw)
      .itemIf("groupBy", RelExplainUtil.fieldToString(grouping, inputRowType), grouping.nonEmpty)
      .item("window", windowing.toSummaryString(inputFieldNames))
      .item("select", RelExplainUtil.streamWindowAggregationToString(
        inputRowType,
        getRowType,
        aggInfoList,
        grouping,
        namedProperties,
        isLocal = true))
  }

  override def copy(
      traitSet: RelTraitSet,
      inputs: util.List[RelNode]): RelNode = {
    new StreamPhysicalLocalWindowAggregate(
      cluster,
      traitSet,
      inputs.get(0),
      grouping,
      aggCalls,
      windowing
    )
  }

  override def translateToExecNode(): ExecNode[_] = {
    checkEmitConfiguration(FlinkRelOptUtil.getTableConfigFromContext(this))
    new StreamExecLocalWindowAggregate(
      grouping,
      aggCalls.toArray,
      windowing,
      InputProperty.DEFAULT,
      FlinkTypeFactory.toLogicalRowType(getRowType),
      getRelDetailedDescription
    )
  }
}
