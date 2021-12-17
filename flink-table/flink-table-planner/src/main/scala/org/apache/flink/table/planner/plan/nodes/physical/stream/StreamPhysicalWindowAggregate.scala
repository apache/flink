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
import org.apache.flink.table.planner.expressions.{PlannerNamedWindowProperty, PlannerProctimeAttribute, PlannerRowtimeAttribute, PlannerWindowEnd, PlannerWindowStart}
import org.apache.flink.table.planner.plan.logical.WindowingStrategy
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecWindowAggregate
import org.apache.flink.table.planner.plan.nodes.exec.{ExecNode, InputProperty}
import org.apache.flink.table.planner.plan.utils.{AggregateInfoList, AggregateUtil, FlinkRelOptUtil, RelExplainUtil, WindowUtil}
import org.apache.flink.table.planner.plan.utils.WindowUtil.checkEmitConfiguration
import org.apache.flink.table.types.logical.utils.LogicalTypeUtils
import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.{Aggregate, AggregateCall}
import org.apache.calcite.rel.{RelNode, RelWriter, SingleRel}
import org.apache.calcite.util.ImmutableBitSet

import java.util
import java.util.Collections

import scala.collection.JavaConverters._

/**
 * Streaming window aggregate physical node which will be translate to window aggregate operator.
 *
 * Note: The differences between [[StreamPhysicalWindowAggregate]] and
 * [[StreamPhysicalGroupWindowAggregate]] is that, [[StreamPhysicalWindowAggregate]] is translated
 * from window TVF syntax, but the other is from the legacy GROUP WINDOW FUNCTION syntax.
 * In the long future, [[StreamPhysicalGroupWindowAggregate]] will be dropped.
 */
class StreamPhysicalWindowAggregate(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputRel: RelNode,
    val grouping: Array[Int],
    val aggCalls: Seq[AggregateCall],
    val windowing: WindowingStrategy,
    val namedWindowProperties: Seq[PlannerNamedWindowProperty])
  extends SingleRel(cluster, traitSet, inputRel)
  with StreamPhysicalRel {

  lazy val aggInfoList: AggregateInfoList = AggregateUtil.deriveStreamWindowAggregateInfoList(
    FlinkTypeFactory.toLogicalRowType(inputRel.getRowType),
    aggCalls,
    windowing.getWindow,
    isStateBackendDataViews = true)

  override def requireWatermark: Boolean = windowing.isRowtime

  override def deriveRowType(): RelDataType = {
    WindowUtil.deriveWindowAggregateRowType(
      grouping,
      aggCalls,
      windowing,
      namedWindowProperties,
      inputRel.getRowType,
      cluster.getTypeFactory.asInstanceOf[FlinkTypeFactory])
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    val inputRowType = getInput.getRowType
    val inputFieldNames = inputRowType.getFieldNames.asScala.toArray
    super.explainTerms(pw)
      .itemIf("groupBy", RelExplainUtil.fieldToString(grouping, inputRowType), grouping.nonEmpty)
      .item("window", windowing.toSummaryString(inputFieldNames))
      .item("select", RelExplainUtil.streamWindowAggregationToString(
        inputRowType,
        getRowType,
        aggInfoList,
        grouping,
        namedWindowProperties))
  }

  override def copy(
      traitSet: RelTraitSet,
      inputs: util.List[RelNode]): RelNode = {
    new StreamPhysicalWindowAggregate(
      cluster,
      traitSet,
      inputs.get(0),
      grouping,
      aggCalls,
      windowing,
      namedWindowProperties
    )
  }

  override def translateToExecNode(): ExecNode[_] = {
    checkEmitConfiguration(FlinkRelOptUtil.getTableConfigFromContext(this))
    new StreamExecWindowAggregate(
      grouping,
      aggCalls.toArray,
      windowing,
      namedWindowProperties.toArray,
      InputProperty.DEFAULT,
      FlinkTypeFactory.toLogicalRowType(getRowType),
      getRelDetailedDescription
    )
  }
}
