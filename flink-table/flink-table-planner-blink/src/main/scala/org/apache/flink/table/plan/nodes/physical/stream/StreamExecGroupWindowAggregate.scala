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

package org.apache.flink.table.plan.nodes.physical.stream

import org.apache.flink.table.api.TableConfigOptions
import org.apache.flink.table.calcite.FlinkRelBuilder.NamedWindowProperty
import org.apache.flink.table.plan.logical._
import org.apache.flink.table.plan.util.AggregateUtil.{isRowtimeIndicatorType, isTimeIntervalType}
import org.apache.flink.table.plan.util.{RelExplainUtil, WindowEmitStrategy}

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.rel.{RelNode, RelWriter, SingleRel}

/**
  * Streaming group window aggregate physical node which will be translate to window operator.
  *
  * If requirements satisfied, it will be translated into minibatch window operator, otherwise,
  * will be translated into general window operator.
  *
  * The requirements including:
  * 1. [[TableConfigOptions.SQL_EXEC_MINI_BATCH_WINDOW_ENABLED]] is enabled
  * 2. It's an aligned window, e.g. tumbling windows, sliding windows with size is an integral
  *     multiple of slide. So that session window is not supported.
  * 3. Only support event time windows, not processing time windows or count windows.
  * 4. No early/late fire is configured.
  * 5. All the aggregate function should support merge.
  */
class StreamExecGroupWindowAggregate(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputRel: RelNode,
    outputRowType: RelDataType,
    inputRowType: RelDataType,
    grouping: Array[Int],
    val aggCalls: Seq[AggregateCall],
    val window: LogicalWindow,
    namedProperties: Seq[NamedWindowProperty],
    inputTimestampIndex: Int,
    val emitStrategy: WindowEmitStrategy)
  extends SingleRel(cluster, traitSet, inputRel)
   with StreamPhysicalRel {

  override def producesUpdates: Boolean = emitStrategy.produceUpdates

  override def consumesRetractions = true

  override def needsUpdatesAsRetraction(input: RelNode) = true

  override def producesRetractions: Boolean = false

  override def requireWatermark: Boolean = window match {
    case TumblingGroupWindow(_, timeField, size)
      if isRowtimeIndicatorType(timeField.getResultType) && isTimeIntervalType(size.getType) => true
    case SlidingGroupWindow(_, timeField, size, _)
      if isRowtimeIndicatorType(timeField.getResultType) && isTimeIntervalType(size.getType) => true
    case SessionGroupWindow(_, timeField, _)
      if isRowtimeIndicatorType(timeField.getResultType) => true
    case _ => false
  }

  def getGroupings: Array[Int] = grouping

  def getWindowProperties: Seq[NamedWindowProperty] = namedProperties

  override def deriveRowType(): RelDataType = outputRowType

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    new StreamExecGroupWindowAggregate(
      cluster,
      traitSet,
      inputs.get(0),
      outputRowType,
      inputRowType,
      grouping,
      aggCalls,
      window,
      namedProperties,
      inputTimestampIndex,
      emitStrategy)
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    super.explainTerms(pw)
      .itemIf("groupBy", RelExplainUtil.fieldToString(grouping, inputRowType), grouping.nonEmpty)
      .item("window", window)
      .itemIf("properties", namedProperties.map(_.name).mkString(", "), namedProperties.nonEmpty)
      .item("select", RelExplainUtil.streamWindowAggregationToString(
        inputRowType,
        grouping,
        outputRowType,
        aggCalls,
        namedProperties))
      .itemIf("emit", emitStrategy, !emitStrategy.toString.isEmpty)
  }

}
