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

import org.apache.flink.table.planner.expressions.PlannerNamedWindowProperty
import org.apache.flink.table.planner.plan.logical._
import org.apache.flink.table.planner.plan.utils.AggregateUtil._
import org.apache.flink.table.planner.plan.utils._

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.rel.{RelNode, RelWriter, SingleRel}

/**
 * Base streaming group window aggregate physical node for either aggregate or table aggregate.
 */
abstract class StreamPhysicalGroupWindowAggregateBase(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputRel: RelNode,
    outputRowType: RelDataType,
    val grouping: Array[Int],
    val aggCalls: Seq[AggregateCall],
    val window: LogicalWindow,
    val namedWindowProperties: Seq[PlannerNamedWindowProperty],
    val emitStrategy: WindowEmitStrategy)
  extends SingleRel(cluster, traitSet, inputRel)
  with StreamPhysicalRel {

  override def requireWatermark: Boolean = window match {
    case TumblingGroupWindow(_, timeField, size)
      if isRowtimeAttribute(timeField) && hasTimeIntervalType(size) => true
    case SlidingGroupWindow(_, timeField, size, _)
      if isRowtimeAttribute(timeField) && hasTimeIntervalType(size) => true
    case SessionGroupWindow(_, timeField, _)
      if isRowtimeAttribute(timeField) => true
    case _ => false
  }

  override def deriveRowType(): RelDataType = outputRowType

  override def explainTerms(pw: RelWriter): RelWriter = {
    val inputRowType = getInput.getRowType
    super.explainTerms(pw)
      .itemIf("groupBy", RelExplainUtil.fieldToString(grouping, inputRowType), grouping.nonEmpty)
      .item("window", window)
      .itemIf("properties", namedWindowProperties.map(_.getName).mkString(", "),
        namedWindowProperties.nonEmpty)
      .item("select", RelExplainUtil.legacyStreamWindowAggregationToString(
        inputRowType,
        grouping,
        outputRowType,
        aggCalls,
        namedWindowProperties))
      .itemIf("emit", emitStrategy, !emitStrategy.toString.isEmpty)
  }
}
