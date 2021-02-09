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

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.{Aggregate, AggregateCall}
import org.apache.calcite.rel.{RelNode, RelWriter, SingleRel}
import org.apache.calcite.util.ImmutableBitSet
import org.apache.flink.table.planner.calcite.FlinkRelBuilder.PlannerNamedWindowProperty
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.expressions.{PlannerProctimeAttribute, PlannerRowtimeAttribute, PlannerWindowEnd, PlannerWindowStart}
import org.apache.flink.table.planner.plan.logical.WindowingStrategy
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode
import org.apache.flink.table.planner.plan.utils.RelExplainUtil
import org.apache.flink.table.types.logical.utils.LogicalTypeUtils

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

  override def requireWatermark: Boolean = windowing.isRowtime

  override def deriveRowType(): RelDataType = {
    val groupSet = ImmutableBitSet.of(grouping: _*)
    val baseType = Aggregate.deriveRowType(
      cluster.getTypeFactory,
      getInput.getRowType,
      false,
      groupSet,
      Collections.singletonList(groupSet),
      aggCalls.asJava)
    val typeFactory = getCluster.getTypeFactory.asInstanceOf[FlinkTypeFactory]
    val builder = typeFactory.builder
    builder.addAll(baseType.getFieldList)
    namedWindowProperties.foreach { namedProp =>
      // use types from windowing strategy which keeps the precision and timestamp type
      // cast the type to not null type, because window properties should never be null
      val timeType = namedProp.property match {
        case PlannerWindowStart(_) | PlannerWindowEnd(_) =>
          LogicalTypeUtils.removeTimeAttributes(windowing.timeAttributeType).copy(false)
        case PlannerRowtimeAttribute(_) | PlannerProctimeAttribute(_) =>
          windowing.timeAttributeType.copy(false)
      }
      builder.add(namedProp.name, typeFactory.createFieldTypeFromLogicalType(timeType))
    }
    builder.build()
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    val inputRowType = getInput.getRowType
    val inputFieldNames = inputRowType.getFieldNames.asScala.toArray
    super.explainTerms(pw)
      .itemIf("groupBy", RelExplainUtil.fieldToString(grouping, inputRowType), grouping.nonEmpty)
      .item("window", windowing.toSummaryString(inputFieldNames))
      .item("select", RelExplainUtil.streamWindowAggregationToString(
        inputRowType,
        grouping,
        getRowType,
        aggCalls,
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
    ???
  }
}
