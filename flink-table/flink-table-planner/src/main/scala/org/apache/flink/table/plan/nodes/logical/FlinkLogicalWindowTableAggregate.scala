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

package org.apache.flink.table.plan.nodes.logical

import java.util

import org.apache.calcite.plan._
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rel.{RelNode, RelShuttle, RelWriter}
import org.apache.calcite.util.ImmutableBitSet
import org.apache.flink.table.calcite.FlinkRelBuilder.NamedWindowProperty
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.plan.logical.LogicalWindow
import org.apache.flink.table.plan.logical.rel.{LogicalWindowTableAggregate, TableAggregate}
import org.apache.flink.table.plan.nodes.FlinkConventions

class FlinkLogicalWindowTableAggregate(
    window: LogicalWindow,
    namedProperties: Seq[NamedWindowProperty],
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    input: RelNode,
    indicator: Boolean,
    groupSet: ImmutableBitSet,
    groupSets: util.List[ImmutableBitSet],
    aggCalls: util.List[AggregateCall])
  extends TableAggregate(cluster, traitSet, input, indicator, groupSet, groupSets, aggCalls)
    with FlinkLogicalRel {

  def getWindow: LogicalWindow = window

  def getNamedProperties: Seq[NamedWindowProperty] = namedProperties

  override def explainTerms(pw: RelWriter): RelWriter = {
    super.explainTerms(pw)
    for (property <- namedProperties) {
      pw.item(property.name, property.property)
    }
    pw.item("window", window.toString)
  }

  override def copy(traitSet: RelTraitSet, inputs: util.List[RelNode]): TableAggregate = {
    new FlinkLogicalWindowTableAggregate(
      window,
      namedProperties,
      cluster,
      traitSet,
      inputs.get(0),
      indicator,
      groupSet,
      groupSets,
      aggCalls)
  }

  override def accept(shuttle: RelShuttle): RelNode = shuttle.visit(this)

  override def deriveRowType(): RelDataType = {
    val aggregateRowType = super.deriveRowType()
    val typeFactory = getCluster.getTypeFactory.asInstanceOf[FlinkTypeFactory]
    val builder = typeFactory.builder
    // add group ley types and agg types
    builder.addAll(aggregateRowType.getFieldList)
    namedProperties.foreach { namedProp =>
      builder.add(
        namedProp.name,
        typeFactory.createTypeFromTypeInfo(namedProp.property.resultType, isNullable = false)
      )
    }
    builder.build()
  }

  override def computeSelfCost(planner: RelOptPlanner, metadata: RelMetadataQuery): RelOptCost = {
    val child = this.getInput
    val rowCnt = metadata.getRowCount(child)
    val rowSize = this.estimateRowSize(child.getRowType)
    val aggCnt = this.aggCalls.size
    planner.getCostFactory.makeCost(rowCnt, rowCnt * aggCnt, rowCnt * rowSize)
  }
}

class FlinkLogicalWindowTableAggregateConverter
  extends ConverterRule(
    classOf[LogicalWindowTableAggregate],
    Convention.NONE,
    FlinkConventions.LOGICAL,
    "FlinkLogicalWindowTableAggregateConverter") {

  override def convert(rel: RelNode): RelNode = {
    val agg = rel.asInstanceOf[LogicalWindowTableAggregate]
    val traitSet = rel.getTraitSet.replace(FlinkConventions.LOGICAL)
    val newInput = RelOptRule.convert(agg.getInput, FlinkConventions.LOGICAL)

    new FlinkLogicalWindowTableAggregate(
      agg.getWindow,
      agg.getNamedProperties,
      rel.getCluster,
      traitSet,
      newInput,
      agg.getIndicator,
      agg.getGroupSet,
      agg.getGroupSets,
      agg.getAggCallList)
  }
}

object FlinkLogicalWindowTableAggregate {
  val CONVERTER = new FlinkLogicalWindowTableAggregateConverter
}
