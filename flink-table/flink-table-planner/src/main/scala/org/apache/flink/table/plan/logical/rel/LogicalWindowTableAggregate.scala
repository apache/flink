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

package org.apache.flink.table.plan.logical.rel

import java.util

import org.apache.calcite.plan.{Convention, RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.{Aggregate, AggregateCall}
import org.apache.calcite.rel.{RelNode, RelShuttle, RelWriter}
import org.apache.calcite.util.ImmutableBitSet
import org.apache.flink.table.calcite.FlinkRelBuilder.NamedWindowProperty
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.plan.logical.LogicalWindow

class LogicalWindowTableAggregate(
    window: LogicalWindow,
    namedProperties: Seq[NamedWindowProperty],
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    input: RelNode,
    indicatorFlag: Boolean,
    groupSet: ImmutableBitSet,
    groupSets: util.List[ImmutableBitSet],
    aggCalls: util.List[AggregateCall])
  extends TableAggregate(cluster, traitSet, input, indicatorFlag, groupSet, groupSets, aggCalls) {

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
    new LogicalWindowTableAggregate(
      window,
      namedProperties,
      cluster,
      traitSet,
      inputs.get(0),
      indicatorFlag,
      groupSet,
      groupSets,
      aggCalls)
  }

  override def accept(shuttle: RelShuttle): RelNode = shuttle.visit(this)

  override def deriveRowType(): RelDataType = {
    val aggregateRowType = super.deriveRowType()
    val typeFactory = getCluster.getTypeFactory.asInstanceOf[FlinkTypeFactory]
    val builder = typeFactory.builder
    builder.addAll(aggregateRowType.getFieldList)
    namedProperties.foreach { namedProp =>
      builder.add(
        namedProp.name,
        typeFactory.createTypeFromTypeInfo(namedProp.property.resultType, isNullable = false)
      )
    }
    builder.build()
  }

  override private[flink] def getCorrespondingAggregate: Aggregate = {
    new LogicalWindowAggregate(
      window,
      namedProperties,
      cluster,
      traitSet,
      getInput,
      groupSet,
      groupSets,
      aggCalls
    )
  }
}

object LogicalWindowTableAggregate {

  def create(
    window: LogicalWindow,
    namedProperties: Seq[NamedWindowProperty],
    aggregate: Aggregate)
  : LogicalWindowTableAggregate = {

    val cluster: RelOptCluster = aggregate.getCluster
    val traitSet: RelTraitSet = cluster.traitSetOf(Convention.NONE)
    new LogicalWindowTableAggregate(
      window,
      namedProperties,
      cluster,
      traitSet,
      aggregate.getInput,
      aggregate.indicator,
      aggregate.getGroupSet,
      aggregate.getGroupSets,
      aggregate.getAggCallList)
  }

  def create(logicalWindowAggregate: LogicalWindowAggregate): LogicalWindowTableAggregate = {
    val cluster: RelOptCluster = logicalWindowAggregate.getCluster
    val traitSet: RelTraitSet = cluster.traitSetOf(Convention.NONE)
    new LogicalWindowTableAggregate(
      logicalWindowAggregate.getWindow,
      logicalWindowAggregate.getNamedProperties,
      cluster,
      traitSet,
      logicalWindowAggregate.getInput,
      logicalWindowAggregate.indicator,
      logicalWindowAggregate.getGroupSet,
      logicalWindowAggregate.getGroupSets,
      logicalWindowAggregate.getAggCallList)
  }
}
