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

class LogicalWindowAggregate(
    window: LogicalWindow,
    namedProperties: Seq[NamedWindowProperty],
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    child: RelNode,
    groupSet: ImmutableBitSet,
    groupSets: util.List[ImmutableBitSet],
    aggCalls: util.List[AggregateCall])
  extends Aggregate(cluster, traitSet, child, groupSet, groupSets, aggCalls) {

  def getWindow: LogicalWindow = window

  def getNamedProperties: Seq[NamedWindowProperty] = namedProperties

  override def explainTerms(pw: RelWriter): RelWriter = {
    super.explainTerms(pw)
    for (property <- namedProperties) {
      pw.item(property.name, property.property)
    }
    pw.item("window", window.toString)
  }

  override def copy(
      traitSet: RelTraitSet,
      input: RelNode,
      groupSet: ImmutableBitSet,
      groupSets: util.List[ImmutableBitSet],
      aggCalls: util.List[AggregateCall])
    : Aggregate = {

    new LogicalWindowAggregate(
      window,
      namedProperties,
      cluster,
      traitSet,
      input,
      groupSet,
      groupSets,
      aggCalls)
  }

  def copy(namedProperties: Seq[NamedWindowProperty]): LogicalWindowAggregate = {
    new LogicalWindowAggregate(
      window,
      namedProperties,
      cluster,
      traitSet,
      input,
      getGroupSet,
      getGroupSets,
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
}

object LogicalWindowAggregate {

  def create(
      window: LogicalWindow,
      namedProperties: Seq[NamedWindowProperty],
      aggregate: Aggregate)
    : LogicalWindowAggregate = {

    val cluster: RelOptCluster = aggregate.getCluster
    val traitSet: RelTraitSet = cluster.traitSetOf(Convention.NONE)
    new LogicalWindowAggregate(
      window,
      namedProperties,
      cluster,
      traitSet,
      aggregate.getInput,
      aggregate.getGroupSet,
      aggregate.getGroupSets,
      aggregate.getAggCallList)
  }
}
