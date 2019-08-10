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

package org.apache.flink.table.planner.plan.nodes.calcite

import java.util

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.rel.{RelNode, RelWriter}
import org.apache.calcite.util.ImmutableBitSet
import org.apache.flink.table.planner.calcite.FlinkRelBuilder.PlannerNamedWindowProperty
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.plan.logical.LogicalWindow

/**
  * Relational operator that represents a window table aggregate. A TableAggregate is similar to the
  * [[org.apache.calcite.rel.core.Aggregate]] but may output 0 or more records for a group.
  */
abstract class WindowTableAggregate(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    input: RelNode,
    groupSet: ImmutableBitSet,
    groupSets: util.List[ImmutableBitSet],
    aggCalls: util.List[AggregateCall],
    window: LogicalWindow,
    namedProperties: Seq[PlannerNamedWindowProperty])
  extends TableAggregate(cluster, traitSet, input, groupSet, groupSets, aggCalls) {

  def getWindow: LogicalWindow = window

  def getNamedProperties: Seq[PlannerNamedWindowProperty] = namedProperties

  override def deriveRowType(): RelDataType = {
    val aggregateRowType = super.deriveRowType()
    val typeFactory = getCluster.getTypeFactory.asInstanceOf[FlinkTypeFactory]
    val builder = typeFactory.builder
    builder.addAll(aggregateRowType.getFieldList)
    namedProperties.foreach { namedProp =>
      builder.add(
        namedProp.name,
        typeFactory.createFieldTypeFromLogicalType(namedProp.property.resultType)
      )
    }
    builder.build()
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    super.explainTerms(pw)
      .item("window", window)
      .item("properties", namedProperties.map(_.name).mkString(", "))
  }
}
