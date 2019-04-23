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

import org.apache.flink.table.calcite.FlinkRelBuilder.NamedWindowProperty
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.plan.logical.LogicalWindow
import org.apache.flink.table.plan.nodes.FlinkConventions
import org.apache.flink.table.plan.nodes.calcite.LogicalWindowAggregate

import org.apache.calcite.plan._
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rel.core.{Aggregate, AggregateCall}
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rel.{RelNode, RelShuttle, RelWriter}
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.util.ImmutableBitSet

import java.util

import scala.collection.JavaConverters._

class FlinkLogicalWindowAggregate(
    window: LogicalWindow,
    namedProperties: Seq[NamedWindowProperty],
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    child: RelNode,
    indicator: Boolean,
    groupSet: ImmutableBitSet,
    groupSets: util.List[ImmutableBitSet],
    aggCalls: util.List[AggregateCall])
  extends Aggregate(cluster, traitSet, child, indicator, groupSet, groupSets, aggCalls)
  with FlinkLogicalRel {

  def getWindow: LogicalWindow = window

  def getNamedProperties: Seq[NamedWindowProperty] = namedProperties

  override def copy(
      traitSet: RelTraitSet,
      input: RelNode,
      indicator: Boolean,
      groupSet: ImmutableBitSet,
      groupSets: util.List[ImmutableBitSet],
      aggCalls: util.List[AggregateCall])
    : Aggregate = {
    new FlinkLogicalWindowAggregate(
      window,
      namedProperties,
      cluster,
      traitSet,
      input,
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
    builder.addAll(aggregateRowType.getFieldList)
    namedProperties.foreach { namedProp =>
      builder.add(
        namedProp.name,
        typeFactory.createTypeFromInternalType(namedProp.property.resultType, isNullable = true)
      )
    }
    builder.build()
  }

  override def computeSelfCost(planner: RelOptPlanner, mq: RelMetadataQuery): RelOptCost = {
    val child = this.getInput
    val rowCnt = mq.getRowCount(child)
    val rowSize = mq.getAverageRowSize(child)
    val aggCnt = this.getAggCallList.size
    // group by CPU cost(multiple by 1.1 to encourage less group keys) + agg call CPU cost
    val cpuCost: Double = rowCnt * getGroupCount * 1.1 + rowCnt * aggCnt
    planner.getCostFactory.makeCost(rowCnt, cpuCost, rowCnt * rowSize)
  }

  /**
    * The [[getDigest]] should be uniquely identifies the node; another node
    * is equivalent if and only if it has the same value. The [[getDigest]] is
    * computed by [[explainTerms(pw)]], so it should contains window information
    * to identify different WindowAggregate nodes, otherwise WindowAggregate node
    * can be replaced by any other WindowAggregate node.
    */
  override def explainTerms(pw: RelWriter): RelWriter = {
    super.explainTerms(pw)
      .item("window", window)
      .item("properties", namedProperties.map(_.name).mkString(", "))
  }
}

class FlinkLogicalWindowAggregateConverter
  extends ConverterRule(
    classOf[LogicalWindowAggregate],
    Convention.NONE,
    FlinkConventions.LOGICAL,
    "FlinkLogicalWindowAggregateConverter") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val agg = call.rel(0).asInstanceOf[LogicalWindowAggregate]

    // we do not support these functions natively
    // they have to be converted using the WindowAggregateReduceFunctionsRule
    agg.getAggCallList.asScala.map(_.getAggregation.getKind).forall {
      // we support AVG
      case SqlKind.AVG => true
      // but none of the other AVG agg functions
      case k if SqlKind.AVG_AGG_FUNCTIONS.contains(k) => false
      case _ => true
    }
  }

  override def convert(rel: RelNode): RelNode = {
    val agg = rel.asInstanceOf[LogicalWindowAggregate]
    val newInput = RelOptRule.convert(agg.getInput, FlinkConventions.LOGICAL)
    val traitSet = newInput.getCluster.traitSet().replace(FlinkConventions.LOGICAL).simplify()

    new FlinkLogicalWindowAggregate(
      agg.getWindow,
      agg.getNamedProperties,
      rel.getCluster,
      traitSet,
      newInput,
      agg.indicator,
      agg.getGroupSet,
      agg.getGroupSets,
      agg.getAggCallList)
  }

}

object FlinkLogicalWindowAggregate {
  val CONVERTER = new FlinkLogicalWindowAggregateConverter
}
