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

import org.apache.calcite.plan.{Convention, RelOptCluster, RelOptRule, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rel.core.{Correlate, CorrelationId}
import org.apache.calcite.rel.logical.LogicalCorrelate
import org.apache.calcite.sql.SemiJoinType
import org.apache.calcite.util.ImmutableBitSet
import org.apache.flink.table.plan.nodes.FlinkConventions

class FlinkLogicalCorrelate(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    left: RelNode,
    right: RelNode,
    correlationId: CorrelationId,
    requiredColumns: ImmutableBitSet,
    joinType: SemiJoinType)
  extends Correlate(cluster, traitSet, left, right, correlationId, requiredColumns, joinType)
  with FlinkLogicalRel {

  override def copy(
      traitSet: RelTraitSet,
      left: RelNode,
      right: RelNode,
      correlationId: CorrelationId,
      requiredColumns: ImmutableBitSet,
      joinType: SemiJoinType): Correlate = {

    new FlinkLogicalCorrelate(
      cluster,
      traitSet,
      left,
      right,
      correlationId,
      requiredColumns,
      joinType)
  }
}

class FlinkLogicalCorrelateConverter
  extends ConverterRule(
    classOf[LogicalCorrelate],
    Convention.NONE,
    FlinkConventions.LOGICAL,
    "FlinkLogicalCorrelateConverter") {

  override def convert(rel: RelNode): RelNode = {
    val correlate = rel.asInstanceOf[LogicalCorrelate]
    val traitSet = rel.getTraitSet.replace(FlinkConventions.LOGICAL)
    val newLeft = RelOptRule.convert(correlate.getLeft, FlinkConventions.LOGICAL)
    val newRight = RelOptRule.convert(correlate.getRight, FlinkConventions.LOGICAL)

    new FlinkLogicalCorrelate(
      rel.getCluster,
      traitSet,
      newLeft,
      newRight,
      correlate.getCorrelationId,
      correlate.getRequiredColumns,
      correlate.getJoinType)
  }
}

object FlinkLogicalCorrelate {
  val CONVERTER: ConverterRule = new FlinkLogicalCorrelateConverter()
}
