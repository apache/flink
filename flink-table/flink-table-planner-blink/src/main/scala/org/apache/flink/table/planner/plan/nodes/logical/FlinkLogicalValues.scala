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

package org.apache.flink.table.planner.plan.nodes.logical

import org.apache.flink.table.planner.plan.nodes.FlinkConventions

import com.google.common.collect.ImmutableList
import org.apache.calcite.plan._
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rel.core.Values
import org.apache.calcite.rel.logical.LogicalValues
import org.apache.calcite.rel.metadata.{RelMdCollation, RelMetadataQuery}
import org.apache.calcite.rel.{RelCollation, RelCollationTraitDef, RelNode}
import org.apache.calcite.rex.RexLiteral

import java.util
import java.util.function.Supplier

/**
  * Sub-class of [[Values]] that is a relational expression
  * whose value is a sequence of zero or more literal row values in Flink.
  */
class FlinkLogicalValues(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    rowRelDataType: RelDataType,
    tuples: ImmutableList[ImmutableList[RexLiteral]])
  extends Values(cluster, rowRelDataType, tuples, traitSet)
  with FlinkLogicalRel {

  override def copy(traitSet: RelTraitSet, inputs: util.List[RelNode]): RelNode = {
    new FlinkLogicalValues(cluster, traitSet, rowRelDataType, tuples)
  }

  override def computeSelfCost(planner: RelOptPlanner, mq: RelMetadataQuery): RelOptCost = {
    val dRows = mq.getRowCount(this)
    // Assume CPU is negligible since values are precomputed.
    val dCpu = 1
    val dIo = 0
    planner.getCostFactory.makeCost(dRows, dCpu, dIo)
  }

}

private class FlinkLogicalValuesConverter
  extends ConverterRule(
    classOf[LogicalValues],
    Convention.NONE,
    FlinkConventions.LOGICAL,
    "FlinkLogicalValuesConverter") {

  override def convert(rel: RelNode): RelNode = {
    val values = rel.asInstanceOf[LogicalValues]
    FlinkLogicalValues.create(rel.getCluster, values.getRowType, values.getTuples())
  }
}

object FlinkLogicalValues {
  val CONVERTER: ConverterRule = new FlinkLogicalValuesConverter()

  def create(
      cluster: RelOptCluster,
      rowType: RelDataType,
      tuples: ImmutableList[ImmutableList[RexLiteral]]): FlinkLogicalValues = {
    val mq = cluster.getMetadataQuery
    val traitSet = cluster.traitSetOf(FlinkConventions.LOGICAL).replaceIfs(
      RelCollationTraitDef.INSTANCE, new Supplier[util.List[RelCollation]]() {
        def get: util.List[RelCollation] = RelMdCollation.values(mq, rowType, tuples)
      }).simplify()
    new FlinkLogicalValues(cluster, traitSet, rowType, tuples)
  }
}
