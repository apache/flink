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

import org.apache.calcite.plan._
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rel.core.Snapshot
import org.apache.calcite.rel.logical.LogicalSnapshot
import org.apache.calcite.rel.metadata.{RelMdCollation, RelMetadataQuery}
import org.apache.calcite.rel.{RelCollation, RelCollationTraitDef, RelNode}
import org.apache.calcite.rex.RexNode

import java.util
import java.util.function.Supplier

/**
  * Sub-class of [[Snapshot]] that is a relational expression which returns
  * the contents of a relation expression as it was at a given time in the past.
  */
class FlinkLogicalSnapshot(
    cluster: RelOptCluster,
    traits: RelTraitSet,
    child: RelNode,
    period: RexNode)
  extends Snapshot(cluster, traits, child, period)
  with FlinkLogicalRel {

  override def copy(
    traitSet: RelTraitSet,
    input: RelNode,
    period: RexNode): Snapshot = {
    new FlinkLogicalSnapshot(cluster, traitSet, input, period)
  }

  override def computeSelfCost(planner: RelOptPlanner, mq: RelMetadataQuery): RelOptCost = {
    val rowCnt = mq.getRowCount(this)
    val rowSize = mq.getAverageRowSize(this)
    planner.getCostFactory.makeCost(rowCnt, rowCnt, rowCnt * rowSize)
  }

}

class FlinkLogicalSnapshotConverter
  extends ConverterRule(
    classOf[LogicalSnapshot],
    Convention.NONE,
    FlinkConventions.LOGICAL,
    "FlinkLogicalSnapshotConverter") {

  def convert(rel: RelNode): RelNode = {
    val snapshot = rel.asInstanceOf[LogicalSnapshot]
    val newInput = RelOptRule.convert(snapshot.getInput, FlinkConventions.LOGICAL)
    FlinkLogicalSnapshot.create(newInput, snapshot.getPeriod)
  }
}

object FlinkLogicalSnapshot {

  val CONVERTER = new FlinkLogicalSnapshotConverter

  def create(input: RelNode, period: RexNode): FlinkLogicalSnapshot = {
    val cluster = input.getCluster
    val mq = cluster.getMetadataQuery
    val traitSet = cluster.traitSet.replace(Convention.NONE).replaceIfs(
      RelCollationTraitDef.INSTANCE, new Supplier[util.List[RelCollation]]() {
        def get: util.List[RelCollation] = RelMdCollation.snapshot(mq, input)
      })
    val snapshot = new FlinkLogicalSnapshot(cluster, traitSet, input, period)
    val newTraitSet = snapshot.getTraitSet
      .replace(FlinkConventions.LOGICAL).simplify()
    snapshot.copy(newTraitSet, input, period).asInstanceOf[FlinkLogicalSnapshot]
  }
}
