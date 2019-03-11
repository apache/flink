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

import org.apache.flink.table.plan.nodes.FlinkConventions
import org.apache.flink.table.plan.util.{FlinkRelOptUtil, RelExplainUtil}

import org.apache.calcite.plan._
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rel.core.Sort
import org.apache.calcite.rel.logical.LogicalSort
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rel.{RelCollation, RelCollationTraitDef, RelNode}
import org.apache.calcite.rex.{RexLiteral, RexNode}

/**
  * Sub-class of [[Sort]] that is a relational expression which imposes
  * a particular sort order on its input without otherwise changing its content in Flink.
  */
class FlinkLogicalSort(
    cluster: RelOptCluster,
    traits: RelTraitSet,
    child: RelNode,
    collation: RelCollation,
    offset: RexNode,
    fetch: RexNode)
  extends Sort(cluster, traits, child, collation, offset, fetch)
  with FlinkLogicalRel {

  private lazy val limitStart: Long = FlinkRelOptUtil.getLimitStart(offset)

  override def copy(
      traitSet: RelTraitSet,
      newInput: RelNode,
      newCollation: RelCollation,
      offset: RexNode,
      fetch: RexNode): Sort = {
    new FlinkLogicalSort(cluster, traitSet, newInput, newCollation, offset, fetch)
  }

  override def estimateRowCount(mq: RelMetadataQuery): Double = {
    val inputRowCnt = mq.getRowCount(this.getInput)
    if (inputRowCnt == null) {
      inputRowCnt
    } else {
      val rowCount = (inputRowCnt - limitStart).max(1.0)
      if (fetch != null) {
        val limit = RexLiteral.intValue(fetch)
        rowCount.min(limit)
      } else {
        rowCount
      }
    }
  }

  override def computeSelfCost(planner: RelOptPlanner, mq: RelMetadataQuery): RelOptCost = {
    // by default, assume cost is proportional to number of rows
    val rowCount: Double = mq.getRowCount(this)
    planner.getCostFactory.makeCost(rowCount, rowCount, 0)
  }

}

class FlinkLogicalSortStreamConverter
  extends ConverterRule(
    classOf[LogicalSort],
    Convention.NONE,
    FlinkConventions.LOGICAL,
    "FlinkLogicalSortStreamConverter") {

  override def convert(rel: RelNode): RelNode = {
    val sort = rel.asInstanceOf[LogicalSort]
    val newInput = RelOptRule.convert(sort.getInput, FlinkConventions.LOGICAL)
    FlinkLogicalSort.create(newInput, sort.getCollation, sort.offset, sort.fetch)
  }
}

class FlinkLogicalSortBatchConverter extends ConverterRule(
  classOf[LogicalSort],
  Convention.NONE,
  FlinkConventions.LOGICAL,
  "FlinkLogicalSortBatchConverter") {

  override def convert(rel: RelNode): RelNode = {
    val sort = rel.asInstanceOf[LogicalSort]
    val newInput = RelOptRule.convert(sort.getInput, FlinkConventions.LOGICAL)
    // TODO supports range sort
    FlinkLogicalSort.create(newInput, sort.getCollation, sort.offset, sort.fetch)
  }
}

object FlinkLogicalSort {
  val BATCH_CONVERTER: RelOptRule = new FlinkLogicalSortBatchConverter
  val STREAM_CONVERTER: RelOptRule = new FlinkLogicalSortStreamConverter

  def create(
      input: RelNode,
      collation: RelCollation,
      sortOffset: RexNode,
      sortFetch: RexNode): FlinkLogicalSort = {
    val cluster = input.getCluster
    val collationTrait = RelCollationTraitDef.INSTANCE.canonize(collation)
    val traitSet = input.getTraitSet.replace(FlinkConventions.LOGICAL)
      .replace(collationTrait).simplify()
    new FlinkLogicalSort(cluster, traitSet, input, collation, sortOffset, sortFetch)
  }
}
