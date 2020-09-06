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

import org.apache.calcite.plan._
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rel.core.Sort
import org.apache.calcite.rel.logical.LogicalSort
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rel.{RelCollation, RelNode}
import org.apache.calcite.rex.{RexLiteral, RexNode}
import org.apache.flink.table.plan.nodes.FlinkConventions

class FlinkLogicalSort(
    cluster: RelOptCluster,
    traits: RelTraitSet,
    child: RelNode,
    collation: RelCollation,
    sortOffset: RexNode,
    sortFetch: RexNode)
  extends Sort(cluster, traits, child, collation, sortOffset, sortFetch)
  with FlinkLogicalRel {

  private val limitStart: Long = if (offset != null) {
    RexLiteral.intValue(offset)
  } else {
    0L
  }

  override def copy(
      traitSet: RelTraitSet,
      newInput: RelNode,
      newCollation: RelCollation,
      offset: RexNode,
      fetch: RexNode): Sort = {

    new FlinkLogicalSort(cluster, traitSet, newInput, newCollation, offset, fetch)
  }

  override def estimateRowCount(metadata: RelMetadataQuery): Double = {
    val inputRowCnt = metadata.getRowCount(this.getInput)
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

class FlinkLogicalSortConverter
  extends ConverterRule(
    classOf[LogicalSort],
    Convention.NONE,
    FlinkConventions.LOGICAL,
    "FlinkLogicalSortConverter") {

  override def convert(rel: RelNode): RelNode = {
    val sort = rel.asInstanceOf[LogicalSort]
    val traitSet = rel.getTraitSet.replace(FlinkConventions.LOGICAL)
    val newInput = RelOptRule.convert(sort.getInput, FlinkConventions.LOGICAL)

    new FlinkLogicalSort(rel.getCluster,
      traitSet,
      newInput,
      sort.getCollation,
      sort.offset,
      sort.fetch)
  }
}

object FlinkLogicalSort {
  val CONVERTER: RelOptRule = new FlinkLogicalSortConverter
}
