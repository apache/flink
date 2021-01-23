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
package org.apache.flink.table.planner.plan.nodes.physical.stream

import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecSortLimit
import org.apache.flink.table.planner.plan.nodes.exec.{ExecEdge, ExecNode}
import org.apache.flink.table.planner.plan.utils._

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.core.Sort
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rel.{RelCollation, RelNode, RelWriter}
import org.apache.calcite.rex.{RexLiteral, RexNode}

import scala.collection.JavaConversions._

/**
  * Stream physical RelNode for [[Sort]].
  *
  * This RelNode take the `limit` elements beginning with the first `offset` elements.
  **/
class StreamPhysicalSortLimit(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputRel: RelNode,
    sortCollation: RelCollation,
    offset: RexNode,
    fetch: RexNode,
    rankStrategy: RankProcessStrategy)
  extends Sort(cluster, traitSet, inputRel, sortCollation, offset, fetch)
  with StreamPhysicalRel {

  private val limitStart: Long = SortUtil.getLimitStart(offset)
  private val limitEnd: Long = SortUtil.getLimitEnd(offset, fetch)

  override def requireWatermark: Boolean = false

  override def copy(
      traitSet: RelTraitSet,
      newInput: RelNode,
      newCollation: RelCollation,
      offset: RexNode,
      fetch: RexNode): Sort = {
    new StreamPhysicalSortLimit(
        cluster, traitSet, newInput, newCollation, offset, fetch, rankStrategy)
  }

  def copy(newStrategy: RankProcessStrategy): StreamPhysicalSortLimit = {
    new StreamPhysicalSortLimit(cluster, traitSet, input, sortCollation, offset, fetch, newStrategy)
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    pw.input("input", getInput)
      .item("orderBy", RelExplainUtil.collationToString(sortCollation, getRowType))
      .item("offset", limitStart)
      .item("fetch", RelExplainUtil.fetchToString(fetch))
      .item("strategy", rankStrategy)
  }

  override def estimateRowCount(mq: RelMetadataQuery): Double = {
    val inputRows = mq.getRowCount(this.getInput)
    if (inputRows == null) {
      inputRows
    } else {
      val rowCount = (inputRows - limitStart).max(1.0)
      if (fetch != null) {
        rowCount.min(RexLiteral.intValue(fetch))
      } else {
        rowCount
      }
    }
  }

  override def translateToExecNode(): ExecNode[_] = {
    val generateUpdateBefore = ChangelogPlanUtils.generateUpdateBefore(this)
    new StreamExecSortLimit(
      SortUtil.getSortSpec(sortCollation.getFieldCollations),
      limitStart,
      limitEnd,
      rankStrategy,
      generateUpdateBefore,
      ExecEdge.DEFAULT,
      FlinkTypeFactory.toLogicalRowType(getRowType),
      getRelDetailedDescription
    )
  }
}
