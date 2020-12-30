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
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecLimit
import org.apache.flink.table.planner.plan.nodes.exec.{ExecEdge, ExecNode}
import org.apache.flink.table.planner.plan.utils.{ChangelogPlanUtils, RelExplainUtil, SortUtil}

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel._
import org.apache.calcite.rel.core.Sort
import org.apache.calcite.rex.RexNode

/**
  * Stream physical RelNode for [[Sort]].
  *
  * This node will output `limit` records beginning with the first `offset` records without sort.
  */
class StreamPhysicalLimit(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputRel: RelNode,
    offset: RexNode,
    fetch: RexNode)
  extends Sort(
    cluster,
    traitSet,
    inputRel,
    RelCollations.EMPTY,
    offset,
    fetch)
  with StreamPhysicalRel {

  private lazy val limitStart: Long = SortUtil.getLimitStart(offset)
  private lazy val limitEnd: Long = SortUtil.getLimitEnd(offset, fetch)

  override def requireWatermark: Boolean = false

  override def copy(
      traitSet: RelTraitSet,
      newInput: RelNode,
      newCollation: RelCollation,
      offset: RexNode,
      fetch: RexNode): Sort = {
    new StreamPhysicalLimit(cluster, traitSet, newInput, offset, fetch)
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    pw.input("input", getInput)
      .item("offset", limitStart)
      .item("fetch", RelExplainUtil.fetchToString(fetch))
  }

  override def translateToExecNode(): ExecNode[_] = {
    val generateUpdateBefore = ChangelogPlanUtils.generateUpdateBefore(this)
    val needRetraction = !ChangelogPlanUtils.inputInsertOnly(this)
    new StreamExecLimit(
        limitStart,
        limitEnd,
        generateUpdateBefore,
        needRetraction,
        ExecEdge.DEFAULT,
        FlinkTypeFactory.toLogicalRowType(getRowType),
        getRelDetailedDescription
    )
  }
}
