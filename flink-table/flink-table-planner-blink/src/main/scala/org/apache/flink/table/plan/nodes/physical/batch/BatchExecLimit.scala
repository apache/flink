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
package org.apache.flink.table.plan.nodes.physical.batch

import org.apache.flink.table.plan.cost.FlinkCost._
import org.apache.flink.table.plan.cost.FlinkCostFactory

import org.apache.calcite.plan.{RelOptCluster, RelOptCost, RelOptPlanner, RelTraitSet}
import org.apache.calcite.rel._
import org.apache.calcite.rel.core.Sort
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rex.{RexLiteral, RexNode}

/**
  * Batch physical RelNode for [[Sort]].
  *
  * This node will output `limit` records beginning with the first `offset` records without sort.
  */
class BatchExecLimit(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputRel: RelNode,
    limitOffset: RexNode,
    limit: RexNode,
    val isGlobal: Boolean)
  extends Sort(
    cluster,
    traitSet,
    inputRel,
    traitSet.getTrait(RelCollationTraitDef.INSTANCE),
    limitOffset,
    limit)
  with BatchPhysicalRel {

  private lazy val limitStart: Long = if (offset != null) RexLiteral.intValue(offset) else 0L
  private lazy val limitEnd: Long = if (limit != null) {
    RexLiteral.intValue(limit) + limitStart
  } else {
    Long.MaxValue
  }

  override def copy(
      traitSet: RelTraitSet,
      newInput: RelNode,
      newCollation: RelCollation,
      offset: RexNode,
      fetch: RexNode): Sort = {
    new BatchExecLimit(cluster, traitSet, newInput, offset, fetch, isGlobal)
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    pw.input("input", getInput)
      .item("offset", offsetToString)
      .item("limit", limitToString)
      .item("global", isGlobal)
  }

  private def offsetToString: String = s"$limitStart"

  private def limitToString: String = {
    if (limit != null) {
      s"${RexLiteral.intValue(limit)}"
    } else {
      "unlimited"
    }
  }

  override def computeSelfCost(planner: RelOptPlanner, mq: RelMetadataQuery): RelOptCost = {
    val rowCount = mq.getRowCount(this)
    val cpuCost = COMPARE_CPU_COST * rowCount
    val costFactory = planner.getCostFactory.asInstanceOf[FlinkCostFactory]
    costFactory.makeCost(rowCount, cpuCost, 0, 0, 0)
  }
}
