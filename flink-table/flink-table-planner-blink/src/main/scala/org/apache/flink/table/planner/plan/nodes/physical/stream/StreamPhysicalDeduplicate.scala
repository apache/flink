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
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecDeduplicate
import org.apache.flink.table.planner.plan.nodes.exec.{InputProperty, ExecNode}
import org.apache.flink.table.planner.plan.utils.ChangelogPlanUtils

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.{RelNode, RelWriter, SingleRel}

import java.util

/**
 * Stream physical RelNode which deduplicate on keys and keeps only first row or last row.
 * This node is an optimization of [[StreamPhysicalRank]] for some special cases.
 * Compared to [[StreamPhysicalRank]], this node could use mini-batch and access less state.
 */
class StreamPhysicalDeduplicate(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputRel: RelNode,
    uniqueKeys: Array[Int],
    val isRowtime: Boolean,
    val keepLastRow: Boolean)
  extends SingleRel(cluster, traitSet, inputRel)
  with StreamPhysicalRel {

  def getUniqueKeys: Array[Int] = uniqueKeys

  override def requireWatermark: Boolean = isRowtime

  override def deriveRowType(): RelDataType = getInput.getRowType

  override def copy(traitSet: RelTraitSet, inputs: util.List[RelNode]): RelNode = {
    new StreamPhysicalDeduplicate(
      cluster,
      traitSet,
      inputs.get(0),
      uniqueKeys,
      isRowtime,
      keepLastRow)
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    val fieldNames = getRowType.getFieldNames
    val orderString = if (isRowtime) "ROWTIME" else "PROCTIME"
    val keep = if (keepLastRow) "LastRow" else "FirstRow"
    super.explainTerms(pw)
      .item("keep", keep)
      .item("key", uniqueKeys.map(fieldNames.get).mkString(", "))
      .item("order", orderString)
  }

  override def translateToExecNode(): ExecNode[_] = {
    val generateUpdateBefore = ChangelogPlanUtils.generateUpdateBefore(this)
    new StreamExecDeduplicate(
      uniqueKeys,
      isRowtime,
      keepLastRow,
      generateUpdateBefore,
      InputProperty.DEFAULT,
      FlinkTypeFactory.toLogicalRowType(getRowType),
      getRelDetailedDescription
    )
  }
}
