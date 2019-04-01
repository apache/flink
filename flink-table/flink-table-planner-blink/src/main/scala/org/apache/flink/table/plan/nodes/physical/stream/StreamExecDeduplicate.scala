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

package org.apache.flink.table.plan.nodes.physical.stream

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.{RelNode, RelWriter, SingleRel}

import java.util

/**
  * Stream physical RelNode which deduplicate on keys and keeps only first row or last row.
  * This node is an optimization of [[StreamExecRank]] for some special cases.
  * Compared to [[StreamExecRank]], this node could use mini-batch and access less state.
  * <p>NOTES: only supports sort on proctime now.
  */
class StreamExecDeduplicate(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputRel: RelNode,
    uniqueKeys: Array[Int],
    isRowtime: Boolean,
    keepLastRow: Boolean)
  extends SingleRel(cluster, traitSet, inputRel)
  with StreamPhysicalRel {

  def getUniqueKeys: Array[Int] = uniqueKeys

  override def producesUpdates: Boolean = keepLastRow

  override def needsUpdatesAsRetraction(input: RelNode): Boolean = true

  override def consumesRetractions: Boolean = true

  override def producesRetractions: Boolean = false

  override def requireWatermark: Boolean = false

  override def deriveRowType(): RelDataType = getInput.getRowType

  override def copy(traitSet: RelTraitSet, inputs: util.List[RelNode]): RelNode = {
    new StreamExecDeduplicate(
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
    super.explainTerms(pw)
      .item("keepLastRow", keepLastRow)
      .item("key", uniqueKeys.map(fieldNames.get).mkString(", "))
      .item("order", orderString)
  }

}
