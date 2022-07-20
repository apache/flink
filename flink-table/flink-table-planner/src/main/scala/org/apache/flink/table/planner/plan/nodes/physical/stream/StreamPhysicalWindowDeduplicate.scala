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
import org.apache.flink.table.planner.plan.logical.WindowingStrategy
import org.apache.flink.table.planner.plan.nodes.exec.{ExecNode, InputProperty}
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecWindowDeduplicate
import org.apache.flink.table.planner.plan.utils._
import org.apache.flink.table.planner.utils.ShortcutUtils.unwrapTableConfig

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel._
import org.apache.calcite.rel.`type`.RelDataType

import java.util

import scala.collection.JavaConverters._

/**
 * Stream physical RelNode which deduplicate on keys and keeps only first row or last row for each
 * window. This node is an optimization of [[StreamPhysicalWindowRank]]. Compared to
 * [[StreamPhysicalWindowRank]], this node could access/write state with higher performance. The
 * RelNode also requires PARTITION BY clause contains start and end columns of the windowing TVF.
 */
class StreamPhysicalWindowDeduplicate(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputRel: RelNode,
    partitionKeys: Array[Int],
    orderKey: Int,
    keepLastRow: Boolean,
    windowing: WindowingStrategy)
  extends SingleRel(cluster, traitSet, inputRel)
  with StreamPhysicalRel {

  override def requireWatermark: Boolean = windowing.isRowtime

  override def deriveRowType(): RelDataType = getInput.getRowType

  override def copy(traitSet: RelTraitSet, inputs: util.List[RelNode]): RelNode = {
    new StreamPhysicalWindowDeduplicate(
      cluster,
      traitSet,
      inputs.get(0),
      partitionKeys,
      orderKey,
      keepLastRow,
      windowing)
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    val inputRowType = inputRel.getRowType
    val inputFieldNames = inputRowType.getFieldNames.asScala.toArray
    val keep = if (keepLastRow) "LastRow" else "FirstRow"
    val orderString = if (windowing.isRowtime) "ROWTIME" else "PROCTIME"
    pw.input("input", getInput)
      .item("window", windowing.toSummaryString(inputFieldNames))
      .item("keep", keep)
      .item("partitionKeys", RelExplainUtil.fieldToString(partitionKeys, inputRowType))
      .item("orderKey", inputFieldNames(orderKey))
      .item("order", orderString)
  }

  override def translateToExecNode(): ExecNode[_] = {
    new StreamExecWindowDeduplicate(
      unwrapTableConfig(this),
      partitionKeys,
      orderKey,
      keepLastRow,
      windowing,
      InputProperty.DEFAULT,
      FlinkTypeFactory.toLogicalRowType(getRowType),
      getRelDetailedDescription)
  }
}
