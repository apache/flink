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

import org.apache.flink.table.data.RowData
import org.apache.flink.table.planner.plan.nodes.exec.LegacyStreamExecNode
import org.apache.flink.table.planner.plan.utils._

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.rel.{RelNode, RelWriter, SingleRel}

/**
  * Base Stream physical RelNode for unbounded group table aggregate.
  */
abstract class StreamExecGroupTableAggregateBase(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputRel: RelNode,
    outputRowType: RelDataType,
    val grouping: Array[Int],
    val aggCalls: Seq[AggregateCall])
  extends SingleRel(cluster, traitSet, inputRel)
  with StreamPhysicalRel
  with LegacyStreamExecNode[RowData] {

  val aggInfoList: AggregateInfoList = AggregateUtil.deriveAggregateInfoList(
    this,
    aggCalls,
    grouping)

  override def requireWatermark: Boolean = false

  override def deriveRowType(): RelDataType = outputRowType

  override def explainTerms(pw: RelWriter): RelWriter = {
    val inputRowType = getInput.getRowType
    super.explainTerms(pw)
      .itemIf("groupBy",
              RelExplainUtil.fieldToString(grouping, inputRowType), grouping.nonEmpty)
      .item("select", RelExplainUtil.streamGroupAggregationToString(
        inputRowType,
        getRowType,
        aggInfoList,
        grouping))
  }
}
