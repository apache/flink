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

package org.apache.flink.table.planner.plan.nodes.physical.batch

import org.apache.flink.api.dag.Transformation
import org.apache.flink.table.data.RowData
import org.apache.flink.table.planner.delegation.BatchPlanner
import org.apache.flink.table.planner.plan.nodes.exec.{BatchExecNode, ExecEdge, ExecNode}
import org.apache.flink.table.planner.plan.nodes.physical.MultipleInputRel

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.RelNode

import java.util

import scala.collection.JavaConversions._

/**
 * Batch physical node for [[MultipleInputRel]].
 *
 * @param inputRels the input rels of multiple input rel,
 *                  which are not a part of the multiple input rel.
 * @param outputRel the root rel of the sub-graph of the multiple input rel.
 * @param inputEdges the [[ExecEdge]]s related to each input.
 */
class BatchExecMultipleInputNode(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputRels: Array[RelNode],
    outputRel: RelNode,
    inputEdges: Array[ExecEdge])
  extends MultipleInputRel(cluster, traitSet, inputRels, outputRel, inputEdges.map(_.getPriority))
  with BatchExecNode[RowData]
  with BatchPhysicalRel {

  //~ ExecNode methods -----------------------------------------------------------

  override def getInputNodes: util.List[ExecNode[BatchPlanner, _]] = {
    getInputs.map(_.asInstanceOf[ExecNode[BatchPlanner, _]])
  }

  override def getInputEdges: util.List[ExecEdge] = inputEdges.toList

  override def replaceInputNode(
      ordinalInParent: Int,
      newInputNode: ExecNode[BatchPlanner, _]): Unit = {
    throw new UnsupportedOperationException()
  }

  override protected def translateToPlanInternal(
      planner: BatchPlanner): Transformation[RowData] = ???

}
