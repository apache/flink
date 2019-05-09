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

import org.apache.flink.streaming.api.transformations.StreamTransformation
import org.apache.flink.table.api.{StreamTableEnvironment, TableException}
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.plan.nodes.exec.{ExecNode, StreamExecNode}
import org.apache.flink.table.plan.nodes.physical.PhysicalTableSourceScan
import org.apache.flink.table.plan.schema.FlinkRelOptTable
import org.apache.flink.table.sources.StreamTableSource

import org.apache.calcite.plan._
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.metadata.RelMetadataQuery

import java.util

import scala.collection.JavaConversions._

/**
  * Stream physical RelNode to read data from an external source defined by a [[StreamTableSource]].
  */
class StreamExecTableSourceScan(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    relOptTable: FlinkRelOptTable)
  extends PhysicalTableSourceScan(cluster, traitSet, relOptTable)
  with StreamPhysicalRel
  with StreamExecNode[BaseRow] {

  override def producesUpdates: Boolean = false

  override def needsUpdatesAsRetraction(input: RelNode): Boolean = false

  override def consumesRetractions: Boolean = false

  override def producesRetractions: Boolean = false

  override def requireWatermark: Boolean = false

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    new StreamExecTableSourceScan(cluster, traitSet, relOptTable)
  }

  override def computeSelfCost(planner: RelOptPlanner, mq: RelMetadataQuery): RelOptCost = {
    val rowCnt = mq.getRowCount(this)
    val rowSize = mq.getAverageRowSize(this)
    planner.getCostFactory.makeCost(rowCnt, rowCnt, rowCnt * rowSize)
  }

  //~ ExecNode methods -----------------------------------------------------------

  override def getInputNodes: util.List[ExecNode[StreamTableEnvironment, _]] = {
    getInputs.map(_.asInstanceOf[ExecNode[StreamTableEnvironment, _]])
  }

  override def replaceInputNode(
      ordinalInParent: Int,
      newInputNode: ExecNode[StreamTableEnvironment, _]): Unit = {
    replaceInput(ordinalInParent, newInputNode.asInstanceOf[RelNode])
  }

  override protected def translateToPlanInternal(
      tableEnv: StreamTableEnvironment): StreamTransformation[BaseRow] = {
    throw new TableException("Implements this")
  }
}
