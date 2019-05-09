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

import org.apache.flink.runtime.operators.DamBehavior
import org.apache.flink.streaming.api.transformations.StreamTransformation
import org.apache.flink.table.api.{BatchTableEnvironment, TableException}
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.plan.nodes.exec.{BatchExecNode, ExecNode}
import org.apache.flink.table.plan.nodes.physical.PhysicalTableSourceScan
import org.apache.flink.table.plan.schema.FlinkRelOptTable
import org.apache.flink.table.sources.BatchTableSource

import org.apache.calcite.plan._
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.metadata.RelMetadataQuery

import java.util

import scala.collection.JavaConversions._

/**
  * Batch physical RelNode to read data from an external source defined by a [[BatchTableSource]].
  */
class BatchExecTableSourceScan(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    relOptTable: FlinkRelOptTable)
  extends PhysicalTableSourceScan(cluster, traitSet, relOptTable)
  with BatchPhysicalRel
  with BatchExecNode[BaseRow]{

  override def copy(traitSet: RelTraitSet, inputs: util.List[RelNode]): RelNode = {
    new BatchExecTableSourceScan(cluster, traitSet, relOptTable)
  }

  override def computeSelfCost(planner: RelOptPlanner, mq: RelMetadataQuery): RelOptCost = {
    val rowCnt = mq.getRowCount(this)
    if (rowCnt == null) {
      return null
    }
    val cpu = 0
    val rowSize = mq.getAverageRowSize(this)
    val size = rowCnt * rowSize
    planner.getCostFactory.makeCost(rowCnt, cpu, size)
  }

  //~ ExecNode methods -----------------------------------------------------------

  override def getDamBehavior: DamBehavior = DamBehavior.PIPELINED

  override def getInputNodes: util.List[ExecNode[BatchTableEnvironment, _]] = List()

  override def replaceInputNode(
      ordinalInParent: Int,
      newInputNode: ExecNode[BatchTableEnvironment, _]): Unit = {
    replaceInput(ordinalInParent, newInputNode.asInstanceOf[RelNode])
  }

  override def translateToPlanInternal(
      tableEnv: BatchTableEnvironment): StreamTransformation[BaseRow] = {
    throw new TableException("Implements this")
  }

}
