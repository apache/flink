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

import org.apache.flink.api.common.io.InputFormat
import org.apache.flink.api.dag.Transformation
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.data.RowData
import org.apache.flink.table.planner.delegation.StreamPlanner
import org.apache.flink.table.planner.plan.nodes.common.CommonPhysicalTableSourceScan
import org.apache.flink.table.planner.plan.nodes.exec.{ExecNode, StreamExecNode}
import org.apache.flink.table.planner.plan.schema.TableSourceTable
import org.apache.flink.table.runtime.typeutils.RowDataTypeInfo
import org.apache.flink.table.sources.StreamTableSource

import org.apache.calcite.plan._
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.metadata.RelMetadataQuery

import java.util

import scala.collection.JavaConversions._

/**
  * Stream physical RelNode to read data from an external source defined by a
 * [[org.apache.flink.table.connector.source.ScanTableSource]].
  */
class StreamExecTableSourceScan(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    tableSourceTable: TableSourceTable)
  extends CommonPhysicalTableSourceScan(cluster, traitSet, tableSourceTable)
  with StreamPhysicalRel
  with StreamExecNode[RowData] {

  override def requireWatermark: Boolean = false

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    new StreamExecTableSourceScan(cluster, traitSet, tableSourceTable)
  }

  override def computeSelfCost(planner: RelOptPlanner, mq: RelMetadataQuery): RelOptCost = {
    val rowCnt = mq.getRowCount(this)
    val rowSize = mq.getAverageRowSize(this)
    planner.getCostFactory.makeCost(rowCnt, rowCnt, rowCnt * rowSize)
  }

  //~ ExecNode methods -----------------------------------------------------------

  override def getInputNodes: util.List[ExecNode[StreamPlanner, _]] = {
    getInputs.map(_.asInstanceOf[ExecNode[StreamPlanner, _]])
  }

  override def replaceInputNode(
      ordinalInParent: Int,
      newInputNode: ExecNode[StreamPlanner, _]): Unit = {
    replaceInput(ordinalInParent, newInputNode.asInstanceOf[RelNode])
  }

  override protected def createInputFormatTransformation(
      env: StreamExecutionEnvironment,
      inputFormat: InputFormat[RowData, _],
      name: String,
      outTypeInfo: RowDataTypeInfo): Transformation[RowData] = {
    // It's better to use StreamExecutionEnvironment.createInput()
    // rather than addLegacySource() for streaming, because it take care of checkpoint.
    env
      .createInput(inputFormat, outTypeInfo)
      .name(name)
      .getTransformation
  }

  override protected def translateToPlanInternal(
      planner: StreamPlanner): Transformation[RowData] = {
    createSourceTransformation(planner.getExecEnv, getRelDetailedDescription)
  }
}
