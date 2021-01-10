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
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecDataStreamScan
import org.apache.flink.table.planner.plan.schema.DataStreamTable

import org.apache.calcite.plan._
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.TableScan
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rel.{RelNode, RelWriter}

import scala.collection.JavaConverters._

/**
  * Flink RelNode which matches along with DataStreamSource.
  * It ensures that types without deterministic field order (e.g. POJOs) are not part of
  * the plan translation.
  */
class StreamPhysicalDataStreamScan(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    table: RelOptTable,
    outputRowType: RelDataType)
  extends TableScan(cluster, traitSet, table)
  with StreamPhysicalRel {

  val dataStreamTable: DataStreamTable[Any] = getTable.unwrap(classOf[DataStreamTable[Any]])

  override def requireWatermark: Boolean = false

  override def deriveRowType(): RelDataType = outputRowType

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    new StreamPhysicalDataStreamScan(cluster, traitSet, getTable, getRowType)
  }

  override def computeSelfCost(planner: RelOptPlanner, mq: RelMetadataQuery): RelOptCost = {
    val rowCnt = mq.getRowCount(this)
    val rowSize = mq.getAverageRowSize(this)
    planner.getCostFactory.makeCost(rowCnt, rowCnt, rowCnt * rowSize)
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    super.explainTerms(pw)
      .item("fields", getRowType.getFieldNames.asScala.mkString(", "))
  }

  override def translateToExecNode(): ExecNode[_] = {
    new StreamExecDataStreamScan(
      dataStreamTable.dataStream,
      dataStreamTable.dataType,
      dataStreamTable.fieldIndexes,
      dataStreamTable.fieldNames,
      getTable.getQualifiedName,
      FlinkTypeFactory.toLogicalRowType(getRowType),
      getRelDetailedDescription)
  }
}
