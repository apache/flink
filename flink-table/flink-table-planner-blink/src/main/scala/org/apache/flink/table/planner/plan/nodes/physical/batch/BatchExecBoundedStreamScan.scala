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
import org.apache.flink.table.planner.codegen.CodeGeneratorContext
import org.apache.flink.table.planner.delegation.BatchPlanner
import org.apache.flink.table.planner.plan.nodes.exec.{LegacyBatchExecNode, ExecEdge}
import org.apache.flink.table.planner.plan.schema.DataStreamTable
import org.apache.flink.table.planner.plan.utils.ScanUtil

import org.apache.calcite.plan._
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.TableScan
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rel.{RelNode, RelWriter}

import java.util

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

/**
  * Flink RelNode which matches along with Transformation.
  * It ensures that types without deterministic field order (e.g. POJOs) are not part of
  * the plan translation.
  */
class BatchExecBoundedStreamScan(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    table: RelOptTable,
    outputRowType: RelDataType)
  extends TableScan(cluster, traitSet, table)
  with BatchPhysicalRel
  with LegacyBatchExecNode[RowData] {

  val boundedStreamTable: DataStreamTable[Any] = getTable.unwrap(classOf[DataStreamTable[Any]])

  override def deriveRowType(): RelDataType = outputRowType

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    new BatchExecBoundedStreamScan(cluster, traitSet, getTable, getRowType)
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

  //~ ExecNode methods -----------------------------------------------------------

  override def getInputEdges: util.List[ExecEdge] = List()

  override protected def translateToPlanInternal(
      planner: BatchPlanner): Transformation[RowData] = {
    val config = planner.getTableConfig
    val batchTransform = boundedStreamTable.dataStream.getTransformation
    if (needInternalConversion) {
      ScanUtil.convertToInternalRow(
        CodeGeneratorContext(config),
        batchTransform,
        boundedStreamTable.fieldIndexes,
        boundedStreamTable.dataType,
        getRowType,
        getTable.getQualifiedName,
        config,
        None)
    } else {
      batchTransform.asInstanceOf[Transformation[RowData]]
    }
  }

  def needInternalConversion: Boolean = {
    ScanUtil.hasTimeAttributeField(boundedStreamTable.fieldIndexes) ||
        ScanUtil.needsConversion(boundedStreamTable.dataType)
  }

}
