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
import org.apache.flink.table.api.BatchTableEnvironment
import org.apache.flink.table.codegen.CodeGeneratorContext
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.plan.nodes.exec.{BatchExecNode, ExecNode}
import org.apache.flink.table.plan.schema.DataStreamTable
import org.apache.flink.table.plan.util.ScanUtil

import org.apache.calcite.plan._
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.TableScan
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rel.{RelNode, RelWriter}

import java.util

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

/**
  * Flink RelNode which matches along with StreamTransformation.
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
  with BatchExecNode[BaseRow] {

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

  override def getDamBehavior = DamBehavior.PIPELINED

  override def getInputNodes: util.List[ExecNode[BatchTableEnvironment, _]] = List()

  override def replaceInputNode(
      ordinalInParent: Int,
      newInputNode: ExecNode[BatchTableEnvironment, _]): Unit = {
    replaceInput(ordinalInParent, newInputNode.asInstanceOf[RelNode])
  }

  override def translateToPlanInternal(
      tableEnv: BatchTableEnvironment): StreamTransformation[BaseRow] = {
    val config = tableEnv.getConfig
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
      batchTransform.asInstanceOf[StreamTransformation[BaseRow]]
    }
  }

  def needInternalConversion: Boolean = {
    ScanUtil.hasTimeAttributeField(boundedStreamTable.fieldIndexes) ||
        ScanUtil.needsConversion(
          boundedStreamTable.dataType,
          boundedStreamTable.dataStream.getType.getTypeClass)
  }

}
