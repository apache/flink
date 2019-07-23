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

import org.apache.flink.api.dag.Transformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.runtime.operators.DamBehavior
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.api.TableException
import org.apache.flink.table.codegen.CodeGeneratorContext
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.plan.nodes.exec.{BatchExecNode, ExecNode}
import org.apache.flink.table.plan.nodes.physical.PhysicalTableSourceScan
import org.apache.flink.table.plan.schema.FlinkRelOptTable
import org.apache.flink.table.plan.util.ScanUtil
import org.apache.flink.table.planner.BatchPlanner
import org.apache.flink.table.sources.{StreamTableSource, TableSourceUtil}
import org.apache.flink.table.types.utils.TypeConversions.fromLegacyInfoToDataType

import org.apache.calcite.plan._
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rex.RexNode

import java.{lang, util}

import scala.collection.JavaConversions._

/**
  * Batch physical RelNode to read data from an external source defined by a
  * bounded [[StreamTableSource]].
  */
class BatchExecTableSourceScan(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    relOptTable: FlinkRelOptTable)
  extends PhysicalTableSourceScan(cluster, traitSet, relOptTable)
  with BatchPhysicalRel
  with BatchExecNode[BaseRow]{

  // cache table source transformation.
  private var sourceTransform: Transformation[_] = _

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

  override def getInputNodes: util.List[ExecNode[BatchPlanner, _]] = List()

  override def replaceInputNode(
      ordinalInParent: Int,
      newInputNode: ExecNode[BatchPlanner, _]): Unit = {
    replaceInput(ordinalInParent, newInputNode.asInstanceOf[RelNode])
  }

  def getSourceTransformation(
      streamEnv: StreamExecutionEnvironment): Transformation[_] = {
    if (sourceTransform == null) {
      sourceTransform = tableSource.asInstanceOf[StreamTableSource[_]].
          getDataStream(streamEnv).getTransformation
    }
    sourceTransform
  }

  override protected def translateToPlanInternal(
      planner: BatchPlanner): Transformation[BaseRow] = {
    val config = planner.getTableConfig
    val inputTransform = getSourceTransformation(planner.getExecEnv)
    inputTransform.setParallelism(getResource.getParallelism)

    val fieldIndexes = TableSourceUtil.computeIndexMapping(
      tableSource,
      isStreamTable = false,
      tableSourceTable.selectedFields)

    val inputDataType = fromLegacyInfoToDataType(inputTransform.getOutputType)
    val producedDataType = tableSource.getProducedDataType

    // check that declared and actual type of table source DataStream are identical
    if (inputDataType != producedDataType) {
      throw new TableException(s"TableSource of type ${tableSource.getClass.getCanonicalName} " +
        s"returned a DataStream of data type $producedDataType that does not match with the " +
        s"data type $producedDataType declared by the TableSource.getProducedDataType() method. " +
        s"Please validate the implementation of the TableSource.")
    }

    // get expression to extract rowtime attribute
    val rowtimeExpression: Option[RexNode] = TableSourceUtil.getRowtimeExtractionExpression(
      tableSource,
      tableSourceTable.selectedFields,
      cluster,
      planner.getRelBuilder
    )
    if (needInternalConversion) {
      val conversionTransform = ScanUtil.convertToInternalRow(
        CodeGeneratorContext(config),
        inputTransform.asInstanceOf[Transformation[Any]],
        fieldIndexes,
        producedDataType,
        getRowType,
        getTable.getQualifiedName,
        config,
        rowtimeExpression)
      conversionTransform.setParallelism(getResource.getParallelism)
      conversionTransform
    } else {
      inputTransform.asInstanceOf[Transformation[BaseRow]]
    }

  }

  def needInternalConversion: Boolean = {
    val fieldIndexes = TableSourceUtil.computeIndexMapping(
      tableSource,
      isStreamTable = false,
      tableSourceTable.selectedFields)
    ScanUtil.hasTimeAttributeField(fieldIndexes) ||
      ScanUtil.needsConversion(
        tableSource.getProducedDataType,
        TypeExtractor.createTypeInfo(
          tableSource, classOf[StreamTableSource[_]], tableSource.getClass, 0)
          .getTypeClass.asInstanceOf[Class[_]])
  }

  def getEstimatedRowCount: lang.Double = {
    getCluster.getMetadataQuery.getRowCount(this)
  }
}
