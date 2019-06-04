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

import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, AssignerWithPunctuatedWatermarks}
import org.apache.flink.streaming.api.transformations.StreamTransformation
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.table.api.{StreamTableEnvironment, TableException}
import org.apache.flink.table.codegen.CodeGeneratorContext
import org.apache.flink.table.codegen.OperatorCodeGenerator._
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.plan.nodes.exec.{ExecNode, StreamExecNode}
import org.apache.flink.table.plan.nodes.physical.PhysicalTableSourceScan
import org.apache.flink.table.plan.schema.FlinkRelOptTable
import org.apache.flink.table.plan.util.ScanUtil
import org.apache.flink.table.runtime.AbstractProcessStreamOperator
import org.apache.flink.table.sources.wmstrategies.{PeriodicWatermarkAssigner, PreserveWatermarks, PunctuatedWatermarkAssigner}
import org.apache.flink.table.sources.{RowtimeAttributeDescriptor, StreamTableSource, TableSourceUtil}
import org.apache.flink.table.types.utils.TypeConversions.{fromDataTypeToLegacyInfo, fromLegacyInfoToDataType}

import org.apache.calcite.plan._
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rex.RexNode

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
    val config = tableEnv.getConfig
    val sts = tableSource.asInstanceOf[StreamTableSource[_]]
    val inputTransform = sts.getDataStream(tableEnv.execEnv).getTransformation

    val fieldIndexes = TableSourceUtil.computeIndexMapping(
      tableSource,
      isStreamTable = true,
      None)

    val inputDataType = fromLegacyInfoToDataType(inputTransform.getOutputType)
    val producedDataType = tableSource.getProducedDataType
    val producedTypeInfo = fromDataTypeToLegacyInfo(producedDataType)

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
      None,
      cluster,
      tableEnv.getRelBuilder
    )

    val streamTransformation = if (needInternalConversion) {
      // extract time if the index is -1 or -2.
      val (extractElement, resetElement) =
        if (ScanUtil.hasTimeAttributeField(fieldIndexes)) {
          (s"ctx.$ELEMENT = $ELEMENT;", s"ctx.$ELEMENT = null;")
        } else {
          ("", "")
        }
      val ctx = CodeGeneratorContext(config).setOperatorBaseClass(
        classOf[AbstractProcessStreamOperator[BaseRow]])
      ScanUtil.convertToInternalRow(
        ctx,
        inputTransform.asInstanceOf[StreamTransformation[Any]],
        fieldIndexes,
        producedDataType,
        getRowType,
        getTable.getQualifiedName,
        config,
        rowtimeExpression,
        beforeConvert = extractElement,
        afterConvert = resetElement)
    } else {
      inputTransform.asInstanceOf[StreamTransformation[BaseRow]]
    }

    val ingestedTable = new DataStream(tableEnv.execEnv, streamTransformation)

    // generate watermarks for rowtime indicator
    val rowtimeDesc: Option[RowtimeAttributeDescriptor] =
      TableSourceUtil.getRowtimeAttributeDescriptor(tableSource, None)

    val withWatermarks = if (rowtimeDesc.isDefined) {
      val rowtimeFieldIdx = getRowType.getFieldNames.indexOf(rowtimeDesc.get.getAttributeName)
      val watermarkStrategy = rowtimeDesc.get.getWatermarkStrategy
      watermarkStrategy match {
        case p: PeriodicWatermarkAssigner =>
          val watermarkGenerator = new PeriodicWatermarkAssignerWrapper(rowtimeFieldIdx, p)
          ingestedTable.assignTimestampsAndWatermarks(watermarkGenerator)
        case p: PunctuatedWatermarkAssigner =>
          val watermarkGenerator = new PunctuatedWatermarkAssignerWrapper(rowtimeFieldIdx, p)
          ingestedTable.assignTimestampsAndWatermarks(watermarkGenerator)
        case _: PreserveWatermarks =>
          // The watermarks have already been provided by the underlying DataStream.
          ingestedTable
      }
    } else {
      // No need to generate watermarks if no rowtime attribute is specified.
      ingestedTable
    }

    withWatermarks.getTransformation
  }

  def needInternalConversion: Boolean = {
    val fieldIndexes = TableSourceUtil.computeIndexMapping(
      tableSource,
      isStreamTable = true,
      None)
    ScanUtil.hasTimeAttributeField(fieldIndexes) ||
      ScanUtil.needsConversion(
        tableSource.getProducedDataType,
        TypeExtractor.createTypeInfo(
          tableSource, classOf[StreamTableSource[_]], tableSource.getClass, 0)
          .getTypeClass.asInstanceOf[Class[_]])
  }
}

/**
  * Generates periodic watermarks based on a [[PeriodicWatermarkAssigner]].
  *
  * @param timeFieldIdx the index of the rowtime attribute.
  * @param assigner the watermark assigner.
  */
private class PeriodicWatermarkAssignerWrapper(
    timeFieldIdx: Int,
    assigner: PeriodicWatermarkAssigner)
  extends AssignerWithPeriodicWatermarks[BaseRow] {

  override def getCurrentWatermark: Watermark = assigner.getWatermark

  override def extractTimestamp(row: BaseRow, previousElementTimestamp: Long): Long = {
    val timestamp: Long = row.getLong(timeFieldIdx)
    assigner.nextTimestamp(timestamp)
    0L
  }
}

/**
  * Generates periodic watermarks based on a [[PunctuatedWatermarkAssigner]].
  *
  * @param timeFieldIdx the index of the rowtime attribute.
  * @param assigner the watermark assigner.
  */
private class PunctuatedWatermarkAssignerWrapper(
    timeFieldIdx: Int,
    assigner: PunctuatedWatermarkAssigner)
  extends AssignerWithPunctuatedWatermarks[BaseRow] {

  override def checkAndGetNextWatermark(row: BaseRow, ts: Long): Watermark = {
    val timestamp: Long = row.getLong(timeFieldIdx)
    assigner.getWatermark(row, timestamp)
  }

  override def extractTimestamp(element: BaseRow, previousElementTimestamp: Long): Long = {
    0L
  }
}
