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

import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.transformations.StreamTransformation
import org.apache.flink.table.api.types.{DataTypes, TypeConverters}
import org.apache.flink.table.api.{StreamTableEnvironment, TableException}
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.plan.nodes.physical.{FlinkPhysicalRel, PhysicalTableSourceScan}
import org.apache.flink.table.plan.schema.FlinkRelOptTable
import org.apache.flink.table.sources.{RowtimeAttributeDescriptor, StreamTableSource, TableSourceUtil}
import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, AssignerWithPunctuatedWatermarks}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.table.plan.nodes.common.CommonScan
import org.apache.flink.table.sources.wmstrategies.{PeriodicWatermarkAssigner, PreserveWatermarks, PunctuatedWatermarkAssigner}

import org.apache.calcite.plan._
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rex.RexNode

/** Flink RelNode to read data from an external source defined by a [[StreamTableSource]]. */
class StreamExecTableSourceScan(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    relOptTable: FlinkRelOptTable)
  extends PhysicalTableSourceScan(cluster, traitSet, relOptTable)
  with StreamPhysicalRel
  with StreamExecScan {

  override def computeSelfCost(planner: RelOptPlanner, mq: RelMetadataQuery): RelOptCost = {
    val rowCnt = mq.getRowCount(this)
    val rowSize = mq.getAverageRowSize(this)
    planner.getCostFactory.makeCost(rowCnt, rowCnt, rowCnt * rowSize)
  }

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    new StreamExecTableSourceScan(
      cluster,
      traitSet,
      relOptTable
    )
  }

  override def copy(
      traitSet: RelTraitSet,
      relOptTable: FlinkRelOptTable): PhysicalTableSourceScan = {
    new StreamExecTableSourceScan(
      cluster,
      traitSet,
      relOptTable
    )
  }

  override def isDeterministic: Boolean = true

  //~ ExecNode methods -----------------------------------------------------------

  override def getFlinkPhysicalRel: FlinkPhysicalRel = this

  override def translateToPlanInternal(
      tableEnv: StreamTableEnvironment): StreamTransformation[BaseRow] = {

    val config = tableEnv.getConfig
    val inputDataStream = tableSource
      .asInstanceOf[StreamTableSource[_]]
      .getDataStream(tableEnv.execEnv)
      .asInstanceOf[DataStream[Any]]
    val fieldIdxs = TableSourceUtil.computeIndexMapping(tableSource, true, None)

    // check that declared and actual type of table source DataStream are identical
    if (!tableSource.getReturnType.toInternalType.equals(
        TypeConverters.createInternalTypeFromTypeInfo(inputDataStream.getType))) {
      throw new TableException(s"TableSource of type ${tableSource.getClass.getCanonicalName} " +
        s"returned a DataStream of type ${inputDataStream.getType} that does not match with the " +
        s"type ${tableSource.getReturnType} declared by the TableSource.getReturnType() method. " +
        s"Please validate the implementation of the TableSource.")
    }

    // get expression to extract rowtime attribute
    val rowtimeExpression: Option[RexNode] = TableSourceUtil.getRowtimeExtractionExpression(
      tableSource,
      None,
      cluster,
      tableEnv.getRelBuilder,
      DataTypes.TIMESTAMP
    )

    inputDataStream.getTransformation.setParallelism(getResource.getParallelism)
    inputDataStream.getTransformation.setResources(sourceResSpec, sourceResSpec)

    val ingestedTable = new DataStream(
      tableEnv.execEnv,
      convertToInternalRow(
        inputDataStream.getTransformation,
        fieldIdxs,
        getRowType,
        tableSource.getReturnType,
        getTable.getQualifiedName,
        config,
        rowtimeExpression))

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

    withWatermarks.getTransformation.setResources(conversionResSpec, conversionResSpec)

    withWatermarks.getTransformation
  }

  override def needInternalConversion: Boolean = {
    val fieldIndexes = TableSourceUtil.computeIndexMapping(
      tableSource,
      isStreamTable = false,
      None)
    hasTimeAttributeField(fieldIndexes) ||
        needsConversion(tableSource.getReturnType,
          CommonScan.extractTableSourceTypeClass(tableSource))
  }

  override private[flink] def getSourceTransformation(streamEnv: StreamExecutionEnvironment) = {
    tableSource.asInstanceOf[StreamTableSource[_]].getDataStream(streamEnv).getTransformation
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
