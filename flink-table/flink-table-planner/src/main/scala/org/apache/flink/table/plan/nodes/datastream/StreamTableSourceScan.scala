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

package org.apache.flink.table.plan.nodes.datastream

import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, AssignerWithPunctuatedWatermarks}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.table.api.{StreamQueryConfig, TableException, TableSchema}
import org.apache.flink.table.plan.nodes.PhysicalTableSourceScan
import org.apache.flink.table.plan.schema.RowSchema
import org.apache.flink.table.planner.StreamPlanner
import org.apache.flink.table.runtime.types.CRow
import org.apache.flink.table.sources._
import org.apache.flink.table.sources.wmstrategies.{PeriodicWatermarkAssigner, PreserveWatermarks, PunctuatedWatermarkAssigner}
import org.apache.flink.table.types.utils.TypeConversions
import org.apache.flink.table.types.utils.TypeConversions.fromLegacyInfoToDataType
import org.apache.flink.table.typeutils.TimeIndicatorTypeInfo
import org.apache.flink.table.utils.TypeMappingUtils

import org.apache.calcite.plan._
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.metadata.RelMetadataQuery

import java.util.function.{Function => JFunction}

import scala.collection.JavaConverters._

/** Flink RelNode to read data from an external source defined by a [[StreamTableSource]]. */
class StreamTableSourceScan(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    table: RelOptTable,
    tableSchema: TableSchema,
    tableSource: StreamTableSource[_],
    selectedFields: Option[Array[Int]])
  extends PhysicalTableSourceScan(
    cluster,
    traitSet,
    table,
    tableSchema,
    tableSource,
    selectedFields)
  with StreamScan {

  override def deriveRowType(): RelDataType = {
    val baseRowType = table.getRowType
    selectedFields.map(idxs => {
      val fields = baseRowType.getFieldList
      val builder = cluster.getTypeFactory.builder()
      idxs.map(fields.get).foreach(builder.add)
      builder.build()
    }).getOrElse(baseRowType)
  }

  override def computeSelfCost (planner: RelOptPlanner, metadata: RelMetadataQuery): RelOptCost = {
    val rowCnt = metadata.getRowCount(this)
    planner.getCostFactory.makeCost(rowCnt, rowCnt, rowCnt * estimateRowSize(getRowType))
  }

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    new StreamTableSourceScan(
      cluster,
      traitSet,
      getTable,
      tableSchema,
      tableSource,
      selectedFields
    )
  }

  override def copy(
      traitSet: RelTraitSet,
      newTableSource: TableSource[_]): PhysicalTableSourceScan = {

    new StreamTableSourceScan(
      cluster,
      traitSet,
      getTable,
      tableSchema,
      newTableSource.asInstanceOf[StreamTableSource[_]],
      selectedFields
    )
  }

  override def translateToPlan(
      planner: StreamPlanner,
      queryConfig: StreamQueryConfig): DataStream[CRow] = {

    val config = planner.getConfig
    val inputDataStream = tableSource.getDataStream(planner.getExecutionEnvironment)
      .asInstanceOf[DataStream[Any]]
    val outputSchema = new RowSchema(this.getRowType)

    val inputDataType = fromLegacyInfoToDataType(inputDataStream.getType)
    val producedDataType = tableSource.getProducedDataType

    // check that declared and actual type of table source DataStream are identical
    if (inputDataType != producedDataType) {
      throw new TableException(s"TableSource of type ${tableSource.getClass.getName} " +
        s"returned a DataStream of data type $inputDataType that does not match with the " +
        s"data type $producedDataType declared by the TableSource.getProducedDataType() method. " +
        s"Please validate the implementation of the TableSource.")
    }

    val nameMapping: JFunction[String, String] = tableSource match {
      case mapping: DefinedFieldMapping if mapping.getFieldMapping != null =>
        new JFunction[String, String] {
          override def apply(t: String): String = mapping.getFieldMapping.get(t)
        }
      case _ => JFunction.identity()
    }

    // get expression to extract rowtime attribute
    val rowtimeExpression = TableSourceUtil.getRowtimeAttributeDescriptor(
      tableSource,
      selectedFields)
      .map(desc => TableSourceUtil.getRowtimeExtractionExpression(
        desc.getTimestampExtractor,
        producedDataType,
        TypeConversions.fromLegacyInfoToDataType(TimeIndicatorTypeInfo.ROWTIME_INDICATOR),
        planner.getRelBuilder,
        nameMapping
      ))

    val fieldIndexes = TypeMappingUtils.computePhysicalIndicesOrTimeAttributeMarkers(
      tableSource,
      selectedFields.map(_.map(tableSchema.getTableColumn(_).get()).toList.asJava)
        .getOrElse(tableSchema.getTableColumns),
      true,
      nameMapping
    )

    // ingest table and convert and extract time attributes if necessary
    val ingestedTable = convertToInternalRow(
      outputSchema,
      inputDataStream,
      fieldIndexes,
      config,
      rowtimeExpression)

    // generate watermarks for rowtime indicator
    val rowtimeDesc: Option[RowtimeAttributeDescriptor] =
      TableSourceUtil.getRowtimeAttributeDescriptor(tableSource, selectedFields)

    val withWatermarks = if (rowtimeDesc.isDefined) {
      val rowtimeFieldIdx = outputSchema.fieldNames.indexOf(rowtimeDesc.get.getAttributeName)
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

    withWatermarks
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
  extends AssignerWithPeriodicWatermarks[CRow] {

  override def getCurrentWatermark: Watermark = assigner.getWatermark

  override def extractTimestamp(crow: CRow, previousElementTimestamp: Long): Long = {
    val timestamp: Long = crow.row.getField(timeFieldIdx).asInstanceOf[Long]
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
  extends AssignerWithPunctuatedWatermarks[CRow] {

  override def checkAndGetNextWatermark(crow: CRow, ts: Long): Watermark = {
    val timestamp: Long = crow.row.getField(timeFieldIdx).asInstanceOf[Long]
    assigner.getWatermark(crow.row, timestamp)
  }

  override def extractTimestamp(element: CRow, previousElementTimestamp: Long): Long = {
    0L
  }
}
