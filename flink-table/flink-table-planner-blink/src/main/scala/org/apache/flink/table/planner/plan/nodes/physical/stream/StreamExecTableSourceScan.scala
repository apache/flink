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
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.dag.Transformation
import org.apache.flink.core.io.InputSplit
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, AssignerWithPunctuatedWatermarks}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.table.api.{DataTypes, TableException}
import org.apache.flink.table.dataformat.DataFormatConverters.DataFormatConverter
import org.apache.flink.table.dataformat.{BaseRow, DataFormatConverters}
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.codegen.CodeGeneratorContext
import org.apache.flink.table.planner.codegen.OperatorCodeGenerator._
import org.apache.flink.table.planner.delegation.StreamPlanner
import org.apache.flink.table.planner.plan.nodes.exec.{ExecNode, StreamExecNode}
import org.apache.flink.table.planner.plan.nodes.physical.PhysicalTableSourceScan
import org.apache.flink.table.planner.plan.schema.TableSourceTable
import org.apache.flink.table.planner.plan.utils.ScanUtil
import org.apache.flink.table.planner.sources.TableSourceUtil
import org.apache.flink.table.runtime.operators.AbstractProcessStreamOperator
import org.apache.flink.table.runtime.types.TypeInfoDataTypeConverter
import org.apache.flink.table.sources.wmstrategies.{PeriodicWatermarkAssigner, PreserveWatermarks, PunctuatedWatermarkAssigner}
import org.apache.flink.table.sources.{DefinedFieldMapping, RowtimeAttributeDescriptor, StreamTableSource}
import org.apache.flink.table.types.{DataType, FieldsDataType}
import org.apache.flink.table.utils.TypeMappingUtils
import org.apache.flink.types.Row

import org.apache.calcite.plan._
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rex.RexNode

import java.util
import java.util.function.{Function => JFunction}

import scala.collection.JavaConversions._

/**
  * Stream physical RelNode to read data from an external source defined by a [[StreamTableSource]].
  */
class StreamExecTableSourceScan(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    tableSourceTable: TableSourceTable[_])
  extends PhysicalTableSourceScan(cluster, traitSet, tableSourceTable)
  with StreamPhysicalRel
  with StreamExecNode[BaseRow] {

  override def producesUpdates: Boolean = false

  override def needsUpdatesAsRetraction(input: RelNode): Boolean = false

  override def consumesRetractions: Boolean = false

  override def producesRetractions: Boolean = false

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

  override protected def translateToPlanInternal(
      planner: StreamPlanner): Transformation[BaseRow] = {
    val config = planner.getTableConfig
    val inputTransform = getSourceTransformation(planner.getExecEnv)

    val fieldIndexes = computeIndexMapping()

    val producedDataType = tableSource.getProducedDataType

    // check that declared and actual type of table source DataStream are identical
    if (inputTransform.getOutputType !=
        TypeInfoDataTypeConverter.fromDataTypeToTypeInfo(producedDataType)) {
      throw new TableException(s"TableSource of type ${tableSource.getClass.getCanonicalName} " +
        s"returned a DataStream of data type $producedDataType that does not match with the " +
        s"data type $producedDataType declared by the TableSource.getProducedDataType() method. " +
        s"Please validate the implementation of the TableSource.")
    }

    // get expression to extract rowtime attribute
    val rowtimeExpression: Option[RexNode] = TableSourceUtil.getRowtimeAttributeDescriptor(
      tableSource,
      tableSourceTable.getRowType
    ).map(desc =>
      TableSourceUtil.getRowtimeExtractionExpression(
        desc.getTimestampExtractor,
        producedDataType,
        planner.getRelBuilder,
        nameMapping
      )
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
      // the produced type may not carry the correct precision user defined in DDL, because
      // it may be converted from legacy type. Fix precision using logical schema from DDL.
      // Code generation requires the correct precision of input fields.
      val fixedProducedDataType = TableSourceUtil.fixPrecisionForProducedDataType(
        tableSource,
        FlinkTypeFactory.toLogicalRowType(tableSourceTable.getRowType))
      val conversionTransform = ScanUtil.convertToInternalRow(
        ctx,
        inputTransform.asInstanceOf[Transformation[Any]],
        fieldIndexes,
        fixedProducedDataType,
        getRowType,
        getTable.getQualifiedName,
        config,
        rowtimeExpression,
        beforeConvert = extractElement,
        afterConvert = resetElement)
      conversionTransform
    } else {
      inputTransform.asInstanceOf[Transformation[BaseRow]]
    }

    val ingestedTable = new DataStream(planner.getExecEnv, streamTransformation)

    // generate watermarks for rowtime indicator
    val rowtimeDescOption: Option[RowtimeAttributeDescriptor] =
      TableSourceUtil.getRowtimeAttributeDescriptor(tableSource, tableSourceTable.getRowType)

    val withWatermarks = rowtimeDescOption match {
      case Some(rowtimeDesc) =>
        val rowtimeFieldIdx = getRowType.getFieldNames.indexOf(rowtimeDesc.getAttributeName)
        val watermarkStrategy = rowtimeDesc.getWatermarkStrategy
        watermarkStrategy match {
          case p: PeriodicWatermarkAssigner =>
            val watermarkGenerator = new PeriodicWatermarkAssignerWrapper(rowtimeFieldIdx, p)
            ingestedTable.assignTimestampsAndWatermarks(watermarkGenerator)
          case p: PunctuatedWatermarkAssigner =>
            val watermarkGenerator =
              new PunctuatedWatermarkAssignerWrapper(rowtimeFieldIdx, p, producedDataType)
            ingestedTable.assignTimestampsAndWatermarks(watermarkGenerator)
          case _: PreserveWatermarks =>
            // The watermarks have already been provided by the underlying DataStream.
            ingestedTable
        }
      case None =>
        // No need to generate watermarks if no rowtime attribute is specified.
        ingestedTable
    }
    withWatermarks.getTransformation
  }

  private def needInternalConversion: Boolean = {
    val fieldIndexes = computeIndexMapping()
    ScanUtil.hasTimeAttributeField(fieldIndexes) ||
      ScanUtil.needsConversion(tableSource.getProducedDataType)
  }

  override def createInput[IN](
      env: StreamExecutionEnvironment,
      format: InputFormat[IN, _ <: InputSplit],
      t: TypeInformation[IN]): Transformation[IN] = {
    // See StreamExecutionEnvironment.createInput, it is better to deal with checkpoint.
    // The disadvantage is that streaming not support multi-paths.
    env.createInput(format, t).name(tableSource.explainSource()).getTransformation
  }

  private def computeIndexMapping()
    : Array[Int] = {
    TypeMappingUtils.computePhysicalIndicesOrTimeAttributeMarkers(
      tableSource,
      FlinkTypeFactory.toTableSchema(getRowType).getTableColumns,
      true,
      nameMapping
    )
  }

  private lazy val nameMapping: JFunction[String, String] = tableSource match {
    case mapping: DefinedFieldMapping if mapping.getFieldMapping != null =>
      new JFunction[String, String] {
        override def apply(t: String): String = mapping.getFieldMapping.get(t)
      }
    case _ => JFunction.identity()
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
    val timestamp: Long = row.getTimestamp(timeFieldIdx, 3).getMillisecond
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
    assigner: PunctuatedWatermarkAssigner,
    sourceType: DataType)
  extends AssignerWithPunctuatedWatermarks[BaseRow] {

  private val converter =
    DataFormatConverters.getConverterForDataType((sourceType match {
      case _: FieldsDataType => sourceType
      case _ => DataTypes.ROW(DataTypes.FIELD("f0", sourceType))
    }).bridgedTo(classOf[Row])).asInstanceOf[DataFormatConverter[BaseRow, Row]]

  override def checkAndGetNextWatermark(row: BaseRow, ts: Long): Watermark = {
    val timestamp: Long = row.getLong(timeFieldIdx)
    assigner.getWatermark(converter.toExternal(row), timestamp)
  }

  override def extractTimestamp(element: BaseRow, previousElementTimestamp: Long): Long = {
    0L
  }
}
