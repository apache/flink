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
import org.apache.flink.streaming.api.transformations.StreamTransformation
import org.apache.flink.table.`type`.TypeConverters.createInternalTypeFromTypeInfo
import org.apache.flink.table.`type`.{InternalTypes, RowType}
import org.apache.flink.table.api.StreamTableEnvironment
import org.apache.flink.table.calcite.FlinkRelBuilder
import org.apache.flink.table.codegen.CodeGeneratorContext
import org.apache.flink.table.codegen.OperatorCodeGenerator.ELEMENT
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.functions.sql.StreamRecordTimestampSqlFunction
import org.apache.flink.table.plan.nodes.exec.{ExecNode, StreamExecNode}
import org.apache.flink.table.plan.schema.DataStreamTable
import org.apache.flink.table.plan.util.ScanUtil
import org.apache.flink.table.runtime.AbstractProcessStreamOperator
import org.apache.flink.table.typeutils.TimeIndicatorTypeInfo.{ROWTIME_INDICATOR, ROWTIME_STREAM_MARKER}

import org.apache.calcite.plan._
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.TableScan
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rel.{RelNode, RelWriter}
import org.apache.calcite.rex.RexNode

import java.util

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

/**
  * Flink RelNode which matches along with DataStreamSource.
  * It ensures that types without deterministic field order (e.g. POJOs) are not part of
  * the plan translation.
  */
class StreamExecDataStreamScan(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    table: RelOptTable,
    outputRowType: RelDataType)
  extends TableScan(cluster, traitSet, table)
  with StreamPhysicalRel
  with StreamExecNode[BaseRow]{

  val dataStreamTable: DataStreamTable[Any] = getTable.unwrap(classOf[DataStreamTable[Any]])

  def isAccRetract: Boolean = getTable.unwrap(classOf[DataStreamTable[Any]]).isAccRetract

  override def producesUpdates: Boolean = dataStreamTable.producesUpdates

  override def needsUpdatesAsRetraction(input: RelNode): Boolean = false

  override def consumesRetractions: Boolean = false

  override def producesRetractions: Boolean = producesUpdates && isAccRetract

  override def requireWatermark: Boolean = false

  override def deriveRowType(): RelDataType = outputRowType

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    new StreamExecDataStreamScan(cluster, traitSet, getTable, getRowType)
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

  override def getInputNodes: util.List[ExecNode[StreamTableEnvironment, _]] = List()

  override def replaceInputNode(
      ordinalInParent: Int,
      newInputNode: ExecNode[StreamTableEnvironment, _]): Unit = {
    replaceInput(ordinalInParent, newInputNode.asInstanceOf[RelNode])
  }

  override protected def translateToPlanInternal(
      tableEnv: StreamTableEnvironment): StreamTransformation[BaseRow] = {
    val config = tableEnv.getConfig
    val inputDataStream: DataStream[Any] = dataStreamTable.dataStream
    val transform = inputDataStream.getTransformation

    val rowtimeExpr = getRowtimeExpression(tableEnv.getRelBuilder)

    // when there is row time extraction expression, we need internal conversion
    // when the physical type of the input date stream is not BaseRow, we need internal conversion.
    if (rowtimeExpr.isDefined || ScanUtil.needsConversion(
      dataStreamTable.typeInfo, dataStreamTable.dataStream.getType.getTypeClass)) {

      // extract time if the index is -1 or -2.
      val (extractElement, resetElement) =
        if (ScanUtil.hasTimeAttributeField(dataStreamTable.fieldIndexes)) {
          (s"ctx.$ELEMENT = $ELEMENT;", s"ctx.$ELEMENT = null;")
        } else {
          ("", "")
        }
      val ctx = CodeGeneratorContext(config).setOperatorBaseClass(
        classOf[AbstractProcessStreamOperator[BaseRow]])
      ScanUtil.convertToInternalRow(
        ctx,
        transform,
        dataStreamTable.fieldIndexes,
        dataStreamTable.typeInfo,
        getRowType,
        getTable.getQualifiedName,
        config,
        rowtimeExpr,
        beforeConvert = extractElement,
        afterConvert = resetElement)
    } else {
      transform.asInstanceOf[StreamTransformation[BaseRow]]
    }
  }

  private def getRowtimeExpression(relBuilder: FlinkRelBuilder): Option[RexNode] = {
    val fieldIdxs = dataStreamTable.fieldIndexes

    if (!fieldIdxs.contains(ROWTIME_STREAM_MARKER)) {
      None
    } else {
      val rowtimeField = dataStreamTable.fieldNames(
        fieldIdxs.indexOf(ROWTIME_STREAM_MARKER))

      // get expression to extract timestamp
      createInternalTypeFromTypeInfo(dataStreamTable.typeInfo) match {
        case dataType: RowType
          if dataType.getFieldNames.contains(rowtimeField) &&
              dataType.getTypeAt(
                dataType.getFieldIndex(rowtimeField)).equals(ROWTIME_INDICATOR) =>
          // if rowtimeField already existed in the data stream, use the default rowtime
          None
        case _ =>
          // extract timestamp from StreamRecord
          Some(
            relBuilder.cast(
              relBuilder.call(new StreamRecordTimestampSqlFunction),
              relBuilder.getTypeFactory.createTypeFromInternalType(
                InternalTypes.ROWTIME_INDICATOR, isNullable = true).getSqlTypeName))
      }
    }
  }

}
