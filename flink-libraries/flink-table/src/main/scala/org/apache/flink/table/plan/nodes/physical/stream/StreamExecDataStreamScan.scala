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
import org.apache.flink.table.api.StreamTableEnvironment
import org.apache.flink.table.api.types.{DataTypes, RowType}
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.expressions.{Cast, Expression}
import org.apache.flink.table.plan.nodes.physical.FlinkPhysicalRel
import org.apache.flink.table.plan.schema.DataStreamTable

import org.apache.calcite.plan._
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.TableScan
import org.apache.calcite.rex.RexNode

/**
  * Flink RelNode which matches along with DataStreamSource.
  * It ensures that types without deterministic field order (e.g. POJOs) are not part of
  * the plan translation.
  */
class StreamExecDataStreamScan(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    table: RelOptTable,
    relDataType: RelDataType)
  extends TableScan(cluster, traitSet, table)
  with StreamPhysicalRel
  with StreamExecScan {

  val dataStreamTable: DataStreamTable[Any] = getTable.unwrap(classOf[DataStreamTable[Any]])

  override def deriveRowType(): RelDataType = relDataType

  def isAccRetract: Boolean = getTable.unwrap(classOf[DataStreamTable[Any]]).isAccRetract

  override def producesUpdates: Boolean =
    getTable.unwrap(classOf[DataStreamTable[Any]]).producesUpdates

  override def producesRetractions: Boolean = producesUpdates && isAccRetract

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    new StreamExecDataStreamScan(
      cluster,
      traitSet,
      getTable,
      relDataType
    )
  }

  override def isDeterministic: Boolean = true

  //~ ExecNode methods -----------------------------------------------------------

  override def getFlinkPhysicalRel: FlinkPhysicalRel = this

  override def translateToPlanInternal(
      tableEnv: StreamTableEnvironment): StreamTransformation[BaseRow] = {

    val config = tableEnv.getConfig
    val inputDataStream: DataStream[Any] = dataStreamTable.dataStream

    val rowtimeExpr: Option[RexNode] =
      getRowtimeExpression().map(_.toRexNode(tableEnv.getRelBuilder))

    convertToInternalRow(
      inputDataStream.getTransformation,
      dataStreamTable.fieldIndexes,
      getRowType,
      dataStreamTable.dataType,
      getTable.getQualifiedName,
      config,
      rowtimeExpr)
  }

  private def getRowtimeExpression(): Option[Expression] = {
    val fieldIdxs = dataStreamTable.fieldIndexes

    if (!fieldIdxs.contains(DataTypes.ROWTIME_STREAM_MARKER)) {
      None
    } else {

      val rowtimeField =
        dataStreamTable.fieldNames(fieldIdxs.indexOf(DataTypes.ROWTIME_STREAM_MARKER))

      // get expression to extract timestamp
      dataStreamTable.dataType.toInternalType match {
        case dataType: RowType
          if (dataType.getFieldNames.contains(rowtimeField) &&
              dataType.getInternalTypeAt(
                dataType.getFieldIndex(rowtimeField)).equals(DataTypes.ROWTIME_INDICATOR)) =>
          // if rowtimeField already existed in the data stream, use the default rowtime
          None
        case _ =>
          // extract timestamp from StreamRecord
          Some(
            Cast(
              org.apache.flink.table.expressions.StreamRecordTimestamp(),
              DataTypes.ROWTIME_INDICATOR))
      }
    }
  }

  override def needInternalConversion: Boolean = {
    // when there is row time extraction expression, we need internal conversion
    // when the physical type of the input date stream is not BaseRow, we need internal conversion.
    getRowtimeExpression().isDefined ||
      needsConversion(dataStreamTable.dataType, dataStreamTable.dataStream.getType.getTypeClass)
  }

  override private[flink] def getSourceTransformation(streamEnv: StreamExecutionEnvironment) =
    dataStreamTable.dataStream.getTransformation
}
