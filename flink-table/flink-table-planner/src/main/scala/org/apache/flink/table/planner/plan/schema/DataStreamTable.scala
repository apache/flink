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

package org.apache.flink.table.planner.plan.schema

import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.table.api.TableException
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.plan.stats.FlinkStatistic
import org.apache.flink.table.runtime.types.LogicalTypeDataTypeConverter
import org.apache.flink.table.types.DataType
import org.apache.flink.table.types.logical.{LocalZonedTimestampType, LogicalType, RowType, TimestampKind, TimestampType}
import org.apache.flink.table.types.utils.TypeConversions.fromLegacyInfoToDataType
import org.apache.flink.table.typeutils.TimeIndicatorTypeInfo

import org.apache.calcite.plan.RelOptSchema
import org.apache.calcite.rel.`type`.RelDataType

import java.util.{List => JList}

/**
  * The class that wraps [[DataStream]] as a Calcite Table.
  */
class DataStreamTable[T](
    relOptSchema: RelOptSchema,
    names: JList[String],
    rowType: RelDataType,
    val dataStream: DataStream[T],
    val fieldIndexes: Array[Int],
    val fieldNames: Array[String],
    statistic: FlinkStatistic = FlinkStatistic.UNKNOWN,
    fieldNullables: Option[Array[Boolean]] = None)
  extends FlinkPreparingTableBase(relOptSchema, rowType, names, statistic) {

  if (fieldIndexes.length != fieldNames.length) {
    throw new TableException(
      s"Number of field names and field indexes must be equal.\n" +
        s"Number of names is ${fieldNames.length}, number of indexes is ${fieldIndexes.length}.\n" +
        s"List of column names: ${fieldNames.mkString("[", ", ", "]")}.\n" +
        s"List of column indexes: ${fieldIndexes.mkString("[", ", ", "]")}.")
  }

  // check uniqueness of field names
  if (fieldNames.length != fieldNames.toSet.size) {
    val duplicateFields = fieldNames
      // count occurrences of field names
      .groupBy(identity).mapValues(_.length)
      // filter for occurrences > 1 and map to field name
      .filter(g => g._2 > 1).keys

    throw new TableException(
      s"Field names must be unique.\n" +
        s"List of duplicate fields: ${duplicateFields.mkString("[", ", ", "]")}.\n" +
        s"List of all fields: ${fieldNames.mkString("[", ", ", "]")}.")
  }

  val dataType: DataType = fromLegacyInfoToDataType(dataStream.getType)

  val fieldTypes: Array[LogicalType] = DataStreamTable.getFieldLogicalTypes(dataType,
    fieldIndexes, fieldNames)
}

object DataStreamTable {

  def getFieldLogicalTypes(rowType: DataType,
      fieldIndexes: Array[Int],
      fieldNames: Array[String]): Array[LogicalType] = {
    LogicalTypeDataTypeConverter.fromDataTypeToLogicalType(rowType) match {
      case rt: RowType =>
        // it is ok to leave out fields
        if (fieldIndexes.count(_ >= 0) > rt.getFieldCount) {
          throw new TableException(
            s"Arity of type (" + rt.getFieldNames.toArray.deep + ") " +
              "must not be greater than number of field names " + fieldNames.deep + ".")
        }
        fieldIndexes.map {
          case TimeIndicatorTypeInfo.ROWTIME_STREAM_MARKER =>
            new TimestampType(true, TimestampKind.ROWTIME, 3)
          case TimeIndicatorTypeInfo.PROCTIME_STREAM_MARKER =>
            new LocalZonedTimestampType(true, TimestampKind.PROCTIME, 3)
          case TimeIndicatorTypeInfo.ROWTIME_BATCH_MARKER =>
            new TimestampType(3)
          case TimeIndicatorTypeInfo.PROCTIME_BATCH_MARKER =>
            new LocalZonedTimestampType(3)
          case i => rt.getTypeAt(i)
        }

      case t: LogicalType =>
        var cnt = 0
        val types = fieldIndexes.map {
          case TimeIndicatorTypeInfo.ROWTIME_STREAM_MARKER =>
            new TimestampType(true, TimestampKind.ROWTIME, 3)
          case TimeIndicatorTypeInfo.PROCTIME_STREAM_MARKER =>
            new LocalZonedTimestampType(true, TimestampKind.PROCTIME, 3)
          case TimeIndicatorTypeInfo.ROWTIME_BATCH_MARKER =>
            new TimestampType(3)
          case TimeIndicatorTypeInfo.PROCTIME_BATCH_MARKER =>
            new LocalZonedTimestampType(3)
          case _ =>
            cnt += 1
            t
        }
        // ensure that the atomic type is matched at most once.
        if (cnt > 1) {
          throw new TableException(
            "Non-composite input type may have only a single field and its index must be 0.")
        } else {
          types
        }
    }
  }

  def getRowType(typeFactory: FlinkTypeFactory,
      dataStream: DataStream[_],
      fieldNames: Array[String],
      fieldIndexes: Array[Int],
      fieldNullables: Option[Array[Boolean]]): RelDataType = {
    val dataType = fromLegacyInfoToDataType(dataStream.getType)
    val fieldTypes = getFieldLogicalTypes(dataType, fieldIndexes, fieldNames)

    fieldNullables match {
      case Some(nulls) => typeFactory.asInstanceOf[FlinkTypeFactory]
        .buildRelNodeRowType(fieldNames, fieldTypes.zip(nulls).map {
          case (t, nullable) => t.copy(nullable)
        })
      case _ => typeFactory.asInstanceOf[FlinkTypeFactory]
        .buildRelNodeRowType(fieldNames, fieldTypes)
    }
  }
}
