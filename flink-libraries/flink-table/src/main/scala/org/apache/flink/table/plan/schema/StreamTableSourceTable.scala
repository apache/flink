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

package org.apache.flink.table.plan.schema

import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeFactory}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.api.{TableEnvironment, TableException, Types}
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.plan.stats.FlinkStatistic
import org.apache.flink.table.sources.{DefinedProctimeAttribute, DefinedRowtimeAttribute, StreamTableSource, TableSource}
import org.apache.flink.table.typeutils.TimeIndicatorTypeInfo

class StreamTableSourceTable[T](
    override val tableSource: TableSource[T],
    override val statistic: FlinkStatistic = FlinkStatistic.UNKNOWN)
  extends TableSourceTable[T](
    tableSource,
    StreamTableSourceTable.adjustFieldIndexes(tableSource),
    StreamTableSourceTable.adjustFieldNames(tableSource),
    statistic) {

  override def getRowType(typeFactory: RelDataTypeFactory): RelDataType = {
    val fieldTypes = StreamTableSourceTable.adjustFieldTypes(tableSource)

    val flinkTypeFactory = typeFactory.asInstanceOf[FlinkTypeFactory]
    flinkTypeFactory.buildLogicalRowType(
      this.fieldNames,
      fieldTypes)
  }

}

object StreamTableSourceTable {

  private def adjustFieldIndexes(tableSource: TableSource[_]): Array[Int] = {
    val (_, proctime) = getTimeIndicators(tableSource)

    val original = TableEnvironment.getFieldIndices(tableSource)

    // append proctime marker
    if (proctime.isDefined) {
      original :+ TimeIndicatorTypeInfo.PROCTIME_MARKER
    } else {
      original
    }
  }

  private def adjustFieldNames(tableSource: TableSource[_]): Array[String] = {
    val (_, proctime) = getTimeIndicators(tableSource)

    val original = TableEnvironment.getFieldNames(tableSource)

    // append proctime field
    if (proctime.isDefined) {
      original :+ proctime.get
    } else {
      original
    }
  }

  private def adjustFieldTypes(tableSource: TableSource[_]): Array[TypeInformation[_]] = {
    val (rowtime, proctime) = StreamTableSourceTable.getTimeIndicators(tableSource)

    val original = TableEnvironment.getFieldTypes(tableSource.getReturnType)

    // update rowtime type
    val withRowtime = if (rowtime.isDefined) {
      // replace field type by RowtimeIndicator type
      val rowtimeIdx = TableEnvironment.getFieldNames(tableSource).indexOf(rowtime.get)
      original.patch(rowtimeIdx, Seq(TimeIndicatorTypeInfo.ROWTIME_INDICATOR), 1)
    } else {
      original
    }

    // append proctime type
    val withProctime = if (proctime.isDefined) {
      withRowtime :+ TimeIndicatorTypeInfo.PROCTIME_INDICATOR
    } else {
      withRowtime
    }

    withProctime.asInstanceOf[Array[TypeInformation[_]]]
  }

  private def getTimeIndicators(tableSource: TableSource[_]): (Option[String], Option[String]) = {

    val fieldNames = TableEnvironment.getFieldNames(tableSource).toList
    val fieldTypes = TableEnvironment.getFieldTypes(tableSource.getReturnType).toList

    val rowtime: Option[String] = tableSource match {
      case timeSource: DefinedRowtimeAttribute if timeSource.getRowtimeAttribute == null =>
        None
      case timeSource: DefinedRowtimeAttribute if timeSource.getRowtimeAttribute.trim.equals("") =>
        throw TableException("The name of the rowtime attribute must not be empty.")

      case timeSource: DefinedRowtimeAttribute =>
        // validate the rowtime field exists and is of type Long or Timestamp
        val rowtimeAttribute = timeSource.getRowtimeAttribute
        val rowtimeIdx = fieldNames.indexOf(rowtimeAttribute)

        if (rowtimeIdx < 0) {
          throw TableException(
            s"Rowtime field '$rowtimeAttribute' is not present in TableSource. " +
            s"Available fields are ${fieldNames.mkString("[", ", ", "]") }.")
        }
        val fieldType = fieldTypes(rowtimeIdx)
        if (fieldType != Types.LONG && fieldType != Types.SQL_TIMESTAMP) {
          throw TableException(
            s"Rowtime field '$rowtimeAttribute' must be of type Long or Timestamp " +
            s"but of type ${fieldTypes(rowtimeIdx)}.")
        }
        Some(rowtimeAttribute)
      case _ =>
        None
    }

    val proctime: Option[String] = tableSource match {
      case timeSource : DefinedProctimeAttribute if timeSource.getProctimeAttribute == null =>
        None
      case timeSource: DefinedProctimeAttribute
        if timeSource.getProctimeAttribute.trim.equals("") =>
        throw TableException("The name of the rowtime attribute must not be empty.")
      case timeSource: DefinedProctimeAttribute =>
        val proctimeAttribute = timeSource.getProctimeAttribute
        Some(proctimeAttribute)
      case _ =>
        None
    }
    (rowtime, proctime)
  }

  def deriveRowTypeOfTableSource(
    tableSource: StreamTableSource[_],
    typeFactory: FlinkTypeFactory): RelDataType = {

    val fieldNames = adjustFieldNames(tableSource)
    val fieldTypes = adjustFieldTypes(tableSource)

    typeFactory.buildLogicalRowType(fieldNames, fieldTypes)
  }
}
