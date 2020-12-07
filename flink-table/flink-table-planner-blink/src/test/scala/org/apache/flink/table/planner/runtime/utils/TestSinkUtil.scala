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

package org.apache.flink.table.planner.runtime.utils

import org.apache.flink.table.api.{Table, TableException}
import org.apache.flink.table.data.GenericRowData
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.runtime.utils.JavaPojos.Pojo1
import org.apache.flink.table.planner.utils.TableTestUtil
import org.apache.flink.table.runtime.types.TypeInfoLogicalTypeConverter
import org.apache.flink.table.sinks.TableSink
import org.apache.flink.table.utils.EncodingUtils
import org.apache.flink.types.Row
import org.apache.calcite.avatica.util.DateTimeUtils
import java.sql.{Date, Time, Timestamp}
import java.util.{Calendar, TimeZone}

import org.apache.flink.util.StringUtils

import scala.collection.JavaConverters._

object TestSinkUtil {

  def configureSink[T <: TableSink[_]](table: Table, sink: T): T = {
    val rowType = TableTestUtil.toRelNode(table).getRowType
    val fieldNames = rowType.getFieldNames.asScala.toArray
    val fieldTypes = rowType.getFieldList.asScala
      .map(field => FlinkTypeFactory.toLogicalType(field.getType))
      .map(TypeInfoLogicalTypeConverter.fromLogicalTypeToTypeInfo).toArray
    sink match {
      case _: TestingAppendTableSink =>
        new TestingAppendTableSink().configure(fieldNames, fieldTypes).asInstanceOf[T]
      case s: TestingUpsertTableSink =>
        new TestingUpsertTableSink(s.keys, s.tz).configure(fieldNames, fieldTypes).asInstanceOf[T]
      case _: TestingRetractTableSink =>
        new TestingRetractTableSink().configure(fieldNames, fieldTypes).asInstanceOf[T]
      case _ => throw new TableException(s"Unsupported sink: $sink")
    }
  }

  def fieldToString(field: Any, tz: TimeZone): String = {
    field match {
      case _: Date | _: Time | _: Timestamp =>
        unixDateTimeToString(field, tz)
      case _ => StringUtils.arrayAwareToString(field)
    }
  }

  def rowToString(row: Row, tz: TimeZone = TimeZone.getTimeZone("UTC")): String = {
    val sb = StringBuilder.newBuilder
    for (i <- 0 until row.getArity ) {
      if (i > 0) {
        sb.append(",")
      }
      sb.append(fieldToString(row.getField(i), tz))
    }
    sb.toString
  }

  def genericRowToString(row: GenericRowData, tz: TimeZone): String = {
    val sb = StringBuilder.newBuilder
    sb.append(row.getRowKind.shortString).append("(")
    for (i <- 0 until row.getArity) {
      if (i > 0) {
        sb.append(",")
      }
      sb.append(fieldToString(row.getField(i), tz))
    }
    sb.append(")")
    sb.toString
  }

  def pojoToString(pojo: Pojo1, tz: TimeZone): String = {
    return "Pojo1{" + "ts=" + fieldToString(pojo.ts, tz) + ", msg='" + pojo.msg + "\'}"
  }

  def unixDateTimeToString(value: Any, tz: TimeZone): String = {
    val offset =
      if (tz.useDaylightTime()) {
        tz.getOffset(value.asInstanceOf[java.util.Date].getTime)
      } else {
        tz.getOffset(Calendar.ZONE_OFFSET)
      }
    val time = value match {
      case _: java.util.Date =>
        val origin = value.asInstanceOf[java.util.Date].getTime
        origin + DateTimeUtils.UTC_ZONE.getOffset(origin)
    }

    value match {
      case _: Date =>
        DateTimeUtils.unixDateToString(
          (time / DateTimeUtils.MILLIS_PER_DAY).asInstanceOf[Int] + offset)
      case _: Time =>
        DateTimeUtils.unixTimeToString(
          ((time % DateTimeUtils.MILLIS_PER_DAY).asInstanceOf[Int] + offset)
            % DateTimeUtils.MILLIS_PER_DAY.asInstanceOf[Int]
        )
      case _: Timestamp =>
        DateTimeUtils.unixTimestampToString(time + offset, 3)
      case _ =>
        value.toString
    }

  }
}
