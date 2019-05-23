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

package org.apache.flink.table.runtime.utils

import org.apache.flink.table.`type`.TypeConverters.createExternalTypeInfoFromInternalType
import org.apache.flink.table.api.{Table, TableImpl}
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.dataformat.GenericRow
import org.apache.flink.table.runtime.utils.JavaPojos.Pojo1
import org.apache.flink.table.sinks.TableSink
import org.apache.flink.types.Row
import org.apache.flink.util.StringUtils

import org.apache.calcite.avatica.util.DateTimeUtils

import java.sql.{Date, Time, Timestamp}
import java.util.TimeZone

import scala.collection.JavaConverters._

object TestSinkUtil {

  def configureSink[T <: TableSink[_]](table: Table, sink: T): T = {
    val rowType = table.asInstanceOf[TableImpl].getRelNode.getRowType
    val fieldNames = rowType.getFieldNames.asScala.toArray
    val fieldTypes = rowType.getFieldList.asScala
        .map(field => FlinkTypeFactory.toInternalType(field.getType))
        .map(createExternalTypeInfoFromInternalType).toArray
    new TestingAppendTableSink().configure(fieldNames, fieldTypes).asInstanceOf[T]
  }

  def fieldToString(field: Any, tz: TimeZone): String = {
    field match {
      case _: Date | _: Time | _: Timestamp =>
        DateTimeUtils.unixDateTimeToString(field, tz)
      case _ => StringUtils.arrayAwareToString(field)
    }
  }

  def rowToString(row: Row, tz: TimeZone): String = {
    val sb = StringBuilder.newBuilder
    for (i <- 0 until row.getArity ) {
      if (i > 0) {
        sb.append(",")
      }
      sb.append(fieldToString(row.getField(i), tz))
    }
    sb.toString
  }

  def genericRowToString(row: GenericRow, tz: TimeZone): String = {
    val sb = StringBuilder.newBuilder
    sb.append(row.getHeader).append("|")
    for (i <- 0 until row.getArity) {
      if (i > 0) {
        sb.append(",")
      }
      sb.append(fieldToString(row.getField(i), tz))
    }
    sb.toString
  }

  def pojoToString(pojo: Pojo1, tz: TimeZone): String = {
    return "Pojo1{" + "ts=" + fieldToString(pojo.ts, tz) + ", msg='" + pojo.msg + "\'}"
  }

}
