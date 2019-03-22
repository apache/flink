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

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.table.dataformat.{BaseRow, GenericRow}
import org.apache.flink.table.dataformat.util.BaseRowUtil
import org.apache.flink.table.typeutils.BaseRowTypeInfo
import org.apache.flink.util.{StringUtils, TimeConvertUtils}

import java.sql.{Date, Time, Timestamp}
import java.util.TimeZone

object TestSinkUtil {

  def fieldToString(field: Any, tz: TimeZone): String = {
    field match {
      case _: Date | _: Time | _: Timestamp =>
        TimeConvertUtils.unixDateTimeToString(field, tz)
      case _ => StringUtils.arrayAwareToString(field)
    }
  }

  def genericRowToString(row: GenericRow, tz: TimeZone, withHeader: Boolean = true): String = {
    val sb = StringBuilder.newBuilder
    if (withHeader) {
      sb.append(row.getHeader).append("|")
    }
    for (i <- 0 until row.getArity) {
      if (i > 0) {
        sb.append(",")
      }
      sb.append(fieldToString(row.getField(i), tz))
    }
    sb.toString
  }

  def baseRowToString(
    value: BaseRow,
    rowTypeInfo: BaseRowTypeInfo,
    tz: TimeZone,
    withHeader: Boolean = true): String = {
    val config = new ExecutionConfig
    val fieldTypes = rowTypeInfo.getFieldTypes
    val fieldSerializers = fieldTypes.map(_.createSerializer(config))
    val genericRow = BaseRowUtil.toGenericRow(value, fieldTypes, fieldSerializers)
    genericRowToString(genericRow, tz, withHeader)
  }

}
