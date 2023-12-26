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
package org.apache.flink.table.planner.utils

import org.apache.flink.table.data.util.DataFormatConverters.{LocalDateConverter, LocalTimeConverter}
import org.apache.flink.table.utils.DateTimeUtils

import java.time.{LocalDate, LocalDateTime, LocalTime, ZoneId}
import java.time.format.DateTimeFormatter

object DateTimeTestUtil {

  def localDate(s: String): LocalDate = {
    if (s == null) {
      null
    } else {
      LocalDateConverter.INSTANCE.toExternal(DateTimeUtils.parseDate(s))
    }
  }

  def localTime(s: String): LocalTime = {
    if (s == null) {
      null
    } else {
      LocalTimeConverter.INSTANCE.toExternal(DateTimeUtils.parseTime(s))
    }
  }

  def localDateTime(s: String): LocalDateTime = {
    if (s == null) {
      null
    } else {
      DateTimeUtils.parseTimestampData(s, 9).toLocalDateTime
    }
  }

  def toEpochMills(s: String, zone: ZoneId): Long = {
    LocalDateTime.parse(s).atZone(zone).toInstant.toEpochMilli
  }

  /** Returns the epoch millisecond using given datetime formatter and time zone id. */
  def toEpochMills(s: String, format: String, zone: ZoneId): Long = {
    LocalDateTime.parse(s, DateTimeFormatter.ofPattern(format)).atZone(zone).toInstant.toEpochMilli
  }

  /** Converts the given timestamp from `fromZone` to `toZone` using specific `format`. */
  def timezoneConvert(s: String, format: String, fromZone: ZoneId, toZone: ZoneId): String = {
    LocalDateTime
      .parse(s, DateTimeFormatter.ofPattern(format))
      .atZone(fromZone)
      .toInstant
      .atZone(toZone)
      .toLocalDateTime
      .format(DateTimeFormatter.ofPattern(format))
  }
}
