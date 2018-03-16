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
package org.apache.flink.table.runtime.functions

import org.joda.time.format.DateTimeFormatter
import org.joda.time.format.DateTimeFormatterBuilder

object DateTimeFunctions {
  private val PIVOT_YEAR = 2020

  private val DATETIME_FORMATTER_CACHE = new ThreadLocalCache[String, DateTimeFormatter](64) {
    protected override def getNewInstance(format: String): DateTimeFormatter
    = createDateTimeFormatter(format)
  }

  def dateFormat(ts: Long, formatString: String): String = {
    val formatter = DATETIME_FORMATTER_CACHE.get(formatString)
    formatter.print(ts)
  }

  def createDateTimeFormatter(format: String): DateTimeFormatter = {
    val builder = new DateTimeFormatterBuilder
    var escaped = false
    var i = 0
    while (i < format.length) {
      val character = format.charAt(i)
      i = i + 1
      if (escaped) {
        character match {
          // %a Abbreviated weekday name (Sun..Sat)
          case 'a' => builder.appendDayOfWeekShortText
          // %b Abbreviated month name (Jan..Dec)
          case 'b' => builder.appendMonthOfYearShortText
          // %c Month, numeric (0..12)
          case 'c' => builder.appendMonthOfYear(1)
          // %d Day of the month, numeric (00..31)
          case 'd' => builder.appendDayOfMonth(2)
          // %e Day of the month, numeric (0..31)
          case 'e' => builder.appendDayOfMonth(1)
          // %f Microseconds (000000..999999)
          case 'f' => builder.appendFractionOfSecond(6, 9)
          // %H Hour (00..23)
          case 'H' => builder.appendHourOfDay(2)
          case 'h' | 'I' => // %h Hour (01..12)
            builder.appendClockhourOfHalfday(2)
          // %i Minutes, numeric (00..59)
          case 'i' => builder.appendMinuteOfHour(2)
          // %j Day of year (001..366)
          case 'j' => builder.appendDayOfYear(3)
          // %k Hour (0..23)
          case 'k' => builder.appendHourOfDay(1)
          // %l Hour (1..12)
          case 'l' => builder.appendClockhourOfHalfday(1)
          // %M Month name (January..December)
          case 'M' => builder.appendMonthOfYearText
          // %m Month, numeric (00..12)
          case 'm' => builder.appendMonthOfYear(2)
          // %p AM or PM
          case 'p' => builder.appendHalfdayOfDayText
          // %r Time, 12-hour (hh:mm:ss followed by AM or PM)
          case 'r' => builder.appendClockhourOfHalfday(2).appendLiteral(':').
            appendMinuteOfHour(2).appendLiteral(':').appendSecondOfMinute(2).
            appendLiteral(' ').appendHalfdayOfDayText
          // %S Seconds (00..59)
          case 'S' | 's' => builder.appendSecondOfMinute(2)
          // %T Time, 24-hour (hh:mm:ss)
          case 'T' => builder.appendHourOfDay(2).appendLiteral(':').
            appendMinuteOfHour(2).appendLiteral(':').appendSecondOfMinute(2)
          // %v Week (01..53), where Monday is the first day of the week; used with %x
          case 'v' => builder.appendWeekOfWeekyear(2)
          // %x Year for the week, where Monday is the first day of the week, numeric,
          // four digits; used with %v
          case 'x' => builder.appendWeekyear(4, 4)
          // %W Weekday name (Sunday..Saturday)
          case 'W' => builder.appendDayOfWeekText
          // %Y Year, numeric, four digits
          case 'Y' => builder.appendYear(4, 4)
          // %y Year, numeric (two digits)
          case 'y' => builder.appendTwoDigitYear(PIVOT_YEAR)

          // %w Day of the week (0=Sunday..6=Saturday)
          // %U Week (00..53), where Sunday is the first day of the week
          // %u Week (00..53), where Monday is the first day of the week
          // %V Week (01..53), where Sunday is the first day of the week; used with %X
          // %X Year for the week where Sunday is the first day of the
          // week, numeric, four digits; used with %V
          // %D Day of the month with English suffix (0th, 1st, 2nd, 3rd, ...)
          case 'w' | 'U' | 'u' | 'V' | 'X' | 'D' =>
            throw new UnsupportedOperationException(
              s"%%$character not supported in date format string")
          // %% A literal "%" character
          case '%' => builder.appendLiteral('%')
          // %<x> The literal character represented by <x>
          case _ => builder.appendLiteral(character)
        }
        escaped = false
      }
      else if (character == '%') { escaped = true }
      else { builder.appendLiteral(character) }
    }
    builder.toFormatter
  }
}
