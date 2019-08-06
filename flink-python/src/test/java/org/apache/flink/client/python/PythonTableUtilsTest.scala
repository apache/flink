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

package org.apache.flink.client.python

import java.time.ZoneId
import java.util.TimeZone

import org.apache.flink.api.common.python.PythonTableUtils
import org.junit.Assert.assertEquals
import org.junit.Test

class PythonTableUtilsTest {

  @Test
  def testGetOffsetFromLocalMillis(): Unit = {
    def testOffset(localMillis: Long, expected: Long): Unit = {
      assertEquals(expected, PythonTableUtils.getOffsetFromLocalMillis(localMillis))
    }

    val EPOCH_JULIAN = 2440588
    val MILLIS_PER_DAY = 86400000
    val MILLIS_PER_HOUR = 3600000
    val MILLIS_PER_MINUTE = 60000
    val MILLIS_PER_SECOND = 1000

    def ymdToUnixDate(year: Int, month: Int, day: Int): Long = {
      val julian = ymdToJulian(year, month, day)
      julian - EPOCH_JULIAN
    }

    def ymdToJulian(year: Int, month: Int, day: Int): Long = {
      val a = (14 - month) / 12
      val y = year + 4800 - a
      val m = month + 12 * a - 3
      var j = day + (153 * m + 2) / 5 + 365 * y + y / 4 - y / 100 + y / 400 - 32045
      if (j < 2299161) j = day + (153 * m + 2) / 5 + 365 * y + y / 4 - 32083
      j
    }

    def dateToLocalMillis(year: Int, month: Int, day: Int, hour: Int, minute: Int, second: Int)
    : Long = {
      ymdToUnixDate(year, month, day) * MILLIS_PER_DAY + hour * MILLIS_PER_HOUR +
        minute * MILLIS_PER_MINUTE + second * MILLIS_PER_SECOND
    }

    val originalZone = TimeZone.getDefault
    try {
      // Daylight Saving Time Test
      TimeZone.setDefault(TimeZone.getTimeZone(ZoneId.of("PST", ZoneId.SHORT_IDS)))

      // 2018-03-11 01:59:59.0 PST
      testOffset(dateToLocalMillis(2018, 3, 11, 1, 59, 59), -28800000)

      // 2018-03-11 03:00:00.0 PST
      testOffset(dateToLocalMillis(2018, 3, 11, 3, 0, 0), -25200000)

      // 2018-11-04 00:59:59.0 PST
      testOffset(dateToLocalMillis(2018, 11, 4, 0, 59, 59), -25200000)

      // 2018-11-04 02:00:00.0 PST
      testOffset(dateToLocalMillis(2018, 11, 4, 2, 0, 0), -28800000)
    } finally {
      TimeZone.setDefault(originalZone)
    }
  }
}
