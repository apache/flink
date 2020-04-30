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

package org.apache.flink.table.util.python

import java.time.ZoneId
import java.util.TimeZone

import org.apache.calcite.avatica.util.DateTimeUtils
import org.junit.Test
import org.junit.Assert.assertEquals

class PythonTableUtilsTest {

  @Test
  def testGetOffsetFromLocalMillis(): Unit = {
    def testOffset(localMillis: Long, expected: Long): Unit = {
      assertEquals(expected, PythonTableUtils.getOffsetFromLocalMillis(localMillis))
    }

    val originalZone = TimeZone.getDefault
    try {
      // Daylight Saving Time Test
      TimeZone.setDefault(TimeZone.getTimeZone(ZoneId.of("PST", ZoneId.SHORT_IDS)))

      // 2018-03-11 01:59:59.0 PST
      testOffset(DateTimeUtils.timestampStringToUnixDate("2018-03-11 01:59:59.0"), -28800000)

      // 2018-03-11 03:00:00.0 PST
      testOffset(DateTimeUtils.timestampStringToUnixDate("2018-03-11 03:00:00.0"), -25200000)

      // 2018-11-04 00:59:59.0 PST
      testOffset(DateTimeUtils.timestampStringToUnixDate("2018-11-04 00:59:59.0"), -25200000)

      // 2018-11-04 02:00:00.0 PST
      testOffset(DateTimeUtils.timestampStringToUnixDate("2018-11-04 02:00:00.0"), -28800000)
    } finally {
      TimeZone.setDefault(originalZone)
    }
  }
}
