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

package org.apache.flink.table.planner.expressions

import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.table.api._
import org.apache.flink.table.planner.expressions.utils.ExpressionTestBase
import org.apache.flink.types.Row

import org.junit.Test

/**
  * Tests that check all non-deterministic functions can be executed.
  */
class NonDeterministicTests extends ExpressionTestBase {

  @Test
  def testCurrentDate(): Unit = {
    testAllApis(
      currentDate().isGreater("1970-01-01".toDate),
      "currentDate() > '1970-01-01'.toDate",
      "CURRENT_DATE > DATE '1970-01-01'",
      "true")
  }

  @Test
  def testCurrentTime(): Unit = {
    testAllApis(
      currentTime().isGreaterOrEqual("00:00:00".toTime),
      "currentTime() >= '00:00:00'.toTime",
      "CURRENT_TIME >= TIME '00:00:00'",
      "true")
  }

  @Test
  def testCurrentTimestamp(): Unit = {
    testAllApis(
      currentTimestamp().isGreater("1970-01-01 00:00:00".toTimestamp),
      "currentTimestamp() > '1970-01-01 00:00:00'.toTimestamp",
      "CURRENT_TIMESTAMP > TIMESTAMP '1970-01-01 00:00:00'",
      "true")
  }

  @Test
  def testLocalTimestamp(): Unit = {
    testAllApis(
      localTimestamp().isGreater("1970-01-01 00:00:00".toTimestamp),
      "localTimestamp() > '1970-01-01 00:00:00'.toTimestamp",
      "LOCALTIMESTAMP > TIMESTAMP '1970-01-01 00:00:00'",
      "true")
  }

  @Test
  def testLocalTime(): Unit = {
    testAllApis(
      localTime().isGreaterOrEqual("00:00:00".toTime),
      "localTime() >= '00:00:00'.toTime",
      "LOCALTIME >= TIME '00:00:00'",
      "true")
  }

  @Test
  def testUUID(): Unit = {
    testAllApis(
      uuid().charLength(),
      "uuid().charLength",
      "CHARACTER_LENGTH(UUID())",
      "36")
  }

  // ----------------------------------------------------------------------------------------------

  override def testData: Row = new Row(0)

  override def typeInfo: RowTypeInfo = new RowTypeInfo()
}
