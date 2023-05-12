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
package org.apache.flink.table.planner.runtime.batch.sql

import org.apache.flink.api.common.typeinfo.BasicTypeInfo.{INT_TYPE_INFO, LONG_TYPE_INFO, STRING_TYPE_INFO}
import org.apache.flink.api.common.typeinfo.LocalTimeTypeInfo.LOCAL_DATE_TIME
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.table.planner.runtime.utils.BatchTestBase
import org.apache.flink.table.planner.runtime.utils.BatchTestBase.row
import org.apache.flink.table.planner.runtime.utils.TestData._
import org.apache.flink.table.planner.utils.DateTimeTestUtil.localDateTime

import org.junit.jupiter.api.{BeforeEach, Test}

class WindowTableFunctionITCase extends BatchTestBase {

  @BeforeEach
  override def before(): Unit = {
    super.before()
    registerCollection(
      "Table3WithTimestamp",
      data3WithTimestamp,
      type3WithTimestamp,
      "a, b, c, ts",
      nullablesOfData3WithTimestamp)
  }

  @Test
  def testTumbleTVF(): Unit = {
    checkResult(
      """
        |SELECT *
        |FROM TABLE(TUMBLE(TABLE Table3WithTimestamp, DESCRIPTOR(ts), INTERVAL '3' SECOND))
        |""".stripMargin,
      Seq(
        row(
          2,
          2,
          "Hello",
          localDateTime("1970-01-01 00:00:02"),
          localDateTime("1970-01-01 00:00:00"),
          localDateTime("1970-01-01 00:00:03"),
          localDateTime("1970-01-01 00:00:02.999")
        ),
        row(
          1,
          1,
          "Hi",
          localDateTime("1970-01-01 00:00:01"),
          localDateTime("1970-01-01 00:00:00"),
          localDateTime("1970-01-01 00:00:03"),
          localDateTime("1970-01-01 00:00:02.999")
        ),
        row(
          3,
          2,
          "Hello world",
          localDateTime("1970-01-01 00:00:03"),
          localDateTime("1970-01-01 00:00:03"),
          localDateTime("1970-01-01 00:00:06"),
          localDateTime("1970-01-01 00:00:05.999")
        ),
        row(
          4,
          3,
          "Hello world, how are you?",
          localDateTime("1970-01-01 00:00:04"),
          localDateTime("1970-01-01 00:00:03"),
          localDateTime("1970-01-01 00:00:06"),
          localDateTime("1970-01-01 00:00:05.999")
        ),
        row(
          5,
          3,
          "I am fine.",
          localDateTime("1970-01-01 00:00:05"),
          localDateTime("1970-01-01 00:00:03"),
          localDateTime("1970-01-01 00:00:06"),
          localDateTime("1970-01-01 00:00:05.999")
        ),
        row(
          6,
          3,
          "Luke Skywalker",
          localDateTime("1970-01-01 00:00:06"),
          localDateTime("1970-01-01 00:00:06"),
          localDateTime("1970-01-01 00:00:09"),
          localDateTime("1970-01-01 00:00:08.999")
        ),
        row(
          7,
          4,
          "Comment#1",
          localDateTime("1970-01-01 00:00:07"),
          localDateTime("1970-01-01 00:00:06"),
          localDateTime("1970-01-01 00:00:09"),
          localDateTime("1970-01-01 00:00:08.999")
        ),
        row(
          8,
          4,
          "Comment#2",
          localDateTime("1970-01-01 00:00:08"),
          localDateTime("1970-01-01 00:00:06"),
          localDateTime("1970-01-01 00:00:09"),
          localDateTime("1970-01-01 00:00:08.999")
        ),
        row(
          9,
          4,
          "Comment#3",
          localDateTime("1970-01-01 00:00:09"),
          localDateTime("1970-01-01 00:00:09"),
          localDateTime("1970-01-01 00:00:12"),
          localDateTime("1970-01-01 00:00:11.999")
        ),
        row(
          10,
          4L,
          "Comment#4",
          localDateTime("1970-01-01 00:00:10"),
          localDateTime("1970-01-01 00:00:09"),
          localDateTime("1970-01-01 00:00:12"),
          localDateTime("1970-01-01 00:00:11.999")
        ),
        row(
          11,
          5,
          "Comment#5",
          localDateTime("1970-01-01 00:00:11"),
          localDateTime("1970-01-01 00:00:09"),
          localDateTime("1970-01-01 00:00:12"),
          localDateTime("1970-01-01 00:00:11.999")
        ),
        row(
          12,
          5,
          "Comment#6",
          localDateTime("1970-01-01 00:00:12"),
          localDateTime("1970-01-01 00:00:12"),
          localDateTime("1970-01-01 00:00:15"),
          localDateTime("1970-01-01 00:00:14.999")
        ),
        row(
          13,
          5,
          "Comment#7",
          localDateTime("1970-01-01 00:00:13"),
          localDateTime("1970-01-01 00:00:12"),
          localDateTime("1970-01-01 00:00:15"),
          localDateTime("1970-01-01 00:00:14.999")
        ),
        row(
          15,
          5,
          "Comment#9",
          localDateTime("1970-01-01 00:00:15"),
          localDateTime("1970-01-01 00:00:15"),
          localDateTime("1970-01-01 00:00:18"),
          localDateTime("1970-01-01 00:00:17.999")
        ),
        row(
          14,
          5,
          "Comment#8",
          localDateTime("1970-01-01 00:00:14"),
          localDateTime("1970-01-01 00:00:12"),
          localDateTime("1970-01-01 00:00:15"),
          localDateTime("1970-01-01 00:00:14.999")
        ),
        row(
          16,
          6,
          "Comment#10",
          localDateTime("1970-01-01 00:00:16"),
          localDateTime("1970-01-01 00:00:15"),
          localDateTime("1970-01-01 00:00:18"),
          localDateTime("1970-01-01 00:00:17.999")
        ),
        row(
          17,
          6,
          "Comment#11",
          localDateTime("1970-01-01 00:00:17"),
          localDateTime("1970-01-01 00:00:15"),
          localDateTime("1970-01-01 00:00:18"),
          localDateTime("1970-01-01 00:00:17.999")
        ),
        row(
          18,
          6,
          "Comment#12",
          localDateTime("1970-01-01 00:00:18"),
          localDateTime("1970-01-01 00:00:18"),
          localDateTime("1970-01-01 00:00:21"),
          localDateTime("1970-01-01 00:00:20.999")
        ),
        row(
          19,
          6,
          "Comment#13",
          localDateTime("1970-01-01 00:00:19"),
          localDateTime("1970-01-01 00:00:18"),
          localDateTime("1970-01-01 00:00:21"),
          localDateTime("1970-01-01 00:00:20.999")
        ),
        row(
          20,
          6,
          "Comment#14",
          localDateTime("1970-01-01 00:00:20"),
          localDateTime("1970-01-01 00:00:18"),
          localDateTime("1970-01-01 00:00:21"),
          localDateTime("1970-01-01 00:00:20.999")
        ),
        row(
          21,
          6,
          "Comment#15",
          localDateTime("1970-01-01 00:00:21"),
          localDateTime("1970-01-01 00:00:21"),
          localDateTime("1970-01-01 00:00:24"),
          localDateTime("1970-01-01 00:00:23.999")
        )
      )
    )
  }

  @Test
  def testHopTVF(): Unit = {
    checkResult(
      """
        |SELECT *
        |FROM TABLE(
        |  HOP(
        |    TABLE Table3WithTimestamp,
        |    DESCRIPTOR(ts),
        |    INTERVAL '5' SECOND,
        |    INTERVAL '9' SECOND))
        |""".stripMargin,
      Seq(
        row(
          1,
          1,
          "Hi",
          localDateTime("1970-01-01 00:00:01"),
          localDateTime("1969-12-31 23:59:55"),
          localDateTime("1970-01-01 00:00:04"),
          localDateTime("1970-01-01 00:00:03.999")
        ),
        row(
          1,
          1,
          "Hi",
          localDateTime("1970-01-01 00:00:01"),
          localDateTime("1970-01-01 00:00:00"),
          localDateTime("1970-01-01 00:00:09"),
          localDateTime("1970-01-01 00:00:08.999")
        ),
        row(
          2,
          2,
          "Hello",
          localDateTime("1970-01-01 00:00:02"),
          localDateTime("1969-12-31 23:59:55"),
          localDateTime("1970-01-01 00:00:04"),
          localDateTime("1970-01-01 00:00:03.999")
        ),
        row(
          2,
          2,
          "Hello",
          localDateTime("1970-01-01 00:00:02"),
          localDateTime("1970-01-01 00:00:00"),
          localDateTime("1970-01-01 00:00:09"),
          localDateTime("1970-01-01 00:00:08.999")
        ),
        row(
          3,
          2,
          "Hello world",
          localDateTime("1970-01-01 00:00:03"),
          localDateTime("1969-12-31 23:59:55"),
          localDateTime("1970-01-01 00:00:04"),
          localDateTime("1970-01-01 00:00:03.999")
        ),
        row(
          3,
          2,
          "Hello world",
          localDateTime("1970-01-01 00:00:03"),
          localDateTime("1970-01-01 00:00:00"),
          localDateTime("1970-01-01 00:00:09"),
          localDateTime("1970-01-01 00:00:08.999")
        ),
        row(
          4,
          3,
          "Hello world, how are you?",
          localDateTime("1970-01-01 00:00:04"),
          localDateTime("1970-01-01 00:00:00"),
          localDateTime("1970-01-01 00:00:09"),
          localDateTime("1970-01-01 00:00:08.999")
        ),
        row(
          5,
          3,
          "I am fine.",
          localDateTime("1970-01-01 00:00:05"),
          localDateTime("1970-01-01 00:00:00"),
          localDateTime("1970-01-01 00:00:09"),
          localDateTime("1970-01-01 00:00:08.999")
        ),
        row(
          5,
          3,
          "I am fine.",
          localDateTime("1970-01-01 00:00:05"),
          localDateTime("1970-01-01 00:00:05"),
          localDateTime("1970-01-01 00:00:14"),
          localDateTime("1970-01-01 00:00:13.999")
        ),
        row(
          6,
          3,
          "Luke Skywalker",
          localDateTime("1970-01-01 00:00:06"),
          localDateTime("1970-01-01 00:00:00"),
          localDateTime("1970-01-01 00:00:09"),
          localDateTime("1970-01-01 00:00:08.999")
        ),
        row(
          6,
          3,
          "Luke Skywalker",
          localDateTime("1970-01-01 00:00:06"),
          localDateTime("1970-01-01 00:00:05"),
          localDateTime("1970-01-01 00:00:14"),
          localDateTime("1970-01-01 00:00:13.999")
        ),
        row(
          7,
          4,
          "Comment#1",
          localDateTime("1970-01-01 00:00:07"),
          localDateTime("1970-01-01 00:00:00"),
          localDateTime("1970-01-01 00:00:09"),
          localDateTime("1970-01-01 00:00:08.999")
        ),
        row(
          7,
          4,
          "Comment#1",
          localDateTime("1970-01-01 00:00:07"),
          localDateTime("1970-01-01 00:00:05"),
          localDateTime("1970-01-01 00:00:14"),
          localDateTime("1970-01-01 00:00:13.999")
        ),
        row(
          8,
          4,
          "Comment#2",
          localDateTime("1970-01-01 00:00:08"),
          localDateTime("1970-01-01 00:00:00"),
          localDateTime("1970-01-01 00:00:09"),
          localDateTime("1970-01-01 00:00:08.999")
        ),
        row(
          8,
          4,
          "Comment#2",
          localDateTime("1970-01-01 00:00:08"),
          localDateTime("1970-01-01 00:00:05"),
          localDateTime("1970-01-01 00:00:14"),
          localDateTime("1970-01-01 00:00:13.999")
        ),
        row(
          9,
          4,
          "Comment#3",
          localDateTime("1970-01-01 00:00:09"),
          localDateTime("1970-01-01 00:00:05"),
          localDateTime("1970-01-01 00:00:14"),
          localDateTime("1970-01-01 00:00:13.999")
        ),
        row(
          10,
          4,
          "Comment#4",
          localDateTime("1970-01-01 00:00:10"),
          localDateTime("1970-01-01 00:00:05"),
          localDateTime("1970-01-01 00:00:14"),
          localDateTime("1970-01-01 00:00:13.999")
        ),
        row(
          10,
          4,
          "Comment#4",
          localDateTime("1970-01-01 00:00:10"),
          localDateTime("1970-01-01 00:00:10"),
          localDateTime("1970-01-01 00:00:19"),
          localDateTime("1970-01-01 00:00:18.999")
        ),
        row(
          11,
          5,
          "Comment#5",
          localDateTime("1970-01-01 00:00:11"),
          localDateTime("1970-01-01 00:00:05"),
          localDateTime("1970-01-01 00:00:14"),
          localDateTime("1970-01-01 00:00:13.999")
        ),
        row(
          11,
          5,
          "Comment#5",
          localDateTime("1970-01-01 00:00:11"),
          localDateTime("1970-01-01 00:00:10"),
          localDateTime("1970-01-01 00:00:19"),
          localDateTime("1970-01-01 00:00:18.999")
        ),
        row(
          12,
          5,
          "Comment#6",
          localDateTime("1970-01-01 00:00:12"),
          localDateTime("1970-01-01 00:00:05"),
          localDateTime("1970-01-01 00:00:14"),
          localDateTime("1970-01-01 00:00:13.999")
        ),
        row(
          12,
          5,
          "Comment#6",
          localDateTime("1970-01-01 00:00:12"),
          localDateTime("1970-01-01 00:00:10"),
          localDateTime("1970-01-01 00:00:19"),
          localDateTime("1970-01-01 00:00:18.999")
        ),
        row(
          13,
          5,
          "Comment#7",
          localDateTime("1970-01-01 00:00:13"),
          localDateTime("1970-01-01 00:00:05"),
          localDateTime("1970-01-01 00:00:14"),
          localDateTime("1970-01-01 00:00:13.999")
        ),
        row(
          13,
          5,
          "Comment#7",
          localDateTime("1970-01-01 00:00:13"),
          localDateTime("1970-01-01 00:00:10"),
          localDateTime("1970-01-01 00:00:19"),
          localDateTime("1970-01-01 00:00:18.999")
        ),
        row(
          14,
          5,
          "Comment#8",
          localDateTime("1970-01-01 00:00:14"),
          localDateTime("1970-01-01 00:00:10"),
          localDateTime("1970-01-01 00:00:19"),
          localDateTime("1970-01-01 00:00:18.999")
        ),
        row(
          15,
          5,
          "Comment#9",
          localDateTime("1970-01-01 00:00:15"),
          localDateTime("1970-01-01 00:00:10"),
          localDateTime("1970-01-01 00:00:19"),
          localDateTime("1970-01-01 00:00:18.999")
        ),
        row(
          15,
          5,
          "Comment#9",
          localDateTime("1970-01-01 00:00:15"),
          localDateTime("1970-01-01 00:00:15"),
          localDateTime("1970-01-01 00:00:24"),
          localDateTime("1970-01-01 00:00:23.999")
        ),
        row(
          16,
          6,
          "Comment#10",
          localDateTime("1970-01-01 00:00:16"),
          localDateTime("1970-01-01 00:00:10"),
          localDateTime("1970-01-01 00:00:19"),
          localDateTime("1970-01-01 00:00:18.999")
        ),
        row(
          16,
          6,
          "Comment#10",
          localDateTime("1970-01-01 00:00:16"),
          localDateTime("1970-01-01 00:00:15"),
          localDateTime("1970-01-01 00:00:24"),
          localDateTime("1970-01-01 00:00:23.999")
        ),
        row(
          17,
          6,
          "Comment#11",
          localDateTime("1970-01-01 00:00:17"),
          localDateTime("1970-01-01 00:00:10"),
          localDateTime("1970-01-01 00:00:19"),
          localDateTime("1970-01-01 00:00:18.999")
        ),
        row(
          17,
          6,
          "Comment#11",
          localDateTime("1970-01-01 00:00:17"),
          localDateTime("1970-01-01 00:00:15"),
          localDateTime("1970-01-01 00:00:24"),
          localDateTime("1970-01-01 00:00:23.999")
        ),
        row(
          18,
          6,
          "Comment#12",
          localDateTime("1970-01-01 00:00:18"),
          localDateTime("1970-01-01 00:00:10"),
          localDateTime("1970-01-01 00:00:19"),
          localDateTime("1970-01-01 00:00:18.999")
        ),
        row(
          18,
          6,
          "Comment#12",
          localDateTime("1970-01-01 00:00:18"),
          localDateTime("1970-01-01 00:00:15"),
          localDateTime("1970-01-01 00:00:24"),
          localDateTime("1970-01-01 00:00:23.999")
        ),
        row(
          19,
          6,
          "Comment#13",
          localDateTime("1970-01-01 00:00:19"),
          localDateTime("1970-01-01 00:00:15"),
          localDateTime("1970-01-01 00:00:24"),
          localDateTime("1970-01-01 00:00:23.999")
        ),
        row(
          20,
          6,
          "Comment#14",
          localDateTime("1970-01-01 00:00:20"),
          localDateTime("1970-01-01 00:00:15"),
          localDateTime("1970-01-01 00:00:24"),
          localDateTime("1970-01-01 00:00:23.999")
        ),
        row(
          20,
          6,
          "Comment#14",
          localDateTime("1970-01-01 00:00:20"),
          localDateTime("1970-01-01 00:00:20"),
          localDateTime("1970-01-01 00:00:29"),
          localDateTime("1970-01-01 00:00:28.999")
        ),
        row(
          21,
          6,
          "Comment#15",
          localDateTime("1970-01-01 00:00:21"),
          localDateTime("1970-01-01 00:00:15"),
          localDateTime("1970-01-01 00:00:24"),
          localDateTime("1970-01-01 00:00:23.999")
        ),
        row(
          21,
          6,
          "Comment#15",
          localDateTime("1970-01-01 00:00:21"),
          localDateTime("1970-01-01 00:00:20"),
          localDateTime("1970-01-01 00:00:29"),
          localDateTime("1970-01-01 00:00:28.999")
        )
      )
    )
  }

  @Test
  def testCumulateTVF(): Unit = {
    checkResult(
      """
        |SELECT *
        |FROM TABLE(
        |  CUMULATE(
        |    TABLE Table3WithTimestamp,
        |    DESCRIPTOR(ts),
        |    INTERVAL '3' SECOND,
        |    INTERVAL '9' SECOND))
        |""".stripMargin,
      Seq(
        row(
          1,
          1,
          "Hi",
          localDateTime("1970-01-01 00:00:01"),
          localDateTime("1970-01-01 00:00:00"),
          localDateTime("1970-01-01 00:00:03"),
          localDateTime("1970-01-01 00:00:02.999")
        ),
        row(
          1,
          1,
          "Hi",
          localDateTime("1970-01-01 00:00:01"),
          localDateTime("1970-01-01 00:00:00"),
          localDateTime("1970-01-01 00:00:06"),
          localDateTime("1970-01-01 00:00:05.999")
        ),
        row(
          1,
          1,
          "Hi",
          localDateTime("1970-01-01 00:00:01"),
          localDateTime("1970-01-01 00:00:00"),
          localDateTime("1970-01-01 00:00:09"),
          localDateTime("1970-01-01 00:00:08.999")
        ),
        row(
          2,
          2,
          "Hello",
          localDateTime("1970-01-01 00:00:02"),
          localDateTime("1970-01-01 00:00:00"),
          localDateTime("1970-01-01 00:00:03"),
          localDateTime("1970-01-01 00:00:02.999")
        ),
        row(
          2,
          2,
          "Hello",
          localDateTime("1970-01-01 00:00:02"),
          localDateTime("1970-01-01 00:00:00"),
          localDateTime("1970-01-01 00:00:06"),
          localDateTime("1970-01-01 00:00:05.999")
        ),
        row(
          2,
          2,
          "Hello",
          localDateTime("1970-01-01 00:00:02"),
          localDateTime("1970-01-01 00:00:00"),
          localDateTime("1970-01-01 00:00:09"),
          localDateTime("1970-01-01 00:00:08.999")
        ),
        row(
          3,
          2,
          "Hello world",
          localDateTime("1970-01-01 00:00:03"),
          localDateTime("1970-01-01 00:00:00"),
          localDateTime("1970-01-01 00:00:06"),
          localDateTime("1970-01-01 00:00:05.999")
        ),
        row(
          3,
          2,
          "Hello world",
          localDateTime("1970-01-01 00:00:03"),
          localDateTime("1970-01-01 00:00:00"),
          localDateTime("1970-01-01 00:00:09"),
          localDateTime("1970-01-01 00:00:08.999")
        ),
        row(
          4,
          3,
          "Hello world, how are you?",
          localDateTime("1970-01-01 00:00:04"),
          localDateTime("1970-01-01 00:00:00"),
          localDateTime("1970-01-01 00:00:06"),
          localDateTime("1970-01-01 00:00:05.999")
        ),
        row(
          4,
          3,
          "Hello world, how are you?",
          localDateTime("1970-01-01 00:00:04"),
          localDateTime("1970-01-01 00:00:00"),
          localDateTime("1970-01-01 00:00:09"),
          localDateTime("1970-01-01 00:00:08.999")
        ),
        row(
          5,
          3,
          "I am fine.",
          localDateTime("1970-01-01 00:00:05"),
          localDateTime("1970-01-01 00:00:00"),
          localDateTime("1970-01-01 00:00:06"),
          localDateTime("1970-01-01 00:00:05.999")
        ),
        row(
          5,
          3,
          "I am fine.",
          localDateTime("1970-01-01 00:00:05"),
          localDateTime("1970-01-01 00:00:00"),
          localDateTime("1970-01-01 00:00:09"),
          localDateTime("1970-01-01 00:00:08.999")
        ),
        row(
          6,
          3,
          "Luke Skywalker",
          localDateTime("1970-01-01 00:00:06"),
          localDateTime("1970-01-01 00:00:00"),
          localDateTime("1970-01-01 00:00:09"),
          localDateTime("1970-01-01 00:00:08.999")
        ),
        row(
          7,
          4,
          "Comment#1",
          localDateTime("1970-01-01 00:00:07"),
          localDateTime("1970-01-01 00:00:00"),
          localDateTime("1970-01-01 00:00:09"),
          localDateTime("1970-01-01 00:00:08.999")
        ),
        row(
          8,
          4,
          "Comment#2",
          localDateTime("1970-01-01 00:00:08"),
          localDateTime("1970-01-01 00:00:00"),
          localDateTime("1970-01-01 00:00:09"),
          localDateTime("1970-01-01 00:00:08.999")
        ),
        row(
          9,
          4,
          "Comment#3",
          localDateTime("1970-01-01 00:00:09"),
          localDateTime("1970-01-01 00:00:09"),
          localDateTime("1970-01-01 00:00:12"),
          localDateTime("1970-01-01 00:00:11.999")
        ),
        row(
          9,
          4,
          "Comment#3",
          localDateTime("1970-01-01 00:00:09"),
          localDateTime("1970-01-01 00:00:09"),
          localDateTime("1970-01-01 00:00:15"),
          localDateTime("1970-01-01 00:00:14.999")
        ),
        row(
          9,
          4,
          "Comment#3",
          localDateTime("1970-01-01 00:00:09"),
          localDateTime("1970-01-01 00:00:09"),
          localDateTime("1970-01-01 00:00:18"),
          localDateTime("1970-01-01 00:00:17.999")
        ),
        row(
          10,
          4,
          "Comment#4",
          localDateTime("1970-01-01 00:00:10"),
          localDateTime("1970-01-01 00:00:09"),
          localDateTime("1970-01-01 00:00:12"),
          localDateTime("1970-01-01 00:00:11.999")
        ),
        row(
          10,
          4,
          "Comment#4",
          localDateTime("1970-01-01 00:00:10"),
          localDateTime("1970-01-01 00:00:09"),
          localDateTime("1970-01-01 00:00:15"),
          localDateTime("1970-01-01 00:00:14.999")
        ),
        row(
          10,
          4,
          "Comment#4",
          localDateTime("1970-01-01 00:00:10"),
          localDateTime("1970-01-01 00:00:09"),
          localDateTime("1970-01-01 00:00:18"),
          localDateTime("1970-01-01 00:00:17.999")
        ),
        row(
          11,
          5,
          "Comment#5",
          localDateTime("1970-01-01 00:00:11"),
          localDateTime("1970-01-01 00:00:09"),
          localDateTime("1970-01-01 00:00:12"),
          localDateTime("1970-01-01 00:00:11.999")
        ),
        row(
          11,
          5,
          "Comment#5",
          localDateTime("1970-01-01 00:00:11"),
          localDateTime("1970-01-01 00:00:09"),
          localDateTime("1970-01-01 00:00:15"),
          localDateTime("1970-01-01 00:00:14.999")
        ),
        row(
          11,
          5,
          "Comment#5",
          localDateTime("1970-01-01 00:00:11"),
          localDateTime("1970-01-01 00:00:09"),
          localDateTime("1970-01-01 00:00:18"),
          localDateTime("1970-01-01 00:00:17.999")
        ),
        row(
          12,
          5,
          "Comment#6",
          localDateTime("1970-01-01 00:00:12"),
          localDateTime("1970-01-01 00:00:09"),
          localDateTime("1970-01-01 00:00:15"),
          localDateTime("1970-01-01 00:00:14.999")
        ),
        row(
          12,
          5,
          "Comment#6",
          localDateTime("1970-01-01 00:00:12"),
          localDateTime("1970-01-01 00:00:09"),
          localDateTime("1970-01-01 00:00:18"),
          localDateTime("1970-01-01 00:00:17.999")
        ),
        row(
          13,
          5,
          "Comment#7",
          localDateTime("1970-01-01 00:00:13"),
          localDateTime("1970-01-01 00:00:09"),
          localDateTime("1970-01-01 00:00:15"),
          localDateTime("1970-01-01 00:00:14.999")
        ),
        row(
          13,
          5,
          "Comment#7",
          localDateTime("1970-01-01 00:00:13"),
          localDateTime("1970-01-01 00:00:09"),
          localDateTime("1970-01-01 00:00:18"),
          localDateTime("1970-01-01 00:00:17.999")
        ),
        row(
          14,
          5,
          "Comment#8",
          localDateTime("1970-01-01 00:00:14"),
          localDateTime("1970-01-01 00:00:09"),
          localDateTime("1970-01-01 00:00:15"),
          localDateTime("1970-01-01 00:00:14.999")
        ),
        row(
          14,
          5,
          "Comment#8",
          localDateTime("1970-01-01 00:00:14"),
          localDateTime("1970-01-01 00:00:09"),
          localDateTime("1970-01-01 00:00:18"),
          localDateTime("1970-01-01 00:00:17.999")
        ),
        row(
          15,
          5,
          "Comment#9",
          localDateTime("1970-01-01 00:00:15"),
          localDateTime("1970-01-01 00:00:09"),
          localDateTime("1970-01-01 00:00:18"),
          localDateTime("1970-01-01 00:00:17.999")
        ),
        row(
          16,
          6,
          "Comment#10",
          localDateTime("1970-01-01 00:00:16"),
          localDateTime("1970-01-01 00:00:09"),
          localDateTime("1970-01-01 00:00:18"),
          localDateTime("1970-01-01 00:00:17.999")
        ),
        row(
          17,
          6,
          "Comment#11",
          localDateTime("1970-01-01 00:00:17"),
          localDateTime("1970-01-01 00:00:09"),
          localDateTime("1970-01-01 00:00:18"),
          localDateTime("1970-01-01 00:00:17.999")
        ),
        row(
          18,
          6,
          "Comment#12",
          localDateTime("1970-01-01 00:00:18"),
          localDateTime("1970-01-01 00:00:18"),
          localDateTime("1970-01-01 00:00:21"),
          localDateTime("1970-01-01 00:00:20.999")
        ),
        row(
          18,
          6,
          "Comment#12",
          localDateTime("1970-01-01 00:00:18"),
          localDateTime("1970-01-01 00:00:18"),
          localDateTime("1970-01-01 00:00:24"),
          localDateTime("1970-01-01 00:00:23.999")
        ),
        row(
          18,
          6,
          "Comment#12",
          localDateTime("1970-01-01 00:00:18"),
          localDateTime("1970-01-01 00:00:18"),
          localDateTime("1970-01-01 00:00:27"),
          localDateTime("1970-01-01 00:00:26.999")
        ),
        row(
          19,
          6,
          "Comment#13",
          localDateTime("1970-01-01 00:00:19"),
          localDateTime("1970-01-01 00:00:18"),
          localDateTime("1970-01-01 00:00:21"),
          localDateTime("1970-01-01 00:00:20.999")
        ),
        row(
          19,
          6,
          "Comment#13",
          localDateTime("1970-01-01 00:00:19"),
          localDateTime("1970-01-01 00:00:18"),
          localDateTime("1970-01-01 00:00:24"),
          localDateTime("1970-01-01 00:00:23.999")
        ),
        row(
          19,
          6,
          "Comment#13",
          localDateTime("1970-01-01 00:00:19"),
          localDateTime("1970-01-01 00:00:18"),
          localDateTime("1970-01-01 00:00:27"),
          localDateTime("1970-01-01 00:00:26.999")
        ),
        row(
          20,
          6,
          "Comment#14",
          localDateTime("1970-01-01 00:00:20"),
          localDateTime("1970-01-01 00:00:18"),
          localDateTime("1970-01-01 00:00:21"),
          localDateTime("1970-01-01 00:00:20.999")
        ),
        row(
          20,
          6,
          "Comment#14",
          localDateTime("1970-01-01 00:00:20"),
          localDateTime("1970-01-01 00:00:18"),
          localDateTime("1970-01-01 00:00:24"),
          localDateTime("1970-01-01 00:00:23.999")
        ),
        row(
          20,
          6,
          "Comment#14",
          localDateTime("1970-01-01 00:00:20"),
          localDateTime("1970-01-01 00:00:18"),
          localDateTime("1970-01-01 00:00:27"),
          localDateTime("1970-01-01 00:00:26.999")
        ),
        row(
          21,
          6,
          "Comment#15",
          localDateTime("1970-01-01 00:00:21"),
          localDateTime("1970-01-01 00:00:18"),
          localDateTime("1970-01-01 00:00:24"),
          localDateTime("1970-01-01 00:00:23.999")
        ),
        row(
          21,
          6,
          "Comment#15",
          localDateTime("1970-01-01 00:00:21"),
          localDateTime("1970-01-01 00:00:18"),
          localDateTime("1970-01-01 00:00:27"),
          localDateTime("1970-01-01 00:00:26.999")
        )
      )
    )
  }

  @Test
  def testWindowAggregate(): Unit = {
    checkResult(
      """
        |SELECT
        |  b,
        |  SUM(a),
        |  window_start
        |FROM TABLE(
        |  HOP(
        |    TABLE Table3WithTimestamp,
        |    DESCRIPTOR(ts),
        |    INTERVAL '5' SECOND,
        |    INTERVAL '9' SECOND))
        |GROUP BY window_start, window_end, b
        |""".stripMargin,
      Seq(
        row(1, 1, localDateTime("1969-12-31 23:59:55.0")),
        row(1, 1, localDateTime("1970-01-01 00:00:00.0")),
        row(2, 5, localDateTime("1969-12-31 23:59:55.0")),
        row(2, 5, localDateTime("1970-01-01 00:00:00.0")),
        row(3, 11, localDateTime("1970-01-01 00:00:05.0")),
        row(3, 15, localDateTime("1970-01-01 00:00:00.0")),
        row(4, 10, localDateTime("1970-01-01 00:00:10.0")),
        row(4, 15, localDateTime("1970-01-01 00:00:00.0")),
        row(4, 34, localDateTime("1970-01-01 00:00:05.0")),
        row(5, 15, localDateTime("1970-01-01 00:00:15.0")),
        row(5, 36, localDateTime("1970-01-01 00:00:05.0")),
        row(5, 65, localDateTime("1970-01-01 00:00:10.0")),
        row(6, 111, localDateTime("1970-01-01 00:00:15.0")),
        row(6, 41, localDateTime("1970-01-01 00:00:20.0")),
        row(6, 51, localDateTime("1970-01-01 00:00:10.0"))
      )
    )
  }

  @Test
  def testWindowAggregateWithProperties(): Unit = {
    registerCollection(
      "T",
      data3WithTimestamp,
      new RowTypeInfo(INT_TYPE_INFO, LONG_TYPE_INFO, STRING_TYPE_INFO, LOCAL_DATE_TIME),
      "a, b, c, ts")
    checkResult(
      """
        |SELECT
        |  b,
        |  COUNT(a),
        |  window_start,
        |  window_end,
        |  window_time
        |FROM TABLE(
        |  HOP(
        |    TABLE T,
        |    DESCRIPTOR(ts),
        |    INTERVAL '5' SECOND,
        |    INTERVAL '10' SECOND))
        |GROUP BY window_start, window_end, window_time, b
        |""".stripMargin,
      Seq(
        row(
          1,
          1,
          localDateTime("1969-12-31 23:59:55.0"),
          localDateTime("1970-01-01 00:00:05.0"),
          localDateTime("1970-01-01 00:00:04.999")),
        row(
          1,
          1,
          localDateTime("1970-01-01 00:00:00.0"),
          localDateTime("1970-01-01 00:00:10.0"),
          localDateTime("1970-01-01 00:00:09.999")),
        row(
          2,
          2,
          localDateTime("1969-12-31 23:59:55.0"),
          localDateTime("1970-01-01 00:00:05.0"),
          localDateTime("1970-01-01 00:00:04.999")),
        row(
          2,
          2,
          localDateTime("1970-01-01 00:00:00.0"),
          localDateTime("1970-01-01 00:00:10.0"),
          localDateTime("1970-01-01 00:00:09.999")),
        row(
          3,
          1,
          localDateTime("1969-12-31 23:59:55.0"),
          localDateTime("1970-01-01 00:00:05.0"),
          localDateTime("1970-01-01 00:00:04.999")),
        row(
          3,
          2,
          localDateTime("1970-01-01 00:00:05.0"),
          localDateTime("1970-01-01 00:00:15.0"),
          localDateTime("1970-01-01 00:00:14.999")),
        row(
          3,
          3,
          localDateTime("1970-01-01 00:00:00.0"),
          localDateTime("1970-01-01 00:00:10.0"),
          localDateTime("1970-01-01 00:00:09.999")),
        row(
          4,
          1,
          localDateTime("1970-01-01 00:00:10.0"),
          localDateTime("1970-01-01 00:00:20.0"),
          localDateTime("1970-01-01 00:00:19.999")),
        row(
          4,
          3,
          localDateTime("1970-01-01 00:00:00.0"),
          localDateTime("1970-01-01 00:00:10.0"),
          localDateTime("1970-01-01 00:00:09.999")),
        row(
          4,
          4,
          localDateTime("1970-01-01 00:00:05.0"),
          localDateTime("1970-01-01 00:00:15.0"),
          localDateTime("1970-01-01 00:00:14.999")),
        row(
          5,
          1,
          localDateTime("1970-01-01 00:00:15.0"),
          localDateTime("1970-01-01 00:00:25.0"),
          localDateTime("1970-01-01 00:00:24.999")),
        row(
          5,
          4,
          localDateTime("1970-01-01 00:00:05.0"),
          localDateTime("1970-01-01 00:00:15.0"),
          localDateTime("1970-01-01 00:00:14.999")),
        row(
          5,
          5,
          localDateTime("1970-01-01 00:00:10.0"),
          localDateTime("1970-01-01 00:00:20.0"),
          localDateTime("1970-01-01 00:00:19.999")),
        row(
          6,
          2,
          localDateTime("1970-01-01 00:00:20.0"),
          localDateTime("1970-01-01 00:00:30.0"),
          localDateTime("1970-01-01 00:00:29.999")),
        row(
          6,
          4,
          localDateTime("1970-01-01 00:00:10.0"),
          localDateTime("1970-01-01 00:00:20.0"),
          localDateTime("1970-01-01 00:00:19.999")),
        row(
          6,
          6,
          localDateTime("1970-01-01 00:00:15.0"),
          localDateTime("1970-01-01 00:00:25.0"),
          localDateTime("1970-01-01 00:00:24.999"))
      )
    )
  }

  @Test
  def testWindowJoin(): Unit = {
    val data1 = Seq(
      row(localDateTime("2016-03-27 09:00:05"), 1),
      row(null, 2),
      row(localDateTime("2016-03-27 09:00:32"), 3),
      row(null, 4)
    )
    registerCollection("T1", data1, new RowTypeInfo(LOCAL_DATE_TIME, INT_TYPE_INFO), "ts, v")
    val data2 = Seq(
      row(null, 1),
      row(null, 2),
      row(null, 3),
      row(null, 4)
    )
    // null columns are dropped
    registerCollection("T2", data2, new RowTypeInfo(LOCAL_DATE_TIME, INT_TYPE_INFO), "ts, v")
    checkResult(
      """
        |SELECT *
        |FROM (
        |  SELECT window_start, window_end, v
        |  FROM TABLE(TUMBLE(TABLE T1, DESCRIPTOR(ts), INTERVAL '10' SECOND))
        |  GROUP BY window_start, window_end, v
        |) L
        |LEFT JOIN (
        |  SELECT window_start, window_end, v
        |  FROM TABLE(TUMBLE(TABLE T2, DESCRIPTOR(ts), INTERVAL '10' SECOND))
        |   GROUP BY window_start, window_end, v
        |) R
        |ON L.window_start = R.window_start AND L.window_end = R.window_end AND L.v = R.v
      """.stripMargin,
      // null columns are dropped
      Seq(
        row(
          localDateTime("2016-03-27 09:00:00.0"),
          localDateTime("2016-03-27 09:00:10.0"),
          1,
          null,
          null,
          null),
        row(
          localDateTime("2016-03-27 09:00:30.0"),
          localDateTime("2016-03-27 09:00:40.0"),
          3,
          null,
          null,
          null)
      )
    )
  }

  @Test
  def testWindowRank(): Unit = {
    checkResult(
      """
        |SELECT a, b, c, ts, window_start, window_end, window_time
        |FROM (
        |SELECT *,
        |  RANK() OVER(PARTITION BY window_start, window_end ORDER BY ts) as rownum
        |FROM TABLE(
        |  CUMULATE(
        |    TABLE Table3WithTimestamp,
        |    DESCRIPTOR(ts),
        |    INTERVAL '3' SECOND,
        |    INTERVAL '9' SECOND))
        |)
        |WHERE rownum <= 1
        |""".stripMargin,
      Seq(
        row(
          1,
          1,
          "Hi",
          localDateTime("1970-01-01 00:00:01"),
          localDateTime("1970-01-01 00:00:00"),
          localDateTime("1970-01-01 00:00:03"),
          localDateTime("1970-01-01 00:00:02.999")
        ),
        row(
          1,
          1,
          "Hi",
          localDateTime("1970-01-01 00:00:01"),
          localDateTime("1970-01-01 00:00:00"),
          localDateTime("1970-01-01 00:00:06"),
          localDateTime("1970-01-01 00:00:05.999")
        ),
        row(
          1,
          1,
          "Hi",
          localDateTime("1970-01-01 00:00:01"),
          localDateTime("1970-01-01 00:00:00"),
          localDateTime("1970-01-01 00:00:09"),
          localDateTime("1970-01-01 00:00:08.999")
        ),
        row(
          9,
          4,
          "Comment#3",
          localDateTime("1970-01-01 00:00:09"),
          localDateTime("1970-01-01 00:00:09"),
          localDateTime("1970-01-01 00:00:12"),
          localDateTime("1970-01-01 00:00:11.999")
        ),
        row(
          9,
          4,
          "Comment#3",
          localDateTime("1970-01-01 00:00:09"),
          localDateTime("1970-01-01 00:00:09"),
          localDateTime("1970-01-01 00:00:15"),
          localDateTime("1970-01-01 00:00:14.999")
        ),
        row(
          9,
          4,
          "Comment#3",
          localDateTime("1970-01-01 00:00:09"),
          localDateTime("1970-01-01 00:00:09"),
          localDateTime("1970-01-01 00:00:18"),
          localDateTime("1970-01-01 00:00:17.999")
        ),
        row(
          18,
          6,
          "Comment#12",
          localDateTime("1970-01-01 00:00:18"),
          localDateTime("1970-01-01 00:00:18"),
          localDateTime("1970-01-01 00:00:21"),
          localDateTime("1970-01-01 00:00:20.999")
        ),
        row(
          18,
          6,
          "Comment#12",
          localDateTime("1970-01-01 00:00:18"),
          localDateTime("1970-01-01 00:00:18"),
          localDateTime("1970-01-01 00:00:24"),
          localDateTime("1970-01-01 00:00:23.999")
        ),
        row(
          18,
          6,
          "Comment#12",
          localDateTime("1970-01-01 00:00:18"),
          localDateTime("1970-01-01 00:00:18"),
          localDateTime("1970-01-01 00:00:27"),
          localDateTime("1970-01-01 00:00:26.999")
        )
      )
    )
  }
}
