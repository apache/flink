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

package org.apache.flink.table.planner.runtime.batch.sql.agg

import org.apache.flink.api.common.typeinfo.BasicTypeInfo.{INT_TYPE_INFO, LONG_TYPE_INFO, STRING_TYPE_INFO}
import org.apache.flink.api.common.typeinfo.LocalTimeTypeInfo.LOCAL_DATE_TIME
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.scala._
import org.apache.flink.table.planner.runtime.utils.BatchTestBase
import org.apache.flink.table.planner.runtime.utils.BatchTestBase.row
import org.apache.flink.table.planner.runtime.utils.TestData._
import org.apache.flink.table.planner.utils.DateTimeTestUtil.localDateTime
import org.apache.flink.table.planner.utils.{CountAggFunction, IntAvgAggFunction, IntSumAggFunction}

import org.junit.{Before, Test}

class WindowAggregateITCase extends BatchTestBase {

  @Before
  override def before(): Unit = {
    super.before()
    // common case
    registerCollection("Table3WithTimestamp", data3WithTimestamp, type3WithTimestamp,
      "a, b, c, ts", nullablesOfData3WithTimestamp)
    // for udagg
    registerFunction("countFun", new CountAggFunction())
    registerFunction("sumFun", new IntSumAggFunction())
    registerFunction("avgFun", new IntAvgAggFunction())
    // time unit
    registerCollection("Table6", data6, type6, "a, b, c, d, e, f", nullablesOfData6)
  }

  @Test
  def testTumblingWindow(): Unit = {
    // SORT; keyed; 2-phase; pre-accumulate without paned optimization; single row group
    checkResult(
      "SELECT a, countFun(a), TUMBLE_START(ts, INTERVAL '3' SECOND)" +
          "FROM Table3WithTimestamp " +
          "GROUP BY a, TUMBLE(ts, INTERVAL '3' SECOND)",
      Seq(
        row(1, 1, localDateTime("1970-01-01 00:00:00.0")),
        row(2, 1, localDateTime("1970-01-01 00:00:00.0")),
        row(3, 1, localDateTime("1970-01-01 00:00:03.0")),
        row(4, 1, localDateTime("1970-01-01 00:00:03.0")),
        row(5, 1, localDateTime("1970-01-01 00:00:03.0")),
        row(6, 1, localDateTime("1970-01-01 00:00:06.0")),
        row(7, 1, localDateTime("1970-01-01 00:00:06.0")),
        row(8, 1, localDateTime("1970-01-01 00:00:06.0")),
        row(9, 1, localDateTime("1970-01-01 00:00:09.0")),
        row(10, 1, localDateTime("1970-01-01 00:00:09.0")),
        row(11, 1, localDateTime("1970-01-01 00:00:09.0")),
        row(12, 1, localDateTime("1970-01-01 00:00:12.0")),
        row(13, 1, localDateTime("1970-01-01 00:00:12.0")),
        row(14, 1, localDateTime("1970-01-01 00:00:12.0")),
        row(15, 1, localDateTime("1970-01-01 00:00:15.0")),
        row(16, 1, localDateTime("1970-01-01 00:00:15.0")),
        row(17, 1, localDateTime("1970-01-01 00:00:15.0")),
        row(18, 1, localDateTime("1970-01-01 00:00:18.0")),
        row(19, 1, localDateTime("1970-01-01 00:00:18.0")),
        row(20, 1, localDateTime("1970-01-01 00:00:18.0")),
        row(21, 1, localDateTime("1970-01-01 00:00:21.0"))
      )
    )

    // SORT; keyed; 2-phase keyed; single row group
    checkResult(
      "SELECT a, countFun(a), TUMBLE_START(ts, INTERVAL '3' SECOND), b " +
          "FROM Table3WithTimestamp " +
          "GROUP BY a, TUMBLE(ts, INTERVAL '3' SECOND), b",
      Seq(
        row(1, 1, localDateTime("1970-01-01 00:00:00.0"), 1),
        row(2, 1, localDateTime("1970-01-01 00:00:00.0"), 2),
        row(3, 1, localDateTime("1970-01-01 00:00:03.0"), 2),
        row(4, 1, localDateTime("1970-01-01 00:00:03.0"), 3),
        row(5, 1, localDateTime("1970-01-01 00:00:03.0"), 3),
        row(6, 1, localDateTime("1970-01-01 00:00:06.0"), 3),
        row(7, 1, localDateTime("1970-01-01 00:00:06.0"), 4),
        row(8, 1, localDateTime("1970-01-01 00:00:06.0"), 4),
        row(9, 1, localDateTime("1970-01-01 00:00:09.0"), 4),
        row(10, 1, localDateTime("1970-01-01 00:00:09.0"), 4),
        row(11, 1, localDateTime("1970-01-01 00:00:09.0"), 5),
        row(12, 1, localDateTime("1970-01-01 00:00:12.0"), 5),
        row(13, 1, localDateTime("1970-01-01 00:00:12.0"), 5),
        row(14, 1, localDateTime("1970-01-01 00:00:12.0"), 5),
        row(15, 1, localDateTime("1970-01-01 00:00:15.0"), 5),
        row(16, 1, localDateTime("1970-01-01 00:00:15.0"), 6),
        row(17, 1, localDateTime("1970-01-01 00:00:15.0"), 6),
        row(18, 1, localDateTime("1970-01-01 00:00:18.0"), 6),
        row(19, 1, localDateTime("1970-01-01 00:00:18.0"), 6),
        row(20, 1, localDateTime("1970-01-01 00:00:18.0"), 6),
        row(21, 1, localDateTime("1970-01-01 00:00:21.0"), 6)
      )
    )

    // HASH 2-phase; sparse inputs
    checkResult(
      "SELECT a, avg(b), min(b), TUMBLE_START(f, INTERVAL '10' SECOND) " +
          "FROM Table6 " +
          "GROUP BY a, TUMBLE(f, INTERVAL '10' SECOND)",
      Seq(
        row(1, 1.1, 1.1, localDateTime("2015-05-20 10:00:00.0")),
        row(2, -2.4, -2.4, localDateTime("2016-09-01 23:07:00.0")),
        row(2, 2.5, 2.5, localDateTime("2019-09-19 08:03:00.0")),
        row(3, -4.885, -9.77, localDateTime("1999-12-12 10:00:00.0")),
        row(3, 0.08, 0.08, localDateTime("1999-12-12 10:03:00.0")),
        row(4, 3.14, 3.14, localDateTime("2017-11-20 09:00:00.0")),
        row(4, 3.145, 3.14, localDateTime("2015-11-19 10:00:00.0")),
        row(4, 3.16, 3.16, localDateTime("2015-11-20 08:59:50.0")),
        row(5, -5.9, -5.9, localDateTime("1989-06-04 10:00:00.0")),
        row(5, -2.8, -2.8, localDateTime("1937-07-07 08:08:00.0")),
        row(5, 0.7, 0.7, localDateTime("2010-06-01 10:00:00.0")),
        row(5, 2.71, 2.71, localDateTime("1997-07-01 09:00:00.0")),
        row(5, 3.9, 3.9, localDateTime("2000-01-01 00:00:00.0"))
      )
    )
  }

  @Test
  def testCascadingTumbleWindow(): Unit = {
    checkResult(
      s"""
         |SELECT b, SUM(cnt)
         |FROM (
         |  SELECT b, COUNT(1) AS cnt, TUMBLE_ROWTIME(ts, INTERVAL '30' SECOND) AS ts
         |  FROM Table3WithTimestamp
         |  GROUP BY a, b, TUMBLE(ts, INTERVAL '30' SECOND)
         |)
         |GROUP BY b, TUMBLE(ts, INTERVAL '30' SECOND)
         |""".stripMargin,
      Seq(
        row(1, 1),
        row(2, 2),
        row(3, 3),
        row(4, 4),
        row(5, 5),
        row(6, 6)
      )
    )
  }

  @Test
  def testSlidingWindow(): Unit = {
    // keyed; 2-phase; pre-accumulate with paned optimization;
    checkResult(
      "SELECT b, sumFun(a), HOP_START(ts, INTERVAL '5' SECOND, INTERVAL '9' SECOND) " +
          "FROM Table3WithTimestamp " +
          "GROUP BY b, HOP(ts, INTERVAL '5' SECOND, INTERVAL '9' SECOND)",
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

    checkResult(
      "SELECT b, sum(a), HOP_START(ts, INTERVAL '5' SECOND, INTERVAL '9' SECOND) " +
          "FROM Table3WithTimestamp " +
          "GROUP BY b, HOP(ts, INTERVAL '5' SECOND, INTERVAL '9' SECOND)",
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

    // keyed; 2-phase; pre-accumulate with windowSize = slideSize
    checkResult(
      "SELECT b, sumFun(a) FROM Table3WithTimestamp " +
          "GROUP BY b, HOP(ts, INTERVAL '3' SECOND, INTERVAL '3' SECOND)",
      Seq(
        row(1, 1),
        row(2, 2),
        row(2, 3),
        row(3, 6),
        row(3, 9),
        row(4, 15),
        row(4, 19),
        row(5, 11),
        row(5, 15),
        row(5, 39),
        row(6, 21),
        row(6, 33),
        row(6, 57)
      )
    )

    checkResult(
      "SELECT b, sum(a) FROM Table3WithTimestamp " +
          "GROUP BY b, HOP(ts, INTERVAL '3' SECOND, INTERVAL '3' SECOND)",
      Seq(
        row(1, 1),
        row(2, 2),
        row(2, 3),
        row(3, 6),
        row(3, 9),
        row(4, 15),
        row(4, 19),
        row(5, 11),
        row(5, 15),
        row(5, 39),
        row(6, 21),
        row(6, 33),
        row(6, 57)
      )
    )

    // keyed; 2-phase; pre-accumulate without pane optimization
    checkResult(
      "SELECT b, sumFun(a), HOP_START(ts, INTERVAL '5.111' SECOND(1,3), INTERVAL '9' SECOND) " +
          "FROM Table3WithTimestamp " +
          "GROUP BY b, HOP(ts, INTERVAL '5.111' SECOND(1,3), INTERVAL '9' SECOND)",
      Seq(
        row(1, 1, localDateTime("1969-12-31 23:59:54.889")),
        row(1, 1, localDateTime("1970-01-01 00:00:00.0")),
        row(2, 5, localDateTime("1969-12-31 23:59:54.889")),
        row(2, 5, localDateTime("1970-01-01 00:00:00.0")),
        row(3, 6, localDateTime("1970-01-01 00:00:05.111")),
        row(3, 15, localDateTime("1970-01-01 00:00:00.0")),
        row(4, 15, localDateTime("1970-01-01 00:00:00.0")),
        row(4, 34, localDateTime("1970-01-01 00:00:05.111")),
        row(5, 50, localDateTime("1970-01-01 00:00:05.111")),
        row(5, 65, localDateTime("1970-01-01 00:00:10.222")),
        row(6, 111, localDateTime("1970-01-01 00:00:15.333")),
        row(6, 21, localDateTime("1970-01-01 00:00:20.444")),
        row(6, 70, localDateTime("1970-01-01 00:00:10.222"))
      )
    )

    checkResult(
      "SELECT b, sum(a), HOP_START(ts, INTERVAL '5.111' SECOND(1,3), INTERVAL '9' SECOND) " +
          "FROM Table3WithTimestamp " +
          "GROUP BY b, HOP(ts, INTERVAL '5.111' SECOND(1,3), INTERVAL '9' SECOND)",
      Seq(
        row(1, 1, localDateTime("1969-12-31 23:59:54.889")),
        row(1, 1, localDateTime("1970-01-01 00:00:00.0")),
        row(2, 5, localDateTime("1969-12-31 23:59:54.889")),
        row(2, 5, localDateTime("1970-01-01 00:00:00.0")),
        row(3, 6, localDateTime("1970-01-01 00:00:05.111")),
        row(3, 15, localDateTime("1970-01-01 00:00:00.0")),
        row(4, 15, localDateTime("1970-01-01 00:00:00.0")),
        row(4, 34, localDateTime("1970-01-01 00:00:05.111")),
        row(5, 50, localDateTime("1970-01-01 00:00:05.111")),
        row(5, 65, localDateTime("1970-01-01 00:00:10.222")),
        row(6, 111, localDateTime("1970-01-01 00:00:15.333")),
        row(6, 21, localDateTime("1970-01-01 00:00:20.444")),
        row(6, 70, localDateTime("1970-01-01 00:00:10.222"))
      )
    )

    // all group; 2-phase; pre-accumulate with windowSize = slideSize
    checkResult(
      "SELECT sumFun(a), " +
          "HOP_START(ts, INTERVAL '3' SECOND, INTERVAL '3' SECOND), " +
          "HOP_END(ts, INTERVAL '3' SECOND, INTERVAL '3' SECOND)" +
          "FROM Table3WithTimestamp " +
          "GROUP BY HOP(ts, INTERVAL '3' SECOND, INTERVAL '3' SECOND)",
      Seq(
        row(12, localDateTime("1970-01-01 00:00:03.0"), localDateTime("1970-01-01 00:00:06.0")),
        row(21, localDateTime("1970-01-01 00:00:06.0"), localDateTime("1970-01-01 00:00:09.0")),
        row(21, localDateTime("1970-01-01 00:00:21.0"), localDateTime("1970-01-01 00:00:24.0")),
        row(3, localDateTime("1970-01-01 00:00:00.0"), localDateTime("1970-01-01 00:00:03.0")),
        row(30, localDateTime("1970-01-01 00:00:09.0"), localDateTime("1970-01-01 00:00:12.0")),
        row(39, localDateTime("1970-01-01 00:00:12.0"), localDateTime("1970-01-01 00:00:15.0")),
        row(48, localDateTime("1970-01-01 00:00:15.0"), localDateTime("1970-01-01 00:00:18.0")),
        row(57, localDateTime("1970-01-01 00:00:18.0"), localDateTime("1970-01-01 00:00:21.0"))
      )
    )

    checkResult(
      "SELECT SUM(a), " +
          "HOP_START(ts, INTERVAL '3' SECOND, INTERVAL '3' SECOND), " +
          "HOP_END(ts, INTERVAL '3' SECOND, INTERVAL '3' SECOND)" +
          "FROM Table3WithTimestamp " +
          "GROUP BY HOP(ts, INTERVAL '3' SECOND, INTERVAL '3' SECOND)",
      Seq(
        row(12, localDateTime("1970-01-01 00:00:03.0"), localDateTime("1970-01-01 00:00:06.0")),
        row(21, localDateTime("1970-01-01 00:00:06.0"), localDateTime("1970-01-01 00:00:09.0")),
        row(21, localDateTime("1970-01-01 00:00:21.0"), localDateTime("1970-01-01 00:00:24.0")),
        row(3, localDateTime("1970-01-01 00:00:00.0"), localDateTime("1970-01-01 00:00:03.0")),
        row(30, localDateTime("1970-01-01 00:00:09.0"), localDateTime("1970-01-01 00:00:12.0")),
        row(39, localDateTime("1970-01-01 00:00:12.0"), localDateTime("1970-01-01 00:00:15.0")),
        row(48, localDateTime("1970-01-01 00:00:15.0"), localDateTime("1970-01-01 00:00:18.0")),
        row(57, localDateTime("1970-01-01 00:00:18.0"), localDateTime("1970-01-01 00:00:21.0"))
      )
    )

    // all group; 2-phase; pre-accumulate with paned optimization
    checkResult(
      "SELECT avgFun(a), sumFun(a), HOP_START(ts, INTERVAL '2' SECOND, INTERVAL '3' SECOND) " +
          "FROM Table3WithTimestamp " +
          "GROUP BY HOP(ts, INTERVAL '2' SECOND, INTERVAL '3' SECOND)",
      Seq(
        row(1, 3, localDateTime("1970-01-01 00:00:00.0")),
        row(11, 33, localDateTime("1970-01-01 00:00:10.0")),
        row(13, 39, localDateTime("1970-01-01 00:00:12.0")),
        row(15, 45, localDateTime("1970-01-01 00:00:14.0")),
        row(17, 51, localDateTime("1970-01-01 00:00:16.0")),
        row(19, 57, localDateTime("1970-01-01 00:00:18.0")),
        row(20, 41, localDateTime("1970-01-01 00:00:20.0")),
        row(3, 9, localDateTime("1970-01-01 00:00:02.0")),
        row(5, 15, localDateTime("1970-01-01 00:00:04.0")),
        row(7, 21, localDateTime("1970-01-01 00:00:06.0")),
        row(9, 27, localDateTime("1970-01-01 00:00:08.0"))
      )
    )

    checkResult(
      "SELECT AVG(a), SUM(a), HOP_START(ts, INTERVAL '2' SECOND, INTERVAL '3' SECOND) " +
          "FROM Table3WithTimestamp " +
          "GROUP BY HOP(ts, INTERVAL '2' SECOND, INTERVAL '3' SECOND)",
      Seq(
        row(1, 3, localDateTime("1970-01-01 00:00:00.0")),
        row(11, 33, localDateTime("1970-01-01 00:00:10.0")),
        row(13, 39, localDateTime("1970-01-01 00:00:12.0")),
        row(15, 45, localDateTime("1970-01-01 00:00:14.0")),
        row(17, 51, localDateTime("1970-01-01 00:00:16.0")),
        row(19, 57, localDateTime("1970-01-01 00:00:18.0")),
        row(20, 41, localDateTime("1970-01-01 00:00:20.0")),
        row(3, 9, localDateTime("1970-01-01 00:00:02.0")),
        row(5, 15, localDateTime("1970-01-01 00:00:04.0")),
        row(7, 21, localDateTime("1970-01-01 00:00:06.0")),
        row(9, 27, localDateTime("1970-01-01 00:00:08.0"))
      )
    )

    // millisecond precision sliding windows
    val data = Seq(
      row(localDateTime("2016-03-27 09:00:00.41"), 3),
      row(localDateTime("2016-03-27 09:00:00.62"), 6),
      row(localDateTime("2016-03-27 09:00:00.715"), 8)
    )
    registerCollection(
      "T2", data, new RowTypeInfo(LOCAL_DATE_TIME, INT_TYPE_INFO),
      "ts, v")
    checkResult(
      """
        |SELECT
        |HOP_START(ts, INTERVAL '0.04' SECOND(1,2), INTERVAL '0.2' SECOND(1,1)),
        |HOP_END(ts, INTERVAL '0.04' SECOND(1,2), INTERVAL '0.2' SECOND(1,1)),
        |count(*)
        |FROM T2
        |GROUP BY HOP(ts, INTERVAL '0.04' SECOND(1,2), INTERVAL '0.2' SECOND(1,1))
      """.stripMargin,
      Seq(row(localDateTime("2016-03-27 09:00:00.24"), localDateTime("2016-03-27 09:00:00.44"), 1),
        row(localDateTime("2016-03-27 09:00:00.28"), localDateTime("2016-03-27 09:00:00.48"), 1),
        row(localDateTime("2016-03-27 09:00:00.32"), localDateTime("2016-03-27 09:00:00.52"), 1),
        row(localDateTime("2016-03-27 09:00:00.36"), localDateTime("2016-03-27 09:00:00.56"), 1),
        row(localDateTime("2016-03-27 09:00:00.4"), localDateTime("2016-03-27 09:00:00.6"), 1),
        row(localDateTime("2016-03-27 09:00:00.44"), localDateTime("2016-03-27 09:00:00.64"), 1),
        row(localDateTime("2016-03-27 09:00:00.48"), localDateTime("2016-03-27 09:00:00.68"), 1),
        row(localDateTime("2016-03-27 09:00:00.52"), localDateTime("2016-03-27 09:00:00.72"), 2),
        row(localDateTime("2016-03-27 09:00:00.56"), localDateTime("2016-03-27 09:00:00.76"), 2),
        row(localDateTime("2016-03-27 09:00:00.6"), localDateTime("2016-03-27 09:00:00.8"), 2),
        row(localDateTime("2016-03-27 09:00:00.64"), localDateTime("2016-03-27 09:00:00.84"), 1),
        row(localDateTime("2016-03-27 09:00:00.68"), localDateTime("2016-03-27 09:00:00.88"), 1))
    )
  }

  @Test
  def testNullValueInputTimestamp(): Unit = {
    // Sliding window
    var data = Seq(
      row(null, 1),
      row(null, 2),
      row(null, 3),
      row(null, 4)
    )
    registerCollection(
      "T1", data, new RowTypeInfo(LOCAL_DATE_TIME, INT_TYPE_INFO),
      "ts, v")
    checkResult(
      """
        |SELECT v
        |FROM T1
        |GROUP BY HOP(ts, INTERVAL '2' SECOND, INTERVAL '3' SECOND), v
      """.stripMargin,
      // null columns are dropped
      Seq()
    )

    // Tumbling window
    data = Seq(
      row(localDateTime("2016-03-27 09:00:05"), 1),
      row(null, 2),
      row(localDateTime("2016-03-27 09:00:32"), 3),
      row(null, 4)
    )
    registerCollection(
      "T2", data, new RowTypeInfo(LOCAL_DATE_TIME, INT_TYPE_INFO),
      "ts, v")
    checkResult(
      """
        |SELECT TUMBLE_START(ts, INTERVAL '10' SECOND), TUMBLE_END(ts, INTERVAL '10' SECOND), v
        |FROM T2
        |GROUP BY TUMBLE(ts, INTERVAL '10' SECOND), v
      """.stripMargin,
      // null columns are dropped
      Seq(
        row(localDateTime("2016-03-27 09:00:00.0"), localDateTime("2016-03-27 09:00:10.0"), 1),
        row(localDateTime("2016-03-27 09:00:30.0"), localDateTime("2016-03-27 09:00:40.0"), 3))
    )
    data = Seq(
      row(null, 1),
      row(null, 2),
      row(null, 3),
      row(null, 4)
    )
    registerCollection(
      "T3", data, new RowTypeInfo(LOCAL_DATE_TIME, INT_TYPE_INFO),
      "ts, v")
    checkResult(
      """
        |SELECT TUMBLE_START(ts, INTERVAL '10' SECOND), TUMBLE_END(ts, INTERVAL '10' SECOND), v
        |FROM T3
        |GROUP BY TUMBLE(ts, INTERVAL '10' SECOND), v
      """.stripMargin,
      // null columns are dropped
      Seq()
    )
  }

  @Test
  def testNegativeInputTimestamp(): Unit = {
    // simple tumbling window with record at window start
    var data = Seq(row(localDateTime("2016-03-27 19:39:30"), 1, "a"))
    registerCollection(
      "T1", data, new RowTypeInfo(LOCAL_DATE_TIME, INT_TYPE_INFO, STRING_TYPE_INFO),
      "ts, value, id")
    checkResult(
      """
        |SELECT TUMBLE_START(ts, INTERVAL '10' SECOND), TUMBLE_END(ts, INTERVAL '10' SECOND),
        |count(*)
        |FROM T1
        |GROUP BY TUMBLE(ts, INTERVAL '10' SECOND)
      """.stripMargin,
      Seq(row(localDateTime("2016-03-27 19:39:30.0"), localDateTime("2016-03-27 19:39:40.0"), 1))
    )

    // simple tumbling window with record at negative timestamp
    data = Seq(row(localDateTime("1916-03-27 19:39:31"), 1, "a"))
    registerCollection(
      "T2", data, new RowTypeInfo(LOCAL_DATE_TIME, INT_TYPE_INFO, STRING_TYPE_INFO),
      "ts, value, id")
    checkResult(
      """
        |SELECT TUMBLE_START(ts, INTERVAL '10' SECOND), TUMBLE_END(ts, INTERVAL '10' SECOND),
        |count(*)
        |FROM T2
        |GROUP BY TUMBLE(ts, INTERVAL '10' SECOND)
      """.stripMargin,
      Seq(row(localDateTime("1916-03-27 19:39:30.0"), localDateTime("1916-03-27 19:39:40.0"), 1))
    )

    // simple sliding window with record at negative timestamp
    checkResult(
      """
        |SELECT
        |HOP_START(ts, INTERVAL '10' SECOND, INTERVAL '11' SECOND),
        |HOP_END(ts, INTERVAL '10' SECOND, INTERVAL '11' SECOND),
        |count(*)
        |FROM T2
        |GROUP BY HOP(ts, INTERVAL '10' SECOND, INTERVAL '11' SECOND)
      """.stripMargin,
      Seq(row(localDateTime("1916-03-27 19:39:30.0"), localDateTime("1916-03-27 19:39:41.0"), 1))
    )

    checkResult(
      """
        |SELECT
        |HOP_START(ts, INTERVAL '0.001' SECOND(1,3), INTERVAL '0.002' SECOND(1,3)),
        |HOP_END(ts, INTERVAL '0.001' SECOND(1,3), INTERVAL '0.002' SECOND(1,3)),
        |count(*)
        |FROM T2
        |GROUP BY HOP(ts, INTERVAL '0.001' SECOND(1,3), INTERVAL '0.002' SECOND(1,3))
      """.stripMargin,
      Seq(
        row(localDateTime("1916-03-27 19:39:30.999"), localDateTime("1916-03-27 19:39:31.001"), 1),
        row(localDateTime("1916-03-27 19:39:31.0"), localDateTime("1916-03-27 19:39:31.002"), 1))
    )

    checkResult(
      """
        |SELECT
        |HOP_START(ts, INTERVAL '0.001' SECOND(1,3), INTERVAL '0.002' SECOND(1,3)),
        |HOP_END(ts, INTERVAL '0.001' SECOND(1,3), INTERVAL '0.002' SECOND(1,3)),
        |countFun(ts)
        |FROM T2
        |GROUP BY HOP(ts, INTERVAL '0.001' SECOND(1,3), INTERVAL '0.002' SECOND(1,3))
      """.stripMargin,
      Seq(
        row(localDateTime("1916-03-27 19:39:30.999"), localDateTime("1916-03-27 19:39:31.001"), 1),
        row(localDateTime("1916-03-27 19:39:31.0"), localDateTime("1916-03-27 19:39:31.002"), 1))
    )
  }

  @Test
  def testTumbleWindowWithProperties(): Unit = {
    registerCollection(
      "T",
      data3WithTimestamp,
      new RowTypeInfo(INT_TYPE_INFO, LONG_TYPE_INFO, STRING_TYPE_INFO, LOCAL_DATE_TIME),
      "a, b, c, ts")

    val sqlQuery =
      "SELECT b, COUNT(a), " +
        "TUMBLE_START(ts, INTERVAL '5' SECOND), " +
        "TUMBLE_END(ts, INTERVAL '5' SECOND), " +
        "TUMBLE_ROWTIME(ts, INTERVAL '5' SECOND)" +
        "FROM T " +
        "GROUP BY b, TUMBLE(ts, INTERVAL '5' SECOND)"

    checkResult(sqlQuery, Seq(
      row(1, 1, localDateTime("1970-01-01 00:00:00.0"),
        localDateTime("1970-01-01 00:00:05.0"), localDateTime("1970-01-01 00:00:04.999")),
      row(2, 2, localDateTime("1970-01-01 00:00:00.0"),
        localDateTime("1970-01-01 00:00:05.0"), localDateTime("1970-01-01 00:00:04.999")),
      row(3, 1, localDateTime("1970-01-01 00:00:00.0"),
        localDateTime("1970-01-01 00:00:05.0"), localDateTime("1970-01-01 00:00:04.999")),
      row(3, 2, localDateTime("1970-01-01 00:00:05.0"),
        localDateTime("1970-01-01 00:00:10.0"), localDateTime("1970-01-01 00:00:09.999")),
      row(4, 1, localDateTime("1970-01-01 00:00:10.0"),
        localDateTime("1970-01-01 00:00:15.0"), localDateTime("1970-01-01 00:00:14.999")),
      row(4, 3, localDateTime("1970-01-01 00:00:05.0"),
        localDateTime("1970-01-01 00:00:10.0"), localDateTime("1970-01-01 00:00:09.999")),
      row(5, 1, localDateTime("1970-01-01 00:00:15.0"),
        localDateTime("1970-01-01 00:00:20.0"), localDateTime("1970-01-01 00:00:19.999")),
      row(5, 4, localDateTime("1970-01-01 00:00:10.0"),
        localDateTime("1970-01-01 00:00:15.0"), localDateTime("1970-01-01 00:00:14.999")),
      row(6, 2, localDateTime("1970-01-01 00:00:20.0"),
        localDateTime("1970-01-01 00:00:25.0"), localDateTime("1970-01-01 00:00:24.999")),
      row(6, 4, localDateTime("1970-01-01 00:00:15.0"),
        localDateTime("1970-01-01 00:00:20.0"), localDateTime("1970-01-01 00:00:19.999"))))
  }

  @Test
  def testHopWindowWithProperties(): Unit = {
    registerCollection(
      "T",
      data3WithTimestamp,
      new RowTypeInfo(INT_TYPE_INFO, LONG_TYPE_INFO, STRING_TYPE_INFO, LOCAL_DATE_TIME),
      "a, b, c, ts")

    val sqlQuery =
      "SELECT b, COUNT(a), " +
        "HOP_START(ts, INTERVAL '5' SECOND, INTERVAL '10' SECOND), " +
        "HOP_END(ts, INTERVAL '5' SECOND, INTERVAL '10' SECOND), " +
        "HOP_ROWTIME(ts, INTERVAL '5' SECOND, INTERVAL '10' SECOND) " +
        "FROM T " +
        "GROUP BY b, HOP(ts, INTERVAL '5' SECOND, INTERVAL '10' SECOND)"

    checkResult(sqlQuery, Seq(
      row(1, 1, localDateTime("1969-12-31 23:59:55.0"), localDateTime("1970-01-01 00:00:05.0"),
        localDateTime("1970-01-01 00:00:04.999")),
      row(1, 1, localDateTime("1970-01-01 00:00:00.0"), localDateTime("1970-01-01 00:00:10.0"),
        localDateTime("1970-01-01 00:00:09.999")),
      row(2, 2, localDateTime("1969-12-31 23:59:55.0"), localDateTime("1970-01-01 00:00:05.0"),
        localDateTime("1970-01-01 00:00:04.999")),
      row(2, 2, localDateTime("1970-01-01 00:00:00.0"), localDateTime("1970-01-01 00:00:10.0"),
        localDateTime("1970-01-01 00:00:09.999")),
      row(3, 1, localDateTime("1969-12-31 23:59:55.0"), localDateTime("1970-01-01 00:00:05.0"),
        localDateTime("1970-01-01 00:00:04.999")),
      row(3, 2, localDateTime("1970-01-01 00:00:05.0"), localDateTime("1970-01-01 00:00:15.0"),
        localDateTime("1970-01-01 00:00:14.999")),
      row(3, 3, localDateTime("1970-01-01 00:00:00.0"), localDateTime("1970-01-01 00:00:10.0"),
        localDateTime("1970-01-01 00:00:09.999")),
      row(4, 1, localDateTime("1970-01-01 00:00:10.0"), localDateTime("1970-01-01 00:00:20.0"),
        localDateTime("1970-01-01 00:00:19.999")),
      row(4, 3, localDateTime("1970-01-01 00:00:00.0"), localDateTime("1970-01-01 00:00:10.0"),
        localDateTime("1970-01-01 00:00:09.999")),
      row(4, 4, localDateTime("1970-01-01 00:00:05.0"), localDateTime("1970-01-01 00:00:15.0"),
        localDateTime("1970-01-01 00:00:14.999")),
      row(5, 1, localDateTime("1970-01-01 00:00:15.0"), localDateTime("1970-01-01 00:00:25.0"),
        localDateTime("1970-01-01 00:00:24.999")),
      row(5, 4, localDateTime("1970-01-01 00:00:05.0"), localDateTime("1970-01-01 00:00:15.0"),
        localDateTime("1970-01-01 00:00:14.999")),
      row(5, 5, localDateTime("1970-01-01 00:00:10.0"), localDateTime("1970-01-01 00:00:20.0"),
        localDateTime("1970-01-01 00:00:19.999")),
      row(6, 2, localDateTime("1970-01-01 00:00:20.0"), localDateTime("1970-01-01 00:00:30.0"),
        localDateTime("1970-01-01 00:00:29.999")),
      row(6, 4, localDateTime("1970-01-01 00:00:10.0"), localDateTime("1970-01-01 00:00:20.0"),
        localDateTime("1970-01-01 00:00:19.999")),
      row(6, 6, localDateTime("1970-01-01 00:00:15.0"), localDateTime("1970-01-01 00:00:25.0"),
        localDateTime("1970-01-01 00:00:24.999"))))
  }

  @Test(expected = classOf[RuntimeException])
  def testSessionWindowWithProperties(): Unit = {
    registerCollection(
      "T",
      data3WithTimestamp,
      new RowTypeInfo(INT_TYPE_INFO, LONG_TYPE_INFO, STRING_TYPE_INFO, LOCAL_DATE_TIME),
      "a, b, c, ts")

    val sqlQuery =
      "SELECT COUNT(a), " +
        "SESSION_START(ts, INTERVAL '4' SECOND), " +
        "SESSION_END(ts, INTERVAL '4' SECOND), " +
        "SESSION_ROWTIME(ts, INTERVAL '4' SECOND) " +
        "FROM T " +
        "GROUP BY SESSION(ts, INTERVAL '4' SECOND)"

    checkResult(sqlQuery, Seq())
  }
}
