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

package org.apache.flink.table.runtime.batch.sql.agg

import org.apache.flink.api.common.typeinfo.BasicTypeInfo.{INT_TYPE_INFO, LONG_TYPE_INFO, STRING_TYPE_INFO}
import org.apache.flink.api.common.typeinfo.SqlTimeTypeInfo.TIMESTAMP
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.io.CollectionInputFormat
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

import scala.collection.JavaConversions._
import org.apache.flink.table.api.{TableSchema, _}
import org.apache.flink.table.runtime.utils.BatchTestBase
import org.apache.flink.table.runtime.utils.BatchTestBase.row
import org.apache.flink.table.runtime.utils.TestData._
import org.apache.flink.table.sources.BatchTableSource
import org.apache.flink.table.util.DateTimeTestUtil.UTCTimestamp
import org.apache.flink.table.util.{CountAggFunction, IntAvgAggFunction, IntSumAggFunction}
import org.apache.flink.types.Row

import org.junit.{Before, Ignore, Test}

class WindowAggregateITCase extends BatchTestBase {

  @Before
  def before(): Unit = {
    tEnv.getConfig.getConf.setInteger(TableConfigOptions.SQL_RESOURCE_DEFAULT_PARALLELISM, 3)
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
        row(1, 1, "1970-01-01 00:00:00.0"),
        row(2, 1, "1970-01-01 00:00:00.0"),
        row(3, 1, "1970-01-01 00:00:03.0"),
        row(4, 1, "1970-01-01 00:00:03.0"),
        row(5, 1, "1970-01-01 00:00:03.0"),
        row(6, 1, "1970-01-01 00:00:06.0"),
        row(7, 1, "1970-01-01 00:00:06.0"),
        row(8, 1, "1970-01-01 00:00:06.0"),
        row(9, 1, "1970-01-01 00:00:09.0"),
        row(10, 1, "1970-01-01 00:00:09.0"),
        row(11, 1, "1970-01-01 00:00:09.0"),
        row(12, 1, "1970-01-01 00:00:12.0"),
        row(13, 1, "1970-01-01 00:00:12.0"),
        row(14, 1, "1970-01-01 00:00:12.0"),
        row(15, 1, "1970-01-01 00:00:15.0"),
        row(16, 1, "1970-01-01 00:00:15.0"),
        row(17, 1, "1970-01-01 00:00:15.0"),
        row(18, 1, "1970-01-01 00:00:18.0"),
        row(19, 1, "1970-01-01 00:00:18.0"),
        row(20, 1, "1970-01-01 00:00:18.0"),
        row(21, 1, "1970-01-01 00:00:21.0")
      )
    )

    // SORT; keyed; 2-phase keyed; single row group
    checkResult(
      "SELECT a, countFun(a), TUMBLE_START(ts, INTERVAL '3' SECOND), b " +
          "FROM Table3WithTimestamp " +
          "GROUP BY a, TUMBLE(ts, INTERVAL '3' SECOND), b",
      Seq(
        row(1, 1, "1970-01-01 00:00:00.0", 1),
        row(2, 1, "1970-01-01 00:00:00.0", 2),
        row(3, 1, "1970-01-01 00:00:03.0", 2),
        row(4, 1, "1970-01-01 00:00:03.0", 3),
        row(5, 1, "1970-01-01 00:00:03.0", 3),
        row(6, 1, "1970-01-01 00:00:06.0", 3),
        row(7, 1, "1970-01-01 00:00:06.0", 4),
        row(8, 1, "1970-01-01 00:00:06.0", 4),
        row(9, 1, "1970-01-01 00:00:09.0", 4),
        row(10, 1, "1970-01-01 00:00:09.0", 4),
        row(11, 1, "1970-01-01 00:00:09.0", 5),
        row(12, 1, "1970-01-01 00:00:12.0", 5),
        row(13, 1, "1970-01-01 00:00:12.0", 5),
        row(14, 1, "1970-01-01 00:00:12.0", 5),
        row(15, 1, "1970-01-01 00:00:15.0", 5),
        row(16, 1, "1970-01-01 00:00:15.0", 6),
        row(17, 1, "1970-01-01 00:00:15.0", 6),
        row(18, 1, "1970-01-01 00:00:18.0", 6),
        row(19, 1, "1970-01-01 00:00:18.0", 6),
        row(20, 1, "1970-01-01 00:00:18.0", 6),
        row(21, 1, "1970-01-01 00:00:21.0", 6)
      )
    )

    // HASH 2-phase; sparse inputs
    checkResult(
      "SELECT a, avg(b), min(b), TUMBLE_START(f, INTERVAL '10' SECOND) " +
          "FROM Table6 " +
          "GROUP BY a, TUMBLE(f, INTERVAL '10' SECOND)",
      Seq(
        row(1, 1.1, 1.1, "2015-05-20 10:00:00.0"),
        row(2, -2.4, -2.4, "2016-09-01 23:07:00.0"),
        row(2, 2.5, 2.5, "2019-09-19 08:03:00.0"),
        row(3, -4.885, -9.77, "1999-12-12 10:00:00.0"),
        row(3, 0.08, 0.08, "1999-12-12 10:03:00.0"),
        row(4, 3.14, 3.14, "2017-11-20 09:00:00.0"),
        row(4, 3.145, 3.14, "2015-11-19 10:00:00.0"),
        row(4, 3.16, 3.16, "2015-11-20 08:59:50.0"),
        row(5, -5.9, -5.9, "1989-06-04 10:00:00.0"),
        row(5, -2.8, -2.8, "1937-07-07 08:08:00.0"),
        row(5, 0.7, 0.7, "2010-06-01 10:00:00.0"),
        row(5, 2.71, 2.71, "1997-07-01 09:00:00.0"),
        row(5, 3.9, 3.9, "2000-01-01 00:00:00.0")
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
        row(1, 1, "1969-12-31 23:59:55.0"),
        row(1, 1, "1970-01-01 00:00:00.0"),
        row(2, 5, "1969-12-31 23:59:55.0"),
        row(2, 5, "1970-01-01 00:00:00.0"),
        row(3, 11, "1970-01-01 00:00:05.0"),
        row(3, 15, "1970-01-01 00:00:00.0"),
        row(4, 10, "1970-01-01 00:00:10.0"),
        row(4, 15, "1970-01-01 00:00:00.0"),
        row(4, 34, "1970-01-01 00:00:05.0"),
        row(5, 15, "1970-01-01 00:00:15.0"),
        row(5, 36, "1970-01-01 00:00:05.0"),
        row(5, 65, "1970-01-01 00:00:10.0"),
        row(6, 111, "1970-01-01 00:00:15.0"),
        row(6, 41, "1970-01-01 00:00:20.0"),
        row(6, 51, "1970-01-01 00:00:10.0")
      )
    )

    checkResult(
      "SELECT b, sum(a), HOP_START(ts, INTERVAL '5' SECOND, INTERVAL '9' SECOND) " +
          "FROM Table3WithTimestamp " +
          "GROUP BY b, HOP(ts, INTERVAL '5' SECOND, INTERVAL '9' SECOND)",
      Seq(
        row(1, 1, "1969-12-31 23:59:55.0"),
        row(1, 1, "1970-01-01 00:00:00.0"),
        row(2, 5, "1969-12-31 23:59:55.0"),
        row(2, 5, "1970-01-01 00:00:00.0"),
        row(3, 11, "1970-01-01 00:00:05.0"),
        row(3, 15, "1970-01-01 00:00:00.0"),
        row(4, 10, "1970-01-01 00:00:10.0"),
        row(4, 15, "1970-01-01 00:00:00.0"),
        row(4, 34, "1970-01-01 00:00:05.0"),
        row(5, 15, "1970-01-01 00:00:15.0"),
        row(5, 36, "1970-01-01 00:00:05.0"),
        row(5, 65, "1970-01-01 00:00:10.0"),
        row(6, 111, "1970-01-01 00:00:15.0"),
        row(6, 41, "1970-01-01 00:00:20.0"),
        row(6, 51, "1970-01-01 00:00:10.0")
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
        row(1, 1, "1969-12-31 23:59:54.889"),
        row(1, 1, "1970-01-01 00:00:00.0"),
        row(2, 5, "1969-12-31 23:59:54.889"),
        row(2, 5, "1970-01-01 00:00:00.0"),
        row(3, 6, "1970-01-01 00:00:05.111"),
        row(3, 15, "1970-01-01 00:00:00.0"),
        row(4, 15, "1970-01-01 00:00:00.0"),
        row(4, 34, "1970-01-01 00:00:05.111"),
        row(5, 50, "1970-01-01 00:00:05.111"),
        row(5, 65, "1970-01-01 00:00:10.222"),
        row(6, 111, "1970-01-01 00:00:15.333"),
        row(6, 21, "1970-01-01 00:00:20.444"),
        row(6, 70, "1970-01-01 00:00:10.222")
      )
    )

    checkResult(
      "SELECT b, sum(a), HOP_START(ts, INTERVAL '5.111' SECOND(1,3), INTERVAL '9' SECOND) " +
          "FROM Table3WithTimestamp " +
          "GROUP BY b, HOP(ts, INTERVAL '5.111' SECOND(1,3), INTERVAL '9' SECOND)",
      Seq(
        row(1, 1, "1969-12-31 23:59:54.889"),
        row(1, 1, "1970-01-01 00:00:00.0"),
        row(2, 5, "1969-12-31 23:59:54.889"),
        row(2, 5, "1970-01-01 00:00:00.0"),
        row(3, 6, "1970-01-01 00:00:05.111"),
        row(3, 15, "1970-01-01 00:00:00.0"),
        row(4, 15, "1970-01-01 00:00:00.0"),
        row(4, 34, "1970-01-01 00:00:05.111"),
        row(5, 50, "1970-01-01 00:00:05.111"),
        row(5, 65, "1970-01-01 00:00:10.222"),
        row(6, 111, "1970-01-01 00:00:15.333"),
        row(6, 21, "1970-01-01 00:00:20.444"),
        row(6, 70, "1970-01-01 00:00:10.222")
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
        row(12, "1970-01-01 00:00:03.0", "1970-01-01 00:00:06.0"),
        row(21, "1970-01-01 00:00:06.0", "1970-01-01 00:00:09.0"),
        row(21, "1970-01-01 00:00:21.0", "1970-01-01 00:00:24.0"),
        row(3, "1970-01-01 00:00:00.0", "1970-01-01 00:00:03.0"),
        row(30, "1970-01-01 00:00:09.0", "1970-01-01 00:00:12.0"),
        row(39, "1970-01-01 00:00:12.0", "1970-01-01 00:00:15.0"),
        row(48, "1970-01-01 00:00:15.0", "1970-01-01 00:00:18.0"),
        row(57, "1970-01-01 00:00:18.0", "1970-01-01 00:00:21.0")
      )
    )

    checkResult(
      "SELECT SUM(a), " +
          "HOP_START(ts, INTERVAL '3' SECOND, INTERVAL '3' SECOND), " +
          "HOP_END(ts, INTERVAL '3' SECOND, INTERVAL '3' SECOND)" +
          "FROM Table3WithTimestamp " +
          "GROUP BY HOP(ts, INTERVAL '3' SECOND, INTERVAL '3' SECOND)",
      Seq(
        row(12, "1970-01-01 00:00:03.0", "1970-01-01 00:00:06.0"),
        row(21, "1970-01-01 00:00:06.0", "1970-01-01 00:00:09.0"),
        row(21, "1970-01-01 00:00:21.0", "1970-01-01 00:00:24.0"),
        row(3, "1970-01-01 00:00:00.0", "1970-01-01 00:00:03.0"),
        row(30, "1970-01-01 00:00:09.0", "1970-01-01 00:00:12.0"),
        row(39, "1970-01-01 00:00:12.0", "1970-01-01 00:00:15.0"),
        row(48, "1970-01-01 00:00:15.0", "1970-01-01 00:00:18.0"),
        row(57, "1970-01-01 00:00:18.0", "1970-01-01 00:00:21.0")
      )
    )

    // all group; 2-phase; pre-accumulate with paned optimization
    checkResult(
      "SELECT avgFun(a), sumFun(a), HOP_START(ts, INTERVAL '2' SECOND, INTERVAL '3' SECOND) " +
          "FROM Table3WithTimestamp " +
          "GROUP BY HOP(ts, INTERVAL '2' SECOND, INTERVAL '3' SECOND)",
      Seq(
        row(1.5, 3, "1970-01-01 00:00:00.0"),
        row(11.0, 33, "1970-01-01 00:00:10.0"),
        row(13.0, 39, "1970-01-01 00:00:12.0"),
        row(15.0, 45, "1970-01-01 00:00:14.0"),
        row(17.0, 51, "1970-01-01 00:00:16.0"),
        row(19.0, 57, "1970-01-01 00:00:18.0"),
        row(20.5, 41, "1970-01-01 00:00:20.0"),
        row(3.0, 9, "1970-01-01 00:00:02.0"),
        row(5.0, 15, "1970-01-01 00:00:04.0"),
        row(7.0, 21, "1970-01-01 00:00:06.0"),
        row(9.0, 27, "1970-01-01 00:00:08.0")
      )
    )

    checkResult(
      "SELECT AVG(a), SUM(a), HOP_START(ts, INTERVAL '2' SECOND, INTERVAL '3' SECOND) " +
          "FROM Table3WithTimestamp " +
          "GROUP BY HOP(ts, INTERVAL '2' SECOND, INTERVAL '3' SECOND)",
      Seq(
        row(1.5, 3, "1970-01-01 00:00:00.0"),
        row(11.0, 33, "1970-01-01 00:00:10.0"),
        row(13.0, 39, "1970-01-01 00:00:12.0"),
        row(15.0, 45, "1970-01-01 00:00:14.0"),
        row(17.0, 51, "1970-01-01 00:00:16.0"),
        row(19.0, 57, "1970-01-01 00:00:18.0"),
        row(20.5, 41, "1970-01-01 00:00:20.0"),
        row(3.0, 9, "1970-01-01 00:00:02.0"),
        row(5.0, 15, "1970-01-01 00:00:04.0"),
        row(7.0, 21, "1970-01-01 00:00:06.0"),
        row(9.0, 27, "1970-01-01 00:00:08.0")
      )
    )

    // millisecond precision sliding windows
    val data = Seq(
      row(UTCTimestamp("2016-03-27 09:00:00.41"), 3),
      row(UTCTimestamp("2016-03-27 09:00:00.62"), 6),
      row(UTCTimestamp("2016-03-27 09:00:00.715"), 8)
    )
    registerCollection(
      "T2", data, new RowTypeInfo(TIMESTAMP, INT_TYPE_INFO),
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
      Seq(row("2016-03-27 09:00:00.24", "2016-03-27 09:00:00.44", 1),
        row("2016-03-27 09:00:00.28", "2016-03-27 09:00:00.48", 1),
        row("2016-03-27 09:00:00.32", "2016-03-27 09:00:00.52", 1),
        row("2016-03-27 09:00:00.36", "2016-03-27 09:00:00.56", 1),
        row("2016-03-27 09:00:00.4", "2016-03-27 09:00:00.6", 1),
        row("2016-03-27 09:00:00.44", "2016-03-27 09:00:00.64", 1),
        row("2016-03-27 09:00:00.48", "2016-03-27 09:00:00.68", 1),
        row("2016-03-27 09:00:00.52", "2016-03-27 09:00:00.72", 2),
        row("2016-03-27 09:00:00.56", "2016-03-27 09:00:00.76", 2),
        row("2016-03-27 09:00:00.6", "2016-03-27 09:00:00.8", 2),
        row("2016-03-27 09:00:00.64", "2016-03-27 09:00:00.84", 1),
        row("2016-03-27 09:00:00.68", "2016-03-27 09:00:00.88", 1))
    )
  }

  @Ignore // TODO support table stats
  @Test
  def testSlidingWindow2(): Unit = {
    // for 1 phase agg
    val tableSchema = new TableSchema(
      Array("a", "b", "c", "ts"),
      Array(
        Types.INT,
        Types.LONG,
        Types.STRING,
        Types.SQL_TIMESTAMP))
    //    val colStats = Map[String, ColumnStats](
    //      "ts" -> new ColumnStats(9000000L, 1L, 8D, 8, null, null),
    //      "a" -> new ColumnStats(10000000L, 1L, 8D, 8, 5, -5),
    //      "b" -> new ColumnStats(8000000L, 0L, 4D, 32, 6.1D, 0D),
    //      "c" -> new ColumnStats(9000000L, 0L, 1024D, 32, 6.1D, 0D))
    val table = new BatchTableSource[Row] {
      override def getReturnType: TypeInformation[Row] =
        new RowTypeInfo(tableSchema.getFieldTypes, tableSchema.getFieldNames)

      //      override def getTableStats: TableStats = new TableStats(10000000L, colStats)

      override def getBoundedStream(streamEnv: StreamExecutionEnvironment): DataStream[Row] = {
        streamEnv.createInput(
          new CollectionInputFormat[Row](data3WithTimestamp,
            type3WithTimestamp.createSerializer(env.getConfig)),
          type3WithTimestamp)
      }

      override def getTableSchema: TableSchema = tableSchema
    }
    tEnv.registerTableSource("Table3WithTimestamp1", table)

    // keyed; 1-phase; pre-accumulate with paned optimization
    checkResult(
      "SELECT c, countFun(a), " +
          "HOP_START(ts, INTERVAL '4' SECOND, INTERVAL '8' SECOND), " +
          "HOP_END(ts, INTERVAL '4' SECOND, INTERVAL '8' SECOND)" +
          "FROM Table3WithTimestamp1 GROUP BY c, HOP(ts, INTERVAL '4' SECOND, INTERVAL '8' SECOND)",
      Seq(
        row("Comment#1", 1, "1970-01-01 00:00:00.0", "1970-01-01 00:00:08.0"),
        row("Comment#1", 1, "1970-01-01 00:00:04.0", "1970-01-01 00:00:12.0"),
        row("Comment#10", 1, "1970-01-01 00:00:12.0", "1970-01-01 00:00:20.0"),
        row("Comment#10", 1, "1970-01-01 00:00:16.0", "1970-01-01 00:00:24.0"),
        row("Comment#11", 1, "1970-01-01 00:00:12.0", "1970-01-01 00:00:20.0"),
        row("Comment#11", 1, "1970-01-01 00:00:16.0", "1970-01-01 00:00:24.0"),
        row("Comment#12", 1, "1970-01-01 00:00:12.0", "1970-01-01 00:00:20.0"),
        row("Comment#12", 1, "1970-01-01 00:00:16.0", "1970-01-01 00:00:24.0"),
        row("Comment#13", 1, "1970-01-01 00:00:12.0", "1970-01-01 00:00:20.0"),
        row("Comment#13", 1, "1970-01-01 00:00:16.0", "1970-01-01 00:00:24.0"),
        row("Comment#14", 1, "1970-01-01 00:00:16.0", "1970-01-01 00:00:24.0"),
        row("Comment#14", 1, "1970-01-01 00:00:20.0", "1970-01-01 00:00:28.0"),
        row("Comment#15", 1, "1970-01-01 00:00:16.0", "1970-01-01 00:00:24.0"),
        row("Comment#15", 1, "1970-01-01 00:00:20.0", "1970-01-01 00:00:28.0"),
        row("Comment#2", 1, "1970-01-01 00:00:04.0", "1970-01-01 00:00:12.0"),
        row("Comment#2", 1, "1970-01-01 00:00:08.0", "1970-01-01 00:00:16.0"),
        row("Comment#3", 1, "1970-01-01 00:00:04.0", "1970-01-01 00:00:12.0"),
        row("Comment#3", 1, "1970-01-01 00:00:08.0", "1970-01-01 00:00:16.0"),
        row("Comment#4", 1, "1970-01-01 00:00:04.0", "1970-01-01 00:00:12.0"),
        row("Comment#4", 1, "1970-01-01 00:00:08.0", "1970-01-01 00:00:16.0"),
        row("Comment#5", 1, "1970-01-01 00:00:04.0", "1970-01-01 00:00:12.0"),
        row("Comment#5", 1, "1970-01-01 00:00:08.0", "1970-01-01 00:00:16.0"),
        row("Comment#6", 1, "1970-01-01 00:00:08.0", "1970-01-01 00:00:16.0"),
        row("Comment#6", 1, "1970-01-01 00:00:12.0", "1970-01-01 00:00:20.0"),
        row("Comment#7", 1, "1970-01-01 00:00:08.0", "1970-01-01 00:00:16.0"),
        row("Comment#7", 1, "1970-01-01 00:00:12.0", "1970-01-01 00:00:20.0"),
        row("Comment#8", 1, "1970-01-01 00:00:08.0", "1970-01-01 00:00:16.0"),
        row("Comment#8", 1, "1970-01-01 00:00:12.0", "1970-01-01 00:00:20.0"),
        row("Comment#9", 1, "1970-01-01 00:00:08.0", "1970-01-01 00:00:16.0"),
        row("Comment#9", 1, "1970-01-01 00:00:12.0", "1970-01-01 00:00:20.0"),
        row("Hello world, how are you?", 1, "1970-01-01 00:00:00.0", "1970-01-01 00:00:08.0"),
        row("Hello world, how are you?", 1, "1970-01-01 00:00:04.0", "1970-01-01 00:00:12.0"),
        row("Hello world", 1, "1969-12-31 23:59:56.0", "1970-01-01 00:00:04.0"),
        row("Hello world", 1, "1970-01-01 00:00:00.0", "1970-01-01 00:00:08.0"),
        row("Hello", 1, "1969-12-31 23:59:56.0", "1970-01-01 00:00:04.0"),
        row("Hello", 1, "1970-01-01 00:00:00.0", "1970-01-01 00:00:08.0"),
        row("Hi", 1, "1969-12-31 23:59:56.0", "1970-01-01 00:00:04.0"),
        row("Hi", 1, "1970-01-01 00:00:00.0", "1970-01-01 00:00:08.0"),
        row("I am fine.", 1, "1970-01-01 00:00:00.0", "1970-01-01 00:00:08.0"),
        row("I am fine.", 1, "1970-01-01 00:00:04.0", "1970-01-01 00:00:12.0"),
        row("Luke Skywalker", 1, "1970-01-01 00:00:00.0", "1970-01-01 00:00:08.0"),
        row("Luke Skywalker", 1, "1970-01-01 00:00:04.0", "1970-01-01 00:00:12.0")
      )
    )

    // keyed; 1-phase; pre-accumulate with windowSize = slideSize
    checkResult(
      "SELECT c, count(a), " +
          "HOP_START(ts, INTERVAL '8' SECOND, INTERVAL '8' SECOND), " +
          "HOP_END(ts, INTERVAL '8' SECOND, INTERVAL '8' SECOND)" +
          "FROM Table3WithTimestamp1 GROUP BY c, HOP(ts, INTERVAL '8' SECOND, INTERVAL '8' SECOND)",
      Seq(
        row("Comment#1", 1, "1970-01-01 00:00:00.0", "1970-01-01 00:00:08.0"),
        row("Comment#10", 1, "1970-01-01 00:00:16.0", "1970-01-01 00:00:24.0"),
        row("Comment#11", 1, "1970-01-01 00:00:16.0", "1970-01-01 00:00:24.0"),
        row("Comment#12", 1, "1970-01-01 00:00:16.0", "1970-01-01 00:00:24.0"),
        row("Comment#13", 1, "1970-01-01 00:00:16.0", "1970-01-01 00:00:24.0"),
        row("Comment#14", 1, "1970-01-01 00:00:16.0", "1970-01-01 00:00:24.0"),
        row("Comment#15", 1, "1970-01-01 00:00:16.0", "1970-01-01 00:00:24.0"),
        row("Comment#2", 1, "1970-01-01 00:00:08.0", "1970-01-01 00:00:16.0"),
        row("Comment#3", 1, "1970-01-01 00:00:08.0", "1970-01-01 00:00:16.0"),
        row("Comment#4", 1, "1970-01-01 00:00:08.0", "1970-01-01 00:00:16.0"),
        row("Comment#5", 1, "1970-01-01 00:00:08.0", "1970-01-01 00:00:16.0"),
        row("Comment#6", 1, "1970-01-01 00:00:08.0", "1970-01-01 00:00:16.0"),
        row("Comment#7", 1, "1970-01-01 00:00:08.0", "1970-01-01 00:00:16.0"),
        row("Comment#8", 1, "1970-01-01 00:00:08.0", "1970-01-01 00:00:16.0"),
        row("Comment#9", 1, "1970-01-01 00:00:08.0", "1970-01-01 00:00:16.0"),
        row("Hello world, how are you?", 1, "1970-01-01 00:00:00.0", "1970-01-01 00:00:08.0"),
        row("Hello world", 1, "1970-01-01 00:00:00.0", "1970-01-01 00:00:08.0"),
        row("Hello", 1, "1970-01-01 00:00:00.0", "1970-01-01 00:00:08.0"),
        row("Hi", 1, "1970-01-01 00:00:00.0", "1970-01-01 00:00:08.0"),
        row("I am fine.", 1, "1970-01-01 00:00:00.0", "1970-01-01 00:00:08.0"),
        row("Luke Skywalker", 1, "1970-01-01 00:00:00.0", "1970-01-01 00:00:08.0")
      )
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
      "T1", data, new RowTypeInfo(TIMESTAMP, INT_TYPE_INFO),
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
      row(UTCTimestamp("2016-03-27 09:00:05"), 1),
      row(null, 2),
      row(UTCTimestamp("2016-03-27 09:00:32"), 3),
      row(null, 4)
    )
    registerCollection(
      "T2", data, new RowTypeInfo(TIMESTAMP, INT_TYPE_INFO),
      "ts, v")
    checkResult(
      """
        |SELECT TUMBLE_START(ts, INTERVAL '10' SECOND), TUMBLE_END(ts, INTERVAL '10' SECOND), v
        |FROM T2
        |GROUP BY TUMBLE(ts, INTERVAL '10' SECOND), v
      """.stripMargin,
      // null columns are dropped
      Seq(
        row("2016-03-27 09:00:00.0", "2016-03-27 09:00:10.0", 1),
        row("2016-03-27 09:00:30.0", "2016-03-27 09:00:40.0", 3))
    )
    data = Seq(
      row(null, 1),
      row(null, 2),
      row(null, 3),
      row(null, 4)
    )
    registerCollection(
      "T3", data, new RowTypeInfo(TIMESTAMP, INT_TYPE_INFO),
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
    var data = Seq(row(UTCTimestamp("2016-03-27 19:39:30"), 1, "a"))
    registerCollection(
      "T1", data, new RowTypeInfo(TIMESTAMP, INT_TYPE_INFO, STRING_TYPE_INFO),
      "ts, value, id")
    checkResult(
      """
        |SELECT TUMBLE_START(ts, INTERVAL '10' SECOND), TUMBLE_END(ts, INTERVAL '10' SECOND),
        |count(*)
        |FROM T1
        |GROUP BY TUMBLE(ts, INTERVAL '10' SECOND)
      """.stripMargin,
      Seq(row("2016-03-27 19:39:30.0", "2016-03-27 19:39:40.0", 1))
    )

    // simple tumbling window with record at negative timestamp
    data = Seq(row(UTCTimestamp("1916-03-27 19:39:31"), 1, "a"))
    registerCollection(
      "T2", data, new RowTypeInfo(TIMESTAMP, INT_TYPE_INFO, STRING_TYPE_INFO),
      "ts, value, id")
    checkResult(
      """
        |SELECT TUMBLE_START(ts, INTERVAL '10' SECOND), TUMBLE_END(ts, INTERVAL '10' SECOND),
        |count(*)
        |FROM T2
        |GROUP BY TUMBLE(ts, INTERVAL '10' SECOND)
      """.stripMargin,
      Seq(row("1916-03-27 19:39:30.0", "1916-03-27 19:39:40.0", 1))
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
      Seq(row("1916-03-27 19:39:30.0", "1916-03-27 19:39:41.0", 1))
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
      Seq(row("1916-03-27 19:39:30.999", "1916-03-27 19:39:31.001", 1),
        row("1916-03-27 19:39:31.0", "1916-03-27 19:39:31.002", 1))
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
      Seq(row("1916-03-27 19:39:30.999", "1916-03-27 19:39:31.001", 1),
        row("1916-03-27 19:39:31.0", "1916-03-27 19:39:31.002", 1))
    )
  }

  @Test
  def testTumbleWindowWithProperties(): Unit = {
    registerCollection(
      "T",
      data3WithTimestamp,
      new RowTypeInfo(INT_TYPE_INFO, LONG_TYPE_INFO, STRING_TYPE_INFO, TIMESTAMP),
      "a, b, c, ts")

    val sqlQuery =
      "SELECT b, COUNT(a), " +
        "TUMBLE_START(ts, INTERVAL '5' SECOND), " +
        "TUMBLE_END(ts, INTERVAL '5' SECOND), " +
        "TUMBLE_ROWTIME(ts, INTERVAL '5' SECOND)" +
        "FROM T " +
        "GROUP BY b, TUMBLE(ts, INTERVAL '5' SECOND)"

    checkResult(sqlQuery, Seq(
      row(1, 1, "1970-01-01 00:00:00.0", "1970-01-01 00:00:05.0", "1970-01-01 00:00:04.999"),
      row(2, 2, "1970-01-01 00:00:00.0", "1970-01-01 00:00:05.0", "1970-01-01 00:00:04.999"),
      row(3, 1, "1970-01-01 00:00:00.0", "1970-01-01 00:00:05.0", "1970-01-01 00:00:04.999"),
      row(3, 2, "1970-01-01 00:00:05.0", "1970-01-01 00:00:10.0", "1970-01-01 00:00:09.999"),
      row(4, 1, "1970-01-01 00:00:10.0", "1970-01-01 00:00:15.0", "1970-01-01 00:00:14.999"),
      row(4, 3, "1970-01-01 00:00:05.0", "1970-01-01 00:00:10.0", "1970-01-01 00:00:09.999"),
      row(5, 1, "1970-01-01 00:00:15.0", "1970-01-01 00:00:20.0", "1970-01-01 00:00:19.999"),
      row(5, 4, "1970-01-01 00:00:10.0", "1970-01-01 00:00:15.0", "1970-01-01 00:00:14.999"),
      row(6, 2, "1970-01-01 00:00:20.0", "1970-01-01 00:00:25.0", "1970-01-01 00:00:24.999"),
      row(6, 4, "1970-01-01 00:00:15.0", "1970-01-01 00:00:20.0", "1970-01-01 00:00:19.999")))
  }

  @Test
  def testHopWindowWithProperties(): Unit = {
    registerCollection(
      "T",
      data3WithTimestamp,
      new RowTypeInfo(INT_TYPE_INFO, LONG_TYPE_INFO, STRING_TYPE_INFO, TIMESTAMP),
      "a, b, c, ts")

    val sqlQuery =
      "SELECT b, COUNT(a), " +
        "HOP_START(ts, INTERVAL '5' SECOND, INTERVAL '10' SECOND), " +
        "HOP_END(ts, INTERVAL '5' SECOND, INTERVAL '10' SECOND), " +
        "HOP_ROWTIME(ts, INTERVAL '5' SECOND, INTERVAL '10' SECOND) " +
        "FROM T " +
        "GROUP BY b, HOP(ts, INTERVAL '5' SECOND, INTERVAL '10' SECOND)"

    checkResult(sqlQuery, Seq(
      row(1, 1, "1969-12-31 23:59:55.0", "1970-01-01 00:00:05.0", "1970-01-01 00:00:04.999"),
      row(1, 1, "1970-01-01 00:00:00.0", "1970-01-01 00:00:10.0", "1970-01-01 00:00:09.999"),
      row(2, 2, "1969-12-31 23:59:55.0", "1970-01-01 00:00:05.0", "1970-01-01 00:00:04.999"),
      row(2, 2, "1970-01-01 00:00:00.0", "1970-01-01 00:00:10.0", "1970-01-01 00:00:09.999"),
      row(3, 1, "1969-12-31 23:59:55.0", "1970-01-01 00:00:05.0", "1970-01-01 00:00:04.999"),
      row(3, 2, "1970-01-01 00:00:05.0", "1970-01-01 00:00:15.0", "1970-01-01 00:00:14.999"),
      row(3, 3, "1970-01-01 00:00:00.0", "1970-01-01 00:00:10.0", "1970-01-01 00:00:09.999"),
      row(4, 1, "1970-01-01 00:00:10.0", "1970-01-01 00:00:20.0", "1970-01-01 00:00:19.999"),
      row(4, 3, "1970-01-01 00:00:00.0", "1970-01-01 00:00:10.0", "1970-01-01 00:00:09.999"),
      row(4, 4, "1970-01-01 00:00:05.0", "1970-01-01 00:00:15.0", "1970-01-01 00:00:14.999"),
      row(5, 1, "1970-01-01 00:00:15.0", "1970-01-01 00:00:25.0", "1970-01-01 00:00:24.999"),
      row(5, 4, "1970-01-01 00:00:05.0", "1970-01-01 00:00:15.0", "1970-01-01 00:00:14.999"),
      row(5, 5, "1970-01-01 00:00:10.0", "1970-01-01 00:00:20.0", "1970-01-01 00:00:19.999"),
      row(6, 2, "1970-01-01 00:00:20.0", "1970-01-01 00:00:30.0", "1970-01-01 00:00:29.999"),
      row(6, 4, "1970-01-01 00:00:10.0", "1970-01-01 00:00:20.0", "1970-01-01 00:00:19.999"),
      row(6, 6, "1970-01-01 00:00:15.0", "1970-01-01 00:00:25.0", "1970-01-01 00:00:24.999")))
  }

  @Test(expected = classOf[RuntimeException])
  def testSessionWindowWithProperties(): Unit = {
    registerCollection(
      "T",
      data3WithTimestamp,
      new RowTypeInfo(INT_TYPE_INFO, LONG_TYPE_INFO, STRING_TYPE_INFO, TIMESTAMP),
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
