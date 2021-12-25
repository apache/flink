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
package org.apache.flink.table.planner.runtime.stream.sql

import org.apache.flink.api.common.time.Time
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.api.internal.TableEnvironmentInternal
import org.apache.flink.table.api.{Types, _}
import org.apache.flink.table.planner.functions.aggfunctions.{ListAggWithRetractAggFunction, ListAggWsWithRetractAggFunction}
import org.apache.flink.table.planner.plan.utils.JavaUserDefinedAggFunctions.VarSumAggFunction
import org.apache.flink.table.planner.runtime.batch.sql.agg.{MyPojoAggFunction, VarArgsAggFunction}
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedAggFunctions.OverloadedMaxFunction
import org.apache.flink.table.planner.runtime.utils.StreamingWithAggTestBase.AggMode
import org.apache.flink.table.planner.runtime.utils.StreamingWithMiniBatchTestBase.MiniBatchMode
import org.apache.flink.table.planner.runtime.utils.StreamingWithStateTestBase.StateBackendMode
import org.apache.flink.table.planner.runtime.utils.TimeTestUtil.TimestampAndWatermarkWithOffset
import org.apache.flink.table.planner.runtime.utils.UserDefinedFunctionTestUtils._
import org.apache.flink.table.planner.runtime.utils._
import org.apache.flink.table.planner.utils.DateTimeTestUtil.{localDate, localDateTime, localTime => mLocalTime}
import org.apache.flink.table.runtime.typeutils.BigDecimalTypeInfo
import org.apache.flink.types.Row

import org.junit.Assert.assertEquals
import org.junit._
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

import java.lang.{Integer => JInt, Long => JLong}
import java.math.{BigDecimal => JBigDecimal}

import scala.collection.{Seq, mutable}
import scala.util.Random

@RunWith(classOf[Parameterized])
class AggregateITCase(
    aggMode: AggMode,
    miniBatch: MiniBatchMode,
    backend: StateBackendMode)
  extends StreamingWithAggTestBase(aggMode, miniBatch, backend) {

  val data = List(
    (1000L, 1, "Hello"),
    (2000L, 2, "Hello"),
    (3000L, 3, "Hello"),
    (4000L, 4, "Hello"),
    (5000L, 5, "Hello"),
    (6000L, 6, "Hello"),
    (7000L, 7, "Hello World"),
    (8000L, 8, "Hello World"),
    (20000L, 20, "Hello World"))

  @Test
  def testEmptyInputAggregation(): Unit = {
    val data = new mutable.MutableList[(Int, Int)]
    data .+= ((1, 1))
    data .+= ((2, 2))
    data .+= ((3, 3))

    val t = failingDataSource(data).toTable(tEnv, 'a, 'b)
    tEnv.registerTable("T", t)

    val t1 = tEnv.sqlQuery(
      "select sum(a), avg(a), min(a), count(a), count(1) from T where a > 9999 group by b")
    val sink = new TestingRetractSink
    t1.toRetractStream[Row].addSink(sink)
    env.execute()
    val expected = List()
    assertEquals(expected, sink.getRetractResults)
  }

  @Test
  def testShufflePojo(): Unit = {
    val data = new mutable.MutableList[(Int, Int)]
    data .+= ((1, 1))
    data .+= ((2, 2))
    data .+= ((3, 3))

    val t = failingDataSource(data).toTable(tEnv, 'a, 'b)
    tEnv.registerTable("T", t)
    tEnv.registerFunction("pojoFunc", MyToPojoFunc)

    val t1 = tEnv.sqlQuery(
      "select sum(a), avg(a), min(a), count(a), count(1) from T group by pojoFunc(b)")
    val sink = new TestingRetractSink
    t1.toRetractStream[Row].addSink(sink)
    env.execute()
    val expected = List(
      "1,1,1,1,1",
      "2,2,2,1,1",
      "3,3,3,1,1")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Ignore("[FLINK-12215] Fix this when introduce SqlProcessFunction.")
  @Test
  def testEmptyInputAggregationWithoutGroupBy(): Unit = {
    val data = new mutable.MutableList[(Int, Int)]
    data .+= ((1, 1))
    data .+= ((2, 2))
    data .+= ((3, 3))

    val t = failingDataSource(data).toTable(tEnv, 'a, 'b)
    tEnv.registerTable("T", t)

    val t1 = tEnv.sqlQuery(
      "select sum(a), avg(a), min(a), count(a), count(1) from T where a > 9999")
    val sink = new TestingRetractSink
    t1.toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()
    val expected = List("null,null,null,0,0")
    assertEquals(expected, sink.getRetractResults)
  }

  @Test
  def testAggregationWithoutWatermark(): Unit = {
    // NOTE: Different from AggregateITCase, we do not set stream time characteristic
    // of environment to event time, so that emitWatermark() actually does nothing.
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

    val data = new mutable.MutableList[(Int, Int)]
    data .+= ((1, 1))
    data .+= ((2, 2))
    data .+= ((3, 3))

    val t = failingDataSource(data).toTable(tEnv, 'a, 'b)
    tEnv.registerTable("T", t)

    val t1 = tEnv.sqlQuery(
      "select sum(a), avg(a), min(a), count(a), count(1) from T")
    val sink = new TestingRetractSink
    t1.toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()
    val expected = List("6,2,1,3,3")
    assertEquals(expected, sink.getRetractResults)
  }

  @Test
  def testDistinctGroupBy(): Unit = {

    val sqlQuery =
      "SELECT b, " +
        "  SUM(DISTINCT (a * 3)), " +
        "  COUNT(DISTINCT SUBSTRING(c FROM 1 FOR 2))," +
        "  COUNT(DISTINCT c) " +
        "FROM MyTable " +
        "GROUP BY b"

    val t = failingDataSource(TestData.tupleData3).toTable(tEnv, 'a, 'b, 'c)
    tEnv.registerTable("MyTable", t)

    val result = tEnv.sqlQuery(sqlQuery).toRetractStream[Row]
    val sink = new TestingRetractSink
    result.addSink(sink)
    env.execute()

    val expected = List(
      "1,3,1,1",
      "2,15,1,2",
      "3,45,3,3",
      "4,102,1,4",
      "5,195,1,5",
      "6,333,1,6")

    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testCountDistinct(): Unit = {
    val ids = List(
      1,
      2, 2,
      3, 3, 3,
      4, 4, 4, 4,
      5, 5, 5, 5, 5)

    val dateTimes = List(
      "1970-01-01 00:00:01",
      "1970-01-01 00:00:02", null,
      "1970-01-01 00:00:04", "1970-01-01 00:00:05", "1970-01-01 00:00:06",
      "1970-01-01 00:00:07", null, null, "1970-01-01 00:00:10",

      "1970-01-01 00:00:11", "1970-01-01 00:00:11", "1970-01-01 00:00:13",
      "1970-01-01 00:00:14", "1970-01-01 00:00:15")

    val dates = List(
      "1970-01-01",
      "1970-01-02", null,
      "1970-01-04", "1970-01-05", "1970-01-06",
      "1970-01-07", null, null, "1970-01-10",
      "1970-01-11", "1970-01-11", "1970-01-13", "1970-01-14", "1970-01-15")

    val times = List(
      "00:00:01",
      "00:00:02", null,
      "00:00:04", "00:00:05", "00:00:06",
      "00:00:07", null, null, "00:00:10",
      "00:00:11", "00:00:11", "00:00:13", "00:00:14", "00:00:15")

    val integers = List(
      "1",
      "2", null,
      "4", "5", "6",
      "7", null, null, "10",
      "11", "11", "13", "14", "15")

    val chars = List(
      "A",
      "B", null,
      "D", "E", "F",
      "H", null, null, "K",
      "L", "L", "N", "O", "P")

    val data = new mutable.MutableList[Row]

    for (i <- ids.indices) {
      val v = integers(i)
      val decimal = if (v == null) null else new JBigDecimal(v)
      val int = if (v == null) null else JInt.valueOf(v)
      val long = if (v == null) null else JLong.valueOf(v)
      data.+=(Row.of(
        Int.box(ids(i)), localDateTime(dateTimes(i)), localDate(dates(i)),
        mLocalTime(times(i)), decimal, int, long, chars(i)))
    }

    val inputs = Random.shuffle(data)

    val rowType = new RowTypeInfo(
      Types.INT, Types.LOCAL_DATE_TIME, Types.LOCAL_DATE, Types.LOCAL_TIME,
      Types.DECIMAL, Types.INT, Types.LONG, Types.STRING)

    val t = failingDataSource(inputs)(rowType).toTable(tEnv, 'id, 'a, 'b, 'c, 'd, 'e, 'f, 'g)
    tEnv.createTemporaryView("T", t)
    val t1 = tEnv.sqlQuery(
      s"""
         |SELECT
         | id,
         | count(distinct a),
         | count(distinct b),
         | count(distinct c),
         | count(distinct d),
         | count(distinct e),
         | count(distinct f),
         | count(distinct g)
         |FROM T GROUP BY id
       """.stripMargin)

    val sink = new TestingRetractSink
    t1.toRetractStream[Row].addSink(sink)
    env.execute()

    val expected = List(
      "1,1,1,1,1,1,1,1",
      "2,1,1,1,1,1,1,1",
      "3,3,3,3,3,3,3,3",
      "4,2,2,2,2,2,2,2",
      "5,4,4,4,4,4,4,4")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testDistinctWithRetract(): Unit = {
    // this case covers LongArrayValueWithRetractionGenerator and LongValueWithRetractionGenerator
    val data = new mutable.MutableList[(Int, Long, String)]
    data.+=((1, 1L, "A"))
    data.+=((1, 1L, "A"))
    data.+=((1, 1L, "A"))
    data.+=((2, 2L, "B"))
    data.+=((3, 2L, "B"))
    data.+=((4, 3L, "C"))
    data.+=((5, 3L, "C"))
    data.+=((6, 3L, "C"))
    data.+=((7, 4L, "B"))
    data.+=((8, 4L, "A"))
    data.+=((9, 4L, "D"))
    data.+=((10, 4L, "E"))
    data.+=((11, 5L, "A"))
    data.+=((12, 5L, "B"))
    // b, count(a) as cnt
    // 1, 3
    // 2, 2
    // 3, 3
    // 4, 4
    // 5, 2

    val t = failingDataSource(data).toTable(tEnv, 'a, 'b, 'c)
    tEnv.registerTable("T", t)

    val sql =
      """
        |SELECT
        |  count(distinct cnt),
        |  sum(distinct cnt),
        |  max(distinct cnt),
        |  min(distinct cnt),
        |  avg(distinct cnt),
        |  count(distinct max_a)
        |FROM (
        | SELECT b, count(a) as cnt, max(a) as max_a
        | FROM T
        | GROUP BY b)
      """.stripMargin

    val t1 = tEnv.sqlQuery(sql)
    val sink = new TestingRetractSink
    t1.toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = List("3,9,4,2,3,5")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testDistinctAggregateMoreThan64(): Unit = {
    // this case is used to cover DistinctAggCodeGen#LongArrayValueWithoutRetractionGenerator
    val data = new mutable.MutableList[(Int, Int)]
    for (i <- 0 until 100) {
      for (j <- 0 until 100 - i) {
        data.+=((j, i))
      }
    }
    val t = failingDataSource(Random.shuffle(data)).toTable(tEnv, 'a, 'b)
    tEnv.registerTable("T", t)

    val distincts = for (i <- 0 until 100) yield {
      s"count(distinct a) filter (where b = $i)"
    }

    val sql =
      s"""
         |SELECT
         |  ${distincts.mkString(", ")}
         |FROM T
       """.stripMargin

    val t1 = tEnv.sqlQuery(sql)
    val sink = new TestingRetractSink
    t1.toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = List((1 to 100).reverse.mkString(","))
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testDistinctAggWithNullValues(): Unit = {
    val data = new mutable.MutableList[(Int, Long, String)]
    data.+=((1, 1L, "A"))
    data.+=((2, 2L, "B"))
    data.+=((3, 2L, "B"))
    data.+=((4, 3L, "C"))
    data.+=((5, 3L, "C"))
    data.+=((6, 3L, null))
    data.+=((7, 3L, "C"))
    data.+=((8, 4L, "B"))
    data.+=((9, 4L, null))
    data.+=((10, 4L, null))
    data.+=((11, 4L, "A"))
    data.+=((12, 4L, "D"))
    data.+=((13, 4L, null))
    data.+=((14, 4L, "E"))
    data.+=((15, 5L, "A"))
    data.+=((16, 5L, null))
    data.+=((17, 5L, "B"))

    val t = failingDataSource(data).toTable(tEnv, 'a, 'b, 'c)
    tEnv.registerTable("T", t)
    tEnv.createTemporarySystemFunction("CntNullNonNull", new CountNullNonNull)
    val t1 = tEnv.sqlQuery(
      "SELECT b, count(*), CntNullNonNull(DISTINCT c)  FROM T GROUP BY b")

    val sink = new TestingRetractSink
    t1.toRetractStream[Row].addSink(sink)
    env.execute()

    val expected = List("1,1,1|0", "2,2,1|0", "3,4,1|1", "4,7,4|1", "5,3,2|1")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testPrecisionForSumAggregationOnDecimal(): Unit = {
    var t = tEnv.sqlQuery(
        "select cast(sum(1.03520274) as DECIMAL(32, 8)), " +
        "cast(sum(12345.035202748654) AS DECIMAL(30, 20)), " +
        "cast(sum(12.345678901234567) AS DECIMAL(25, 22))")
    var sink = new TestingRetractSink
    t.toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()
    var expected = List("1.03520274,12345.03520274865400000000,12.3456789012345670000000")
    assertEquals(expected, sink.getRetractResults)

    val data = new mutable.MutableList[(Double, Int)]
    data .+= ((1.11111111, 1))
    data .+= ((1.11111111, 2))
    env.setParallelism(1)

    t = failingDataSource(data).toTable(tEnv, 'a, 'b)
    tEnv.registerTable("T", t)

    t = tEnv.sqlQuery("select sum(cast(a as decimal(32, 8))) from T")
    sink = new TestingRetractSink
    t.toRetractStream[Row].addSink(sink)
    env.execute()
    expected = List("2.22222222")
    assertEquals(expected, sink.getRetractResults)
  }

  @Test
  def testGroupByAgg(): Unit = {
    val data = new mutable.MutableList[(Int, Long, String)]
    data.+=((1, 1L, "A"))
    data.+=((2, 2L, "B"))
    data.+=((3, 2L, "B"))
    data.+=((4, 3L, "C"))
    data.+=((5, 3L, "C"))
    data.+=((6, 3L, "C"))
    data.+=((7, 4L, "B"))
    data.+=((8, 4L, "A"))
    data.+=((9, 4L, "D"))
    data.+=((10, 4L, "E"))
    data.+=((11, 5L, "A"))
    data.+=((12, 5L, "B"))

    val t = failingDataSource(data).toTable(tEnv, 'a, 'b, 'c)
    tEnv.registerTable("T", t)
    val t1 = tEnv.sqlQuery("SELECT b, count(c), sum(a) FROM T GROUP BY b")

    val sink = new TestingRetractSink
    t1.toRetractStream[Row].addSink(sink)
    env.execute()

    val expected = List("1,1,1", "2,2,5", "3,3,15", "4,4,34", "5,2,23")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  def testCountWithNullableIfCall(): Unit = {
    val data = new mutable.MutableList[(Int, Long, String)]
    data.+=((1, 1L, "A"))
    data.+=((2, 2L, "B"))
    data.+=((3, 2L, "B"))
    data.+=((4, 3L, "C"))
    data.+=((5, 3L, "C"))
    data.+=((6, 3L, "C"))
    data.+=((7, 4L, "B"))
    data.+=((8, 4L, "A"))
    data.+=((9, 4L, "D"))
    data.+=((10, 4L, "E"))
    data.+=((11, 5L, "A"))
    data.+=((12, 5L, "B"))

    val t = failingDataSource(data).toTable(tEnv, 'a, 'b, 'c)
    tEnv.registerTable("T", t)
    val sql =
      s"""
         |select
         |  b
         |  ,count(1)
         |  ,count(if(c in ('A', 'B'), cast(null as integer), 1)) as cnt
         |  ,count(if(c not in ('A', 'B'), 1, cast(null as integer))) as cnt1
         |from T
         |group by b
       """.stripMargin
    val t1 = tEnv.sqlQuery(sql)

    val sink = new TestingRetractSink
    t1.toRetractStream[Row].addSink(sink)
    env.execute()

    val expected = List("1,1,0,0", "2,2,0,0", "3,3,3,3", "4,4,2,2", "5,2,0,0")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testNestedGroupByAgg(): Unit = {
    val data = new mutable.MutableList[(Int, Long, String)]
    data.+=((1, 1L, "A"))
    data.+=((2, 2L, "B"))
    data.+=((3, 2L, "B"))
    data.+=((4, 3L, "C"))
    data.+=((5, 3L, "C"))
    data.+=((6, 3L, "C"))
    data.+=((7, 4L, "B"))
    data.+=((8, 4L, "A"))
    data.+=((9, 4L, "D"))
    data.+=((10, 4L, "E"))
    data.+=((11, 5L, "A"))
    data.+=((12, 5L, "B"))

    val t = failingDataSource(data).toTable(tEnv, 'a, 'b, 'c)
    tEnv.registerTable("T", t)

    val sql =
      """
        |SELECT sum(b), count(a), max(a), min(a), c
        |FROM (
        | SELECT b, count(c) as c, sum(a) as a
        | FROM T
        | GROUP BY b)
        |GROUP BY c
      """.stripMargin

    val t1 = tEnv.sqlQuery(sql)
    val sink = new TestingRetractSink
    t1.toRetractStream[Row].addSink(sink)
    env.execute()

    val expected = List("1,1,1,1,1", "3,1,15,15,3", "4,1,34,34,4", "7,2,23,5,2")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  /** test unbounded groupBy (without window) **/
  @Test
  def testUnboundedGroupBy(): Unit = {
    val t = failingDataSource(TestData.tupleData3).toTable(tEnv, 'a, 'b, 'c)
    tEnv.registerTable("MyTable", t)

    val sqlQuery = "SELECT b, COUNT(a) FROM MyTable GROUP BY b"

    val sink = new TestingRetractSink
    tEnv.sqlQuery(sqlQuery).toRetractStream[Row].addSink(sink)
    env.execute()

    val expected = List("1,1", "2,2", "3,3", "4,4", "5,5", "6,6")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testWindowWithUnboundedAgg(): Unit = {
    val t = failingDataSource(TestData.tupleData5.map {
      case (a, b, c, d, e) => (b, a, c, d, e)
    }).assignTimestampsAndWatermarks(
      new TimestampAndWatermarkWithOffset[(Long, Int, Int, String, Long)](0L))
        .toTable(tEnv, 'rowtime.rowtime, 'a, 'c, 'd, 'e)
    tEnv.registerTable("MyTable", t)

    val innerSql =
      """
        |SELECT a,
        |   SUM(DISTINCT e) b,
        |   MIN(DISTINCT e) c,
        |   COUNT(DISTINCT e) d
        |FROM MyTable
        |GROUP BY a, TUMBLE(rowtime, INTERVAL '0.005' SECOND)
      """.stripMargin

    val sqlQuery = "SELECT c, MAX(a), COUNT(DISTINCT d) FROM (" + innerSql + ") GROUP BY c"

    val results = tEnv.sqlQuery(sqlQuery).toRetractStream[Row]
    val sink = new TestingRetractSink
    results.addSink(sink)
    env.execute()

    val expected = List(
      "1,5,3",
      "2,5,2")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }


  @Test
  def testListAggWithRetraction(): Unit = {
    env.setParallelism(1) // we have to use parallelism=1 to make sure the result is deterministic
    val dataWithNull = List(
      ("1", "a"),
      ("1", "b"),
      ("1", null),
      ("1", "a"))

    val t: DataStream[(String, String)] = failingDataSource(dataWithNull)
    val streamTable = t.toTable(tEnv, 'x, 'y)
    tEnv.registerTable("T", streamTable)

    tEnv.executeSql(
      """
        |CREATE VIEW view1 AS
        |SELECT
        |    x,
        |    y,
        |    CAST(COUNT(1) AS VARCHAR) AS ct
        |FROM T
        |GROUP BY
        |    x, y
        |""".stripMargin)

    // | x | concat_ws  |
    // |---|------------|
    // | 1 | a=2        |
    // | 1 | b=1        |
    // | 1 | 1          |
    val sqlQuery =
      s"""
         |select
         |     x,
         |     '[' || LISTAGG(CONCAT_WS('=', y, ct), ';') || ']' AS list1,
         |     '[' || LISTAGG(CONCAT_WS('=', y, ct)) || ']' AS list2
         |FROM view1
         |GROUP BY x
       """.stripMargin

    val sink = new TestingRetractSink
    tEnv.sqlQuery(sqlQuery).toRetractStream[Row].addSink(sink)
    env.execute()

    val expected = List("1,[b=1;1;a=2],[b=1,1,a=2]")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testListAggWithNullData(): Unit = {
    val dataWithNull = List(
      (1, 1, null),
      (2, 1, null),
      (3, 1, null))

    val t: DataStream[(Int, Int, String)] = failingDataSource(dataWithNull)
    val streamTable = t.toTable(tEnv, 'id, 'len, 'content)
    tEnv.registerTable("T", streamTable)

    val sqlQuery =
      s"""
         |SELECT len, listagg(content, '#') FROM T GROUP BY len
       """.stripMargin

    val sink = new TestingRetractSink
    tEnv.sqlQuery(sqlQuery).toRetractStream[Row].addSink(sink)
    env.execute()

    val expected = List("1,null")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testListAggWithoutDelimiterTreatNull(): Unit = {
    val dataWithNull = List(
      (1, 1, null),
      (2, 1, null),
      (3, 1, null))

    val t: DataStream[(Int, Int, String)] = failingDataSource(dataWithNull)
    val streamTable = t.toTable(tEnv, 'id, 'len, 'content)
    tEnv.registerTable("T", streamTable)

    val sqlQuery =
      s"""
         |SELECT len, listagg(content) FROM T GROUP BY len
       """.stripMargin

    val sink = new TestingRetractSink
    tEnv.sqlQuery(sqlQuery).toRetractStream[Row].addSink(sink)
    env.execute()

    val expected = List("1,null")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testListAggWithDistinct(): Unit = {
    val data = new mutable.MutableList[(Int, Long, String)]
    data.+=((1, 1L, "A"))
    data.+=((2, 2L, "B"))
    data.+=((3, 2L, "B"))
    data.+=((4, 3L, "C"))
    data.+=((5, 3L, "C"))
    data.+=((6, 3L, "A"))
    data.+=((7, 4L, "EF"))
    data.+=((1, 1L, "A"))
    data.+=((8, 4L, "EF"))
    data.+=((8, 4L, null))
    val sqlQuery = "SELECT b, LISTAGG(DISTINCT c, '#') FROM MyTable GROUP BY b"
    tEnv.registerTable("MyTable",
      failingDataSource(data).toTable(tEnv).as("a", "b", "c"))
    val sink = new TestingRetractSink
    tEnv.sqlQuery(sqlQuery).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()
    val expected = List("1,A", "2,B", "3,C#A", "4,EF")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testUnboundedGroupByCollect(): Unit = {
    val sqlQuery = "SELECT b, COLLECT(a) FROM MyTable GROUP BY b"

    val t = failingDataSource(TestData.tupleData3).toTable(tEnv, 'a, 'b, 'c)
    tEnv.registerTable("MyTable", t)

    val sink = new TestingRetractSink
    tEnv.sqlQuery(sqlQuery).toRetractStream[Row].addSink(sink)
    env.execute()

    // TODO: the string result of collect is not deterministic
    // TODO: sort the map result in the future
    val expected = List(
      "1,{1=1}",
      "2,{2=1, 3=1}",
      "3,{4=1, 5=1, 6=1}",
      "4,{7=1, 8=1, 9=1, 10=1}",
      "5,{11=1, 12=1, 13=1, 14=1, 15=1}",
      "6,{16=1, 17=1, 18=1, 19=1, 20=1, 21=1}")
    assertMapStrEquals(expected.sorted.toString, sink.getRetractResults.sorted.toString)
  }

  @Test
  def testUnboundedGroupByCollectWithObject(): Unit = {
    val sqlQuery = "SELECT b, COLLECT(c) FROM MyTable GROUP BY b"

    val data = List(
      (1, 1, List(12, "45.6")),
      (2, 2, List(12, "45.612")),
      (3, 2, List(13, "41.6")),
      (4, 3, List(14, "45.2136")),
      (5, 3, List(18, "42.6"))
    )

    tEnv.registerTable("MyTable",
      failingDataSource(data).toTable(tEnv, 'a, 'b, 'c))

    val sink = new TestingRetractSink
    tEnv.sqlQuery(sqlQuery).toRetractStream[Row].addSink(sink)
    env.execute()

    val expected = List(
      "1,{List(12, 45.6)=1}",
      "2,{List(13, 41.6)=1, List(12, 45.612)=1}",
      "3,{List(18, 42.6)=1, List(14, 45.2136)=1}")
    assertMapStrEquals(expected.sorted.toString, sink.getRetractResults.sorted.toString)
  }

  @Test
  def testGroupBySingleValue(): Unit = {
    val data = new mutable.MutableList[(Int, Long, String)]
    data.+=((1, 1L, "A"))
    data.+=((2, 2L, "B"))
    data.+=((3, 2L, "B"))
    data.+=((4, 3L, "C"))
    data.+=((5, 3L, "C"))
    data.+=((6, 3L, "C"))
    data.+=((6, 3L, "C"))
    data.+=((6, 3L, "C"))
    data.+=((6, 3L, "C"))
    data.+=((6, 3L, "C"))
    data.+=((6, 3L, "C"))
    data.+=((6, 3L, "C"))
    data.+=((6, 3L, "C"))
    data.+=((7, 4L, "B"))
    data.+=((8, 4L, "A"))
    data.+=((9, 4L, "D"))
    data.+=((10, 4L, "E"))
    data.+=((11, 5L, "A"))
    data.+=((12, 5L, "B"))

    val t = failingDataSource(data).toTable(tEnv, 'a, 'b, 'c)
    tEnv.registerTable("T1", t)
    tEnv.registerTable("T2", t)
    val t1 = tEnv.sqlQuery("SELECT * FROM T2 WHERE T2.a < (SELECT count(*) * 0.3 FROM T1)")

    val sink = new TestingRetractSink
    t1.toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = List("1,1,A", "2,2,B", "3,2,B", "4,3,C", "5,3,C")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testPojoField(): Unit = {
    val data = Seq(
      (1, new MyPojo(5, 105)),
      (1, new MyPojo(6, 11)),
      (1, new MyPojo(7, 12)))

    val t = failingDataSource(data).toTable(tEnv, 'a, 'b)
    tEnv.registerTable("MyTable", t)
    tEnv.registerFunction("pojoFunc", new MyPojoAggFunction)
    tEnv.registerFunction("pojoToInt", MyPojoFunc)

    val sql = "SELECT pojoToInt(pojoFunc(b)) FROM MyTable group by a"

    val sink = new TestingRetractSink
    tEnv.sqlQuery(sql).toRetractStream[Row].addSink(sink)
    env.execute()

    val expected = List("128")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testDecimalSum(): Unit = {
    val data = new mutable.MutableList[Row]
    data.+=(Row.of(BigDecimal(1).bigDecimal))
    data.+=(Row.of(BigDecimal(2).bigDecimal))
    data.+=(Row.of(BigDecimal(2).bigDecimal))
    data.+=(Row.of(BigDecimal(3).bigDecimal))

    val rowType = new RowTypeInfo(BigDecimalTypeInfo.of(7, 2))
    val t = failingDataSource(data)(rowType).toTable(tEnv, 'd)
    tEnv.registerTable("T", t)

    val sql =
      """
        |select c, sum(d) from (
        |  select d, count(d) c from T group by d
        |) group by c
      """.stripMargin

    val sink = new TestingRetractSink
    tEnv.sqlQuery(sql).toRetractStream[Row].addSink(sink)
    env.execute()

    val expected = List("1,4.00", "2,2.00")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testDifferentTypesSumWithRetract(): Unit = {
    val data = List(
      (1.toByte, 1.toShort, 1, 1L, 1.0F, 1.0, "a"),
      (2.toByte, 2.toShort, 2, 2L, 2.0F, 2.0, "a"),
      (3.toByte, 3.toShort, 3, 3L, 3.0F, 3.0, "a"),
      (3.toByte, 3.toShort, 3, 3L, 3.0F, 3.0, "a"),
      (1.toByte, 1.toShort, 1, 1L, 1.0F, 1.0, "b"),
      (2.toByte, 2.toShort, 2, 2L, 2.0F, 2.0, "b"),
      (3.toByte, 3.toShort, 3, 3L, 3.0F, 3.0, "c"),
      (3.toByte, 3.toShort, 3, 3L, 3.0F, 3.0, "c")
    )

    val t = failingDataSource(data).toTable(tEnv, 'a, 'b, 'c, 'd, 'e, 'f, 'g)
    tEnv.registerTable("T", t)

    // We use sub-query + limit here to ensure retraction
    val sql =
      """
        |SELECT sum(a), sum(b), sum(c), sum(d), sum(e), sum(f), sum(h) FROM (
        |  SELECT *, CAST(c AS DECIMAL(3, 2)) AS h FROM T LIMIT 8
        |) GROUP BY g
      """.stripMargin

    val sink = new TestingRetractSink
    tEnv.sqlQuery(sql).toRetractStream[Row].addSink(sink)
    env.execute()

    val expected = List("9,9,9,9,9.0,9.0,9.00", "3,3,3,3,3.0,3.0,3.00", "6,6,6,6,6.0,6.0,6.00")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testAggAfterUnion(): Unit = {
    val data = List(
      (1L, 1, "Hello"),
      (2L, 2, "Hello"),
      (2L, 3, "Hello"),
      (3L, 4, "Hello"),
      (3L, 5, "Hello"),
      (7L, 6, "Hello"),
      (7L, 7, "Hello World"),
      (7L, 8, "Hello World"),
      (10L, 20, "Hello World"))

    val t1 = failingDataSource(data).toTable(tEnv, 'a, 'b, 'c)
    tEnv.registerTable("T1", t1)
    val t2 = failingDataSource(data).toTable(tEnv, 'a, 'b, 'c)
    tEnv.registerTable("T2", t2)

    val sql =
      """
        |SELECT a, sum(b), count(distinct c)
        |FROM (
        |  SELECT * FROM T1
        |  UNION ALL
        |  SELECT * FROM T2
        |) GROUP BY a
      """.stripMargin

    val sink = new TestingRetractSink
    tEnv.sqlQuery(sql).toRetractStream[Row].addSink(sink)
    env.execute()

    val expected = List("1,2,1", "2,10,1", "3,18,1", "7,42,2", "10,40,1")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testVarArgsNoGroupBy(): Unit = {
    val data = List(
      (1, 1L, "5", "3"),
      (1, 22L, "15", "13"),
      (3, 33L, "25", "23"))

    val t = failingDataSource(data).toTable(tEnv, 'id, 's, 's1, 's2)
    tEnv.registerTable("MyTable", t)
    tEnv.createTemporarySystemFunction("func", classOf[VarArgsAggFunction])

    val sql = "SELECT func(s, s1, s2) FROM MyTable"
    val sink = new TestingRetractSink
    tEnv.sqlQuery(sql).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = List("140")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testVarArgsWithGroupBy(): Unit = {
    val data = List(
      (1, 1L, "5", "3"),
      (1, 22L, "15", "13"),
      (3, 33L, "25", "23"))

    val t = failingDataSource(data).toTable(tEnv, 'id, 's, 's1, 's2)
    tEnv.registerTable("MyTable", t)
    tEnv.createTemporarySystemFunction("func", classOf[VarArgsAggFunction])

    val sink = new TestingRetractSink
    tEnv
      .sqlQuery("SELECT id, func(s, s1, s2) FROM MyTable group by id")
      .toRetractStream[Row]
      .addSink(sink)
    env.execute()
    val expected = List("1,59", "3,81")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testMinMaxWithBinaryString(): Unit = {
    val data = new mutable.MutableList[(Int, Long, String)]
    data.+=((1, 1L, "A"))
    data.+=((2, 2L, "B"))
    data.+=((3, 2L, "BC"))
    data.+=((4, 3L, "C"))
    data.+=((5, 3L, "CD"))
    data.+=((6, 3L, "DE"))
    data.+=((7, 4L, "EF"))
    data.+=((8, 4L, "FG"))
    data.+=((9, 4L, "HI"))
    data.+=((10, 4L, "IJ"))

    val t = failingDataSource(data).toTable(tEnv, 'a, 'b, 'c)
    tEnv.registerTable("T", t)

    val sql =
      """
        |SELECT b, min(c), max(c)
        |FROM (
        | SELECT a, b, listagg(c) as c
        | FROM T
        | GROUP BY a, b)
        |GROUP BY b
      """.stripMargin

    val sink = new TestingRetractSink
    tEnv.sqlQuery(sql).toRetractStream[Row].addSink(sink)
    env.execute()

    val expected = List("1,A,A", "2,B,BC", "3,C,DE", "4,EF,IJ")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testBigDataOfMinMaxWithBinaryString(): Unit = {
    val data = new mutable.MutableList[(Int, Long, String)]
    for (i <- 0 until 100) {
      data.+=((i % 10, i, i.toString))
    }

    val t = failingDataSource(data).toTable(tEnv, 'a, 'b, 'c)
    tEnv.registerTable("T", t)

    val sql =
      """
        |SELECT a, min(b), max(c), min(c) FROM T GROUP BY a
      """.stripMargin

    val sink = new TestingRetractSink
    tEnv.sqlQuery(sql).toRetractStream[Row].addSink(sink)
    env.execute()

    val expected = List("0,0,90,0", "1,1,91,1", "2,2,92,12", "3,3,93,13",
      "4,4,94,14", "5,5,95,15", "6,6,96,16", "7,7,97,17",
      "8,8,98,18", "9,9,99,19")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testAggWithFilterClause(): Unit = {
    val data = new mutable.MutableList[(Int, Long, String, Boolean)]
    data.+=((1, 5L, "B", true))
    data.+=((1, 4L, "C", false))
    data.+=((1, 2L, "A", true))
    data.+=((2, 1L, "A", true))
    data.+=((2, 2L, "B", false))
    data.+=((1, 6L, "A", true))
    data.+=((2, 2L, "B", false))
    data.+=((3, 5L, "B", true))
    data.+=((2, 3L, "C", true))
    data.+=((2, 3L, "D", true))

    val t = failingDataSource(data).toTable(tEnv, 'a, 'b, 'c, 'd)
    tEnv.registerTable("T", t)
    // test declarative and imperative aggregates
    val sql =
      """
        |SELECT
        |  a,
        |  sum(b) filter (where c = 'A'),
        |  count(distinct c) filter (where d is true),
        |  max(b)
        |FROM T GROUP BY a
      """.stripMargin

    val sink = new TestingRetractSink
    tEnv.sqlQuery(sql).toRetractStream[Row].addSink(sink)
    env.execute()

    val expected = List("1,8,2,6", "2,1,3,3", "3,null,1,5")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testMinMaxWithDecimal(): Unit = {
    val data = new mutable.MutableList[Row]
    data.+=(Row.of(BigDecimal(1).bigDecimal))
    data.+=(Row.of(BigDecimal(2).bigDecimal))
    data.+=(Row.of(BigDecimal(2).bigDecimal))
    data.+=(Row.of(BigDecimal(4).bigDecimal))
    data.+=(Row.of(BigDecimal(3).bigDecimal))
    // a, count(a) as cnt
    // 1, 1
    // 2, 2
    // 4, 1
    // 3, 1
    //
    // cnt, min(a), max(a)
    // 1, 1, 4
    // 2, 2, 2

    val rowType = new RowTypeInfo(BigDecimalTypeInfo.of(7, 2))
    val t = failingDataSource(data)(rowType).toTable(tEnv, 'a)
    tEnv.registerTable("T", t)

    val sql =
      """
        |select cnt, min(a), max(a) from (
        |  select a, count(a) as cnt from T group by a
        |) group by cnt
      """.stripMargin

    val sink = new TestingRetractSink
    tEnv.sqlQuery(sql).toRetractStream[Row].addSink(sink)
    env.execute()

    val expected = List("1,1.00,4.00", "2,2.00,2.00")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testCollectOnClusteredFields(): Unit = {
    val data = List(
      (1, 1, (12, "45.6")),
      (2, 2, (12, "45.612")),
      (3, 2, (13, "41.6")),
      (4, 3, (14, "45.2136")),
      (5, 3, (18, "42.6"))
    )
    tEnv.registerTable("src", env.fromCollection(data).toTable(tEnv, 'a, 'b, 'c))

    val sql = "SELECT a, b, COLLECT(c) as `set` FROM src GROUP BY a, b"
    val view1 = tEnv.sqlQuery(sql)
    tEnv.registerTable("v1", view1)

    tEnv.createTemporarySystemFunction("toCompObj", ToCompositeObj)
    tEnv.createTemporarySystemFunction("anyToString", AnyToStringFunction)

    val sql1 =
      s"""
         |SELECT
         |  a, b, anyToString(COLLECT(toCompObj(t.sid, 'a', 100, t.point)))
         |from (
         | select
         |  a, b, uuid() as u, V.sid, V.point
         | from
         |  v1, unnest(v1.`set`) as V(sid, point)
         |) t
         |group by t.a, t.b, t.u
     """.stripMargin

    val sink = new TestingRetractSink
    tEnv.sqlQuery(sql1).toRetractStream[Row].addSink(sink)
    env.execute()

    val expected = List(
      "1,1,{CompositeObj(12,a,100,45.6)=1}",
      "2,2,{CompositeObj(12,a,100,45.612)=1}",
      "3,2,{CompositeObj(13,a,100,41.6)=1}",
      "4,3,{CompositeObj(14,a,100,45.2136)=1}",
      "5,3,{CompositeObj(18,a,100,42.6)=1}")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  /** Test LISTAGG **/
  @Test
  def testListAgg(): Unit = {
    tEnv.createTemporarySystemFunction(
      "listagg_retract",
      classOf[ListAggWithRetractAggFunction])
    tEnv.createTemporarySystemFunction(
      "listagg_ws_retract",
      classOf[ListAggWsWithRetractAggFunction])
    val sqlQuery =
      s"""
         |SELECT
         |  listagg(c), listagg(c, '-'), listagg_retract(c), listagg_ws_retract(c, '+')
         |FROM MyTable
         |GROUP BY c
         |""".stripMargin

    val data = new mutable.MutableList[(Int, Long, String)]
    for (i <- 0 until 10) {
      data.+=((i, 1L, "Hi"))
    }

    val t = failingDataSource(data).toTable(tEnv, 'a, 'b, 'c)
    tEnv.registerTable("MyTable", t)

    val sink = new TestingRetractSink
    tEnv.sqlQuery(sqlQuery).toRetractStream[Row].addSink(sink)
    env.execute()
    val expected = List("Hi,Hi,Hi,Hi,Hi,Hi,Hi,Hi,Hi,Hi,Hi-Hi-Hi-Hi-Hi-Hi-Hi-Hi-Hi-Hi," +
      "Hi,Hi,Hi,Hi,Hi,Hi,Hi,Hi,Hi,Hi,Hi+Hi+Hi+Hi+Hi+Hi+Hi+Hi+Hi+Hi")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testSTDDEV(): Unit = {
    val sqlQuery = "SELECT STDDEV_SAMP(a), STDDEV_POP(a) FROM MyTable GROUP BY c"

    val data = new mutable.MutableList[(Double, Long, String)]
    for (i <- 0 until 10) {
      data.+=((i, 1L, "Hi"))
    }

    val t = failingDataSource(data).toTable(tEnv, 'a, 'b, 'c)
    tEnv.registerTable("MyTable", t)

    val sink = new TestingRetractSink
    tEnv.sqlQuery(sqlQuery).toRetractStream[Row].addSink(sink)
    env.execute()
    val expected = List("3.0276503540974917,2.8722813232690143")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  /** test VAR_POP **/
  @Test
  def testVAR_POP(): Unit = {
    val sqlQuery = "SELECT VAR_POP(a) FROM MyTable GROUP BY c"

    val data = new mutable.MutableList[(Int, Long, String)]
    data.+=((2900, 1L, "Hi"))
    data.+=((2500, 1L, "Hi"))
    data.+=((2600, 1L, "Hi"))
    data.+=((3100, 1L, "Hello"))
    data.+=((11000, 1L, "Hello"))

    val t = failingDataSource(data).toTable(tEnv, 'a, 'b, 'c)
    tEnv.registerTable("MyTable", t)

    val sink = new TestingRetractSink
    tEnv.sqlQuery(sqlQuery).toRetractStream[Row].addSink(sink)
    env.execute()
    // TODO: define precise behavior of VAR_POP()
    val expected = List(15602500.toString, 28889.toString)
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testLongVarargsAgg(): Unit = {
    tEnv.createTemporarySystemFunction("var_sum", classOf[VarSumAggFunction])
    val sqlQuery = s"SELECT a, " +
      s"var_sum(${0.until(260).map(_ => "b").mkString(",")}) from MyTable group by a"
    val data = Seq[(Int, Int)]((1, 1), (2,2))

    val t = failingDataSource(data).toTable(tEnv, 'a, 'b)
    tEnv.registerTable("MyTable", t)

    val sink = new TestingRetractSink
    tEnv.sqlQuery(sqlQuery).toRetractStream[Row].addSink(sink)
    env.execute()

    val expected = List("1,260", "2,520")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testCountDistinctWithBinaryRowSource(): Unit = {
    // this case is failed before, because of object reuse problem
    val data = (0 until 100).map {i => ("1", "1", s"${i%50}", "1")}.toList
    // use BinaryRowData source here for StringData reuse
    val t = failingBinaryRowSource(data).toTable(tEnv, 'a, 'b, 'c, 'd)
    tEnv.registerTable("src", t)

    val sql =
      s"""
         |SELECT
         |  a,
         |  b,
         |  COUNT(distinct c) as uv
         |FROM (
         |  SELECT
         |    a, b, c, d
         |  FROM
         |    src where b <> ''
         |  UNION ALL
         |  SELECT
         |    a, 'ALL' as b, c, d
         |  FROM
         |    src where b <> ''
         |) t
         |GROUP BY
         |  a, b
     """.stripMargin

    val t1 = tEnv.sqlQuery(sql)
    val sink = new TestingRetractSink
    t1.toRetractStream[Row].addSink(sink)
    env.execute("test")

    val expected = List("1,1,50", "1,ALL,50")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testDistinctWithMultiFilter(): Unit = {
    val t = failingDataSource(TestData.tupleData3).toTable(tEnv).as("a", "b", "c")
    tEnv.registerTable("MyTable", t)

    val sqlQuery =
      s"""
         |SELECT
         |  b,
         |  SUM(DISTINCT (a * 3)),
         |  COUNT(DISTINCT SUBSTRING(c FROM 1 FOR 2)),
         |  COUNT(DISTINCT c),
         |  COUNT(DISTINCT c) filter (where MOD(a, 3) = 0),
         |  COUNT(DISTINCT c) filter (where MOD(a, 3) = 1)
         |FROM MyTable
         |GROUP BY b
       """.stripMargin

    val result = tEnv.sqlQuery(sqlQuery).toRetractStream[Row]
    val sink = new TestingRetractSink
    result.addSink(sink)
    env.execute()
    val expected = List(
      "1,3,1,1,0,1", "2,15,1,2,1,0",
      "3,45,3,3,1,1", "4,102,1,4,1,2",
      "5,195,1,5,2,1", "6,333,1,6,2,2")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testPruneUselessAggCall(): Unit = {
    val data = new mutable.MutableList[(Int, Long, String)]
    data .+= ((1, 1L, "Hi"))
    data .+= ((2, 2L, "Hello"))
    data .+= ((3, 2L, "Hello world"))

    val t = failingDataSource(data).toTable(tEnv, 'a, 'b, 'c)
    tEnv.registerTable("T", t)

    val t1 = tEnv.sqlQuery(
      "select a from (select b, max(a) as a, count(*), max(c) as c from T group by b) T1")
    val sink = new TestingRetractSink
    t1.toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()
    val expected = List("1", "3")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testGenericTypesWithoutStateClean(): Unit = {
    // because we don't provide a way to disable state cleanup.
    // TODO verify all tests with state cleanup closed.
    tEnv.getConfig.setIdleStateRetentionTime(Time.days(0), Time.days(0))
    val t = failingDataSource(Seq(1, 2, 3)).toTable(tEnv, 'a)
    val results = t
        .select(new GenericAggregateFunction()('a))
        .toRetractStream[Row]

    val sink = new TestingRetractSink
    results.addSink(sink).setParallelism(1)
    env.execute()
  }

  @Test
  def testConstantGroupKeyWithUpsertSink(): Unit = {
    val data = new mutable.MutableList[(Int, Long, String)]
    data.+=((1, 1L, "A"))
    data.+=((2, 2L, "B"))
    data.+=((3, 2L, "B"))
    data.+=((4, 3L, "C"))
    data.+=((5, 3L, "C"))

    val t = failingDataSource(data).toTable(tEnv, 'a, 'b, 'c)
    tEnv.registerTable("MyTable", t)

    val tableSink = new TestingUpsertTableSink(Array(0)).configure(
      Array[String]("c", "bMax"), Array[TypeInformation[_]](Types.STRING, Types.LONG))
    tEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal("testSink", tableSink)

    tEnv.executeSql(
      """
        |insert into testSink
        |select c, max(b) from
        | (select b, c, true as f from MyTable) t
        |group by c, f
      """.stripMargin).await()

    val expected = List("A,1", "B,2", "C,3")
    assertEquals(expected.sorted, tableSink.getUpsertResults.sorted)
  }

  @Test
  def testAggregationCodeSplit(): Unit = {

    val t = env.fromCollection(TestData.smallTupleData3)
      .toTable(tEnv, 'a, 'b, 'c)
    tEnv.createTemporaryView("MyTable", t)

    tEnv.getConfig.setMaxGeneratedCodeLength(2048)

    // 50 can make sure all generated methods of [Namespace]AggsHandleFunction is longer than 2048
    val columnNumber = 50

    val selectList = Stream.range(3, columnNumber)
      .map(i => s"SUM(CASE WHEN a IS NOT NULL AND a > $i THEN 0 WHEN a < 0 THEN 0 ELSE $i END)")
      .mkString(",")
    val sqlQuery = s"select $selectList from MyTable group by b, c"

    val result = tEnv.sqlQuery(sqlQuery).toRetractStream[Row]
    val sink = new TestingRetractSink
    result.addSink(sink)
    env.execute()

    val expected = Stream.range(3, columnNumber).map(_.toString).mkString(",")
    assertEquals(sink.getRawResults.size, 3)
    sink.getRetractResults.foreach(result =>
      assertEquals(expected, result)
    )
  }

  @Test
  def testOverloadedAccumulator(): Unit = {
    val data = new mutable.MutableList[(String, Long)]
    data .+= (("x", 1L))
    data .+= (("x", 2L))
    data .+= (("x", 3L))
    data .+= (("y", 1L))
    data .+= (("y", 2L))
    data .+= (("z", 3L))

    val t = failingDataSource(data).toTable(tEnv, 'a, 'b)
    tEnv.createTemporaryView("T", t)
    tEnv.createTemporarySystemFunction("OverloadedMaxFunction", classOf[OverloadedMaxFunction])

    val sink1 = new TestingRetractSink
    val sink2 = new TestingRetractSink

    tEnv.sqlQuery("SELECT a, OverloadedMaxFunction(b) FROM T GROUP BY a")
      .toRetractStream[Row]
      .addSink(sink1)

    tEnv.sqlQuery("SELECT b, OverloadedMaxFunction(a) FROM T GROUP BY b")
      .toRetractStream[Row]
      .addSink(sink2)

    env.execute()

    val expected1 = List(
      "x,3",
      "y,2",
      "z,3")
    assertEquals(expected1.sorted, sink1.getRetractResults.sorted)

    val expected2 = List(
      "1,y",
      "2,y",
      "3,z")
    assertEquals(expected2.sorted, sink2.getRetractResults.sorted)
  }

  @Test
  def testCoalesceOnGroupingSets(): Unit = {
    val empsData = List(
      (100L, "Fred", 10, null, null, 40L, 25, true, false),
      (110L, "Eric", 20, "M", "San Francisco", 3L, 80, null, false),
      (110L, "John", 40, "M", "Vancouver", 2L, null, false, true),
      (120L, "Wilma", 20, "F", null, 1L, 5, null, true),
      (130L, "Alice", 40, "F", "Vancouver", 2L, null, false, true))
    val tableA = failingDataSource(empsData)
      .toTable(tEnv, 'empno, 'name, 'deptno, 'gender, 'city, 'empid, 'age, 'slacker, 'manager)
    tEnv.registerTable("emps", tableA)
    val sql =
      s"""
         |select
         |  gender, city, coalesce(deptno, -1), count(*) as cnt
         |from emps group by grouping sets ((gender, city), (gender, city, deptno))
         |""".stripMargin
    val sink = new TestingRetractSink
    tEnv.sqlQuery(sql).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()
    val expected = List(
      "F,Vancouver,-1,1",
      "F,Vancouver,40,1",
      "F,null,-1,1",
      "F,null,20,1",
      "M,San Francisco,-1,1",
      "M,San Francisco,20,1",
      "M,Vancouver,-1,1",
      "M,Vancouver,40,1",
      "null,null,-1,1",
      "null,null,10,1")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testBooleanColumnOnGroupingSets(): Unit = {
    val empsData = List(
      (100L, "Fred", 10, null, null, 40L, 25, true, false),
      (110L, "Eric", 20, "M", "San Francisco", 3L, 80, null, false),
      (110L, "John", 40, "M", "Vancouver", 2L, null, false, true),
      (120L, "Wilma", 20, "F", null, 1L, 5, null, true),
      (130L, "Alice", 40, "F", "Vancouver", 2L, null, false, true))
    val tableA = failingDataSource(empsData)
      .toTable(tEnv, 'empno, 'name, 'deptno, 'gender, 'city, 'empid, 'age, 'slacker, 'manager)
    tEnv.registerTable("emps", tableA)
    val sql =
      s"""
         |select
         |  gender, city, manager, count(*) as cnt
         |from emps group by grouping sets ((city), (gender, city, manager))
         |""".stripMargin
    val sink = new TestingRetractSink
    tEnv.sqlQuery(sql).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()
    val expected = List(
      "F,Vancouver,true,1",
      "F,null,true,1",
      "M,San Francisco,false,1",
      "M,Vancouver,true,1",
      "null,San Francisco,null,1",
      "null,Vancouver,null,2",
      "null,null,false,1",
      "null,null,null,2")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }
}
