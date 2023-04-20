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

import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.api.config.OptimizerConfigOptions
import org.apache.flink.table.planner.runtime.stream.sql.SplitAggregateITCase.PartialAggMode
import org.apache.flink.table.planner.runtime.utils.{StreamingWithAggTestBase, TestingRetractSink}
import org.apache.flink.table.planner.runtime.utils.StreamingWithAggTestBase.{AggMode, LocalGlobalOff, LocalGlobalOn}
import org.apache.flink.table.planner.runtime.utils.StreamingWithMiniBatchTestBase.MiniBatchOn
import org.apache.flink.table.planner.runtime.utils.StreamingWithStateTestBase.{HEAP_BACKEND, ROCKSDB_BACKEND, StateBackendMode}
import org.apache.flink.table.planner.utils.DateTimeTestUtil.{localDate, localDateTime, localTime => mLocalTime}
import org.apache.flink.types.Row

import org.junit.{Before, Test}
import org.junit.Assert.assertEquals
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

import java.lang.{Integer => JInt, Long => JLong}
import java.math.{BigDecimal => JBigDecimal}
import java.util

import scala.collection.{mutable, Seq}
import scala.collection.JavaConversions._
import scala.util.Random

@RunWith(classOf[Parameterized])
class SplitAggregateITCase(
    partialAggMode: PartialAggMode,
    aggMode: AggMode,
    backend: StateBackendMode)
  extends StreamingWithAggTestBase(aggMode, MiniBatchOn, backend) {

  @Before
  override def before(): Unit = {
    super.before()

    if (partialAggMode.isPartialAggEnabled) {
      tEnv.getConfig
        .set(OptimizerConfigOptions.TABLE_OPTIMIZER_DISTINCT_AGG_SPLIT_ENABLED, Boolean.box(true))
    } else {
      tEnv.getConfig
        .set(OptimizerConfigOptions.TABLE_OPTIMIZER_DISTINCT_AGG_SPLIT_ENABLED, Boolean.box(false))
    }

    val data = List(
      (1L, 1, "Hello 0"),
      (1L, 2, "Hello 1"),
      (2L, 3, "Hello 1"),
      (3L, 5, "Hello 1"),
      (2L, 3, "Hello 2"),
      (2L, 4, "Hello 3"),
      (2L, 4, null),
      (2L, 5, "Hello 4"),
      (3L, 5, "Hello 0"),
      (2L, 4, "Hello 3"),
      (4L, 5, "Hello 2"),
      (2L, 4, "Hello 3"),
      (4L, 5, null),
      (4L, 5, "Hello 3"),
      (2L, 2, "Hello 0"),
      (4L, 6, "Hello 1")
    )

    val t = failingDataSource(data).toTable(tEnv, 'a, 'b, 'c)
    tEnv.createTemporaryView("T", t)
  }

  @Test
  def testCountDistinct(): Unit = {
    val ids = List(1, 2, 2, 3, 3, 3, 4, 4, 4, 4, 5, 5, 5, 5, 5)

    val dateTimes = List(
      "1970-01-01 00:00:01",
      "1970-01-01 00:00:02",
      null,
      "1970-01-01 00:00:04",
      "1970-01-01 00:00:05",
      "1970-01-01 00:00:06",
      "1970-01-01 00:00:07",
      null,
      null,
      "1970-01-01 00:00:10",
      "1970-01-01 00:00:11",
      "1970-01-01 00:00:11",
      "1970-01-01 00:00:13",
      "1970-01-01 00:00:14",
      "1970-01-01 00:00:15"
    )

    val dates = List(
      "1970-01-01",
      "1970-01-02",
      null,
      "1970-01-04",
      "1970-01-05",
      "1970-01-06",
      "1970-01-07",
      null,
      null,
      "1970-01-10",
      "1970-01-11",
      "1970-01-11",
      "1970-01-13",
      "1970-01-14",
      "1970-01-15"
    )

    val times = List(
      "00:00:01",
      "00:00:02",
      null,
      "00:00:04",
      "00:00:05",
      "00:00:06",
      "00:00:07",
      null,
      null,
      "00:00:10",
      "00:00:11",
      "00:00:11",
      "00:00:13",
      "00:00:14",
      "00:00:15")

    val integers =
      List("1", "2", null, "4", "5", "6", "7", null, null, "10", "11", "11", "13", "14", "15")

    val chars = List("A", "B", null, "D", "E", "F", "H", null, null, "K", "L", "L", "N", "O", "P")

    val data = new mutable.MutableList[Row]

    for (i <- ids.indices) {
      val v = integers(i)
      val decimal = if (v == null) null else new JBigDecimal(v)
      val int = if (v == null) null else JInt.valueOf(v)
      val long = if (v == null) null else JLong.valueOf(v)
      data.+=(
        Row.of(
          Int.box(ids(i)),
          localDateTime(dateTimes(i)),
          localDate(dates(i)),
          mLocalTime(times(i)),
          decimal,
          int,
          long,
          chars(i)))
    }

    val inputs = Random.shuffle(data)

    val rowType = new RowTypeInfo(
      Types.INT,
      Types.LOCAL_DATE_TIME,
      Types.LOCAL_DATE,
      Types.LOCAL_TIME,
      Types.DECIMAL,
      Types.INT,
      Types.LONG,
      Types.STRING)

    val t = failingDataSource(inputs)(rowType).toTable(tEnv, 'id, 'a, 'b, 'c, 'd, 'e, 'f, 'g)
    tEnv.createTemporaryView("MyTable", t)
    val t1 = tEnv.sqlQuery(s"""
                              |SELECT
                              | id,
                              | count(distinct a),
                              | count(distinct b),
                              | count(distinct c),
                              | count(distinct d),
                              | count(distinct e),
                              | count(distinct f),
                              | count(distinct g)
                              |FROM MyTable
                              |GROUP BY id
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
  def testSingleDistinctAgg(): Unit = {
    val t1 = tEnv.sqlQuery("SELECT COUNT(DISTINCT c) FROM T")

    val sink = new TestingRetractSink
    t1.toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = List("5")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testMultiCountDistinctAgg(): Unit = {
    val t1 = tEnv.sqlQuery("SELECT COUNT(DISTINCT b), COUNT(DISTINCT c) FROM T")

    val sink = new TestingRetractSink
    t1.toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = List("6,5")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testSingleDistinctAggAndOneOrMultiNonDistinctAgg(): Unit = {
    val t1 = tEnv.sqlQuery("SELECT a, SUM(b), COUNT(DISTINCT c), avg(b) FROM T GROUP BY a")

    val sink = new TestingRetractSink
    t1.toRetractStream[Row].addSink(sink)
    env.execute()

    val expected = List("1,3,2,1", "2,29,5,3", "3,10,2,5", "4,21,3,5")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testSingleDistinctAggWithGroupBy(): Unit = {
    val t1 = tEnv.sqlQuery("SELECT a, COUNT(DISTINCT c) FROM T GROUP BY a")

    val sink = new TestingRetractSink
    t1.toRetractStream[Row].addSink(sink)
    env.execute()

    val expected = List("1,2", "2,5", "3,2", "4,3")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testSingleDistinctAggWithAndNonDistinctAggOnSameColumn(): Unit = {
    val t1 = tEnv.sqlQuery("SELECT a, COUNT(DISTINCT b), MAX(b), MIN(b) FROM T GROUP BY a")

    val sink = new TestingRetractSink
    t1.toRetractStream[Row].addSink(sink)
    env.execute()

    val expected = List("1,2,2,1", "2,4,5,2", "3,1,5,5", "4,2,6,5")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testSomeColumnsBothInDistinctAggAndGroupBy(): Unit = {
    val t1 = tEnv.sqlQuery("SELECT a, COUNT(DISTINCT a), COUNT(b) FROM T GROUP BY a")

    val sink = new TestingRetractSink
    t1.toRetractStream[Row].addSink(sink)
    env.execute()

    val expected = List("1,1,2", "2,1,8", "3,1,2", "4,1,4")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testAggWithFilterClause(): Unit = {
    val t1 = tEnv.sqlQuery(s"""
                              |SELECT
                              |  a,
                              |  COUNT(DISTINCT b) filter (where not b = 2),
                              |  MAX(b) filter (where not b = 5),
                              |  MIN(b) filter (where not b = 2)
                              |FROM T
                              |GROUP BY a
       """.stripMargin)

    val sink = new TestingRetractSink
    t1.toRetractStream[Row].addSink(sink)
    env.execute()

    val expected = List("1,1,2,1", "2,3,4,3", "3,1,null,5", "4,2,6,5")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testMinMaxWithRetraction(): Unit = {
    val t1 = tEnv.sqlQuery(s"""
                              |SELECT
                              |  c, MIN(b), MAX(b), COUNT(DISTINCT a)
                              |FROM(
                              |  SELECT
                              |    a, COUNT(DISTINCT b) as b, MAX(b) as c
                              |  FROM T
                              |  GROUP BY a
                              |) GROUP BY c
       """.stripMargin)

    val sink = new TestingRetractSink
    t1.toRetractStream[Row].addSink(sink)
    env.execute()

    val expected = List("2,2,2,1", "5,1,4,2", "6,2,2,1")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testCountWithSingleDistinctAndRetraction(): Unit = {
    // Test for FLINK-23434. The result is incorrect, because the global agg on incremental agg
    // does not handle retraction message. While if binary mode is on, the result is correct
    // event without this fix. Because mini-batch with binary mode will compact retraction message
    // in this case.
    val t1 = tEnv.sqlQuery(s"""
                              |SELECT
                              |  b, COUNT(DISTINCT b1), COUNT(1)
                              |FROM(
                              |   SELECT
                              |     a, COUNT(b) as b, MAX(b) as b1
                              |   FROM T
                              |   GROUP BY a
                              |) GROUP BY b
       """.stripMargin)

    val sink = new TestingRetractSink
    t1.toRetractStream[Row].addSink(sink)
    env.execute()

    val expected = List("2,2,2", "4,1,1", "8,1,1")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testSumCountWithSingleDistinctAndRetraction(): Unit = {
    // Test for FLINK-23434. The plan and the result is incorrect, because sum with retraction
    // will produce two acc values, while sum without retraction will produce only one acc value,
    // the type consistent validation will fail in the IncrementalAggregateRule because wrong
    // retraction flag is given.
    val t1 = tEnv.sqlQuery(s"""
                              |SELECT
                              |  b, SUM(b1), COUNT(DISTINCT b1), COUNT(1)
                              |FROM(
                              |   SELECT
                              |     a, COUNT(b) as b, MAX(b) as b1
                              |   FROM T
                              |   GROUP BY a
                              |) GROUP BY b
       """.stripMargin)

    val sink = new TestingRetractSink
    t1.toRetractStream[Row].addSink(sink)
    env.execute()

    val expected = List("2,7,2,2", "4,6,1,1", "8,5,1,1")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testAggWithJoin(): Unit = {
    val t1 = tEnv.sqlQuery(s"""
                              |SELECT *
                              |FROM(
                              |  SELECT
                              |    c, MIN(b) as b, MAX(b) as d, COUNT(DISTINCT a) as a
                              |  FROM(
                              |    SELECT
                              |      a, COUNT(DISTINCT b) as b, MAX(b) as c
                              |    FROM T
                              |    GROUP BY a
                              |  ) GROUP BY c
                              |) as T1 JOIN T ON T1.b + 2 = T.a
       """.stripMargin)

    val sink = new TestingRetractSink
    t1.toRetractStream[Row].addSink(sink)
    env.execute()

    val expected = List(
      "2,2,2,1,4,5,Hello 2",
      "2,2,2,1,4,5,Hello 3",
      "2,2,2,1,4,5,null",
      "2,2,2,1,4,6,Hello 1",
      "5,1,4,2,3,5,Hello 0",
      "5,1,4,2,3,5,Hello 1",
      "6,2,2,1,4,5,Hello 2",
      "6,2,2,1,4,5,Hello 3",
      "6,2,2,1,4,5,null",
      "6,2,2,1,4,6,Hello 1"
    )
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testUvWithRetraction(): Unit = {
    val data = (0 until 1000).map(i => (s"${i % 10}", s"${i % 100}", s"$i")).toList
    val t = failingDataSource(data).toTable(tEnv, 'a, 'b, 'c)
    tEnv.createTemporaryView("src", t)

    val sql =
      s"""
         |SELECT
         |  a,
         |  COUNT(distinct b) as uv
         |FROM (
         |  SELECT a, b, last_value(c)
         |  FROM src
         |  GROUP BY a, b
         |) t
         |GROUP BY a
     """.stripMargin

    val t1 = tEnv.sqlQuery(sql)
    val sink = new TestingRetractSink
    t1.toRetractStream[Row].addSink(sink)
    env.execute()

    val expected =
      List("0,10", "1,10", "2,10", "3,10", "4,10", "5,10", "6,10", "7,10", "8,10", "9,10")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testCountDistinctWithBinaryRowSource(): Unit = {
    // this case is failed before, because of object reuse problem
    val data = (0 until 100).map(i => ("1", "1", s"${i % 50}", "1")).toList
    // use BinaryRowData source here for StringData reuse
    val t = failingBinaryRowSource(data).toTable(tEnv, 'a, 'b, 'c, 'd)
    tEnv.createTemporaryView("src", t)

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
    env.execute()

    val expected = List("1,1,50", "1,ALL,50")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testMultipleDistinctAggOnSameColumn(): Unit = {
    val t1 = tEnv.sqlQuery(s"""
                              |SELECT
                              |  a,
                              |  COUNT(DISTINCT b),
                              |  COUNT(DISTINCT b) filter (where not b = 2),
                              |  MAX(b) filter (where not b = 5),
                              |  MIN(b) filter (where not b = 2)
                              |FROM T
                              |GROUP BY a
       """.stripMargin)

    val sink = new TestingRetractSink
    t1.toRetractStream[Row].addSink(sink)
    env.execute()

    val expected = List("1,2,1,2,1", "2,4,3,4,3", "3,1,1,null,5", "4,2,2,6,5")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testAggFilterClauseBothWithAvgAndCount(): Unit = {
    val t1 = tEnv.sqlQuery(s"""
                              |SELECT
                              |  a,
                              |  COUNT(DISTINCT b) FILTER (WHERE NOT b = 2),
                              |  SUM(b) FILTER (WHERE NOT b = 5),
                              |  COUNT(b),
                              |  SUM(b),
                              |  AVG(b)
                              |FROM T
                              |GROUP BY a
       """.stripMargin)

    val sink = new TestingRetractSink
    t1.toRetractStream[Row].addSink(sink)
    env.execute()

    val expected = List("1,1,3,2,3,1", "2,3,24,8,29,3", "3,1,null,2,10,5", "4,2,6,4,21,5")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }
}

object SplitAggregateITCase {

  case class PartialAggMode(isPartialAggEnabled: Boolean) {
    override def toString: String = if (isPartialAggEnabled) "ON" else "OFF"
  }

  val PartialAggOn = PartialAggMode(isPartialAggEnabled = true)
  val PartialAggOff = PartialAggMode(isPartialAggEnabled = false)

  @Parameterized.Parameters(name = "PartialAgg={0}, LocalGlobal={1}, StateBackend={2}")
  def parameters(): util.Collection[Array[java.lang.Object]] = {
    Seq[Array[AnyRef]](
      Array(PartialAggOn, LocalGlobalOff, HEAP_BACKEND),
      Array(PartialAggOn, LocalGlobalOn, HEAP_BACKEND),
      Array(PartialAggOn, LocalGlobalOff, ROCKSDB_BACKEND),
      Array(PartialAggOn, LocalGlobalOn, ROCKSDB_BACKEND)
    )
  }
}
