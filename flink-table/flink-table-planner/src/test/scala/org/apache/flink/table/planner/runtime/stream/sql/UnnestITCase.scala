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

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.api.internal.TableEnvironmentInternal
import org.apache.flink.table.planner.runtime.utils._
import org.apache.flink.table.planner.runtime.utils.StreamingWithStateTestBase.StateBackendMode
import org.apache.flink.table.planner.runtime.utils.TimeTestUtil.TimestampAndWatermarkWithOffset
import org.apache.flink.table.utils.LegacyRowResource
import org.apache.flink.types.Row

import org.junit.{Rule, Test}
import org.junit.Assert.assertEquals
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

@RunWith(classOf[Parameterized])
class UnnestITCase(mode: StateBackendMode) extends StreamingWithStateTestBase(mode) {

  @Rule
  def usesLegacyRows: LegacyRowResource = LegacyRowResource.INSTANCE

  @Test
  def testUnnestPrimitiveArrayFromTable(): Unit = {
    val data = List(
      (1, Array(12, 45), Array(Array(12, 45))),
      (2, Array(41, 5), Array(Array(18), Array(87))),
      (3, Array(18, 42), Array(Array(1), Array(45)))
    )
    val t = env.fromCollection(data).toTable(tEnv, 'a, 'b, 'c)
    tEnv.createTemporaryView("T", t)

    val sqlQuery = "SELECT a, b, s FROM T, UNNEST(T.b) AS A (s)"
    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = List(
      "1,[12, 45],12",
      "1,[12, 45],45",
      "2,[41, 5],41",
      "2,[41, 5],5",
      "3,[18, 42],18",
      "3,[18, 42],42")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testUnnestArrayOfArrayFromTable(): Unit = {
    val data = List(
      (1, Array(12, 45), Array(Array(12, 45))),
      (2, Array(41, 5), Array(Array(18), Array(87))),
      (3, Array(18, 42), Array(Array(1), Array(45)))
    )
    val t = env.fromCollection(data).toTable(tEnv, 'a, 'b, 'c)
    tEnv.createTemporaryView("T", t)

    val sqlQuery = "SELECT a, s FROM T, UNNEST(T.c) AS A (s)"
    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = List("1,[12, 45]", "2,[18]", "2,[87]", "3,[1]", "3,[45]")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testUnnestObjectArrayFromTableWithFilter(): Unit = {
    val data = List(
      (1, Array((12, "45.6"), (12, "45.612"))),
      (2, Array((13, "41.6"), (14, "45.2136"))),
      (3, Array((18, "42.6")))
    )
    val t = env.fromCollection(data).toTable(tEnv, 'a, 'b)
    tEnv.createTemporaryView("T", t)

    val sqlQuery = "SELECT a, b, s, t FROM T, UNNEST(T.b) AS A (s, t) WHERE s > 13"
    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = List("2,[13,41.6, 14,45.2136],14,45.2136", "3,[18,42.6],18,42.6")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testUnnestMultiSetFromCollectResult(): Unit = {
    val data = List(
      (1, 1, (12, "45.6")),
      (2, 2, (12, "45.612")),
      (3, 2, (13, "41.6")),
      (4, 3, (14, "45.2136")),
      (5, 3, (18, "42.6")))
    val t = env.fromCollection(data).toTable(tEnv, 'a, 'b, 'c)
    tEnv.createTemporaryView("T", t)

    val sqlQuery =
      """
        |WITH T1 AS (SELECT b, COLLECT(c) as `set` FROM T GROUP BY b)
        |SELECT b, id, point FROM T1, UNNEST(T1.`set`) AS A(id, point) WHERE b < 3
      """.stripMargin
    val result = tEnv.sqlQuery(sqlQuery).toRetractStream[Row]
    val sink = new TestingRetractSink
    result.addSink(sink)
    env.execute()

    val expected = List("1,12,45.6", "2,12,45.612", "2,13,41.6")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testLeftUnnestMultiSetFromCollectResult(): Unit = {
    val data = List(
      (1, "1", "Hello"),
      (1, "2", "Hello2"),
      (2, "2", "Hello"),
      (3, null.asInstanceOf[String], "Hello"),
      (4, "4", "Hello"),
      (5, "5", "Hello"),
      (5, null.asInstanceOf[String], "Hello"),
      (6, "6", "Hello"),
      (7, "7", "Hello World"),
      (7, "8", "Hello World")
    )

    val t = env.fromCollection(data).toTable(tEnv, 'a, 'b, 'c)
    tEnv.createTemporaryView("T", t)

    val sqlQuery =
      """
        |WITH T1 AS (SELECT a, COLLECT(b) as `set` FROM T GROUP BY a)
        |SELECT a, s FROM T1 LEFT JOIN UNNEST(T1.`set`) AS A(s) ON TRUE WHERE a < 5
      """.stripMargin
    val result = tEnv.sqlQuery(sqlQuery).toRetractStream[Row]
    val sink = new TestingRetractSink
    result.addSink(sink).setParallelism(1)
    env.execute()

    val expected = List(
      "1,1",
      "1,2",
      "2,2",
      "3,null",
      "4,4"
    )
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testTumbleWindowAggregateWithCollectUnnest(): Unit = {
    val data = TestData.tupleData3.map { case (i, l, s) => (l, i, s) }
    val stream = failingDataSource(data)
      .assignTimestampsAndWatermarks(new TimestampAndWatermarkWithOffset[(Long, Int, String)](0L))
    val t = stream.toTable(tEnv, 'b, 'a, 'c, 'rowtime.rowtime)
    tEnv.createTemporaryView("T", t)

    val sqlQuery =
      """
        |WITH T1 AS (SELECT b, COLLECT(b) as `set`
        |    FROM T
        |    GROUP BY b, TUMBLE(rowtime, INTERVAL '3' SECOND)
        |)
        |SELECT b, s FROM T1, UNNEST(T1.`set`) AS A(s) where b < 3
      """.stripMargin
    val result = tEnv.sqlQuery(sqlQuery).toRetractStream[Row]
    val sink = new TestingRetractSink
    result.addSink(sink).setParallelism(1)
    env.execute()

    val expected = List(
      "1,1",
      "2,2",
      "2,2"
    )
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testCrossWithUnnest(): Unit = {
    val data = List(
      (1, 1L, Array("Hi", "w")),
      (2, 2L, Array("Hello", "k")),
      (3, 2L, Array("Hello world", "x"))
    )

    val t = env.fromCollection(data).toTable(tEnv, 'a, 'b, 'c)
    tEnv.createTemporaryView("T", t)

    val sqlQuery = "SELECT a, s FROM T, UNNEST(T.c) as A (s)"
    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = List("1,Hi", "1,w", "2,Hello", "2,k", "3,Hello world", "3,x")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testCrossWithUnnestForMap(): Unit = {
    val data = List(
      Row.of(
        Int.box(1),
        Long.box(11L), {
          val map = new java.util.HashMap[String, String]()
          map.put("a", "10")
          map.put("b", "11")
          map
        }),
      Row.of(
        Int.box(2),
        Long.box(22L), {
          val map = new java.util.HashMap[String, String]()
          map.put("c", "20")
          map
        }),
      Row.of(
        Int.box(3),
        Long.box(33L), {
          val map = new java.util.HashMap[String, String]()
          map.put("d", "30")
          map.put("e", "31")
          map
        })
    )

    implicit val typeInfo = Types.ROW(
      Array("a", "b", "c"),
      Array[TypeInformation[_]](Types.INT, Types.LONG, Types.MAP(Types.STRING, Types.STRING))
    )
    val t = env.fromCollection(data).toTable(tEnv, 'a, 'b, 'c)
    tEnv.createTemporaryView("T", t)

    val sqlQuery = "SELECT a, b, v FROM T CROSS JOIN UNNEST(c) as f (k, v)"
    val result = tEnv.sqlQuery(sqlQuery)

    val sink = new TestingRetractTableSink()
      .configure(Array("a", "b", "v"), Array(Types.INT, Types.LONG, Types.STRING))
    tEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal("MySink", sink)
    result.executeInsert("MySink").await()

    val expected = List("1,11,10", "1,11,11", "2,22,20", "3,33,30", "3,33,31")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testJoinWithUnnestOfTuple(): Unit = {
    val data = List(
      (1, Array((12, "45.6"), (2, "45.612"))),
      (2, Array((13, "41.6"), (1, "45.2136"))),
      (3, Array((18, "42.6")))
    )

    val t = env.fromCollection(data).toTable(tEnv, 'a, 'b)
    tEnv.createTemporaryView("T", t)

    val sqlQuery = "SELECT a, b, x, y " +
      "FROM " +
      "  (SELECT a, b FROM T WHERE a < 3) as tf, " +
      "  UNNEST(tf.b) as A (x, y) " +
      "WHERE x > a"
    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = List(
      "1,[12,45.6, 2,45.612],12,45.6",
      "1,[12,45.6, 2,45.612],2,45.612",
      "2,[13,41.6, 1,45.2136],13,41.6")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testUnnestObjectArrayWithoutAlias(): Unit = {
    val data = List(
      (1, Array((12, "45.6"), (12, "45.612"))),
      (2, Array((13, "41.6"), (14, "45.2136"))),
      (3, Array((18, "42.6")))
    )
    val t = env.fromCollection(data).toTable(tEnv, 'a, 'b)
    tEnv.createTemporaryView("T", t)

    val sqlQuery = "SELECT a, b, A._1, A._2 FROM T, UNNEST(T.b) AS A where A._1 > 13"
    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = List("2,[13,41.6, 14,45.2136],14,45.2136", "3,[18,42.6],18,42.6")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testUnnestWithNestedFilter(): Unit = {
    val data = List(
      (1, Array((12, "45.6"), (12, "45.612"))),
      (2, Array((13, "41.6"), (14, "45.2136"))),
      (3, Array((18, "42.6")))
    )
    val t = env.fromCollection(data).toTable(tEnv, 'a, 'b)
    tEnv.createTemporaryView("MyTable", t)

    val sqlQuery =
      """
        |SELECT * FROM (
        |   SELECT a, b1, b2 FROM
        |       (SELECT a, b FROM MyTable) T
        |       CROSS JOIN
        |       UNNEST(T.b) as S(b1, b2)
        |       WHERE S.b1 >= 12
        |   ) tmp
        |WHERE b2 <> '42.6'
    """.stripMargin

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = List("1,12,45.612", "1,12,45.6", "2,13,41.6", "2,14,45.2136")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

}
