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

package org.apache.flink.table.runtime.stream.sql

import org.apache.flink.api.scala._
import org.apache.flink.table.api.{TableConfigOptions, TableException}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.runtime.utils.StreamingWithStateTestBase.StateBackendMode
import org.apache.flink.table.runtime.utils.{StreamingWithStateTestBase, TestingRetractSink}
import org.apache.flink.types.Row

import org.junit.Assert.assertEquals
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

import scala.collection.mutable

@RunWith(classOf[Parameterized])
class SubQueryITCase(mode: StateBackendMode)
    extends StreamingWithStateTestBase(mode) {

  @Test
  def testInUncorrelated(): Unit = {
    val sqlQuery =
      s"""
         |SELECT * FROM tableA
         |WHERE a IN (SELECT x FROM tableB)
       """.stripMargin

    val dataA = Seq(
      (1, 1L, "Hello"),
      (2, 2L, "Hello"),
      (3, 3L, "Hello World"),
      (4, 4L, "Hello")
    )

    val dataB = Seq(
      (1, "hello"),
      (2, "co-hello"),
      (4, "hello")
    )

    tEnv.registerTable("tableA",
      env.fromCollection(dataA).toTable(tEnv).as('a, 'b, 'c))

    tEnv.registerTable("tableB",
      env.fromCollection(dataB).toTable(tEnv).as('x, 'y))

    val sink = new TestingRetractSink
    val results = tEnv.sqlQuery(sqlQuery).toRetractStream[Row]
    results.addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq(
      "1,1,Hello", "2,2,Hello", "4,4,Hello"
    )

    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testInUncorrelatedWithConditionAndAgg(): Unit = {
    env.setParallelism(1)
    val sqlQuery =
      s"""
         |SELECT * FROM tableA
         |WHERE a IN (SELECT SUM(x) FROM tableB GROUP BY y HAVING y LIKE '%Hanoi%')
       """.stripMargin

    val dataA = Seq(
      (1, 1L, "Hello"),
      (2, 2L, "Hello"),
      (3, 3L, "Hello World"),
      (4, 4L, "Hello")
    )

    val dataB = Seq(
      (1, "hello"),
      (1, "Hanoi"),
      (1, "Hanoi"),
      (2, "Hanoi-1"),
      (2, "Hanoi-1"),
      (-1, "Hanoi-1")
    )

    tEnv.registerTable("tableA",
      env.fromCollection(dataA).toTable(tEnv).as('a, 'b, 'c))

    tEnv.registerTable("tableB",
      env.fromCollection(dataB).toTable(tEnv).as('x, 'y))

    val sink = new TestingRetractSink
    val results = tEnv.sqlQuery(sqlQuery).toRetractStream[Row]
    results.addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq(
      "2,2,Hello", "3,3,Hello World"
    )

    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testInWithMultiUncorrelatedCondition(): Unit = {
    val sqlQuery =
      s"""
         |SELECT * FROM tableA
         |WHERE a IN (SELECT x FROM tableB)
         |AND b IN (SELECT w FROM tableC)
       """.stripMargin

    val dataA = Seq(
      (1, 1L, "Hello"),
      (2, 2L, "Hello"),
      (3, 3L, "Hello World"),
      (4, 4L, "Hello")
    )

    val dataB = Seq(
      (1, "hello"),
      (2, "co-hello"),
      (4, "hello")
    )

    val dataC = Seq(
      (1L, "Joker"),
      (1L, "Sanity"),
      (2L, "Cool")
    )

    tEnv.registerTable("tableA",
      env.fromCollection(dataA).toTable(tEnv).as('a, 'b, 'c))

    tEnv.registerTable("tableB",
      env.fromCollection(dataB).toTable(tEnv).as('x, 'y))

    tEnv.registerTable("tableC",
      env.fromCollection(dataC).toTable(tEnv).as('w, 'z))

    val sink = new TestingRetractSink
    val results = tEnv.sqlQuery(sqlQuery).toRetractStream[Row]
    results.addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq(
      "1,1,Hello", "2,2,Hello"
    )

    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testEmptyAll(): Unit = {
    val sqlQuery =
      """
        |SELECT * FROM MyTable WHERE a > ALL(
        |  SELECT b FROM MyTable WHERE false
        |)
      """.stripMargin
    val data = Seq((-1, 1L), (2, 2L), (-3, 3L))

    tEnv.registerTable("MyTable", env.fromCollection(data).toTable(tEnv).as('a, 'b))
    val sink = new TestingRetractSink
    tEnv.getConfig.getConf.setBoolean(
      TableConfigOptions.SQL_EXEC_SOURCE_VALUES_INPUT_ENABLED, true) // enable values source input
    val results = tEnv.sqlQuery(sqlQuery).toRetractStream[Row]
    results.addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq("-1,1", "2,2", "-3,3")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test(expected = classOf[TableException])
  def testEmptyAllWhenDisableValuesSourceInput(): Unit = {
    val sqlQuery =
      """
        |SELECT * FROM MyTable WHERE a > ALL(
        |  SELECT b FROM MyTable WHERE false
        |)
      """.stripMargin
    val data = Seq((-1, 1L), (2, 2L), (-3, 3L))

    tEnv.registerTable("MyTable", env.fromCollection(data).toTable(tEnv).as('a, 'b))
    val sink = new TestingRetractSink
    // default disable values source input
    val results = tEnv.sqlQuery(sqlQuery).toRetractStream[Row]
    results.addSink(sink).setParallelism(1)
    env.execute()
  }

  @Test
  def testInWithNullColumn(): Unit = {
    val sqlQuery =
      s"""
         |SELECT a, b FROM tableA
         |WHERE (b, d) IN
         |(SELECT b, d FROM tableB WHERE tableA.c = tableB.c)
       """.stripMargin


    val dataA = new mutable.MutableList[(Int, String, Int, Int)]
    dataA.+=((1, null, 2, 3))
    dataA.+=((1, "1", 1, 1))

    val dataB = new mutable.MutableList[(Int, String, Int, Int)]
    dataB.+=((1, null, 2, 3))
    dataB.+=((1, "1", 1, 1))

    tEnv.registerTable("tableA",
      env.fromCollection(dataA).toTable(tEnv).as('a, 'b, 'c, 'd))

    tEnv.registerTable("tableB",
      env.fromCollection(dataB).toTable(tEnv).as('a, 'b, 'c, 'd))

    val sink = new TestingRetractSink
    val results = tEnv.sqlQuery(sqlQuery).toRetractStream[Row]
    results.addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq("1,1")

    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testInWithOutNullColumn(): Unit = {
    val sqlQuery =
      s"""
         |SELECT a, b FROM tableA
         |WHERE (c, d) IN
         |(SELECT c, d FROM tableB WHERE tableA.c = tableB.c)
       """.stripMargin


    val dataA = new mutable.MutableList[(Int, String, Int, Int)]
    dataA.+=((1, null, 2, 3))
    dataA.+=((1, "1", 1, 1))

    val dataB = new mutable.MutableList[(Int, String, Int, Int)]
    dataB.+=((1, null, 2, 3))
    dataB.+=((1, "1", 1, 1))

    tEnv.registerTable("tableA",
      env.fromCollection(dataA).toTable(tEnv).as('a, 'b, 'c, 'd))

    tEnv.registerTable("tableB",
      env.fromCollection(dataB).toTable(tEnv).as('a, 'b, 'c, 'd))

    val sink = new TestingRetractSink
    val results = tEnv.sqlQuery(sqlQuery).toRetractStream[Row]
    results.addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq(
      "1,1",
      "1,null"
    )

    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testNotInWithNull(): Unit = {
    val sqlQuery =
      s"""
         |SELECT a1, a2
         |  FROM   a
         |  WHERE  a1 NOT IN (SELECT b.b1
         |                    FROM   b
         |                    WHERE  a.a2 = b.b2)
       """.stripMargin


    val dataA = new mutable.MutableList[(String, String)]
    dataA.+=(("1", "2"))
    dataA.+=(("2", null))

    val dataB = new mutable.MutableList[(String, String)]
    dataB.+=(("1", "1"))
    dataB.+=(("2", null))

    tEnv.registerTable("a",
      env.fromCollection(dataA).toTable(tEnv).as('a1, 'a2))

    tEnv.registerTable("b",
      env.fromCollection(dataB).toTable(tEnv).as('b1, 'b2))

    val sink = new TestingRetractSink
    val results = tEnv.sqlQuery(sqlQuery).toRetractStream[Row]
    results.addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq(
      "1,2", "2,null"
    )

    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }
}
