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
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.table.runtime.utils.{StreamITCase, StreamingWithStateTestBase}
import org.apache.flink.types.Row
import org.junit.Assert.assertEquals
import org.junit.Test

class SetOperatorsITCase extends StreamingWithStateTestBase {

  @Test
  def testInUncorrelatedWithConditionAndAgg(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env)
    StreamITCase.clear

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

    val results = tEnv.sqlQuery(sqlQuery).toRetractStream[Row]
    results.addSink(new StreamITCase.RetractingSink)
    env.execute()

    val expected = Seq(
      "2,2,Hello", "3,3,Hello World"
    )

    assertEquals(expected.sorted, StreamITCase.retractedResults.sorted)
  }

  @Test
  def testInWithMultiUncorrelatedCondition(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env)
    StreamITCase.clear

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

    val results = tEnv.sqlQuery(sqlQuery).toRetractStream[Row]
    results.addSink(new StreamITCase.RetractingSink)
    env.execute()

    val expected = Seq(
      "1,1,Hello", "2,2,Hello"
    )

    assertEquals(expected.sorted, StreamITCase.retractedResults.sorted)
  }

  @Test
  def testNotInUncorrelated(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env)
    StreamITCase.clear

    val sqlQuery =
      s"""
         |SELECT * FROM tableA
         |WHERE a NOT IN (SELECT x FROM tableB)
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

    val results = tEnv.sqlQuery(sqlQuery).toRetractStream[Row]
    results.addSink(new StreamITCase.RetractingSink)
    env.execute()

    val expected = Seq(
      "3,3,Hello World"
    )

    assertEquals(expected.sorted, StreamITCase.retractedResults.sorted)
  }
}
