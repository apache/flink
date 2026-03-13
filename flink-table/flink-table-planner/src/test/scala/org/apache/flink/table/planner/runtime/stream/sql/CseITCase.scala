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
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.table.api.Expressions.$
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.api.typeutils.Types
import org.apache.flink.table.planner.runtime.utils._
import org.apache.flink.table.planner.runtime.utils.CseTestFunctions._
import org.apache.flink.types.Row

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.{BeforeEach, Test}
import org.junit.jupiter.api.Assertions.assertEquals

/**
 * Integration test for Common Sub-expression Elimination (CSE) optimization in Flink SQL.
 *
 * <p>CSE identifies duplicate sub-expressions in SQL projections and ensures each is evaluated only
 * once. The result is cached in a local variable and reused by subsequent references. This is
 * especially beneficial for expensive UDF calls.
 *
 * <p>Example: Given {@code SELECT f2(f(x)), f3(f(x)), f4(f(x)) FROM T}, the sub-expression {@code
 * f(x)} is evaluated once and reused 3 times, instead of being computed 3 times.
 */
class CseITCase extends StreamingTestBase {

  @BeforeEach
  def resetCounters(): Unit = {
    CseTestFunctions.resetCounters()
  }

  /**
   * Tests that common sub-expressions in projections produce correct results.
   *
   * SQL:
   * {{{
   *   SELECT testcse2(testcse(c)), testcse3(testcse(c)), testcse4(testcse(c))
   *   FROM input
   * }}}
   *
   * Expected: testcse(c) is evaluated once per row; outer functions each see the cached result.
   */
  @Test
  def testCseCorrectness(): Unit = {
    tEnv.createTemporarySystemFunction("testcse", classOf[TestCseFunc])
    tEnv.createTemporarySystemFunction("testcse2", classOf[TestCse2Func])
    tEnv.createTemporarySystemFunction("testcse3", classOf[TestCse3Func])
    tEnv.createTemporarySystemFunction("testcse4", classOf[TestCse4Func])

    val data = List(
      Row.of("hello"),
      Row.of("world"),
      Row.of("flink")
    )

    implicit val tpe: TypeInformation[Row] = new RowTypeInfo(Types.STRING)

    val ds = StreamingEnvUtil.fromCollection(env, data)
    val t = ds.toTable(tEnv, $("c"))
    tEnv.createTemporaryView("input", t)

    val sqlQuery =
      """
        |SELECT
        |  testcse2(testcse(c)) AS region1,
        |  testcse3(testcse(c)) AS region2,
        |  testcse4(testcse(c)) AS region3
        |FROM input
        |""".stripMargin

    val result = tEnv.sqlQuery(sqlQuery).toDataStream
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    // Verify correctness: testcse converts to uppercase, then each wrapper appends suffix
    val expected = List(
      "HELLO_2,HELLO_3,HELLO_4",
      "WORLD_2,WORLD_3,WORLD_4",
      "FLINK_2,FLINK_3,FLINK_4"
    )
    assertThat(sink.getAppendResults.sorted).isEqualTo(expected.sorted)
  }

  /**
   * Tests that the inner common sub-expression (testcse) is called only once per row when CSE
   * optimization is active. Without CSE it would be called 3 times per row.
   */
  @Test
  def testCseCallCount(): Unit = {
    // Use parallelism 1 to get deterministic call counts
    env.setParallelism(1)

    tEnv.createTemporarySystemFunction("testcse", classOf[TestCseFunc])
    tEnv.createTemporarySystemFunction("testcse2", classOf[TestCse2Func])
    tEnv.createTemporarySystemFunction("testcse3", classOf[TestCse3Func])
    tEnv.createTemporarySystemFunction("testcse4", classOf[TestCse4Func])

    val data = List(
      Row.of("a"),
      Row.of("b")
    )

    implicit val tpe: TypeInformation[Row] = new RowTypeInfo(Types.STRING)

    val ds = StreamingEnvUtil.fromCollection(env, data)
    val t = ds.toTable(tEnv, $("c"))
    tEnv.createTemporaryView("input", t)

    val sqlQuery =
      """
        |SELECT
        |  testcse2(testcse(c)) AS r1,
        |  testcse3(testcse(c)) AS r2,
        |  testcse4(testcse(c)) AS r3
        |FROM input
        |""".stripMargin

    val result = tEnv.sqlQuery(sqlQuery).toDataStream
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    // With CSE: testcse should be called once per row (total 2 for 2 rows)
    // Without CSE: testcse would be called 3 times per row (total 6 for 2 rows)
    val totalCseCalls = TestCseFunc.CALL_COUNT.get()
    assertEquals(
      2,
      totalCseCalls,
      "testcse should be called once per row due to CSE, " +
        "not 3 times. Total calls for 2 rows should be 2, but got " + totalCseCalls)

    // Each wrapper function should also be called once per row (total 2 each)
    assertEquals(2, TestCse2Func.CALL_COUNT.get())
    assertEquals(2, TestCse3Func.CALL_COUNT.get())
    assertEquals(2, TestCse4Func.CALL_COUNT.get())
  }

  /**
   * Tests CSE with no duplicate sub-expressions. All expressions are unique, so CSE should not
   * affect the result.
   */
  @Test
  def testNoCseCandidates(): Unit = {
    tEnv.createTemporarySystemFunction("testcse", classOf[TestCseFunc])
    tEnv.createTemporarySystemFunction("testcse2", classOf[TestCse2Func])
    tEnv.createTemporarySystemFunction("testcse3", classOf[TestCse3Func])

    val data = List(Row.of("hello", "world"))

    implicit val tpe: TypeInformation[Row] = new RowTypeInfo(Types.STRING, Types.STRING)

    val ds = StreamingEnvUtil.fromCollection(env, data)
    val t = ds.toTable(tEnv, $("a"), $("b"))
    tEnv.createTemporaryView("input", t)

    // No duplicates: testcse(a) and testcse(b) are different expressions
    val sqlQuery =
      """
        |SELECT
        |  testcse2(testcse(a)) AS r1,
        |  testcse3(testcse(b)) AS r2
        |FROM input
        |""".stripMargin

    val result = tEnv.sqlQuery(sqlQuery).toDataStream
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = List("HELLO_2,WORLD_3")
    assertThat(sink.getAppendResults.sorted).isEqualTo(expected.sorted)
  }

  /**
   * Tests CSE with null handling. The common sub-expression result may be null, and the CSE
   * mechanism must correctly propagate null flags.
   */
  @Test
  def testCseWithNullValues(): Unit = {
    tEnv.createTemporarySystemFunction("testcse", classOf[TestCseFunc])
    tEnv.createTemporarySystemFunction("testcse2", classOf[TestCse2Func])
    tEnv.createTemporarySystemFunction("testcse3", classOf[TestCse3Func])

    val data = List(
      Row.of("hello"),
      Row.of(null: String)
    )

    implicit val tpe: TypeInformation[Row] = new RowTypeInfo(Types.STRING)

    val ds = StreamingEnvUtil.fromCollection(env, data)
    val t = ds.toTable(tEnv, $("c"))
    tEnv.createTemporaryView("input", t)

    val sqlQuery =
      """
        |SELECT
        |  testcse2(testcse(c)) AS r1,
        |  testcse3(testcse(c)) AS r2
        |FROM input
        |""".stripMargin

    val result = tEnv.sqlQuery(sqlQuery).toDataStream
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = List(
      "HELLO_2,HELLO_3",
      "null,null"
    )
    assertThat(sink.getAppendResults.sorted).isEqualTo(expected.sorted)
  }

  /**
   * Tests CSE with deeply nested common sub-expressions.
   *
   * SQL:
   * {{{
   *   SELECT testcse2(testcse(testcse(c))), testcse3(testcse(testcse(c)))
   *   FROM input
   * }}}
   *
   * Here both testcse(c) and testcse(testcse(c)) are common sub-expressions.
   */
  @Test
  def testCseWithNestedCommonSubExpressions(): Unit = {
    tEnv.createTemporarySystemFunction("testcse", classOf[TestCseFunc])
    tEnv.createTemporarySystemFunction("testcse2", classOf[TestCse2Func])
    tEnv.createTemporarySystemFunction("testcse3", classOf[TestCse3Func])

    val data = List(Row.of("hi"))

    implicit val tpe: TypeInformation[Row] = new RowTypeInfo(Types.STRING)

    val ds = StreamingEnvUtil.fromCollection(env, data)
    val t = ds.toTable(tEnv, $("c"))
    tEnv.createTemporaryView("input", t)

    val sqlQuery =
      """
        |SELECT
        |  testcse2(testcse(testcse(c))) AS r1,
        |  testcse3(testcse(testcse(c))) AS r2
        |FROM input
        |""".stripMargin

    val result = tEnv.sqlQuery(sqlQuery).toDataStream
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    // testcse("hi") = "HI", testcse("HI") = "HI" (already uppercase)
    // testcse2("HI") = "HI_2", testcse3("HI") = "HI_3"
    val expected = List("HI_2,HI_3")
    assertThat(sink.getAppendResults.sorted).isEqualTo(expected.sorted)
  }
}
