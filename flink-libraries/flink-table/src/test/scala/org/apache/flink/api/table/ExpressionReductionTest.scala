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
package org.apache.flink.api.table

import org.apache.flink.api.java.{DataSet => JDataSet}
import org.apache.flink.api.scala.table._
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment, createTypeInformation}
import org.apache.flink.streaming.api.datastream.{DataStream => JDataStream}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.junit.Assert._
import org.junit.Test
import org.mockito.Mockito.{mock, when}

class ExpressionReductionTest {

  private def mockBatchTableEnvironment(): BatchTableEnvironment = {
    val env = mock(classOf[ExecutionEnvironment])
    val tEnv = TableEnvironment.getTableEnvironment(env)

    val ds = mock(classOf[DataSet[(Int, Long, String)]])
    val jDs = mock(classOf[JDataSet[(Int, Long, String)]])
    when(ds.javaSet).thenReturn(jDs)
    when(jDs.getType).thenReturn(createTypeInformation[(Int, Long, String)])

    val t = ds.toTable(tEnv).as('a, 'b, 'c)
    tEnv.registerTable("MyTable", t)
    tEnv
  }

  private def mockStreamTableEnvironment(): StreamTableEnvironment = {
    val env = mock(classOf[StreamExecutionEnvironment])
    val tEnv = TableEnvironment.getTableEnvironment(env)

    val ds = mock(classOf[DataStream[(Int, Long, String)]])
    val jDs = mock(classOf[JDataStream[(Int, Long, String)]])
    when(ds.javaStream).thenReturn(jDs)
    when(jDs.getType).thenReturn(createTypeInformation[(Int, Long, String)])

    val t = ds.toTable(tEnv).as('a, 'b, 'c)
    tEnv.registerTable("MyTable", t)
    tEnv
  }

  @Test
  def testReduceCalcExpressionForBatchSQL(): Unit = {
    val tEnv = mockBatchTableEnvironment()

    val sqlQuery = "SELECT " +
      "(3+4)+a, " +
      "b+(1+2), " +
      "CASE 11 WHEN 1 THEN 'a' ELSE 'b' END, " +
      "TRIM(BOTH ' STRING '),  " +
      "'test' || 'string', " +
      "NULLIF(1, 1), " +
      "TIMESTAMP '1990-10-14 23:00:00.123' + INTERVAL '10 00:00:01' DAY TO SECOND, " +
      "EXTRACT(DAY FROM INTERVAL '19 12:10:10.123' DAY TO SECOND(3)),  " +
      "1 IS NULL, " +
      "'TEST' LIKE '%EST', " +
      "FLOOR(2.5), " +
      "'TEST' IN ('west', 'TEST', 'rest'), " +
      "CAST(TRUE AS VARCHAR) || 'X'" +
      "FROM MyTable WHERE a>(1+7)"

    val table = tEnv.sql(sqlQuery)

    val optimized = tEnv.optimize(table.getRelNode)
    val optimizedString = optimized.toString
    assertTrue(optimizedString.contains(">(_1, 8)"))
    assertTrue(optimizedString.contains("+(7, _1) AS EXPR$0"))
    assertTrue(optimizedString.contains("+(_2, 3) AS EXPR$1"))
    assertTrue(optimizedString.contains("'b' AS EXPR$2"))
    assertTrue(optimizedString.contains("'STRING' AS EXPR$3"))
    assertTrue(optimizedString.contains("'teststring' AS EXPR$4"))
    assertTrue(optimizedString.contains("null AS EXPR$5"))
    assertTrue(optimizedString.contains("1990-10-24 23:00:01 AS EXPR$6"))
    assertTrue(optimizedString.contains("19 AS EXPR$7"))
    assertTrue(optimizedString.contains("false AS EXPR$8"))
    assertTrue(optimizedString.contains("true AS EXPR$9"))
    assertTrue(optimizedString.contains("2 AS EXPR$10"))
    assertTrue(optimizedString.contains("true AS EXPR$11"))
    assertTrue(optimizedString.contains("'TRUEX' AS EXPR$12"))
  }

  @Test
  def testReduceProjectExpressionForBatchSQL(): Unit = {
    val tEnv = mockBatchTableEnvironment()

    val sqlQuery = "SELECT " +
      "(3+4)+a, " +
      "b+(1+2), " +
      "CASE 11 WHEN 1 THEN 'a' ELSE 'b' END, " +
      "TRIM(BOTH ' STRING '),  " +
      "'test' || 'string', " +
      "NULLIF(1, 1), " +
      "TIMESTAMP '1990-10-14 23:00:00.123' + INTERVAL '10 00:00:01' DAY TO SECOND, " +
      "EXTRACT(DAY FROM INTERVAL '19 12:10:10.123' DAY TO SECOND(3)),  " +
      "1 IS NULL, " +
      "'TEST' LIKE '%EST', " +
      "FLOOR(2.5), " +
      "'TEST' IN ('west', 'TEST', 'rest'), " +
      "CAST(TRUE AS VARCHAR) || 'X'" +
      "FROM MyTable"

    val table = tEnv.sql(sqlQuery)

    val optimized = tEnv.optimize(table.getRelNode)
    val optimizedString = optimized.toString
    assertTrue(optimizedString.contains("+(7, _1) AS EXPR$0"))
    assertTrue(optimizedString.contains("+(_2, 3) AS EXPR$1"))
    assertTrue(optimizedString.contains("'b' AS EXPR$2"))
    assertTrue(optimizedString.contains("'STRING' AS EXPR$3"))
    assertTrue(optimizedString.contains("'teststring' AS EXPR$4"))
    assertTrue(optimizedString.contains("null AS EXPR$5"))
    assertTrue(optimizedString.contains("1990-10-24 23:00:01 AS EXPR$6"))
    assertTrue(optimizedString.contains("19 AS EXPR$7"))
    assertTrue(optimizedString.contains("false AS EXPR$8"))
    assertTrue(optimizedString.contains("true AS EXPR$9"))
    assertTrue(optimizedString.contains("2 AS EXPR$10"))
    assertTrue(optimizedString.contains("true AS EXPR$11"))
    assertTrue(optimizedString.contains("'TRUEX' AS EXPR$12"))
  }

  @Test
  def testReduceFilterExpressionForBatchSQL(): Unit = {
    val tEnv = mockBatchTableEnvironment()

    val sqlQuery = "SELECT " +
      "*" +
      "FROM MyTable WHERE a>(1+7)"

    val table = tEnv.sql(sqlQuery)

    val optimized = tEnv.optimize(table.getRelNode)
    val optimizedString = optimized.toString
    assertTrue(optimizedString.contains(">(_1, 8)"))
  }

  @Test
  def testReduceCalcExpressionForBatchTableAPI(): Unit = {
    val tEnv = mockBatchTableEnvironment()

    val table = tEnv
      .scan("MyTable")
      .where('a > (1 + 7))
      .select((3 + 4).toExpr + 6,
              (11 === 1) ? ("a", "b"),
              " STRING ".trim,
              "test" + "string",
              "1990-10-14 23:00:00.123".toTimestamp + 10.day + 1.second,
              1.isNull,
              "TEST".like("%EST"),
              2.5.toExpr.floor(),
              true.cast(Types.STRING) + "X")


    val optimized = tEnv.optimize(table.getRelNode)
    val optimizedString = optimized.toString
    assertTrue(optimizedString.contains(">(_1, 8)"))
    assertTrue(optimizedString.contains("13 AS _c0"))
    assertTrue(optimizedString.contains("'b' AS _c1"))
    assertTrue(optimizedString.contains("'STRING' AS _c2"))
    assertTrue(optimizedString.contains("'teststring' AS _c3"))
    assertTrue(optimizedString.contains("1990-10-24 23:00:01 AS _c4"))
    assertTrue(optimizedString.contains("false AS _c5"))
    assertTrue(optimizedString.contains("true AS _c6"))
    assertTrue(optimizedString.contains("2E0 AS _c7"))
    assertTrue(optimizedString.contains("'TRUEX' AS _c8"))
  }

  @Test
  def testReduceProjectExpressionForBatchTableAPI(): Unit = {
    val tEnv = mockBatchTableEnvironment()

    val table = tEnv
      .scan("MyTable")
      .select((3 + 4).toExpr + 6,
              (11 === 1) ? ("a", "b"),
              " STRING ".trim,
              "test" + "string",
              "1990-10-14 23:00:00.123".toTimestamp + 10.day + 1.second,
              1.isNull,
              "TEST".like("%EST"),
              2.5.toExpr.floor(),
              true.cast(Types.STRING) + "X")


    val optimized = tEnv.optimize(table.getRelNode)
    val optimizedString = optimized.toString
    assertTrue(optimizedString.contains("13 AS _c0"))
    assertTrue(optimizedString.contains("'b' AS _c1"))
    assertTrue(optimizedString.contains("'STRING' AS _c2"))
    assertTrue(optimizedString.contains("'teststring' AS _c3"))
    assertTrue(optimizedString.contains("1990-10-24 23:00:01 AS _c4"))
    assertTrue(optimizedString.contains("false AS _c5"))
    assertTrue(optimizedString.contains("true AS _c6"))
    assertTrue(optimizedString.contains("2E0 AS _c7"))
    assertTrue(optimizedString.contains("'TRUEX' AS _c8"))
  }

  @Test
  def testReduceFilterExpressionForBatchTableAPI(): Unit = {
    val tEnv = mockBatchTableEnvironment()

    val table = tEnv
      .scan("MyTable")
      .where('a > (1 + 7))

    val optimized = tEnv.optimize(table.getRelNode)
    val optimizedString = optimized.toString
    assertTrue(optimizedString.contains(">(_1, 8)"))
  }

  @Test
  def testReduceCalcExpressionForStreamSQL(): Unit = {
    val tEnv = mockStreamTableEnvironment()

    val sqlQuery = "SELECT STREAM " +
      "(3+4)+a, " +
      "b+(1+2), " +
      "CASE 11 WHEN 1 THEN 'a' ELSE 'b' END, " +
      "TRIM(BOTH ' STRING '),  " +
      "'test' || 'string', " +
      "NULLIF(1, 1), " +
      "TIMESTAMP '1990-10-14 23:00:00.123' + INTERVAL '10 00:00:01' DAY TO SECOND, " +
      "EXTRACT(DAY FROM INTERVAL '19 12:10:10.123' DAY TO SECOND(3)),  " +
      "1 IS NULL, " +
      "'TEST' LIKE '%EST', " +
      "FLOOR(2.5), " +
      "'TEST' IN ('west', 'TEST', 'rest'), " +
      "CAST(TRUE AS VARCHAR) || 'X'" +
      "FROM MyTable WHERE a>(1+7)"

    val table = tEnv.sql(sqlQuery)

    val optimized = tEnv.optimize(table.getRelNode)
    val optimizedString = optimized.toString
    assertTrue(optimizedString.contains(">(_1, 8)"))
    assertTrue(optimizedString.contains("+(7, _1) AS EXPR$0"))
    assertTrue(optimizedString.contains("+(_2, 3) AS EXPR$1"))
    assertTrue(optimizedString.contains("'b' AS EXPR$2"))
    assertTrue(optimizedString.contains("'STRING' AS EXPR$3"))
    assertTrue(optimizedString.contains("'teststring' AS EXPR$4"))
    assertTrue(optimizedString.contains("null AS EXPR$5"))
    assertTrue(optimizedString.contains("1990-10-24 23:00:01 AS EXPR$6"))
    assertTrue(optimizedString.contains("19 AS EXPR$7"))
    assertTrue(optimizedString.contains("false AS EXPR$8"))
    assertTrue(optimizedString.contains("true AS EXPR$9"))
    assertTrue(optimizedString.contains("2 AS EXPR$10"))
    assertTrue(optimizedString.contains("true AS EXPR$11"))
    assertTrue(optimizedString.contains("'TRUEX' AS EXPR$12"))
  }

  @Test
  def testReduceProjectExpressionForStreamSQL(): Unit = {
    val tEnv = mockStreamTableEnvironment()

    val sqlQuery = "SELECT STREAM " +
      "(3+4)+a, " +
      "b+(1+2), " +
      "CASE 11 WHEN 1 THEN 'a' ELSE 'b' END, " +
      "TRIM(BOTH ' STRING '),  " +
      "'test' || 'string', " +
      "NULLIF(1, 1), " +
      "TIMESTAMP '1990-10-14 23:00:00.123' + INTERVAL '10 00:00:01' DAY TO SECOND, " +
      "EXTRACT(DAY FROM INTERVAL '19 12:10:10.123' DAY TO SECOND(3)),  " +
      "1 IS NULL, " +
      "'TEST' LIKE '%EST', " +
      "FLOOR(2.5), " +
      "'TEST' IN ('west', 'TEST', 'rest'), " +
      "CAST(TRUE AS VARCHAR) || 'X'" +
      "FROM MyTable"

    val table = tEnv.sql(sqlQuery)

    val optimized = tEnv.optimize(table.getRelNode)
    val optimizedString = optimized.toString
    assertTrue(optimizedString.contains("+(7, a) AS EXPR$0"))
    assertTrue(optimizedString.contains("+(b, 3) AS EXPR$1"))
    assertTrue(optimizedString.contains("'b' AS EXPR$2"))
    assertTrue(optimizedString.contains("'STRING' AS EXPR$3"))
    assertTrue(optimizedString.contains("'teststring' AS EXPR$4"))
    assertTrue(optimizedString.contains("null AS EXPR$5"))
    assertTrue(optimizedString.contains("1990-10-24 23:00:01 AS EXPR$6"))
    assertTrue(optimizedString.contains("19 AS EXPR$7"))
    assertTrue(optimizedString.contains("false AS EXPR$8"))
    assertTrue(optimizedString.contains("true AS EXPR$9"))
    assertTrue(optimizedString.contains("2 AS EXPR$10"))
    assertTrue(optimizedString.contains("true AS EXPR$11"))
    assertTrue(optimizedString.contains("'TRUEX' AS EXPR$12"))
  }

  @Test
  def testReduceFilterExpressionForStreamSQL(): Unit = {
    val tEnv = mockStreamTableEnvironment()

    val sqlQuery = "SELECT STREAM " +
      "*" +
      "FROM MyTable WHERE a>(1+7)"

    val table = tEnv.sql(sqlQuery)

    val optimized = tEnv.optimize(table.getRelNode)
    val optimizedString = optimized.toString
    assertTrue(optimizedString.contains(">(_1, 8)"))
  }

  @Test
  def testReduceCalcExpressionForStreamTableAPI(): Unit = {
    val tEnv = mockStreamTableEnvironment()

    val table = tEnv
      .ingest("MyTable")
      .where('a > (1 + 7))
      .select((3 + 4).toExpr + 6,
              (11 === 1) ? ("a", "b"),
              " STRING ".trim,
              "test" + "string",
              "1990-10-14 23:00:00.123".toTimestamp + 10.day + 1.second,
              1.isNull,
              "TEST".like("%EST"),
              2.5.toExpr.floor(),
              true.cast(Types.STRING) + "X")


    val optimized = tEnv.optimize(table.getRelNode)
    val optimizedString = optimized.toString
    assertTrue(optimizedString.contains(">(_1, 8)"))
    assertTrue(optimizedString.contains("13 AS _c0"))
    assertTrue(optimizedString.contains("'b' AS _c1"))
    assertTrue(optimizedString.contains("'STRING' AS _c2"))
    assertTrue(optimizedString.contains("'teststring' AS _c3"))
    assertTrue(optimizedString.contains("1990-10-24 23:00:01 AS _c4"))
    assertTrue(optimizedString.contains("false AS _c5"))
    assertTrue(optimizedString.contains("true AS _c6"))
    assertTrue(optimizedString.contains("2E0 AS _c7"))
    assertTrue(optimizedString.contains("'TRUEX' AS _c8"))
  }

  @Test
  def testReduceProjectExpressionForStreamTableAPI(): Unit = {
    val tEnv = mockStreamTableEnvironment()

    val table =  tEnv
      .ingest("MyTable")
      .where('a > (1 + 7))
      .select((3 + 4).toExpr + 6,
              (11 === 1) ? ("a", "b"),
              " STRING ".trim,
              "test" + "string",
              "1990-10-14 23:00:00.123".toTimestamp + 10.day + 1.second,
              1.isNull,
              "TEST".like("%EST"),
              2.5.toExpr.floor(),
              true.cast(Types.STRING) + "X")


    val optimized = tEnv.optimize(table.getRelNode)
    val optimizedString = optimized.toString
    assertTrue(optimizedString.contains(">(_1, 8)"))
    assertTrue(optimizedString.contains("13 AS _c0"))
    assertTrue(optimizedString.contains("'b' AS _c1"))
    assertTrue(optimizedString.contains("'STRING' AS _c2"))
    assertTrue(optimizedString.contains("'teststring' AS _c3"))
    assertTrue(optimizedString.contains("1990-10-24 23:00:01 AS _c4"))
    assertTrue(optimizedString.contains("false AS _c5"))
    assertTrue(optimizedString.contains("true AS _c6"))
    assertTrue(optimizedString.contains("2E0 AS _c7"))
    assertTrue(optimizedString.contains("'TRUEX' AS _c8"))
  }

  @Test
  def testReduceFilterExpressionForStreamTableAPI(): Unit = {
    val tEnv = mockStreamTableEnvironment()

    val table = tEnv
      .ingest("MyTable")
      .where('a > (1 + 7))


    val optimized = tEnv.optimize(table.getRelNode)
    val optimizedString = optimized.toString
    assertTrue(optimizedString.contains(">(_1, 8)"))
  }

}
