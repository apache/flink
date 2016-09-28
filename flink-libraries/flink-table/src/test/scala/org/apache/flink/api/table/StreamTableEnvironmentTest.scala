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

import org.apache.flink.api.scala.stream.utils.StreamTestData
import org.apache.flink.api.scala.table._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.util.StreamingMultipleProgramsTestBase
import org.junit.Test
import org.junit.Assert._



class StreamTableEnvironmentTest extends StreamingMultipleProgramsTestBase{

  @Test
  def testReduceExpression(): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)

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
      "'TEST' IN ('west', 'TEST', 'rest') " +
      "FROM MyTable WHERE a>(1+7)"

    val t = StreamTestData.getSmall3TupleDataStream(env).toTable(tEnv).as('a, 'b, 'c)
    tEnv.registerTable("MyTable", t)

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
  }

  @Test
  def testReduceExpressionForTableAPI(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)

    val t = StreamTestData.getSmall3TupleDataStream(env).toTable(tEnv).as('a, 'b, 'c)

    val table = t
      .where('a > (1+7))
      .select((3+4).toExpr + 6,
              (11 === 1) ? ("a", "b"),
              " STRING ".trim,
              "test" + "string",
              "1990-10-14 23:00:00.123".toTimestamp + 10.day + 1.second,
              1.isNull,
              "TEST".like("%EST"),
              2.5.toExpr.floor())


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
  }

}
