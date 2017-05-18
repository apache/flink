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
package org.apache.flink.table.api.scala.stream.sql

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.utils.TableTestBase
import org.apache.flink.table.utils.TableTestUtil._
import org.junit.Test

class ExpressionReductionTest extends TableTestBase {

  @Test
  def testReduceCalcExpression(): Unit = {
    val util = streamTestUtil()
    util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)

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

    val expected = unaryNode(
      "DataStreamCalc",
      streamTableNode(0),
      term("select",
        "+(7, a) AS EXPR$0",
        "+(b, 3) AS EXPR$1",
        "'b' AS EXPR$2",
        "'STRING' AS EXPR$3",
        "'teststring' AS EXPR$4",
        "null AS EXPR$5",
        "1990-10-24 23:00:01.123 AS EXPR$6",
        "19 AS EXPR$7",
        "false AS EXPR$8",
        "true AS EXPR$9",
        "2 AS EXPR$10",
        "true AS EXPR$11",
        "'trueX' AS EXPR$12"
      ),
      term("where", ">(a, 8)")
    )

    util.verifySql(sqlQuery, expected)
  }

  @Test
  def testReduceProjectExpression(): Unit = {
    val util = streamTestUtil()
    util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)

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

    val expected = unaryNode(
      "DataStreamCalc",
      streamTableNode(0),
      term("select",
        "+(7, a) AS EXPR$0",
        "+(b, 3) AS EXPR$1",
        "'b' AS EXPR$2",
        "'STRING' AS EXPR$3",
        "'teststring' AS EXPR$4",
        "null AS EXPR$5",
        "1990-10-24 23:00:01.123 AS EXPR$6",
        "19 AS EXPR$7",
        "false AS EXPR$8",
        "true AS EXPR$9",
        "2 AS EXPR$10",
        "true AS EXPR$11",
        "'trueX' AS EXPR$12"
      )
    )

    util.verifySql(sqlQuery, expected)
  }

  @Test
  def testReduceFilterExpression(): Unit = {
    val util = streamTestUtil()
    util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)

    val sqlQuery = "SELECT " +
      "*" +
      "FROM MyTable WHERE a>(1+7)"

    val expected = unaryNode(
      "DataStreamCalc",
      streamTableNode(0),
      term("select", "a", "b", "c"),
      term("where", ">(a, 8)")
    )

    util.verifySql(sqlQuery, expected)
  }

}
