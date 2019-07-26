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
package org.apache.flink.table.api.stream.sql

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.utils.TableTestUtil._
import org.apache.flink.table.utils.TableTestBase
import org.junit.Test

class SetOperatorsTest extends TableTestBase {

  @Test
  def testInOnLiterals(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)

    val resultStr = (1 to 30).map(i => s"$i:BIGINT").mkString(", ")
    val expected = unaryNode(
      "DataStreamCalc",
      streamTableNode(table),
      term("select", "a", "b", "c"),
      term("where", s"IN(b, $resultStr)")
    )

    val inStr = (1 to 30).mkString(", ")
    util.verifySql(
      s"SELECT * FROM MyTable WHERE b in ($inStr)",
      expected)
  }

  @Test
  def testNotInOnLiterals(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)

    val resultStr = (1 to 30).map(i => s"$i:BIGINT").mkString(", ")
    val expected = unaryNode(
      "DataStreamCalc",
      streamTableNode(table),
      term("select", "a", "b", "c"),
      term("where", s"NOT IN(b, $resultStr)")
    )

    val notInStr = (1 to 30).mkString(", ")
    util.verifySql(
      s"SELECT * FROM MyTable WHERE b NOT IN ($notInStr)",
      expected)
  }

  @Test
  def testInUncorrelated(): Unit = {
    val streamUtil = streamTestUtil()
    val table = streamUtil.addTable[(Int, Long, String)]("tableA", 'a, 'b, 'c)
    val table1 = streamUtil.addTable[(Int, String)]("tableB", 'x, 'y)

    val sqlQuery =
      s"""
         |SELECT * FROM tableA
         |WHERE a IN (SELECT x FROM tableB)
       """.stripMargin

    val expected =
      unaryNode(
        "DataStreamCalc",
        binaryNode(
          "DataStreamJoin",
          streamTableNode(table),
          unaryNode(
            "DataStreamGroupAggregate",
            unaryNode(
              "DataStreamCalc",
              streamTableNode(table1),
              term("select", "x")
            ),
            term("groupBy", "x"),
            term("select", "x")
          ),
          term("where", "=(a, x)"),
          term("join", "a", "b", "c", "x"),
          term("joinType", "InnerJoin")
        ),
        term("select", "a", "b", "c")
      )

    streamUtil.verifySql(sqlQuery, expected)
  }

  @Test
  def testInUncorrelatedWithConditionAndAgg(): Unit = {
    val streamUtil = streamTestUtil()
    val table = streamUtil.addTable[(Int, Long, String)]("tableA", 'a, 'b, 'c)
    val table1 = streamUtil.addTable[(Int, String)]("tableB", 'x, 'y)

    val sqlQuery =
      s"""
         |SELECT * FROM tableA
         |WHERE a IN (SELECT SUM(x) FROM tableB GROUP BY y HAVING y LIKE '%Hanoi%')
       """.stripMargin

    val expected =
      unaryNode(
        "DataStreamCalc",
        binaryNode(
          "DataStreamJoin",
          streamTableNode(table),
          unaryNode(
            "DataStreamGroupAggregate",
            unaryNode(
              "DataStreamCalc",
              unaryNode(
                "DataStreamGroupAggregate",
                unaryNode(
                  "DataStreamCalc",
                  streamTableNode(table1),
                  term("select", "x", "y"),
                  term("where", "LIKE(y, '%Hanoi%')")
                ),
                term("groupBy", "y"),
                term("select", "y, SUM(x) AS EXPR$0")
              ),
              term("select", "EXPR$0")
            ),
            term("groupBy", "EXPR$0"),
            term("select", "EXPR$0")
          ),
          term("where", "=(a, EXPR$0)"),
          term("join", "a", "b", "c", "EXPR$0"),
          term("joinType", "InnerJoin")
        ),
        term("select", "a", "b", "c")
      )

    streamUtil.verifySql(sqlQuery, expected)
  }

  @Test
  def testInWithMultiUncorrelatedCondition(): Unit = {
    val streamUtil = streamTestUtil()
    val table = streamUtil.addTable[(Int, Long, String)]("tableA", 'a, 'b, 'c)
    val table1 = streamUtil.addTable[(Int, String)]("tableB", 'x, 'y)
    val table2 = streamUtil.addTable[(Long, Int)]("tableC", 'w, 'z)

    val sqlQuery =
      s"""
         |SELECT * FROM tableA
         |WHERE a IN (SELECT x FROM tableB)
         |AND b IN (SELECT w FROM tableC)
       """.stripMargin

    val expected =
      unaryNode(
        "DataStreamCalc",
        binaryNode(
          "DataStreamJoin",
          unaryNode(
            "DataStreamCalc",
            binaryNode(
              "DataStreamJoin",
              streamTableNode(table),
              unaryNode(
                "DataStreamGroupAggregate",
                unaryNode(
                  "DataStreamCalc",
                  streamTableNode(table1),
                  term("select", "x")
                ),
                term("groupBy", "x"),
                term("select", "x")
              ),
              term("where", "=(a, x)"),
              term("join", "a", "b", "c", "x"),
              term("joinType", "InnerJoin")
            ),
            term("select", "a", "b", "c")
          ),
          unaryNode(
            "DataStreamGroupAggregate",
            unaryNode(
              "DataStreamCalc",
              streamTableNode(table2),
              term("select", "w")
            ),
            term("groupBy", "w"),
            term("select", "w")
          ),
          term("where", "=(b, w)"),
          term("join", "a", "b", "c", "w"),
          term("joinType", "InnerJoin")
      ),
        term("select", "a", "b", "c")
      )

    streamUtil.verifySql(sqlQuery, expected)
  }

  @Test
  def testValuesWithCast(): Unit = {
    val util = streamTestUtil()

    val expected = naryNode(
      "DataStreamUnion",
      List(
        unaryNode("DataStreamCalc",
          values("DataStreamValues",
            tuples(List("0"))),
          term("select", "1 AS EXPR$0, 1:BIGINT AS EXPR$1")),
        unaryNode("DataStreamCalc",
          values("DataStreamValues",
            tuples(List("0"))),
          term("select", "2 AS EXPR$0, 2:BIGINT AS EXPR$1")),
        unaryNode("DataStreamCalc",
          values("DataStreamValues",
            tuples(List("0"))),
          term("select", "3 AS EXPR$0, 3:BIGINT AS EXPR$1"))
      ),
      term("all", "true"),
      term("union all", "EXPR$0, EXPR$1")
    )

    util.verifySql(
      "VALUES (1, cast(1 as BIGINT) ),(2, cast(2 as BIGINT)),(3, cast(3 as BIGINT))",
      expected
    )
  }
}
