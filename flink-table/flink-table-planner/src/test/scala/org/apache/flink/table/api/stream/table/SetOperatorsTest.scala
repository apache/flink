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

package org.apache.flink.table.api.stream.table

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.utils.TableTestBase
import org.apache.flink.table.utils.TableTestUtil.{binaryNode, streamTableNode, term, unaryNode}
import org.junit.Test

class SetOperatorsTest extends TableTestBase {

  @Test
  def testFilterUnionTranspose(): Unit = {
    val util = streamTestUtil()
    val left = util.addTable[(Int, Long, String)]("left", 'a, 'b, 'c)
    val right = util.addTable[(Int, Long, String)]("right", 'a, 'b, 'c)

    val result = left.unionAll(right)
      .where('a > 0)
      .groupBy('b)
      .select('a.sum as 'a, 'b as 'b, 'c.count as 'c)

    val expected = unaryNode(
      "DataStreamCalc",
        unaryNode(
          "DataStreamGroupAggregate",
          binaryNode(
            "DataStreamUnion",
            unaryNode(
              "DataStreamCalc",
              streamTableNode(left),
              term("select", "a", "b", "c"),
              term("where", ">(a, 0)")
            ),
            unaryNode(
              "DataStreamCalc",
              streamTableNode(right),
              term("select", "a", "b", "c"),
              term("where", ">(a, 0)")
            ),
            term("all", "true"),
            term("union all", "a", "b", "c")
          ),
          term("groupBy", "b"),
          term("select", "b", "SUM(a) AS EXPR$0", "COUNT(c) AS EXPR$1")
        ),
      term("select", "EXPR$0 AS a", "b", "EXPR$1 AS c")
    )

    util.verifyTable(result, expected)
  }

  @Test
  def testProjectUnionTranspose(): Unit = {
    val util = streamTestUtil()
    val left = util.addTable[(Int, Long, String)]("left", 'a, 'b, 'c)
    val right = util.addTable[(Int, Long, String)]("right", 'a, 'b, 'c)

    val result = left.select('a, 'b, 'c)
      .unionAll(right.select('a, 'b, 'c))
      .select('b, 'c)

    val expected = binaryNode(
      "DataStreamUnion",
      unaryNode(
        "DataStreamCalc",
        streamTableNode(left),
        term("select", "b", "c")
      ),
      unaryNode(
        "DataStreamCalc",
        streamTableNode(right),
        term("select", "b", "c")
      ),
      term("all", "true"),
      term("union all", "b", "c")
    )

    util.verifyTable(result, expected)
  }

  @Test
  def testInUncorrelated(): Unit = {
    val streamUtil = streamTestUtil()
    val tableA = streamUtil.addTable[(Int, Long, String)]('a, 'b, 'c)
    val tableB = streamUtil.addTable[(Int, String)]('x, 'y)

    val result = tableA.where('a.in(tableB.select('x)))

    val expected =
      unaryNode(
        "DataStreamCalc",
        binaryNode(
          "DataStreamJoin",
          streamTableNode(tableA),
          unaryNode(
            "DataStreamGroupAggregate",
            unaryNode(
              "DataStreamCalc",
              streamTableNode(tableB),
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

    streamUtil.verifyTable(result, expected)
  }

  @Test
  def testInUncorrelatedWithConditionAndAgg(): Unit = {
    val streamUtil = streamTestUtil()
    val tableA = streamUtil.addTable[(Int, Long, String)]("tableA", 'a, 'b, 'c)
    val tableB = streamUtil.addTable[(Int, String)]("tableB", 'x, 'y)

    val result = tableA
      .where('a.in(tableB.where('y.like("%Hanoi%")).groupBy('y).select('x.sum)))

    val expected =
      unaryNode(
        "DataStreamCalc",
        binaryNode(
          "DataStreamJoin",
          streamTableNode(tableA),
          unaryNode(
            "DataStreamGroupAggregate",
            unaryNode(
              "DataStreamCalc",
              unaryNode(
                "DataStreamGroupAggregate",
                unaryNode(
                  "DataStreamCalc",
                  streamTableNode(tableB),
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

    streamUtil.verifyTable(result, expected)
  }

  @Test
  def testInWithMultiUncorrelatedCondition(): Unit = {
    val streamUtil = streamTestUtil()
    val tableA = streamUtil.addTable[(Int, Long, String)]("tableA", 'a, 'b, 'c)
    val tableB = streamUtil.addTable[(Int, String)]("tableB", 'x, 'y)
    val tableC = streamUtil.addTable[(Long, Int)]("tableC", 'w, 'z)

    val result = tableA
      .where('a.in(tableB.select('x)) && 'b.in(tableC.select('w)))

    val expected =
      unaryNode(
        "DataStreamCalc",
        binaryNode(
          "DataStreamJoin",
          unaryNode(
            "DataStreamCalc",
            binaryNode(
              "DataStreamJoin",
              streamTableNode(tableA),
              unaryNode(
                "DataStreamGroupAggregate",
                unaryNode(
                  "DataStreamCalc",
                  streamTableNode(tableB),
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
              streamTableNode(tableC),
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

    streamUtil.verifyTable(result, expected)
  }
}
