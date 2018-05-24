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
import org.apache.flink.table.utils.TableTestUtil._
import org.apache.flink.table.utils.TableTestBase
import org.junit.Test

class SubQueryTest extends TableTestBase {

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
          streamTableNode(0),
          unaryNode(
            "DataStreamGroupAggregate",
            unaryNode(
              "DataStreamCalc",
              streamTableNode(1),
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
          streamTableNode(0),
          unaryNode(
            "DataStreamGroupAggregate",
            unaryNode(
              "DataStreamCalc",
              unaryNode(
                "DataStreamGroupAggregate",
                unaryNode(
                  "DataStreamCalc",
                  streamTableNode(1),
                  term("select", "x", "y"),
                  term("where", "LIKE(y, '%Hanoi%')")
                ),
                term("groupBy", "y"),
                term("select", "y, SUM(x) AS TMP_0")
              ),
              term("select", "TMP_0")
            ),
            term("groupBy", "TMP_0"),
            term("select", "TMP_0")
          ),
          term("where", "=(a, TMP_0)"),
          term("join", "a", "b", "c", "TMP_0"),
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
      .where(('a.in(tableB.select('x)) && ('b.in(tableC.select('w)))))

    val expected =
      unaryNode(
        "DataStreamCalc",
        binaryNode(
          "DataStreamJoin",
          unaryNode(
            "DataStreamCalc",
            binaryNode(
              "DataStreamJoin",
              streamTableNode(0),
              unaryNode(
                "DataStreamGroupAggregate",
                unaryNode(
                  "DataStreamCalc",
                  streamTableNode(1),
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
              streamTableNode(2),
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
