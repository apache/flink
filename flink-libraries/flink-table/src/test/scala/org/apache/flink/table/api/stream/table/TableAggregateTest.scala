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
import org.apache.flink.table.expressions.utils.Func0
import org.apache.flink.table.utils.{EmptyTableAggFunc, TableTestBase, TopN}
import org.apache.flink.table.utils.TableTestUtil._
import org.junit.Test

class TableAggregateTest extends TableTestBase {

  @Test
  def testTableAggregateWithGroupBy(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Int, Long, String)]('a, 'b, 'c)

    val top3 = new TopN(3)
    val resultTable = table
      .groupBy('b % 5)
      .flatAggregate(top3('a + 1, 'b))
      .select('f0 + 1 as 'a, 'f1 as 'b, 'f2 as 'c)

    val expected =
      unaryNode(
        "DataStreamCalc",
        unaryNode(
          "DataStreamGroupTableAggregate",
          unaryNode(
            "DataStreamCalc",
            streamTableNode(0),
            term("select", "a", "b", "MOD(b, 5) AS $f2", "+(a, 1) AS $f3")
          ),
          term("groupBy", "$f2"),
          term("flatAggregate", "TopN($f3, b) AS (f0, f1, f2)")
        ),
        term("select", "+(f0, 1) AS a", "f1 AS b", "f2 AS c")
      )
    util.verifyTable(resultTable, expected)
  }

  @Test
  def testTableAggregateWithoutGroupBy(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Int, Long, String)]('a, 'b, 'c)

    val top3 = new TopN(3)
    val resultTable = table
      .flatAggregate(top3('a, 'b))
      .select(Func0('f0) as 'a, 'f1 as 'b, 'f2 as 'c)

    val expected =
      unaryNode(
        "DataStreamCalc",
        unaryNode(
          "DataStreamGroupTableAggregate",
          streamTableNode(0),
          term("flatAggregate", "TopN(a, b) AS (f0, f1, f2)")
        ),
        term("select", "Func0$(f0) AS a", "f1 AS b", "f2 AS c")
      )
    util.verifyTable(resultTable, expected)
  }

  @Test
  def testTableAggregateWithTimeIndicator(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, Long)]('a.rowtime, 'b, 'c, 'd.proctime)

    val emptyFunc = new EmptyTableAggFunc
    val resultTable = table
      .flatAggregate(emptyFunc('a, 'd))
      .select('_1 as 'a, '_2 as 'b, '_3 as 'c)

    val expected =
      unaryNode(
        "DataStreamGroupTableAggregate",
        unaryNode(
          "DataStreamCalc",
          streamTableNode(0),
          term("select", "CAST(a) AS a", "PROCTIME(d) AS d")
        ),
        term("flatAggregate", "EmptyTableAggFunc(a, d) AS (_1, _2, _3)")
      )
    util.verifyTable(resultTable, expected)
  }

  @Test
  def testTableAggregateWithoutSelectStar(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Int, Long, String)]('a, 'b, 'c)

    val top3 = new TopN(3)
    val resultTable = table
      .flatAggregate(top3('a, 'b))
      .select("*")

    val expected =
      unaryNode(
        "DataStreamGroupTableAggregate",
        streamTableNode(0),
        term("flatAggregate", "TopN(a, b) AS (f0, f1, f2)")
      )
    util.verifyTable(resultTable, expected)
  }

  @Test
  def testTableAggregateWithAlias(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Int, Long, String)]('a, 'b, 'c)

    val top3 = new TopN(3)
    val resultTable = table
      .flatAggregate(top3('a, 'b) as ('a, 'b, 'c))
      .select('*)

    val expected =
      unaryNode(
        "DataStreamGroupTableAggregate",
        streamTableNode(0),
        term("flatAggregate", "TopN(a, b) AS (a, b, c)")
      )
    util.verifyTable(resultTable, expected)
  }

  @Test
  def testTableAggregateWithDistinct(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Int, Long, String)]('a, 'b, 'c)

    val top3 = new TopN(3)
    val resultTable = table
      .flatAggregate(top3.distinct('a, 'b) as ('a, 'b, 'c))
      .select('*)

    val expected =
      unaryNode(
        "DataStreamGroupTableAggregate",
        unaryNode(
          "DataStreamGroupAggregate",
          unaryNode("DataStreamCalc",
            streamTableNode(0),
            term("select", "a", "b")
          ),
          term("groupBy", "a", "b"),
          term("select", "a", "b")
        ),
        term("flatAggregate", "TopN(a, b) AS (a, b, c)")
      )
    util.verifyTable(resultTable, expected)
  }
}
