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

import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.api.scala._
import org.apache.flink.table.api.{Session, Slide, Tumble}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.runtime.utils.JavaUserDefinedAggFunctions.WeightedAvg
import org.apache.flink.table.utils.{CountMinMax, TableTestBase}
import org.apache.flink.table.utils.TableTestUtil._
import org.junit.Test

class AggregateTest extends TableTestBase {

  @Test
  def testGroupDistinctAggregate(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('a, 'b, 'c)

    val resultTable = table
      .groupBy('b)
      .select('a.sum.distinct, 'c.count.distinct)

    val expected =
      unaryNode(
        "DataStreamCalc",
        unaryNode(
          "DataStreamGroupAggregate",
          streamTableNode(table),
          term("groupBy", "b"),
          term("select", "b", "SUM(DISTINCT a) AS EXPR$0", "COUNT(DISTINCT c) AS EXPR$1")
        ),
        term("select", "EXPR$0", "EXPR$1")
      )
    util.verifyTable(resultTable, expected)
  }

  @Test
  def testGroupDistinctAggregateWithUDAGG(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('a, 'b, 'c)
    val weightedAvg = new WeightedAvg

    val resultTable = table
      .groupBy('c)
      .select(weightedAvg.distinct('a, 'b), weightedAvg('a, 'b))

    val expected =
      unaryNode(
        "DataStreamCalc",
        unaryNode(
          "DataStreamGroupAggregate",
          streamTableNode(table),
          term("groupBy", "c"),
          term(
            "select",
            "c",
            "WeightedAvg(DISTINCT a, b) AS EXPR$0",
            "WeightedAvg(a, b) AS EXPR$1")
        ),
        term("select", "EXPR$0", "EXPR$1")
      )
    util.verifyTable(resultTable, expected)
  }

  @Test
  def testGroupAggregate() = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('a, 'b, 'c)

    val resultTable = table
      .groupBy('b)
      .select('a.count)

    val expected =
      unaryNode(
        "DataStreamCalc",
        unaryNode(
          "DataStreamGroupAggregate",
          unaryNode(
            "DataStreamCalc",
            streamTableNode(table),
            term("select", "a", "b")
          ),
          term("groupBy", "b"),
          term("select", "b", "COUNT(a) AS EXPR$0")
        ),
        term("select", "EXPR$0")
      )
    util.verifyTable(resultTable, expected)
  }

  @Test
  def testGroupAggregateWithConstant1(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('a, 'b, 'c)

    val resultTable = table
      .select('a, 4 as 'four, 'b)
      .groupBy('four, 'a)
      .select('four, 'b.sum)

    val expected =
      unaryNode(
        "DataStreamCalc",
        unaryNode(
          "DataStreamGroupAggregate",
          unaryNode(
            "DataStreamCalc",
            streamTableNode(table),
            term("select", "a", "4 AS four", "b")
          ),
          term("groupBy", "a", "four"),
          term("select", "a", "four", "SUM(b) AS EXPR$0")
        ),
        term("select", "4 AS four", "EXPR$0")
      )
    util.verifyTable(resultTable, expected)
  }

  @Test
  def testGroupAggregateWithConstant2(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('a, 'b, 'c)

    val resultTable = table
      .select('b, 4 as 'four, 'a)
      .groupBy('b, 'four)
      .select('four, 'a.sum)

    val expected =
      unaryNode(
        "DataStreamCalc",
        unaryNode(
          "DataStreamGroupAggregate",
          unaryNode(
            "DataStreamCalc",
            streamTableNode(table),
            term("select", "b", "4 AS four", "a")
          ),
          term("groupBy", "b", "four"),
          term("select", "b", "four", "SUM(a) AS EXPR$0")
        ),
        term("select", "4 AS four", "EXPR$0")
      )
    util.verifyTable(resultTable, expected)
  }

  @Test
  def testGroupAggregateWithExpressionInSelect(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('a, 'b, 'c)

    val resultTable = table
      .select('a as 'a, 'b % 3 as 'd, 'c as 'c)
      .groupBy('d)
      .select('c.min, 'a.avg)

    val expected =
      unaryNode(
        "DataStreamCalc",
        unaryNode(
          "DataStreamGroupAggregate",
          unaryNode(
            "DataStreamCalc",
            streamTableNode(table),
            term("select", "a", "MOD(b, 3) AS d", "c")
          ),
          term("groupBy", "d"),
          term("select", "d", "MIN(c) AS EXPR$0", "AVG(a) AS EXPR$1")
        ),
        term("select", "EXPR$0", "EXPR$1")
      )
    util.verifyTable(resultTable, expected)
  }

  @Test
  def testGroupAggregateWithFilter(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('a, 'b, 'c)

    val resultTable = table
      .groupBy('b)
      .select('b, 'a.sum)
      .where('b === 2)

    val expected =
      unaryNode(
        "DataStreamGroupAggregate",
        unaryNode(
          "DataStreamCalc",
          streamTableNode(table),
          term("select", "a", "b"),
          term("where", "=(b, 2)")
        ),
        term("groupBy", "b"),
        term("select", "b", "SUM(a) AS EXPR$0")
      )
    util.verifyTable(resultTable, expected)
  }

  @Test
  def testGroupAggregateWithAverage(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('a, 'b, 'c)

    val resultTable = table
      .groupBy('b)
      .select('b, 'a.cast(BasicTypeInfo.DOUBLE_TYPE_INFO).avg)

    val expected =
      unaryNode(
        "DataStreamGroupAggregate",
        unaryNode(
          "DataStreamCalc",
          streamTableNode(table),
          term("select", "b", "CAST(a) AS a0")
        ),
        term("groupBy", "b"),
        term("select", "b", "AVG(a0) AS EXPR$0")
      )

    util.verifyTable(resultTable, expected)
  }

  @Test
  def testDistinctAggregateOnTumbleWindow(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Int, Long, String)](
      "MyTable", 'a, 'b, 'c, 'rowtime.rowtime)
    val result = table
      .window(Tumble over 15.minute on 'rowtime as 'w)
      .groupBy('w)
      .select('a.count.distinct, 'a.sum)

    val expected = unaryNode(
      "DataStreamGroupWindowAggregate",
      unaryNode(
        "DataStreamCalc",
        streamTableNode(table),
        term("select", "a", "rowtime")
      ),
      term("window", "TumblingGroupWindow('w, 'rowtime, 900000.millis)"),
      term("select", "COUNT(DISTINCT a) AS EXPR$0", "SUM(a) AS EXPR$1")
    )

    util.verifyTable(result, expected)
  }

  @Test
  def testMultiDistinctAggregateSameFieldOnHopWindow(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Int, Long, String)](
      "MyTable", 'a, 'b, 'c, 'rowtime.rowtime)
    val result = table
      .window(Slide over 1.hour every 15.minute on 'rowtime as 'w)
      .groupBy('w)
      .select('a.count.distinct, 'a.sum.distinct, 'a.max.distinct)

    val expected = unaryNode(
      "DataStreamGroupWindowAggregate",
      unaryNode(
        "DataStreamCalc",
        streamTableNode(table),
        term("select", "a", "rowtime")
      ),
      term("window", "SlidingGroupWindow('w, 'rowtime, 3600000.millis, 900000.millis)"),
      term("select", "COUNT(DISTINCT a) AS EXPR$0", "SUM(DISTINCT a) AS EXPR$1",
           "MAX(DISTINCT a) AS EXPR$2")
    )

    util.verifyTable(result, expected)
  }

  @Test
  def testDistinctAggregateWithGroupingOnSessionWindow(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Int, Long, String)](
      "MyTable", 'a, 'b, 'c, 'rowtime.rowtime)
    val result = table
      .window(Session withGap 15.minute on 'rowtime as 'w)
      .groupBy('a, 'w)
      .select('a, 'a.count, 'c.count.distinct)

    val expected = unaryNode(
      "DataStreamGroupWindowAggregate",
      unaryNode(
        "DataStreamCalc",
        streamTableNode(table),
        term("select", "a", "c", "rowtime")
      ),
      term("groupBy", "a"),
      term("window", "SessionGroupWindow('w, 'rowtime, 900000.millis)"),
      term("select", "a", "COUNT(a) AS EXPR$0", "COUNT(DISTINCT c) AS EXPR$1")
    )

    util.verifyTable(result, expected)
  }

  @Test
  def testSimpleAggregate(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Int, Long, String)](
      "MyTable", 'a, 'b, 'c)

    val testAgg = new CountMinMax
    val resultTable = table
      .groupBy('b)
      .aggregate(testAgg('a))
      .select('b, 'f0, 'f1)

    val expected =
      unaryNode(
        "DataStreamCalc",
        unaryNode(
          "DataStreamGroupAggregate",
          unaryNode(
            "DataStreamCalc",
            streamTableNode(table),
            term("select", "a", "b")
          ),
          term("groupBy", "b"),
          term("select", "b", "CountMinMax(a) AS TMP_0")
        ),
        term("select", "b", "TMP_0.f0 AS f0", "TMP_0.f1 AS f1")
      )
    util.verifyTable(resultTable, expected)
  }

  @Test
  def testSelectStarAndGroupByCall(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Int, Long, String)](
      "MyTable", 'a, 'b, 'c)

    val testAgg = new CountMinMax
    val resultTable = table
      .groupBy('b % 5)
      .aggregate(testAgg('a))
      .select('*)

    val expected =
      unaryNode(
        "DataStreamCalc",
        unaryNode(
          "DataStreamGroupAggregate",
          unaryNode(
            "DataStreamCalc",
            streamTableNode(table),
            term("select", "a", "MOD(b, 5) AS TMP_0")
          ),
          term("groupBy", "TMP_0"),
          term("select", "TMP_0", "CountMinMax(a) AS TMP_1")
        ),
        term("select", "TMP_0", "TMP_1.f0 AS f0", "TMP_1.f1 AS f1", "TMP_1.f2 AS f2")
      )
    util.verifyTable(resultTable, expected)
  }

  @Test
  def testAggregateWithScalarResult(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Int, Long, String)](
      "MyTable", 'a, 'b, 'c)

    val resultTable = table
      .groupBy('b)
      .aggregate('a.count)
      .select('b, 'TMP_0)

    val expected =
      unaryNode(
        "DataStreamGroupAggregate",
        unaryNode(
          "DataStreamCalc",
          streamTableNode(table),
          term("select", "a", "b")
        ),
        term("groupBy", "b"),
        term("select", "b", "COUNT(a) AS TMP_0")
      )
    util.verifyTable(resultTable, expected)
  }

  @Test
  def testAggregateWithAlias(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Int, Long, String)](
      "MyTable", 'a, 'b, 'c)

    val testAgg = new CountMinMax
    val resultTable = table
      .groupBy('b)
      .aggregate(testAgg('a) as ('x, 'y, 'z))
      .select('b, 'x, 'y)

    val expected =
      unaryNode(
        "DataStreamCalc",
        unaryNode(
          "DataStreamGroupAggregate",
          unaryNode(
            "DataStreamCalc",
            streamTableNode(table),
            term("select", "a", "b")
          ),
          term("groupBy", "b"),
          term("select", "b", "CountMinMax(a) AS TMP_0")
        ),
        term("select", "b", "TMP_0.f0 AS x", "TMP_0.f1 AS y")
      )
    util.verifyTable(resultTable, expected)
  }

  @Test
  def testAggregateOnWindowedTable(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Int, Long, String)](
      "MyTable", 'a, 'b, 'c, 'rowtime.rowtime)
    val testAgg = new CountMinMax

    val result = table
      .window(Tumble over 15.minute on 'rowtime as 'w)
      .groupBy('w, 'b % 3)
      .aggregate(testAgg('a) as ('x, 'y, 'z))
      .select('w.start, 'x, 'y)

    val expected =
      unaryNode(
        "DataStreamCalc",
        unaryNode(
          "DataStreamGroupWindowAggregate",
          unaryNode(
            "DataStreamCalc",
            streamTableNode(table),
            term("select", "a", "rowtime", "MOD(b, 3) AS TMP_0")
          ),
          term("groupBy", "TMP_0"),
          term("window", "TumblingGroupWindow('w, 'rowtime, 900000.millis)"),
          term("select", "TMP_0", "CountMinMax(a) AS TMP_1", "start('w) AS EXPR$0")
        ),
        term("select", "EXPR$0", "TMP_1.f0 AS x", "TMP_1.f1 AS y")
      )

    util.verifyTable(result, expected)
  }
}
