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
import org.apache.flink.table.api.scala._
import org.apache.flink.table.plan.logical.{SessionGroupWindow, SlidingGroupWindow, TumblingGroupWindow}
import org.apache.flink.table.runtime.utils.JavaUserDefinedAggFunctions.WeightedAvg
import org.apache.flink.table.utils.TableTestUtil._
import org.apache.flink.table.utils.TableTestBase
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
          streamTableNode(0),
          term("groupBy", "b"),
          term("select", "b", "SUM(DISTINCT a) AS TMP_0", "COUNT(DISTINCT c) AS TMP_1")
        ),
        term("select", "TMP_0", "TMP_1")
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
          streamTableNode(0),
          term("groupBy", "c"),
          term(
            "select",
            "c",
            "WeightedAvg(DISTINCT a, b) AS TMP_0",
            "WeightedAvg(a, b) AS TMP_1")
        ),
        term("select", "TMP_0", "TMP_1")
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
            streamTableNode(0),
            term("select", "a", "b")
          ),
          term("groupBy", "b"),
          term("select", "b", "COUNT(a) AS TMP_0")
        ),
        term("select", "TMP_0")
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
            streamTableNode(0),
            term("select", "4 AS four", "b", "a")
          ),
          term("groupBy", "four", "a"),
          term("select", "four", "a", "SUM(b) AS TMP_0")
        ),
        term("select", "4 AS four", "TMP_0")
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
            streamTableNode(0),
            term("select", "4 AS four", "a", "b")
          ),
          term("groupBy", "four", "b"),
          term("select", "four", "b", "SUM(a) AS TMP_0")
        ),
        term("select", "4 AS four", "TMP_0")
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
            streamTableNode(0),
            term("select", "a", "MOD(b, 3) AS d", "c")
          ),
          term("groupBy", "d"),
          term("select", "d", "MIN(c) AS TMP_0", "AVG(a) AS TMP_1")
        ),
        term("select", "TMP_0", "TMP_1")
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
          streamTableNode(0),
          term("select", "b", "a"),
          term("where", "=(b, 2)")
        ),
        term("groupBy", "b"),
        term("select", "b", "SUM(a) AS TMP_0")
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
          streamTableNode(0),
          term("select", "b", "a", "CAST(a) AS a0")
        ),
        term("groupBy", "b"),
        term("select", "b", "AVG(a0) AS TMP_0")
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
        streamTableNode(0),
        term("select", "a", "rowtime")
      ),
      term("window", TumblingGroupWindow('w, 'rowtime, 900000.millis)),
      term("select", "COUNT(DISTINCT a) AS TMP_0", "SUM(a) AS TMP_1")
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
        streamTableNode(0),
        term("select", "a", "rowtime")
      ),
      term("window", SlidingGroupWindow('w, 'rowtime, 3600000.millis, 900000.millis)),
      term("select", "COUNT(DISTINCT a) AS TMP_0", "SUM(DISTINCT a) AS TMP_1",
           "MAX(DISTINCT a) AS TMP_2")
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
        streamTableNode(0),
        term("select", "a", "c", "rowtime")
      ),
      term("groupBy", "a"),
      term("window", SessionGroupWindow('w, 'rowtime, 900000.millis)),
      term("select", "a", "COUNT(a) AS TMP_0", "COUNT(DISTINCT c) AS TMP_1")
    )

    util.verifyTable(result, expected)
  }
}
