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
import org.apache.flink.table.api._
import org.apache.flink.table.expressions.utils.Func1
import org.apache.flink.table.runtime.utils.JavaUserDefinedAggFunctions.WeightedAvgWithRetract
import org.apache.flink.table.utils.TableTestUtil._
import org.apache.flink.table.utils.{StreamTableTestUtil, TableTestBase}

import org.junit.Test

class OverWindowTest extends TableTestBase {
  private val streamUtil: StreamTableTestUtil = streamTestUtil()
  val table: Table = streamUtil.addTable[(Int, String, Long)]("MyTable",
    'a, 'b, 'c, 'proctime.proctime, 'rowtime.rowtime)

  @Test
  def testScalarFunctionsOnOverWindow() = {
    val weightedAvg = new WeightedAvgWithRetract
    val plusOne = Func1

    val result = table
      .window(Over partitionBy 'b orderBy 'proctime preceding UNBOUNDED_ROW as 'w)
      .select(
        plusOne('a.sum over 'w as 'wsum) as 'd,
        ('a.count over 'w).exp(),
        (weightedAvg('c, 'a) over 'w) + 1,
        "AVG:".toExpr + (weightedAvg('c, 'a) over 'w),
        array(weightedAvg('c, 'a) over 'w, 'a.count over 'w))

    val expected =
      unaryNode(
        "DataStreamCalc",
        unaryNode(
          "DataStreamOverAggregate",
          unaryNode(
            "DataStreamCalc",
            streamTableNode(table),
            term("select", "a", "b", "c", "proctime")
          ),
          term("partitionBy", "b"),
          term("orderBy", "proctime"),
          term("rows", "BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"),
          term("select", "a", "b", "c", "proctime",
               "SUM(a) AS w0$o0",
               "COUNT(a) AS w0$o1",
               "WeightedAvgWithRetract(c, a) AS w0$o2")
        ),
        term("select",
             s"Func1$$(w0$$o0) AS d",
             "EXP(w0$o1) AS _c1",
             "+(w0$o2, 1:BIGINT) AS _c2",
             "||('AVG:', CAST(w0$o2)) AS _c3",
             "ARRAY(w0$o2, w0$o1) AS _c4")
      )
    streamUtil.verifyTable(result, expected)
  }

  @Test
  def testProcTimeBoundedPartitionedRowsOver() = {
    val weightedAvg = new WeightedAvgWithRetract

    val result = table
      .window(Over partitionBy 'b orderBy 'proctime preceding 2.rows following CURRENT_ROW as 'w)
      .select('c, weightedAvg('c, 'a) over 'w)

    val expected =
      unaryNode(
        "DataStreamCalc",
        unaryNode(
          "DataStreamOverAggregate",
          unaryNode(
            "DataStreamCalc",
            streamTableNode(table),
            term("select", "a", "b", "c", "proctime")
          ),
          term("partitionBy", "b"),
          term("orderBy", "proctime"),
          term("rows", "BETWEEN 2 PRECEDING AND CURRENT ROW"),
          term("select", "a", "b", "c", "proctime", "WeightedAvgWithRetract(c, a) AS w0$o0")
        ),
        term("select", "c", "w0$o0 AS _c1")
      )
    streamUtil.verifyTable(result, expected)
  }

  @Test
  def testProcTimeBoundedPartitionedRangeOver() = {
    val weightedAvg = new WeightedAvgWithRetract

    val result = table
      .window(
        Over partitionBy 'a orderBy 'proctime preceding 2.hours following CURRENT_RANGE as 'w)
      .select('a, weightedAvg('c, 'a) over 'w as 'myAvg)

    val expected =
      unaryNode(
        "DataStreamCalc",
        unaryNode(
          "DataStreamOverAggregate",
          unaryNode(
            "DataStreamCalc",
            streamTableNode(table),
            term("select", "a", "c", "proctime")
          ),
          term("partitionBy", "a"),
          term("orderBy", "proctime"),
          term("range", "BETWEEN 7200000 PRECEDING AND CURRENT ROW"),
          term(
            "select",
            "a",
            "c",
            "proctime",
            "WeightedAvgWithRetract(c, a) AS w0$o0"
          )
        ),
        term("select", "a", "w0$o0 AS myAvg")
      )

    streamUtil.verifyTable(result, expected)
  }

  @Test
  def testProcTimeBoundedNonPartitionedRangeOver() = {
    val result = table
      .window(Over orderBy 'proctime preceding 10.second as 'w)
      .select('a, 'c.count over 'w)

    val expected =
      unaryNode(
        "DataStreamCalc",
        unaryNode(
          "DataStreamOverAggregate",
          unaryNode(
            "DataStreamCalc",
            streamTableNode(table),
            term("select", "a", "c", "proctime")
          ),
          term("orderBy", "proctime"),
          term("range", "BETWEEN 10000 PRECEDING AND CURRENT ROW"),
          term("select", "a", "c", "proctime", "COUNT(c) AS w0$o0")
        ),
        term("select", "a", "w0$o0 AS _c1")
      )

    streamUtil.verifyTable(result, expected)
  }

  @Test
  def testProcTimeBoundedNonPartitionedRowsOver() = {
    val result = table
      .window(Over orderBy 'proctime preceding 2.rows as 'w)
      .select('c, 'a.count over 'w)

    val expected =
      unaryNode(
        "DataStreamCalc",
        unaryNode(
          "DataStreamOverAggregate",
          unaryNode(
            "DataStreamCalc",
            streamTableNode(table),
            term("select", "a", "c", "proctime")
          ),
          term("orderBy", "proctime"),
          term("rows", "BETWEEN 2 PRECEDING AND CURRENT ROW"),
          term("select", "a", "c", "proctime", "COUNT(a) AS w0$o0")
        ),
        term("select", "c", "w0$o0 AS _c1")
      )

    streamUtil.verifyTable(result, expected)
  }

  @Test
  def testProcTimeUnboundedPartitionedRangeOver() = {
    val weightedAvg = new WeightedAvgWithRetract

    val result = table
      .window(Over partitionBy 'c orderBy 'proctime preceding UNBOUNDED_RANGE as 'w)
      .select('a, 'c, 'a.count over 'w, weightedAvg('c, 'a) over 'w)

    val result2 = table
      .window(Over partitionBy 'c orderBy 'proctime as 'w)
      .select('a, 'c, 'a.count over 'w, weightedAvg('c, 'a) over 'w)

    streamUtil.verify2Tables(result, result2)

    val expected =
      unaryNode(
        "DataStreamCalc",
        unaryNode(
          "DataStreamOverAggregate",
          unaryNode(
            "DataStreamCalc",
            streamTableNode(table),
            term("select", "a", "c", "proctime")
          ),
          term("partitionBy", "c"),
          term("orderBy", "proctime"),
          term("range", "BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"),
          term(
            "select",
            "a",
            "c",
            "proctime",
            "COUNT(a) AS w0$o0",
            "WeightedAvgWithRetract(c, a) AS w0$o1"
          )
        ),
        term(
          "select",
          "a",
          "c",
          "w0$o0 AS _c2",
          "w0$o1 AS _c3"
        )
      )
    streamUtil.verifyTable(result, expected)
  }

  @Test
  def testProcTimeUnboundedPartitionedRowsOver() = {
    val weightedAvg = new WeightedAvgWithRetract

    val result = table
      .window(
        Over partitionBy 'c orderBy 'proctime preceding UNBOUNDED_ROW following CURRENT_ROW as 'w)
      .select('c, 'a.count over 'w, weightedAvg('c, 'a) over 'w)

    val expected =
      unaryNode(
        "DataStreamCalc",
        unaryNode(
          "DataStreamOverAggregate",
          unaryNode(
            "DataStreamCalc",
            streamTableNode(table),
            term("select", "a", "c", "proctime")
          ),
          term("partitionBy", "c"),
          term("orderBy", "proctime"),
          term("rows", "BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"),
          term("select", "a", "c", "proctime",
               "COUNT(a) AS w0$o0",
               "WeightedAvgWithRetract(c, a) AS w0$o1")
        ),
        term("select", "c", "w0$o0 AS _c1", "w0$o1 AS _c2")
      )

    streamUtil.verifyTable(result, expected)
  }

  @Test
  def testProcTimeUnboundedNonPartitionedRangeOver() = {
    val result = table
      .window(
        Over orderBy 'proctime preceding UNBOUNDED_RANGE as 'w)
      .select('a, 'c, 'a.count over 'w, 'a.sum over 'w)

    val expected =
      unaryNode(
        "DataStreamCalc",
        unaryNode(
          "DataStreamOverAggregate",
          unaryNode(
            "DataStreamCalc",
            streamTableNode(table),
            term("select", "a", "c", "proctime")
          ),
          term("orderBy", "proctime"),
          term("range", "BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"),
          term(
            "select",
            "a",
            "c",
            "proctime",
            "COUNT(a) AS w0$o0",
            "SUM(a) AS w0$o1"
          )
        ),
        term(
          "select",
          "a",
          "c",
          "w0$o0 AS _c2",
          "w0$o1 AS _c3"
        )
      )

    streamUtil.verifyTable(result, expected)
  }

  @Test
  def testProcTimeUnboundedNonPartitionedRowsOver() = {
    val result = table
      .window(Over orderBy 'proctime preceding UNBOUNDED_ROW as 'w)
      .select('c, 'a.count over 'w)

    val expected =
      unaryNode(
        "DataStreamCalc",
        unaryNode(
          "DataStreamOverAggregate",
          unaryNode(
            "DataStreamCalc",
            streamTableNode(table),
            term("select", "a", "c", "proctime")
          ),
          term("orderBy", "proctime"),
          term("rows", "BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"),
          term("select", "a", "c", "proctime", "COUNT(a) AS w0$o0")
        ),
        term("select", "c", "w0$o0 AS _c1")
      )

    streamUtil.verifyTable(result, expected)
  }

  @Test
  def testRowTimeBoundedPartitionedRowsOver() = {
    val weightedAvg = new WeightedAvgWithRetract

    val result = table
      .window(
        Over partitionBy 'b orderBy 'rowtime preceding 2.rows following CURRENT_ROW as 'w)
      .select('c, 'b.count over 'w, weightedAvg('c, 'a) over 'w as 'wAvg)

    val expected =
      unaryNode(
        "DataStreamCalc",
        unaryNode(
          "DataStreamOverAggregate",
          unaryNode(
            "DataStreamCalc",
            streamTableNode(table),
            term("select", "a", "b", "c", "rowtime")
          ),
          term("partitionBy", "b"),
          term("orderBy", "rowtime"),
          term("rows", "BETWEEN 2 PRECEDING AND CURRENT ROW"),
          term("select", "a", "b", "c", "rowtime",
               "COUNT(b) AS w0$o0",
               "WeightedAvgWithRetract(c, a) AS w0$o1")
        ),
        term("select", "c", "w0$o0 AS _c1", "w0$o1 AS wAvg")
      )

    streamUtil.verifyTable(result, expected)
  }

  @Test
  def testRowTimeBoundedPartitionedRangeOver() = {
    val weightedAvg = new WeightedAvgWithRetract

    val result = table
      .window(
        Over partitionBy 'a orderBy 'rowtime preceding 2.hours following CURRENT_RANGE as 'w)
      .select('a, 'c.avg over 'w, weightedAvg('c, 'a) over 'w as 'wAvg)

    val expected =
      unaryNode(
        "DataStreamCalc",
        unaryNode(
          "DataStreamOverAggregate",
          unaryNode(
            "DataStreamCalc",
            streamTableNode(table),
            term("select", "a", "c", "rowtime")
          ),
          term("partitionBy", "a"),
          term("orderBy", "rowtime"),
          term("range", "BETWEEN 7200000 PRECEDING AND CURRENT ROW"),
          term(
            "select",
            "a",
            "c",
            "rowtime",
            "AVG(c) AS w0$o0",
            "WeightedAvgWithRetract(c, a) AS w0$o1"
          )
        ),
        term("select", "a", "w0$o0 AS _c1", "w0$o1 AS wAvg")
      )

    streamUtil.verifyTable(result, expected)
  }

  @Test
  def testRowTimeBoundedNonPartitionedRangeOver() = {
    val result = table
      .window(Over orderBy 'rowtime preceding 10.second as 'w)
      .select('a, 'c.count over 'w)

    val expected =
      unaryNode(
        "DataStreamCalc",
        unaryNode(
          "DataStreamOverAggregate",
          unaryNode(
            "DataStreamCalc",
            streamTableNode(table),
            term("select", "a", "c", "rowtime")
          ),
          term("orderBy", "rowtime"),
          term("range", "BETWEEN 10000 PRECEDING AND CURRENT ROW"),
          term("select", "a", "c", "rowtime", "COUNT(c) AS w0$o0")
        ),
        term("select", "a", "w0$o0 AS _c1")
      )

    streamUtil.verifyTable(result, expected)
  }

  @Test
  def testRowTimeBoundedNonPartitionedRowsOver() = {
    val result = table
      .window(Over orderBy 'rowtime preceding 2.rows as 'w)
      .select('c, 'a.count over 'w)

    val expected =
      unaryNode(
        "DataStreamCalc",
        unaryNode(
          "DataStreamOverAggregate",
          unaryNode(
            "DataStreamCalc",
            streamTableNode(table),
            term("select", "a", "c", "rowtime")
          ),
          term("orderBy", "rowtime"),
          term("rows", "BETWEEN 2 PRECEDING AND CURRENT ROW"),
          term("select", "a", "c", "rowtime", "COUNT(a) AS w0$o0")
        ),
        term("select", "c", "w0$o0 AS _c1")
      )

    streamUtil.verifyTable(result, expected)
  }

  @Test
  def testRowTimeUnboundedPartitionedRangeOver() = {
    val weightedAvg = new WeightedAvgWithRetract

    val result = table
      .window(Over partitionBy 'c orderBy 'rowtime preceding UNBOUNDED_RANGE following
         CURRENT_RANGE as 'w)
      .select('a, 'c, 'a.count over 'w, weightedAvg('c, 'a) over 'w as 'wAvg)

    val result2 = table
      .window(Over partitionBy 'c orderBy 'rowtime as 'w)
      .select('a, 'c, 'a.count over 'w, weightedAvg('c, 'a) over 'w as 'wAvg)

    streamUtil.verify2Tables(result, result2)

    val expected =
      unaryNode(
        "DataStreamCalc",
        unaryNode(
          "DataStreamOverAggregate",
          unaryNode(
            "DataStreamCalc",
            streamTableNode(table),
            term("select", "a", "c", "rowtime")
          ),
          term("partitionBy", "c"),
          term("orderBy", "rowtime"),
          term("range", "BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"),
          term(
            "select",
            "a",
            "c",
            "rowtime",
            "COUNT(a) AS w0$o0",
            "WeightedAvgWithRetract(c, a) AS w0$o1"
          )
        ),
        term(
          "select",
          "a",
          "c",
          "w0$o0 AS _c2",
          "w0$o1 AS wAvg"
        )
      )

    streamUtil.verifyTable(result, expected)
  }

  @Test
  def testRowTimeUnboundedPartitionedRowsOver() = {
    val weightedAvg = new WeightedAvgWithRetract

    val result = table
      .window(Over partitionBy 'c orderBy 'rowtime preceding UNBOUNDED_ROW following
         CURRENT_ROW as 'w)
      .select('c, 'a.count over 'w, weightedAvg('c, 'a) over 'w as 'wAvg)

    val expected =
      unaryNode(
        "DataStreamCalc",
        unaryNode(
          "DataStreamOverAggregate",
          unaryNode(
            "DataStreamCalc",
            streamTableNode(table),
            term("select", "a", "c", "rowtime")
          ),
          term("partitionBy", "c"),
          term("orderBy", "rowtime"),
          term("rows", "BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"),
          term("select", "a", "c", "rowtime",
               "COUNT(a) AS w0$o0",
               "WeightedAvgWithRetract(c, a) AS w0$o1")
        ),
        term("select", "c", "w0$o0 AS _c1", "w0$o1 AS wAvg")
      )

    streamUtil.verifyTable(result, expected)
  }

  @Test
  def testRowTimeUnboundedNonPartitionedRangeOver() = {
    val result = table
      .window(
        Over orderBy 'rowtime preceding UNBOUNDED_RANGE as 'w)
      .select('a, 'c, 'a.count over 'w, 'a.sum over 'w)

    val expected =
      unaryNode(
        "DataStreamCalc",
        unaryNode(
          "DataStreamOverAggregate",
          unaryNode(
            "DataStreamCalc",
            streamTableNode(table),
            term("select", "a", "c", "rowtime")
          ),
          term("orderBy", "rowtime"),
          term("range", "BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"),
          term(
            "select",
            "a",
            "c",
            "rowtime",
            "COUNT(a) AS w0$o0",
            "SUM(a) AS w0$o1"
          )
        ),
        term(
          "select",
          "a",
          "c",
          "w0$o0 AS _c2",
          "w0$o1 AS _c3"
        )
      )

    streamUtil.verifyTable(result, expected)
  }

  @Test
  def testRowTimeUnboundedNonPartitionedRowsOver() = {
    val result = table
      .window(Over orderBy 'rowtime preceding UNBOUNDED_ROW as 'w)
      .select('c, 'a.count over 'w)

    val expected =
      unaryNode(
        "DataStreamCalc",
        unaryNode(
          "DataStreamOverAggregate",
          unaryNode(
            "DataStreamCalc",
            streamTableNode(table),
            term("select", "a", "c", "rowtime")
          ),
          term("orderBy", "rowtime"),
          term("rows", "BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"),
          term("select", "a", "c", "rowtime", "COUNT(a) AS w0$o0")
        ),
        term("select", "c", "w0$o0 AS _c1")
      )

    streamUtil.verifyTable(result, expected)
  }
}


