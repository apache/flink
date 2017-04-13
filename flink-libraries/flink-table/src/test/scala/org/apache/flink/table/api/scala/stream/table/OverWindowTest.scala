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
package org.apache.flink.table.api.scala.stream.table

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.utils.TableTestUtil._
import org.apache.flink.table.utils.{StreamTableTestUtil, TableTestBase}
import org.junit.Test

class OverWindowTest extends TableTestBase {
  private val streamUtil: StreamTableTestUtil = streamTestUtil()
  val table = streamUtil.addTable[(Int, String, Long)]("MyTable", 'a, 'b, 'c)

  @Test
  def testProcTimeBoundedPartitionedRowsOver() = {
    val result = table
      .window(
        Over partitionBy 'c orderBy 'proctime preceding 2.rows following CURRENT_ROW as 'w)
      .select('c, 'b.count over 'w)

    val expected =
      unaryNode(
        "DataStreamCalc",
        unaryNode(
          "DataStreamOverAggregate",
          unaryNode(
            "DataStreamCalc",
            streamTableNode(0),
            term("select", "b", "c", "PROCTIME() AS $2")
          ),
          term("partitionBy", "c"),
          term("orderBy", "PROCTIME"),
          term("rows", "BETWEEN 2 PRECEDING AND CURRENT ROW"),
          term("select", "b", "c", "PROCTIME", "COUNT(b) AS w0$o0")
        ),
        term("select", "c", "w0$o0 AS _c1")
      )
    streamUtil.verifyTable(result, expected)
  }

  @Test
  def testProcTimeBoundedPartitionedRangeOver() = {
    val result = table
      .window(
        Over partitionBy 'a orderBy 'proctime preceding 2.hours following CURRENT_RANGE as 'w)
      .select('a, 'c.avg over 'w as 'myAvg)

    val expected =
      unaryNode(
        "DataStreamCalc",
        unaryNode(
          "DataStreamOverAggregate",
          unaryNode(
            "DataStreamCalc",
            streamTableNode(0),
            term("select", "a", "c", "PROCTIME() AS $2")
          ),
          term("partitionBy", "a"),
          term("orderBy", "PROCTIME"),
          term("range", "BETWEEN 7200000 PRECEDING AND CURRENT ROW"),
          term(
            "select",
            "a",
            "c",
            "PROCTIME",
            "AVG(c) AS w0$o0"
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
            streamTableNode(0),
            term("select", "a", "c", "PROCTIME() AS $2")
          ),
          term("orderBy", "PROCTIME"),
          term("range", "BETWEEN 10000 PRECEDING AND CURRENT ROW"),
          term("select", "a", "c", "PROCTIME", "COUNT(c) AS w0$o0")
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
            streamTableNode(0),
            term("select", "a", "c", "PROCTIME() AS $2")
          ),
          term("orderBy", "PROCTIME"),
          term("rows", "BETWEEN 2 PRECEDING AND CURRENT ROW"),
          term("select", "a", "c", "PROCTIME", "COUNT(a) AS w0$o0")
        ),
        term("select", "c", "w0$o0 AS _c1")
      )

    streamUtil.verifyTable(result, expected)
  }

  @Test
  def testProcTimeUnboundedPartitionedRangeOver() = {
    val result = table
      .window(Over partitionBy 'c orderBy 'proctime preceding UNBOUNDED_RANGE following
         CURRENT_RANGE as 'w)
      .select('a, 'c, 'a.count over 'w, 'a.sum over 'w)

    val expected =
      unaryNode(
        "DataStreamCalc",
        unaryNode(
          "DataStreamOverAggregate",
          unaryNode(
            "DataStreamCalc",
            streamTableNode(0),
            term("select", "a", "c", "PROCTIME() AS $2")
          ),
          term("partitionBy", "c"),
          term("orderBy", "PROCTIME"),
          term("range", "BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"),
          term(
            "select",
            "a",
            "c",
            "PROCTIME",
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
  def testProcTimeUnboundedPartitionedRowsOver() = {
    val result = table
      .window(
        Over partitionBy 'c orderBy 'proctime preceding UNBOUNDED_ROW following CURRENT_ROW as 'w)
      .select('c, 'a.count over 'w)

    val expected =
      unaryNode(
        "DataStreamCalc",
        unaryNode(
          "DataStreamOverAggregate",
          unaryNode(
            "DataStreamCalc",
            streamTableNode(0),
            term("select", "a", "c", "PROCTIME() AS $2")
          ),
          term("partitionBy", "c"),
          term("orderBy", "PROCTIME"),
          term("rows", "BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"),
          term("select", "a", "c", "PROCTIME", "COUNT(a) AS w0$o0")
        ),
        term("select", "c", "w0$o0 AS _c1")
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
            streamTableNode(0),
            term("select", "a", "c", "PROCTIME() AS $2")
          ),
          term("orderBy", "PROCTIME"),
          term("range", "BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"),
          term(
            "select",
            "a",
            "c",
            "PROCTIME",
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
            streamTableNode(0),
            term("select", "a", "c", "PROCTIME() AS $2")
          ),
          term("orderBy", "PROCTIME"),
          term("rows", "BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"),
          term("select", "a", "c", "PROCTIME", "COUNT(a) AS w0$o0")
        ),
        term("select", "c", "w0$o0 AS _c1")
      )

    streamUtil.verifyTable(result, expected)
  }

  @Test
  def testRowTimeBoundedPartitionedRowsOver() = {
    val result = table
      .window(
        Over partitionBy 'c orderBy 'rowtime preceding 2.rows following CURRENT_ROW as 'w)
      .select('c, 'b.count over 'w)

    val expected =
      unaryNode(
        "DataStreamCalc",
        unaryNode(
          "DataStreamOverAggregate",
          unaryNode(
            "DataStreamCalc",
            streamTableNode(0),
            term("select", "b", "c", "ROWTIME() AS $2")
          ),
          term("partitionBy", "c"),
          term("orderBy", "ROWTIME"),
          term("rows", "BETWEEN 2 PRECEDING AND CURRENT ROW"),
          term("select", "b", "c", "ROWTIME", "COUNT(b) AS w0$o0")
        ),
        term("select", "c", "w0$o0 AS _c1")
      )

    streamUtil.verifyTable(result, expected)
  }

  @Test
  def testRowTimeBoundedPartitionedRangeOver() = {
    val result = table
      .window(
        Over partitionBy 'a orderBy 'rowtime preceding 2.hours following CURRENT_RANGE as 'w)
      .select('a, 'c.avg over 'w)

    val expected =
      unaryNode(
        "DataStreamCalc",
        unaryNode(
          "DataStreamOverAggregate",
          unaryNode(
            "DataStreamCalc",
            streamTableNode(0),
            term("select", "a", "c", "ROWTIME() AS $2")
          ),
          term("partitionBy", "a"),
          term("orderBy", "ROWTIME"),
          term("range", "BETWEEN 7200000 PRECEDING AND CURRENT ROW"),
          term(
            "select",
            "a",
            "c",
            "ROWTIME",
            "AVG(c) AS w0$o0"
          )
        ),
        term("select", "a", "w0$o0 AS _c1")
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
            streamTableNode(0),
            term("select", "a", "c", "ROWTIME() AS $2")
          ),
          term("orderBy", "ROWTIME"),
          term("range", "BETWEEN 10000 PRECEDING AND CURRENT ROW"),
          term("select", "a", "c", "ROWTIME", "COUNT(c) AS w0$o0")
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
            streamTableNode(0),
            term("select", "a", "c", "ROWTIME() AS $2")
          ),
          term("orderBy", "ROWTIME"),
          term("rows", "BETWEEN 2 PRECEDING AND CURRENT ROW"),
          term("select", "a", "c", "ROWTIME", "COUNT(a) AS w0$o0")
        ),
        term("select", "c", "w0$o0 AS _c1")
      )

    streamUtil.verifyTable(result, expected)
  }

  @Test
  def testRowTimeUnboundedPartitionedRangeOver() = {
    val result = table
      .window(Over partitionBy 'c orderBy 'rowtime preceding UNBOUNDED_RANGE following
         CURRENT_RANGE as 'w)
      .select('a, 'c, 'a.count over 'w, 'a.sum over 'w)

    val expected =
      unaryNode(
        "DataStreamCalc",
        unaryNode(
          "DataStreamOverAggregate",
          unaryNode(
            "DataStreamCalc",
            streamTableNode(0),
            term("select", "a", "c", "ROWTIME() AS $2")
          ),
          term("partitionBy", "c"),
          term("orderBy", "ROWTIME"),
          term("range", "BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"),
          term(
            "select",
            "a",
            "c",
            "ROWTIME",
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
  def testRowTimeUnboundedPartitionedRowsOver() = {
    val result = table
      .window(Over partitionBy 'c orderBy 'rowtime preceding UNBOUNDED_ROW following
         CURRENT_ROW as 'w)
      .select('c, 'a.count over 'w)

    val expected =
      unaryNode(
        "DataStreamCalc",
        unaryNode(
          "DataStreamOverAggregate",
          unaryNode(
            "DataStreamCalc",
            streamTableNode(0),
            term("select", "a", "c", "ROWTIME() AS $2")
          ),
          term("partitionBy", "c"),
          term("orderBy", "ROWTIME"),
          term("rows", "BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"),
          term("select", "a", "c", "ROWTIME", "COUNT(a) AS w0$o0")
        ),
        term("select", "c", "w0$o0 AS _c1")
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
            streamTableNode(0),
            term("select", "a", "c", "ROWTIME() AS $2")
          ),
          term("orderBy", "ROWTIME"),
          term("range", "BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"),
          term(
            "select",
            "a",
            "c",
            "ROWTIME",
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
            streamTableNode(0),
            term("select", "a", "c", "ROWTIME() AS $2")
          ),
          term("orderBy", "ROWTIME"),
          term("rows", "BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"),
          term("select", "a", "c", "ROWTIME", "COUNT(a) AS w0$o0")
        ),
        term("select", "c", "w0$o0 AS _c1")
      )

    streamUtil.verifyTable(result, expected)
  }

}
