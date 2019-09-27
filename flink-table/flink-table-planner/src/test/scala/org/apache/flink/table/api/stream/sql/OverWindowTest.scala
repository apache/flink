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
import org.apache.flink.table.utils.{StreamTableTestUtil, TableTestBase}
import org.junit.Test

class OverWindowTest extends TableTestBase {
  private val streamUtil: StreamTableTestUtil = streamTestUtil()
  private val table = streamUtil.addTable[(Int, String, Long)](
    "MyTable",
    'a, 'b, 'c,
    'proctime.proctime, 'rowtime.rowtime)

  @Test
  def testProctimeBoundedDistinctWithNonDistinctPartitionedRowOver() = {
    val sql = "SELECT " +
      "b, " +
      "count(a) OVER (PARTITION BY b ORDER BY proctime ROWS BETWEEN 2 preceding AND " +
      "CURRENT ROW) as cnt1, " +
      "sum(a) OVER (PARTITION BY b ORDER BY proctime ROWS BETWEEN 2 preceding AND " +
      "CURRENT ROW) as sum1, " +
      "count(DISTINCT a) OVER (PARTITION BY b ORDER BY proctime ROWS BETWEEN 2 preceding AND " +
      "CURRENT ROW) as cnt2, " +
      "sum(DISTINCT c) OVER (PARTITION BY b ORDER BY proctime ROWS BETWEEN 2 preceding AND " +
      "CURRENT ROW) as sum2 " +
      "from MyTable"

    val sql2 = "SELECT " +
      "b, " +
      "count(a) OVER w as cnt1, " +
      "sum(a) OVER w as sum1, " +
      "count(DISTINCT a) OVER w as cnt2, " +
      "sum(DISTINCT c) OVER w as sum2 " +
      "from MyTable " +
      "WINDOW w AS (PARTITION BY b ORDER BY proctime ROWS BETWEEN 2 preceding AND CURRENT ROW)"
    streamUtil.verifySqlPlansIdentical(sql, sql2)
    
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
          term("select", "a", "b", "c", "proctime", "COUNT(a) AS w0$o0, $SUM0(a) AS w0$o1, " +
            "COUNT(DISTINCT a) AS w0$o2, COUNT(DISTINCT c) AS w0$o3, $SUM0(DISTINCT c) AS w0$o4")
        ),
        term("select", "b", "w0$o0 AS cnt1, CASE(>(w0$o0, 0:BIGINT), w0$o1, " +
          "null:INTEGER) AS sum1, w0$o2 AS cnt2, CASE(>(w0$o3, 0:BIGINT), w0$o4, " +
          "null:BIGINT) AS sum2")
      )
    streamUtil.verifySql(sql, expected)
  }

  @Test
  def testProctimeBoundedDistinctPartitionedRowOver() = {
    val sql = "SELECT " +
      "c, " +
      "count(DISTINCT a) OVER (PARTITION BY c ORDER BY proctime ROWS BETWEEN 2 preceding AND " +
      "CURRENT ROW) as cnt1, " +
      "sum(DISTINCT a) OVER (PARTITION BY c ORDER BY proctime ROWS BETWEEN 2 preceding AND " +
      "CURRENT ROW) as sum1 " +
      "from MyTable"

    val sql2 = "SELECT " +
      "c, " +
      "count(DISTINCT a) OVER w as cnt1, " +
      "sum(DISTINCT a) OVER (PARTITION BY c ORDER BY proctime ROWS BETWEEN 2 preceding AND " +
      "CURRENT ROW) as sum1 " +
      "FROM MyTable " +
      "WINDOW w AS (PARTITION BY c ORDER BY proctime ROWS BETWEEN 2 preceding AND CURRENT ROW)"
    streamUtil.verifySqlPlansIdentical(sql, sql2)

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
          term("rows", "BETWEEN 2 PRECEDING AND CURRENT ROW"),
          term("select", "a", "c", "proctime",
            "COUNT(DISTINCT a) AS w0$o0, $SUM0(DISTINCT a) AS w0$o1")
        ),
        term("select", "c", "w0$o0 AS cnt1, CASE(>(w0$o0, 0:BIGINT), w0$o1, null:INTEGER) AS sum1")
      )
    streamUtil.verifySql(sql, expected)
  }

  @Test
  def testProcTimeBoundedPartitionedRowsOver() = {
    val sql = "SELECT " +
      "c, " +
      "count(a) OVER (PARTITION BY c ORDER BY proctime ROWS BETWEEN 2 preceding AND " +
      "CURRENT ROW) as cnt1, " +
      "sum(a) OVER (PARTITION BY c ORDER BY proctime ROWS BETWEEN 2 preceding AND " +
      "CURRENT ROW) as sum1 " +
      "from MyTable"

    val sql2 = "SELECT " +
      "c, " +
      "count(a) OVER (PARTITION BY c ORDER BY proctime ROWS BETWEEN 2 preceding AND " +
      "CURRENT ROW) as cnt1, " +
      "sum(a) OVER w as sum1 " +
      "from MyTable " +
      "WINDOW w AS (PARTITION BY c ORDER BY proctime ROWS BETWEEN 2 preceding AND CURRENT ROW)"
    streamUtil.verifySqlPlansIdentical(sql, sql2)
    
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
          term("rows", "BETWEEN 2 PRECEDING AND CURRENT ROW"),
          term("select", "a", "c", "proctime", "COUNT(a) AS w0$o0, $SUM0(a) AS w0$o1")
        ),
        term("select", "c", "w0$o0 AS cnt1, CASE(>(w0$o0, 0:BIGINT), w0$o1, null:INTEGER) AS sum1")
      )
    streamUtil.verifySql(sql, expected)
  }

  @Test
  def testProcTimeBoundedPartitionedRangeOver() = {

    val sqlQuery =
      "SELECT a, " +
        "  AVG(c) OVER (PARTITION BY a ORDER BY proctime " +
        "    RANGE BETWEEN INTERVAL '2' HOUR PRECEDING AND CURRENT ROW) AS avgA " +
        "FROM MyTable"

    val sqlQuery2 =
      "SELECT a, " +
        "  AVG(c) OVER w AS avgA " +
        "FROM MyTable " +
        "WINDOW w AS (PARTITION BY a ORDER BY proctime " +
        "  RANGE BETWEEN INTERVAL '2' HOUR PRECEDING AND CURRENT ROW)"
    streamUtil.verifySqlPlansIdentical(sqlQuery, sqlQuery2)

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
            "COUNT(c) AS w0$o0",
            "$SUM0(c) AS w0$o1"
          )
        ),
        term("select", "a", "/(CASE(>(w0$o0, 0:BIGINT)", "w0$o1, null:BIGINT), w0$o0) AS avgA")
      )

    streamUtil.verifySql(sqlQuery, expected)
  }

  @Test
  def testProcTimeBoundedNonPartitionedRangeOver() = {

    val sqlQuery =
      "SELECT a, " +
        "  COUNT(c) OVER (ORDER BY proctime " +
        "    RANGE BETWEEN INTERVAL '10' SECOND PRECEDING AND CURRENT ROW) " +
        "FROM MyTable"

    val sqlQuery2 =
      "SELECT a, " +
        "  COUNT(c) OVER w " +
        "FROM MyTable " +
        "WINDOW w AS (ORDER BY proctime " +
        "  RANGE BETWEEN INTERVAL '10' SECOND PRECEDING AND CURRENT ROW)"
    streamUtil.verifySqlPlansIdentical(sqlQuery, sqlQuery2)

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
        term("select", "a", "w0$o0 AS $1")
      )

    streamUtil.verifySql(sqlQuery, expected)
  }

  @Test
  def testProcTimeBoundedNonPartitionedRowsOver() = {
    val sql = "SELECT " +
      "c, " +
      "count(a) OVER (ORDER BY proctime ROWS BETWEEN 2 preceding AND " +
      "CURRENT ROW)" +
      "from MyTable"

    val sql2 = "SELECT " +
      "c, " +
      "count(a) OVER w " +
      "FROM MyTable " +
      "WINDOW w AS (ORDER BY proctime ROWS BETWEEN 2 preceding AND CURRENT ROW)"
    streamUtil.verifySqlPlansIdentical(sql, sql2)

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
        term("select", "c", "w0$o0 AS $1")
      )
    streamUtil.verifySql(sql, expected)
  }


  @Test
  def testProcTimeUnboundedPartitionedRangeOver() = {
    val sql = "SELECT " +
      "c, " +
      "count(a) OVER (PARTITION BY c ORDER BY proctime RANGE UNBOUNDED preceding) as cnt1, " +
      "sum(a) OVER (PARTITION BY c ORDER BY proctime RANGE UNBOUNDED preceding) as cnt2 " +
      "from MyTable"

    val sql2 = "SELECT " +
      "c, " +
      "count(a) OVER w as cnt1, " +
      "sum(a) OVER (PARTITION BY c ORDER BY proctime RANGE UNBOUNDED preceding) as cnt2 " +
      "FROM MyTable " +
      "WINDOW w AS (PARTITION BY c ORDER BY proctime RANGE UNBOUNDED preceding)"

    val sql3 = "SELECT " +
      "c, " +
      "count(a) OVER (PARTITION BY c ORDER BY proctime) as cnt1, " +
      "sum(a) OVER (PARTITION BY c ORDER BY proctime) as cnt2 " +
      "from MyTable"

    streamUtil.verifySqlPlansIdentical(sql, sql2)
    streamUtil.verifySqlPlansIdentical(sql, sql3)

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
          term("select", "a", "c", "proctime", "COUNT(a) AS w0$o0", "$SUM0(a) AS w0$o1")
        ),
        term("select", "c", "w0$o0 AS cnt1", "CASE(>(w0$o0, 0:BIGINT)",
          "w0$o1, null:INTEGER) AS cnt2")
      )
    streamUtil.verifySql(sql, expected)
  }

  @Test
  def testProcTimeUnboundedPartitionedRowsOver() = {
    val sql = "SELECT " +
      "c, " +
      "count(a) OVER (PARTITION BY c ORDER BY proctime ROWS BETWEEN UNBOUNDED preceding AND " +
      "CURRENT ROW) " +
      "from MyTable"

    val sql2 = "SELECT " +
      "c, " +
      "count(a) OVER w " +
      "FROM MyTable " +
      "WINDOW w AS (PARTITION BY c ORDER BY proctime ROWS UNBOUNDED preceding)"
    streamUtil.verifySqlPlansIdentical(sql, sql2)

    val expected =
      unaryNode(
        "DataStreamCalc",
        unaryNode(
          "DataStreamOverAggregate",
          streamTableNode(table),
          term("partitionBy", "c"),
          term("orderBy", "proctime"),
          term("rows", "BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"),
          term("select", "a", "b", "c", "proctime", "rowtime", "COUNT(a) AS w0$o0")
        ),
        term("select", "c", "w0$o0 AS $1")
      )
    streamUtil.verifySql(sql, expected)
  }

  @Test
  def testProcTimeUnboundedNonPartitionedRangeOver() = {
    val sql = "SELECT " +
      "c, " +
      "count(a) OVER (ORDER BY proctime RANGE UNBOUNDED preceding) as cnt1, " +
      "sum(a) OVER (ORDER BY proctime RANGE UNBOUNDED preceding) as cnt2 " +
      "from MyTable"

    val sql2 = "SELECT " +
      "c, " +
      "count(a) OVER (ORDER BY proctime RANGE UNBOUNDED preceding) as cnt1, " +
      "sum(a) OVER w as cnt2 " +
      "FROM MyTable " +
      "WINDOW w AS(ORDER BY proctime RANGE UNBOUNDED preceding)"
    streamUtil.verifySqlPlansIdentical(sql, sql2)

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
          term("select", "a", "c", "proctime", "COUNT(a) AS w0$o0", "$SUM0(a) AS w0$o1")
        ),
        term("select", "c", "w0$o0 AS cnt1", "CASE(>(w0$o0, 0:BIGINT)",
          "w0$o1, null:INTEGER) AS cnt2")
      )
    streamUtil.verifySql(sql, expected)
  }

  @Test
  def testProcTimeUnboundedNonPartitionedRowsOver() = {
    val sql = "SELECT " +
      "c, " +
      "count(a) OVER (ORDER BY proctime ROWS BETWEEN UNBOUNDED preceding AND " +
      "CURRENT ROW) " +
      "from MyTable"

    val sql2 = "SELECT " +
      "c, " +
      "count(a) OVER w " +
      "FROM MyTable " +
      "WINDOW w AS (ORDER BY proctime ROWS BETWEEN UNBOUNDED preceding AND CURRENT ROW)"
    streamUtil.verifySqlPlansIdentical(sql, sql2)

    val expected =
      unaryNode(
        "DataStreamCalc",
        unaryNode(
          "DataStreamOverAggregate",
          streamTableNode(table),
          term("orderBy", "proctime"),
          term("rows", "BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"),
          term("select", "a", "b", "c", "proctime", "rowtime", "COUNT(a) AS w0$o0")
        ),
        term("select", "c", "w0$o0 AS $1")
      )
    streamUtil.verifySql(sql, expected)
  }

  @Test
  def testRowTimeBoundedPartitionedRowsOver() = {
    val sql = "SELECT " +
      "c, " +
      "count(a) OVER (PARTITION BY c ORDER BY rowtime ROWS BETWEEN 5 preceding AND " +
      "CURRENT ROW) " +
      "from MyTable"

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
          term("rows", "BETWEEN 5 PRECEDING AND CURRENT ROW"),
          term("select", "a", "c", "rowtime", "COUNT(a) AS w0$o0")
        ),
        term("select", "c", "w0$o0 AS $1")
      )
    streamUtil.verifySql(sql, expected)
  }

  @Test
  def testRowTimeBoundedPartitionedRangeOver() = {
    val sql = "SELECT " +
      "c, " +
      "count(a) OVER (PARTITION BY c ORDER BY rowtime " +
      "RANGE BETWEEN INTERVAL '1' SECOND  preceding AND CURRENT ROW) " +
      "from MyTable"

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
          term("range", "BETWEEN 1000 PRECEDING AND CURRENT ROW"),
          term("select", "a", "c", "rowtime", "COUNT(a) AS w0$o0")
        ),
        term("select", "c", "w0$o0 AS $1")
      )
    streamUtil.verifySql(sql, expected)
  }

  @Test
  def testRowTimeBoundedNonPartitionedRowsOver() = {
    val sql = "SELECT " +
      "c, " +
      "count(a) OVER (ORDER BY rowtime ROWS BETWEEN 5 preceding AND " +
      "CURRENT ROW) " +
      "from MyTable"

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
          term("rows", "BETWEEN 5 PRECEDING AND CURRENT ROW"),
          term("select", "a", "c", "rowtime", "COUNT(a) AS w0$o0")
        ),
        term("select", "c", "w0$o0 AS $1")
      )
    streamUtil.verifySql(sql, expected)
  }

  @Test
  def testRowTimeBoundedNonPartitionedRangeOver() = {
    val sql = "SELECT " +
      "c, " +
      "count(a) OVER (ORDER BY rowtime " +
      "RANGE BETWEEN INTERVAL '1' SECOND  preceding AND CURRENT ROW) as cnt1 " +
      "from MyTable"

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
          term("range", "BETWEEN 1000 PRECEDING AND CURRENT ROW"),
          term("select", "a", "c", "rowtime", "COUNT(a) AS w0$o0")
        ),
        term("select", "c", "w0$o0 AS $1")
      )
    streamUtil.verifySql(sql, expected)
  }

  @Test
  def testRowTimeUnboundedPartitionedRangeOver() = {
    val sql = "SELECT " +
      "c, " +
      "count(a) OVER (PARTITION BY c ORDER BY rowtime RANGE UNBOUNDED preceding) as cnt1, " +
      "sum(a) OVER (PARTITION BY c ORDER BY rowtime RANGE UNBOUNDED preceding) as cnt2 " +
      "from MyTable"

    val sql1 = "SELECT " +
      "c, " +
      "count(a) OVER (PARTITION BY c ORDER BY rowtime) as cnt1, " +
      "sum(a) OVER (PARTITION BY c ORDER BY rowtime) as cnt2 " +
      "from MyTable"
    streamUtil.verifySqlPlansIdentical(sql, sql1)

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
          term("select", "a", "c", "rowtime", "COUNT(a) AS w0$o0", "$SUM0(a) AS w0$o1")
        ),
        term("select", "c", "w0$o0 AS cnt1", "CASE(>(w0$o0, 0:BIGINT)",
          "w0$o1, null:INTEGER) AS cnt2")
      )
    streamUtil.verifySql(sql, expected)
  }

  @Test
  def testRowTimeUnboundedPartitionedRowsOver() = {
    val sql = "SELECT " +
      "c, " +
      "count(a) OVER (PARTITION BY c ORDER BY rowtime ROWS UNBOUNDED preceding) as cnt1, " +
      "sum(a) OVER (PARTITION BY c ORDER BY rowtime ROWS UNBOUNDED preceding) as cnt2 " +
      "from MyTable"

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
          term(
            "select",
            "a",
            "c",
            "rowtime",
            "COUNT(a) AS w0$o0",
            "$SUM0(a) AS w0$o1"
          )
        ),
        term(
          "select",
          "c",
          "w0$o0 AS cnt1",
          "CASE(>(w0$o0, 0:BIGINT)",
          "w0$o1, null:INTEGER) AS cnt2"
        )
      )
    streamUtil.verifySql(sql, expected)
  }

  @Test
  def testRowTimeUnboundedNonPartitionedRangeOver() = {
    val sql = "SELECT " +
      "c, " +
      "count(a) OVER (ORDER BY rowtime RANGE UNBOUNDED preceding) as cnt1, " +
      "sum(a) OVER (ORDER BY rowtime RANGE UNBOUNDED preceding) as cnt2 " +
      "from MyTable"

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
          term("select", "a", "c", "rowtime", "COUNT(a) AS w0$o0", "$SUM0(a) AS w0$o1")
        ),
        term("select", "c", "w0$o0 AS cnt1", "CASE(>(w0$o0, 0:BIGINT)",
          "w0$o1, null:INTEGER) AS cnt2")
      )
    streamUtil.verifySql(sql, expected)
  }

  @Test
  def testRowTimeUnboundedNonPartitionedRowsOver() = {
    val sql = "SELECT " +
      "c, " +
      "count(a) OVER (ORDER BY rowtime ROWS UNBOUNDED preceding) as cnt1, " +
      "sum(a) OVER (ORDER BY rowtime ROWS UNBOUNDED preceding) as cnt2 " +
      "from MyTable"

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
          term(
            "select",
            "a",
            "c",
            "rowtime",
            "COUNT(a) AS w0$o0",
            "$SUM0(a) AS w0$o1"
          )
        ),
        term(
          "select",
          "c",
          "w0$o0 AS cnt1",
          "CASE(>(w0$o0, 0:BIGINT)",
          "w0$o1, null:INTEGER) AS cnt2"
        )
      )
    streamUtil.verifySql(sql, expected)
  }

  @Test
  def testProcTimeBoundedPartitionedRowsOverDifferentWindows() = {
    val sql = "SELECT " +
      "a, " +
      "SUM(c) OVER (PARTITION BY a ORDER BY proctime ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), " +
      "MIN(c) OVER (PARTITION BY a ORDER BY proctime ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) " +
      "FROM MyTable"

    val sql2 = "SELECT " +
      "a, " +
      "SUM(c) OVER w1, " +
      "MIN(c) OVER w2 " +
      "FROM MyTable " +
      "WINDOW w1 AS (PARTITION BY a ORDER BY proctime ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)," +
      "w2 AS (PARTITION BY a ORDER BY proctime ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)"
    streamUtil.verifySqlPlansIdentical(sql, sql2)

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
          term("rows", "BETWEEN 3 PRECEDING AND CURRENT ROW"),
          term("select", "a", "c", "proctime", "COUNT(c) AS w0$o0",
            "$SUM0(c) AS w0$o1")
        ),
        term("select", "a", "CASE(>(w0$o0, 0:BIGINT)", "w0$o1, null:BIGINT) AS EXPR$1",
          "w1$o0 AS EXPR$2")
      )

    streamUtil.verifySql(sql, expected)
  }
}
