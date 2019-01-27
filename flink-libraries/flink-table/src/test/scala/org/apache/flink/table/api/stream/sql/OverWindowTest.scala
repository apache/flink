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
import org.apache.flink.table.util.{StreamTableTestUtil, TableTestBase}
import org.junit.Test

class OverWindowTest extends TableTestBase {
  private val streamUtil: StreamTableTestUtil = streamTestUtil()
  streamUtil.addTable[(Int, String, Long)](
    "MyTable",
    'a, 'b, 'c,
    'proctime.proctime, 'rowtime.rowtime)

  @Test
  def testProctimeBoundedDistinctWithNonDistinctPartitionedRowOver(): Unit = {
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
    streamUtil.verifyPlan(sql)
  }

  @Test
  def testProctimeBoundedDistinctPartitionedRowOver(): Unit = {
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
    streamUtil.verifyPlan(sql)
  }

  @Test
  def testProcTimeBoundedPartitionedRowsOver(): Unit = {
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
    streamUtil.verifyPlan(sql)
  }

  @Test
  def testProcTimeBoundedPartitionedRangeOver(): Unit = {

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
    streamUtil.verifyPlan(sqlQuery)
  }

  @Test
  def testProcTimeBoundedNonPartitionedRangeOver(): Unit = {

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
    streamUtil.verifyPlan(sqlQuery)
  }

  @Test
  def testProcTimeBoundedNonPartitionedRowsOver(): Unit = {
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
    streamUtil.verifyPlan(sql)
  }


  @Test
  def testProcTimeUnboundedPartitionedRangeOver(): Unit = {
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

    streamUtil.verifySqlPlansIdentical(sql, sql2)
    streamUtil.verifyPlan(sql)
  }

  @Test
  def testProcTimeUnboundedPartitionedRowsOver(): Unit = {
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
    streamUtil.verifyPlan(sql)
  }

  @Test
  def testProcTimeUnboundedNonPartitionedRangeOver(): Unit = {
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
    streamUtil.verifyPlan(sql)
  }

  @Test
  def testProcTimeUnboundedNonPartitionedRowsOver(): Unit = {
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
    streamUtil.verifyPlan(sql)
  }

  @Test
  def testRowTimeBoundedPartitionedRowsOver(): Unit = {
    val sql = "SELECT " +
      "c, " +
      "count(a) OVER (PARTITION BY c ORDER BY rowtime ROWS BETWEEN 5 preceding AND " +
      "CURRENT ROW) " +
      "from MyTable"

    streamUtil.verifyPlan(sql)
  }

  @Test
  def testRowTimeBoundedPartitionedRangeOver(): Unit = {
    val sql = "SELECT " +
      "c, " +
      "count(a) OVER (PARTITION BY c ORDER BY rowtime " +
      "RANGE BETWEEN INTERVAL '1' SECOND  preceding AND CURRENT ROW) " +
      "from MyTable"

    streamUtil.verifyPlan(sql)
  }

  @Test
  def testRowTimeBoundedNonPartitionedRowsOver(): Unit = {
    val sql = "SELECT " +
      "c, " +
      "count(a) OVER (ORDER BY rowtime ROWS BETWEEN 5 preceding AND " +
      "CURRENT ROW) " +
      "from MyTable"

    streamUtil.verifyPlan(sql)
  }

  @Test
  def testRowTimeBoundedNonPartitionedRangeOver(): Unit = {
    val sql = "SELECT " +
      "c, " +
      "count(a) OVER (ORDER BY rowtime " +
      "RANGE BETWEEN INTERVAL '1' SECOND  preceding AND CURRENT ROW) as cnt1 " +
      "from MyTable"

    streamUtil.verifyPlan(sql)
  }

  @Test
  def testRowTimeUnboundedPartitionedRangeOver(): Unit = {
    val sql = "SELECT " +
      "c, " +
      "count(a) OVER (PARTITION BY c ORDER BY rowtime RANGE UNBOUNDED preceding) as cnt1, " +
      "sum(a) OVER (PARTITION BY c ORDER BY rowtime RANGE UNBOUNDED preceding) as cnt2 " +
      "from MyTable"

    streamUtil.verifyPlan(sql)
  }

  @Test
  def testRowTimeUnboundedPartitionedRowsOver(): Unit = {
    val sql = "SELECT " +
      "c, " +
      "count(a) OVER (PARTITION BY c ORDER BY rowtime ROWS UNBOUNDED preceding) as cnt1, " +
      "sum(a) OVER (PARTITION BY c ORDER BY rowtime ROWS UNBOUNDED preceding) as cnt2 " +
      "from MyTable"

    streamUtil.verifyPlan(sql)
  }

  @Test
  def testRowTimeUnboundedNonPartitionedRangeOver(): Unit = {
    val sql = "SELECT " +
      "c, " +
      "count(a) OVER (ORDER BY rowtime RANGE UNBOUNDED preceding) as cnt1, " +
      "sum(a) OVER (ORDER BY rowtime RANGE UNBOUNDED preceding) as cnt2 " +
      "from MyTable"

    streamUtil.verifyPlan(sql)
  }

  @Test
  def testRowTimeUnboundedNonPartitionedRowsOver(): Unit = {
    val sql = "SELECT " +
      "c, " +
      "count(a) OVER (ORDER BY rowtime ROWS UNBOUNDED preceding) as cnt1, " +
      "sum(a) OVER (ORDER BY rowtime ROWS UNBOUNDED preceding) as cnt2 " +
      "from MyTable"

    streamUtil.verifyPlan(sql)
  }

  @Test
  def testProcTimeBoundedPartitionedRowsOverDifferentWindows(): Unit = {
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
    streamUtil.verifyPlan(sql)
  }
}
