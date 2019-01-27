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

package org.apache.flink.table.plan.batch.sql

import java.sql.Timestamp

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.runtime.utils
  .JavaUserDefinedAggFunctions.WeightedAvgWithMerge
import org.apache.flink.table.util.TableTestBase
import org.junit.Test

class GroupWindowTest extends TableTestBase {

  @Test
  def testNonPartitionedTumbleWindow(): Unit = {
    val util = batchTestUtil()
    util.addTable[(Int, Long, String, Timestamp)]("T", 'a, 'b, 'c, 'ts)

    val sqlQuery =
      "SELECT SUM(a) AS sumA, COUNT(b) AS cntB FROM T GROUP BY TUMBLE(ts, INTERVAL '2' HOUR)"

    util.verifyPlan(sqlQuery)
  }

  @Test
  def testPartitionedTumbleWindow(): Unit = {
    val util = batchTestUtil()
    util.addTable[(Int, Long, String, Timestamp)]("T", 'a, 'b, 'c, 'ts)

    val sqlQuery =
      "SELECT " +
        "  TUMBLE_START(ts, INTERVAL '4' MINUTE), " +
        "  TUMBLE_END(ts, INTERVAL '4' MINUTE), " +
        "  TUMBLE_ROWTIME(ts, INTERVAL '4' MINUTE), " +
        "  c, " +
        "  SUM(a) AS sumA, " +
        "  MIN(b) AS minB " +
        "FROM T " +
        "GROUP BY TUMBLE(ts, INTERVAL '4' MINUTE), c"

    util.verifyPlan(sqlQuery)
  }

  @Test
  def testTumbleWindowWithUdAgg(): Unit = {
    val util = batchTestUtil()
    util.addTable[(Int, Long, String, Timestamp)]("T", 'a, 'b, 'c, 'ts)

    val weightedAvg = new WeightedAvgWithMerge
    util.tableEnv.registerFunction("weightedAvg", weightedAvg)

    val sql = "SELECT weightedAvg(b, a) AS wAvg " +
      "FROM T " +
      "GROUP BY TUMBLE(ts, INTERVAL '4' MINUTE)"

    util.verifyPlan(sql)
  }

  @Test
  def testNonPartitionedHopWindow(): Unit = {
    val util = batchTestUtil()
    util.addTable[(Int, Long, String, Timestamp)]("T", 'a, 'b, 'c, 'ts)

    val sqlQuery =
      "SELECT SUM(a) AS sumA, COUNT(b) AS cntB " +
        "FROM T " +
        "GROUP BY HOP(ts, INTERVAL '15' MINUTE, INTERVAL '90' MINUTE)"

    util.verifyPlan(sqlQuery)
  }

  @Test
  def testPartitionedHopWindow(): Unit = {
    val util = batchTestUtil()
    util.addTable[(Int, Long, String, Long, Timestamp)]("T", 'a, 'b, 'c, 'd, 'ts)

    val sqlQuery =
      "SELECT " +
        "  c, " +
        "  HOP_END(ts, INTERVAL '1' HOUR, INTERVAL '3' HOUR), " +
        "  HOP_START(ts, INTERVAL '1' HOUR, INTERVAL '3' HOUR), " +
        "  HOP_ROWTIME(ts, INTERVAL '1' HOUR, INTERVAL '3' HOUR), " +
        "  SUM(a) AS sumA, " +
        "  AVG(b) AS avgB " +
        "FROM T " +
        "GROUP BY HOP(ts, INTERVAL '1' HOUR, INTERVAL '3' HOUR), d, c"

    util.verifyPlan(sqlQuery)
  }

  @Test(expected = classOf[RuntimeException])
  def testNonPartitionedSessionWindow(): Unit = {
    val util = batchTestUtil()
    util.addTable[(Int, Long, String, Timestamp)]("T", 'a, 'b, 'c, 'ts)

    val sqlQuery =
      "SELECT COUNT(*) AS cnt FROM T GROUP BY SESSION(ts, INTERVAL '30' MINUTE)"

    util.verifyPlan(sqlQuery)
  }

  @Test(expected = classOf[RuntimeException])
  def testPartitionedSessionWindow(): Unit = {
    val util = batchTestUtil()
    util.addTable[(Int, Long, String, Int, Timestamp)]("T", 'a, 'b, 'c, 'd, 'ts)

    val sqlQuery =
      "SELECT " +
        "  c, d, " +
        "  SESSION_START(ts, INTERVAL '12' HOUR), " +
        "  SESSION_END(ts, INTERVAL '12' HOUR), " +
        "  SESSION_ROWTIME(ts, INTERVAL '12' HOUR), " +
        "  SUM(a) AS sumA, " +
        "  MIN(b) AS minB " +
        "FROM T " +
        "GROUP BY SESSION(ts, INTERVAL '12' HOUR), c, d"

    util.verifyPlan(sqlQuery)
  }

  @Test
  def testWindowEndOnly(): Unit = {
    val util = batchTestUtil()
    util.addTable[(Int, Long, String, Timestamp)]("T", 'a, 'b, 'c, 'ts)

    val sqlQuery =
      "SELECT " +
        "  TUMBLE_END(ts, INTERVAL '4' MINUTE)" +
        "FROM T " +
        "GROUP BY TUMBLE(ts, INTERVAL '4' MINUTE), c"

    util.verifyPlan(sqlQuery)
  }

  @Test
  def testExpressionOnWindowHavingFunction(): Unit = {
    val util = batchTestUtil()
    util.addTable[(Int, Long, String, Timestamp)]("T", 'a, 'b, 'c, 'ts)

    val sql =
      "SELECT " +
        "  COUNT(*), " +
        "  HOP_START(ts, INTERVAL '15' MINUTE, INTERVAL '1' MINUTE) " +
        "FROM T " +
        "GROUP BY HOP(ts, INTERVAL '15' MINUTE, INTERVAL '1' MINUTE) " +
        "HAVING " +
        "  SUM(a) > 0 AND " +
        "  QUARTER(HOP_START(ts, INTERVAL '15' MINUTE, INTERVAL '1' MINUTE)) = 1"

    util.verifyPlan(sql)
  }

  @Test
  def testDecomposableAggFunctions(): Unit = {
    val util = batchTestUtil()
    util.addTable[(Int, String, Long, Timestamp)]("MyTable", 'a, 'b, 'c, 'rowtime)

    val sql =
      "SELECT " +
        "  VAR_POP(c), VAR_SAMP(c), STDDEV_POP(c), STDDEV_SAMP(c), " +
        "  TUMBLE_START(rowtime, INTERVAL '15' MINUTE), " +
        "  TUMBLE_END(rowtime, INTERVAL '15' MINUTE)" +
        "FROM MyTable " +
        "GROUP BY TUMBLE(rowtime, INTERVAL '15' MINUTE)"

    util.verifyPlan(sql)
  }

  @Test
  def testMultiHopWindowsJoin(): Unit = {
    val util = batchTestUtil()
    util.addTable[(Int, String, Long, Timestamp)]("MyTable", 'a, 'b, 'c, 'rowtime)

    val sql =
      """
        |SELECT * FROM
        | (SELECT
        |   HOP_START(rowtime, INTERVAL '1' MINUTE, INTERVAL '1' HOUR) as hs1,
        |   HOP_END(rowtime, INTERVAL '1' MINUTE, INTERVAL '1' HOUR) as he1,
        |   count(*) as c1,
        |   sum(c) as s1
        | FROM MyTable
        | GROUP BY HOP(rowtime, INTERVAL '1' MINUTE, INTERVAL '1' HOUR)) t1
        |JOIN
        | (SELECT
        |   HOP_START(rowtime, INTERVAL '1' MINUTE, INTERVAL '1' DAY) as hs2,
        |   HOP_END(rowtime, INTERVAL '1' MINUTE, INTERVAL '1' DAY) as he2,
        |   count(*) as c2,
        |   sum(c) as s2
        | FROM MyTable
        | GROUP BY HOP(rowtime, INTERVAL '1' MINUTE, INTERVAL '1' DAY)) t2 ON t1.he1 = t2.he2
        |WHERE t1.s1 IS NOT NULL
      """.stripMargin
    util.verifyPlan(sql)
  }
}
