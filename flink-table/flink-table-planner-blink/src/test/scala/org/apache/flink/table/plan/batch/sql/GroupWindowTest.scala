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

import org.apache.flink.api.scala._
import org.apache.flink.table.plan.util.JavaUserDefinedAggFunctions.WeightedAvgWithMerge
import org.apache.flink.table.util.TableTestBase

import org.junit.{Ignore, Test}

import java.sql.Timestamp

class GroupWindowTest extends TableTestBase {

  @Test
  def testNonPartitionedTumbleWindow(): Unit = {
    val util = batchTestUtil()
    util.addTableSource[(Int, Long, String, Timestamp)]("T", 'a, 'b, 'c, 'ts)

    val sqlQuery =
      "SELECT SUM(a) AS sumA, COUNT(b) AS cntB FROM T GROUP BY TUMBLE(ts, INTERVAL '2' HOUR)"

    util.verifyPlan(sqlQuery)
  }

  //TODO to support.
  @Ignore
  @Test
  def testPartitionedTumbleWindow(): Unit = {
    val util = batchTestUtil()
    util.addTableSource[(Int, Long, String, Timestamp)]("T", 'a, 'b, 'c, 'ts)

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
    util.addTableSource[(Int, Long, String, Timestamp)]("T", 'a, 'b, 'c, 'ts)

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
    util.addTableSource[(Int, Long, String, Timestamp)]("T", 'a, 'b, 'c, 'ts)

    val sqlQuery =
      "SELECT SUM(a) AS sumA, COUNT(b) AS cntB " +
        "FROM T " +
        "GROUP BY HOP(ts, INTERVAL '15' MINUTE, INTERVAL '90' MINUTE)"

    util.verifyPlan(sqlQuery)
  }

  //TODO to support.
  @Ignore
  @Test
  def testPartitionedHopWindow(): Unit = {
    val util = batchTestUtil()
    util.addTableSource[(Int, Long, String, Long, Timestamp)]("T", 'a, 'b, 'c, 'd, 'ts)

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

  //TODO to support.
  @Ignore
  @Test
  def testNonPartitionedSessionWindow(): Unit = {
    val util = batchTestUtil()
    util.addTableSource[(Int, Long, String, Timestamp)]("T", 'a, 'b, 'c, 'ts)

    val sqlQuery =
      "SELECT COUNT(*) AS cnt FROM T GROUP BY SESSION(ts, INTERVAL '30' MINUTE)"

    util.verifyPlan(sqlQuery)
  }

  //TODO to support.
  @Ignore
  @Test
  def testPartitionedSessionWindow(): Unit = {
    val util = batchTestUtil()
    util.addTableSource[(Int, Long, String, Int, Timestamp)]("T", 'a, 'b, 'c, 'd, 'ts)

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
    util.addTableSource[(Int, Long, String, Timestamp)]("T", 'a, 'b, 'c, 'ts)

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
    util.addTableSource[(Int, Long, String, Timestamp)]("T", 'a, 'b, 'c, 'ts)

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

  @Ignore // TODO support VAR_POP etc..
  @Test
  def testDecomposableAggFunctions(): Unit = {
    val util = batchTestUtil()
    util.addTableSource[(Int, String, Long, Timestamp)]("MyTable", 'a, 'b, 'c, 'ts)

    val sql =
      "SELECT " +
        "  VAR_POP(c), VAR_SAMP(c), STDDEV_POP(c), STDDEV_SAMP(c), " +
        "  TUMBLE_START(ts, INTERVAL '15' MINUTE), " +
        "  TUMBLE_END(ts, INTERVAL '15' MINUTE)" +
        "FROM MyTable " +
        "GROUP BY TUMBLE(ts, INTERVAL '15' MINUTE)"

    util.verifyPlan(sql)
  }
}
