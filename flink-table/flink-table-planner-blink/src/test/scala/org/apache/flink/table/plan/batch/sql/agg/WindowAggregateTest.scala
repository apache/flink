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
package org.apache.flink.table.plan.batch.sql.agg

import org.apache.flink.api.scala._
import org.apache.flink.table.api.AggPhaseEnforcer.AggPhaseEnforcer
import org.apache.flink.table.api.{AggPhaseEnforcer, PlannerConfigOptions, TableException, ValidationException}
import org.apache.flink.table.plan.util.JavaUserDefinedAggFunctions.WeightedAvgWithMerge
import org.apache.flink.table.util.{CountAggFunction, TableTestBase}

import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.{Before, Test}

import java.sql.Timestamp
import java.util

import scala.collection.JavaConversions._

@RunWith(classOf[Parameterized])
class WindowAggregateTest(aggStrategy: AggPhaseEnforcer) extends TableTestBase {

  private val util = batchTestUtil()

  @Before
  def before(): Unit = {
    util.tableEnv.getConfig.getConf.setString(
      PlannerConfigOptions.SQL_OPTIMIZER_AGG_PHASE_ENFORCER, aggStrategy.toString)
    util.addFunction("countFun", new CountAggFunction)
    util.addTableSource[(Int, Timestamp, Int, Long)]("MyTable", 'a, 'b, 'c, 'd)
    util.addTableSource[(Timestamp, Long, Int, String)]("MyTable1", 'ts, 'a, 'b, 'c)
    util.addTableSource[(Int, Long, String, Int, Timestamp)]("MyTable2", 'a, 'b, 'c, 'd, 'ts)
  }

  @Test(expected = classOf[TableException])
  def testHopWindowNoOffset(): Unit = {
    val sqlQuery =
      "SELECT SUM(a) AS sumA, COUNT(b) AS cntB FROM MyTable2 " +
        "GROUP BY HOP(ts, INTERVAL '1' HOUR, INTERVAL '2' HOUR, TIME '10:00:00')"
    util.verifyPlan(sqlQuery)
  }

  @Test(expected = classOf[TableException])
  def testSessionWindowNoOffset(): Unit = {
    val sqlQuery =
      "SELECT SUM(a) AS sumA, COUNT(b) AS cntB FROM MyTable2 " +
        "GROUP BY SESSION(ts, INTERVAL '2' HOUR, TIME '10:00:00')"
    util.verifyPlan(sqlQuery)
  }

  @Test(expected = classOf[TableException])
  def testVariableWindowSize(): Unit = {
    util.verifyPlan("SELECT COUNT(*) FROM MyTable2 GROUP BY TUMBLE(ts, b * INTERVAL '1' MINUTE)")
  }

  @Test(expected = classOf[ValidationException])
  def testTumbleWindowWithInvalidUdAggArgs(): Unit = {
    val weightedAvg = new WeightedAvgWithMerge
    util.tableEnv.registerFunction("weightedAvg", weightedAvg)

    val sql = "SELECT weightedAvg(c, a) AS wAvg FROM MyTable2 " +
      "GROUP BY TUMBLE(ts, INTERVAL '4' MINUTE)"
    util.verifyPlan(sql)
  }

  @Test(expected = classOf[ValidationException])
  def testWindowProctime(): Unit = {
    val sqlQuery =
      "SELECT TUMBLE_PROCTIME(ts, INTERVAL '4' MINUTE) FROM MyTable2 " +
        "GROUP BY TUMBLE(ts, INTERVAL '4' MINUTE), c"
    // should fail because PROCTIME properties are not yet supported in batch
    util.verifyPlan(sqlQuery)
  }

  @Test(expected = classOf[AssertionError])
  def testWindowAggWithGroupSets(): Unit = {
    // TODO supports group sets
    // currently, the optimized plan is not collect, and an exception will be thrown in code-gen
    val sql =
    """
      |SELECT COUNT(*),
      |    TUMBLE_END(ts, INTERVAL '15' MINUTE) + INTERVAL '1' MINUTE
      |FROM MyTable1
      |    GROUP BY rollup(TUMBLE(ts, INTERVAL '15' MINUTE), b)
    """.stripMargin
    util.verifyPlanNotExpected(sql, "TUMBLE(ts")
  }

  @Test
  def testNoGroupingTumblingWindow(): Unit = {
    val sqlQuery = "SELECT AVG(c), SUM(a) FROM MyTable GROUP BY TUMBLE(b, INTERVAL '3' SECOND)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testTumblingWindowSortAgg1(): Unit = {
    val sqlQuery = "SELECT MAX(c) FROM MyTable1 GROUP BY a, TUMBLE(ts, INTERVAL '3' SECOND)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testTumblingWindowSortAgg2(): Unit = {
    val sqlQuery = "SELECT AVG(c), countFun(a) FROM MyTable " +
      "GROUP BY a, d, TUMBLE(b, INTERVAL '3' SECOND)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testTumblingWindowHashAgg1(): Unit = {
    val sqlQuery = "SELECT COUNT(c) FROM MyTable1 GROUP BY a, TUMBLE(ts, INTERVAL '3' SECOND)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testTumblingWindowHashAgg2(): Unit = {
    val sql = "SELECT AVG(c), COUNT(a) FROM MyTable GROUP BY a, d, TUMBLE(b, INTERVAL '3' SECOND)"
    util.verifyPlan(sql)
  }

  @Test
  def testNonPartitionedTumblingWindow(): Unit = {
    val sqlQuery =
      "SELECT SUM(a) AS sumA, COUNT(b) AS cntB FROM MyTable2 GROUP BY TUMBLE(ts, INTERVAL '2' HOUR)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testPartitionedTumblingWindow(): Unit = {
    val sqlQuery =
      """
        |SELECT TUMBLE_START(ts, INTERVAL '4' MINUTE),
        |    TUMBLE_END(ts, INTERVAL '4' MINUTE),
        |    TUMBLE_ROWTIME(ts, INTERVAL '4' MINUTE),
        |    c,
        |    SUM(a) AS sumA,
        |    MIN(b) AS minB
        |FROM MyTable2
        |    GROUP BY TUMBLE(ts, INTERVAL '4' MINUTE), c
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testTumblingWindowWithUdAgg(): Unit = {
    util.addFunction("weightedAvg", new WeightedAvgWithMerge)
    val sql = "SELECT weightedAvg(b, a) AS wAvg FROM MyTable2 " +
      "GROUP BY TUMBLE(ts, INTERVAL '4' MINUTE)"
    util.verifyPlan(sql)
  }

  @Test
  def testNoGroupingSlidingWindow(): Unit = {
    val sqlQuery =
      """
        |SELECT SUM(a),
        |    HOP_START(b, INTERVAL '3' SECOND, INTERVAL '3' SECOND),
        |    HOP_END(b, INTERVAL '3' SECOND, INTERVAL '3' SECOND)
        |FROM MyTable
        |    GROUP BY HOP(b, INTERVAL '3' SECOND, INTERVAL '3' SECOND)
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testSlidingWindowSortAgg1(): Unit = {
    val sqlQuery = "SELECT MAX(c) FROM MyTable1 " +
      "GROUP BY a, HOP(ts, INTERVAL '3' SECOND, INTERVAL '1' HOUR)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testSlidingWindowSortAgg2(): Unit = {
    val sqlQuery = "SELECT MAX(c) FROM MyTable1 " +
      "GROUP BY b, HOP(ts, INTERVAL '0.111' SECOND(1,3), INTERVAL '1' SECOND)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testSlidingWindowSortAgg3(): Unit = {
    val sqlQuery = "SELECT countFun(c) FROM MyTable " +
      " GROUP BY a, d, HOP(b, INTERVAL '3' SECOND, INTERVAL '1' HOUR)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testSlidingWindowSortAggWithPaneOptimization(): Unit = {
    val sqlQuery = "SELECT COUNT(c) FROM MyTable1 " +
      "GROUP BY a, HOP(ts, INTERVAL '3' SECOND, INTERVAL '1' HOUR)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testSlidingWindowHashAgg(): Unit = {
    val sqlQuery = "SELECT count(c) FROM MyTable1 " +
      "GROUP BY b, HOP(ts, INTERVAL '3' SECOND, INTERVAL '1' HOUR)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testNonPartitionedSlidingWindow(): Unit = {
    val sqlQuery =
      "SELECT SUM(a) AS sumA, COUNT(b) AS cntB " +
        "FROM MyTable2 " +
        "GROUP BY HOP(ts, INTERVAL '15' MINUTE, INTERVAL '90' MINUTE)"

    util.verifyPlan(sqlQuery)
  }

  @Test
  def testPartitionedSlidingWindow(): Unit = {
    val sqlQuery =
      "SELECT " +
        "  c, " +
        "  HOP_END(ts, INTERVAL '1' HOUR, INTERVAL '3' HOUR), " +
        "  HOP_START(ts, INTERVAL '1' HOUR, INTERVAL '3' HOUR), " +
        "  HOP_ROWTIME(ts, INTERVAL '1' HOUR, INTERVAL '3' HOUR), " +
        "  SUM(a) AS sumA, " +
        "  AVG(b) AS avgB " +
        "FROM MyTable2 " +
        "GROUP BY HOP(ts, INTERVAL '1' HOUR, INTERVAL '3' HOUR), d, c"

    util.verifyPlan(sqlQuery)
  }

  @Test(expected = classOf[TableException])
  // TODO session window is not supported now
  def testNonPartitionedSessionWindow(): Unit = {
    val sqlQuery = "SELECT COUNT(*) AS cnt FROM MyTable2 GROUP BY SESSION(ts, INTERVAL '30' MINUTE)"
    util.verifyPlan(sqlQuery)
  }

  @Test(expected = classOf[TableException])
  // TODO session window is not supported now
  def testPartitionedSessionWindow(): Unit = {
    val sqlQuery =
      """
        |SELECT c, d,
        |    SESSION_START(ts, INTERVAL '12' HOUR),
        |    SESSION_END(ts, INTERVAL '12' HOUR),
        |    SESSION_ROWTIME(ts, INTERVAL '12' HOUR),
        |    SUM(a) AS sumA,
        |    MIN(b) AS minB
        |FROM MyTable2
        |    GROUP BY SESSION(ts, INTERVAL '12' HOUR), c, d
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testWindowEndOnly(): Unit = {
    val sqlQuery =
      "SELECT TUMBLE_END(ts, INTERVAL '4' MINUTE) FROM MyTable2 " +
        "GROUP BY TUMBLE(ts, INTERVAL '4' MINUTE), c"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testExpressionOnWindowHavingFunction(): Unit = {
    val sql =
      """
        |SELECT COUNT(*),
        |    HOP_START(ts, INTERVAL '15' MINUTE, INTERVAL '1' MINUTE)
        |FROM MyTable2
        |    GROUP BY HOP(ts, INTERVAL '15' MINUTE, INTERVAL '1' MINUTE)
        |    HAVING
        |     SUM(a) > 0 AND
        |     QUARTER(HOP_START(ts, INTERVAL '15' MINUTE, INTERVAL '1' MINUTE)) = 1
      """.stripMargin
    util.verifyPlan(sql)
  }

  @Test
  def testDecomposableAggFunctions(): Unit = {
    val sql =
      """
        |SELECT VAR_POP(b),
        |    VAR_SAMP(b),
        |    STDDEV_POP(b),
        |    STDDEV_SAMP(b),
        |    TUMBLE_START(ts, INTERVAL '15' MINUTE),
        |    TUMBLE_END(ts, INTERVAL '15' MINUTE)
        |FROM MyTable1
        |    GROUP BY TUMBLE(ts, INTERVAL '15' MINUTE)
      """.stripMargin
    util.verifyPlan(sql)
  }
}

object WindowAggregateTest {

  @Parameterized.Parameters(name = "aggStrategy={0}")
  def parameters(): util.Collection[AggPhaseEnforcer] = {
    Seq[AggPhaseEnforcer](
      AggPhaseEnforcer.NONE,
      AggPhaseEnforcer.ONE_PHASE,
      AggPhaseEnforcer.TWO_PHASE
    )
  }
}
