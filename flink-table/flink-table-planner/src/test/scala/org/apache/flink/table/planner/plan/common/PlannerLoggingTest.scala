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
package org.apache.flink.table.planner.plan.common

import org.apache.flink.table.api._
import org.apache.flink.table.api.config.OptimizerConfigOptions
import org.apache.flink.table.planner.utils.TableTestBase
import org.apache.flink.testutils.junit.extensions.parameterized.{ParameterizedTestExtension, Parameters}

import org.apache.calcite.plan.RelOptPlanner
import org.apache.logging.log4j.Level
import org.apache.logging.log4j.core.LoggerContext
import org.apache.logging.log4j.core.config.LoggerConfig
import org.junit.jupiter.api.TestTemplate
import org.junit.jupiter.api.extension.ExtendWith
import org.junit.jupiter.api.parallel.Isolated

/** Tests that planner logging does not change streaming or batch SQL plans. */
@Isolated
@ExtendWith(Array(classOf[ParameterizedTestExtension]))
class PlannerLoggingTest(isBatch: Boolean) extends TableTestBase {

  @TestTemplate
  def testFilterProjectUnion(): Unit = {
    verifyPlanWithDifferentLogLevels(
      "SELECT a, b + 1 FROM T1 WHERE b > 10 " +
        "UNION ALL SELECT a, b * 2 FROM T1 WHERE a < 3")
  }

  @TestTemplate
  def testGroupAggregate(): Unit = {
    verifyPlanWithDifferentLogLevels("SELECT a, COUNT(*) FROM T1 GROUP BY a")
  }

  @TestTemplate
  def testDistinctAggregate(): Unit = {
    verifyPlanWithDifferentLogLevels("SELECT c, COUNT(DISTINCT a), SUM(b) FROM T1 GROUP BY c")
  }

  @TestTemplate
  def testJoinAndAggregate(): Unit = {
    verifyPlanWithDifferentLogLevels(
      "SELECT t1.c, SUM(t1.b + t2.b), COUNT(*) " +
        "FROM T1 t1 JOIN T2 t2 ON t1.a = t2.a WHERE t1.b >= 10 GROUP BY t1.c")
  }

  @TestTemplate
  def testSemiJoin(): Unit = {
    verifyPlanWithDifferentLogLevels(
      "SELECT a, c FROM T1 WHERE a IN (SELECT a FROM T2 WHERE b >= 20)")
  }

  @TestTemplate
  def testJoinReorder(): Unit = {
    verifyPlanWithDifferentLogLevels(
      "SELECT t1.a, t1.b + t2.b + t3.b " +
        "FROM T1 t1 JOIN T2 t2 ON t1.a = t2.a JOIN T3 t3 ON t1.a = t3.a")
  }

  @TestTemplate
  def testPartitionedTopN(): Unit = {
    verifyPlanWithDifferentLogLevels(
      "SELECT a, b, c FROM (" +
        "SELECT a, b, c, ROW_NUMBER() OVER (PARTITION BY c ORDER BY b DESC, a) AS rn " +
        "FROM T1) WHERE rn <= 2")
  }

  @TestTemplate
  def testSort(): Unit = {
    val sql = "SELECT * FROM T1 ORDER BY a"
    verifyPlanWithDifferentLogLevels(if (isBatch) sql else sql + " LIMIT 10")
  }

  private def verifyPlanWithDifferentLogLevels(sql: String): Unit = {
    val util = if (isBatch) batchTestUtil() else streamTestUtil()
    util.getTableEnv.getConfig
      .set(OptimizerConfigOptions.TABLE_OPTIMIZER_JOIN_REORDER_ENABLED, Boolean.box(true))
    Seq("T1", "T2", "T3").foreach(
      name => util.addTableSource[(Int, Long, String)](name, 'a, 'b, 'c))

    val name = classOf[RelOptPlanner].getName
    val context = LoggerContext.getContext(false)
    val config = context.getConfiguration
    val previous = config.getLoggers.get(name)
    // Exercise HEP's trace path without sending large plan dumps to the test log.
    val logger = new LoggerConfig(name, Level.INFO, false)
    config.removeLogger(name)
    config.addLogger(name, logger)
    try {
      Seq(Level.INFO, Level.DEBUG, Level.TRACE).foreach {
        level =>
          logger.setLevel(level)
          context.updateLoggers()
          util.verifyRelPlan(sql)
      }
    } finally {
      config.removeLogger(name)
      if (previous != null) {
        config.addLogger(name, previous)
      }
      context.updateLoggers()
    }
  }
}

object PlannerLoggingTest {
  @Parameters(name = "isBatch: {0}")
  def parameters(): java.util.Collection[Boolean] = {
    java.util.Arrays.asList(true, false)
  }
}
