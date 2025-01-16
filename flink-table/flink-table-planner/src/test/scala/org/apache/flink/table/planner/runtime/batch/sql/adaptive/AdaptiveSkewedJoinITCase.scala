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
package org.apache.flink.table.planner.runtime.batch.sql.adaptive

import org.apache.flink.configuration.{BatchExecutionOptions, MemorySize}
import org.apache.flink.table.api.config.OptimizerConfigOptions

import org.junit.jupiter.api.{BeforeEach, Test}

/** IT cases for adaptive skewed join. */
class AdaptiveSkewedJoinITCase extends AdaptiveJoinITCase {
  @BeforeEach
  override def before(): Unit = {
    super.before()

    tEnv.getConfig.set(BatchExecutionOptions.ADAPTIVE_AUTO_PARALLELISM_ENABLED, Boolean.box(true))
    tEnv.getConfig
      .set(
        OptimizerConfigOptions.TABLE_OPTIMIZER_ADAPTIVE_BROADCAST_JOIN_STRATEGY,
        OptimizerConfigOptions.AdaptiveBroadcastJoinStrategy.NONE)
    tEnv.getConfig
      .set(
        OptimizerConfigOptions.TABLE_OPTIMIZER_ADAPTIVE_SKEWED_JOIN_OPTIMIZATION_SKEWED_THRESHOLD,
        MemorySize.parse("100k"))

    registerCollection(
      "T",
      AdaptiveJoinITCase.generateRandomData(30, 0),
      AdaptiveJoinITCase.rowType,
      "a, b, c, d",
      AdaptiveJoinITCase.nullables,
      forceNonParallel = false
    )
    registerCollection(
      "T1",
      AdaptiveJoinITCase.generateRandomData(3000, 0.99),
      AdaptiveJoinITCase.rowType,
      "a1, b1, c1, d1",
      AdaptiveJoinITCase.nullables,
      forceNonParallel = false
    )
    registerCollection(
      "T2",
      AdaptiveJoinITCase.generateRandomData(30, 0),
      AdaptiveJoinITCase.rowType,
      "a2, b2, c2, d2",
      AdaptiveJoinITCase.nullables,
      forceNonParallel = false)
    registerCollection(
      "T3",
      AdaptiveJoinITCase.generateRandomData(30, 0),
      AdaptiveJoinITCase.rowType,
      "a3, b3, c3, d3",
      AdaptiveJoinITCase.nullables,
      forceNonParallel = false)
  }

  @Test
  def testJoinWithSkewedUnionInput(): Unit = {
    val sql =
      """
        |SELECT * FROM
        |  (SELECT * FROM (SELECT a1 as a, b1 as b, c1 as c, d1 as d FROM T1) UNION ALL (SELECT a2 as a, b2 as b, c2 as c, d2 as d FROM T2)) Y
        |  LEFT JOIN T ON T.a = Y.a
        |""".stripMargin
    checkResult(sql)
  }

  @Test
  def testJoinWithUnspecifiedForwardOutput(): Unit = {
    val sql = "SELECT a1 as a, b1 as b, c1 as c, d1 as d FROM T1, T2 WHERE a1 = a2"
    checkResult(sql)
  }

  override def checkResult(sql: String): Unit = {
    tEnv.getConfig
      .set(
        OptimizerConfigOptions.TABLE_OPTIMIZER_ADAPTIVE_SKEWED_JOIN_OPTIMIZATION_STRATEGY,
        OptimizerConfigOptions.AdaptiveSkewedJoinOptimizationStrategy.NONE)
    val expected = executeQuery(sql)
    tEnv.getConfig
      .set(
        OptimizerConfigOptions.TABLE_OPTIMIZER_ADAPTIVE_SKEWED_JOIN_OPTIMIZATION_STRATEGY,
        OptimizerConfigOptions.AdaptiveSkewedJoinOptimizationStrategy.AUTO)
    checkResult(sql, expected)
    tEnv.getConfig
      .set(
        OptimizerConfigOptions.TABLE_OPTIMIZER_ADAPTIVE_SKEWED_JOIN_OPTIMIZATION_STRATEGY,
        OptimizerConfigOptions.AdaptiveSkewedJoinOptimizationStrategy.FORCED
      )
    checkResult(sql, expected)
  }
}
