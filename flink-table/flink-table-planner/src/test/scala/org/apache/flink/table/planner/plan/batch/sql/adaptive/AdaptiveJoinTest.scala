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
package org.apache.flink.table.planner.plan.batch.sql.adaptive

import org.apache.flink.configuration.BatchExecutionOptions
import org.apache.flink.table.api._
import org.apache.flink.table.api.config.{ExecutionConfigOptions, OptimizerConfigOptions}
import org.apache.flink.table.planner.utils.TableTestBase

import org.junit.jupiter.api.{BeforeEach, Test}

/** Tests for AdaptiveJoinProcessor. */
class AdaptiveJoinTest extends TableTestBase {

  private val util = batchTestUtil()

  @BeforeEach
  def before(): Unit = {
    util.addTableSource[(Long, Long, String, Long)]("T", 'a, 'b, 'c, 'd)
    util.addTableSource[(Long, Long, String, Long)]("T1", 'a1, 'b1, 'c1, 'd1)
    util.addTableSource[(Long, Long, String, Long)]("T2", 'a2, 'b2, 'c2, 'd2)
    util.addTableSource[(Long, Long, String, Long)]("T3", 'a3, 'b3, 'c3, 'd3)
    util.tableConfig.set(
      OptimizerConfigOptions.TABLE_OPTIMIZER_ADAPTIVE_BROADCAST_JOIN_STRATEGY,
      OptimizerConfigOptions.AdaptiveBroadcastJoinStrategy.AUTO)
  }

  @Test
  def testWithShuffleHashJoin(): Unit = {
    util.tableEnv.getConfig
      .set(ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "NestedLoopJoin,SortMergeJoin")
    val sql = "SELECT * FROM T1, T2 WHERE a1 = a2"
    util.verifyExecPlan(sql)
  }

  @Test
  def testWithShuffleMergeJoin(): Unit = {
    util.tableEnv.getConfig
      .set(ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "NestedLoopJoin,ShuffleHashJoin")
    val sql = "SELECT * FROM T1, T2 WHERE a1 = a2"
    util.verifyExecPlan(sql)
  }

  // For join nodes that have already been converted as broadcast join during the compilation phase,
  // no further transformation will be performed.
  @Test
  def testWithStaticBroadcastJoin(): Unit = {
    util.tableEnv.getConfig.set(
      ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS,
      "SortMergeJoin,ShuffleHashJoin,NestedLoopJoin")
    util.tableEnv.getConfig
      .set(OptimizerConfigOptions.TABLE_OPTIMIZER_BROADCAST_JOIN_THRESHOLD, Long.box(Long.MaxValue))
    val sql = "SELECT * FROM T1, T2 WHERE a1 = a2"
    util.verifyExecPlan(sql)
  }

  @Test
  def testWithBroadcastJoinRuntimeOnly(): Unit = {
    util.tableEnv.getConfig
      .set(ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "NestedLoopJoin")
    util.tableEnv.getConfig
      .set(OptimizerConfigOptions.TABLE_OPTIMIZER_BROADCAST_JOIN_THRESHOLD, Long.box(Long.MaxValue))
    util.tableEnv.getConfig.set(
      OptimizerConfigOptions.TABLE_OPTIMIZER_ADAPTIVE_BROADCAST_JOIN_STRATEGY,
      OptimizerConfigOptions.AdaptiveBroadcastJoinStrategy.RUNTIME_ONLY)
    val sql = "SELECT * FROM T1, T2 WHERE a1 = a2"
    util.verifyExecPlan(sql)
  }

  @Test
  def testJoinWithUnionInput(): Unit = {
    val sql =
      """
        |SELECT * FROM
        |  (SELECT a FROM (SELECT a1 as a FROM T1) UNION ALL (SELECT a2 as a FROM T2)) Y
        |  LEFT JOIN T ON T.a = Y.a
        |""".stripMargin
    util.verifyExecPlan(sql)
  }

  // AdaptiveJoin does not support case of ForwardForConsecutiveHash, it may lead to data incorrectness.
  @Test
  def testShuffleJoinWithForwardForConsecutiveHash(): Unit = {
    util.tableEnv.getConfig
      .set(OptimizerConfigOptions.TABLE_OPTIMIZER_MULTIPLE_INPUT_ENABLED, Boolean.box(false))
    val sql =
      """
        |WITH
        |  r AS (SELECT * FROM T1, T2, T3 WHERE a1 = a2 and a1 = a3)
        |SELECT sum(b1) FROM r group by a1
        |""".stripMargin
    util.verifyExecPlan(sql)
  }

  // AdaptiveJoin does not support case of MultipleInput, because the optimizer is unable to see the
  // join node within the MultipleInput, but this might be supported in the future.
  @Test
  def testJoinWithMultipleInput(): Unit = {
    val sql =
      """
        |SELECT * FROM
        |  (SELECT a FROM T1 JOIN T ON a = a1) t1
        |  INNER JOIN
        |  (SELECT d2 FROM T JOIN T2 ON d2 = a) t2
        |ON t1.a = t2.d2
        |""".stripMargin
    util.verifyExecPlan(sql)
  }

  // Currently, adaptive join optimization and batch job progress recovery cannot be enabled
  // simultaneously so we should disable it here.
  // TODO: If job recovery for adaptive execution is supported in the future, this logic will
  //  need to be removed.
  @Test
  def testAdaptiveJoinWithBatchJobRecovery(): Unit = {
    util.tableEnv.getConfig
      .set(BatchExecutionOptions.JOB_RECOVERY_ENABLED, Boolean.box(true))
    val sql = "SELECT * FROM T1, T2 WHERE a1 = a2"
    util.verifyExecPlan(sql)
  }

  @Test
  def testAdaptiveJoinWithAdaptiveSkewedOptimizationEnabled(): Unit = {
    util.tableConfig.set(
      OptimizerConfigOptions.TABLE_OPTIMIZER_ADAPTIVE_BROADCAST_JOIN_STRATEGY,
      OptimizerConfigOptions.AdaptiveBroadcastJoinStrategy.NONE)
    util.tableConfig.set(
      OptimizerConfigOptions.TABLE_OPTIMIZER_ADAPTIVE_SKEWED_JOIN_OPTIMIZATION_STRATEGY,
      OptimizerConfigOptions.AdaptiveSkewedJoinOptimizationStrategy.AUTO)
    val sql = "SELECT * FROM T1, T2 WHERE a1 = a2"
    util.verifyExecPlan(sql)
  }
}
