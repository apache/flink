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

package org.apache.flink.table.planner.plan.stream.table

import org.apache.flink.api.common.time.Time
import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.config.{ExecutionConfigOptions, OptimizerConfigOptions}
import org.apache.flink.table.planner.utils.{AggregatePhaseStrategy, StreamTableTestUtil, TableTestBase}

import java.time.Duration

import org.junit.{Before, Test}

class TwoStageAggregateTest extends TableTestBase {

  private var util: StreamTableTestUtil = _
  @Before
  def before(): Unit = {
    util = streamTestUtil()
    util.tableEnv.getConfig
      .setIdleStateRetentionTime(Time.hours(1), Time.hours(2))
    util.tableEnv.getConfig.getConfiguration
      .set(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ALLOW_LATENCY, Duration.ofSeconds(1))
    util.tableEnv.getConfig.getConfiguration
        .setBoolean(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED, true)
    util.tableEnv.getConfig.getConfiguration
      .setLong(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_SIZE, 3)
    util.tableEnv.getConfig.getConfiguration.setString(
      OptimizerConfigOptions.TABLE_OPTIMIZER_AGG_PHASE_STRATEGY,
      AggregatePhaseStrategy.TWO_PHASE.toString)
  }

  @Test
  def testGroupAggregate(): Unit = {
    val table = util.addTableSource[(Long, Int, String)]('a, 'b, 'c)
    val resultTable = table
      .groupBy('b)
      .select('a.count)

    util.verifyPlan(resultTable)
  }

  @Test
  def testGroupAggregateWithConstant1(): Unit = {
    val table = util.addTableSource[(Long, Int, String)]('a, 'b, 'c)
    val resultTable = table
      .select('a, 4 as 'four, 'b)
      .groupBy('four, 'a)
      .select('four, 'b.sum)

    util.verifyPlan(resultTable)
  }

  @Test
  def testGroupAggregateWithConstant2(): Unit = {
    val table = util.addTableSource[(Long, Int, String)]('a, 'b, 'c)
    val resultTable = table
      .select('b, 4 as 'four, 'a)
      .groupBy('b, 'four)
      .select('four, 'a.sum)

    util.verifyPlan(resultTable)
  }

  @Test
  def testGroupAggregateWithExpressionInSelect(): Unit = {
    val table = util.addTableSource[(Long, Int, String)]('a, 'b, 'c)
    val resultTable = table
      .select('a as 'a, 'b % 3 as 'd, 'c as 'c)
      .groupBy('d)
      .select('c.min, 'a.avg)

    util.verifyPlan(resultTable)
  }

  @Test
  def testGroupAggregateWithFilter(): Unit = {
    val table = util.addTableSource[(Long, Int, String)]('a, 'b, 'c)
    val resultTable = table
      .groupBy('b)
      .select('b, 'a.sum)
      .where('b === 2)

    util.verifyPlan(resultTable)
  }

  @Test
  def testGroupAggregateWithAverage(): Unit = {
    val table = util.addTableSource[(Long, Int, String)]('a, 'b, 'c)
    val resultTable = table
      .groupBy('b)
      .select('b, 'a.cast(DataTypes.DOUBLE()).avg)

    util.verifyPlan(resultTable)
  }
}
