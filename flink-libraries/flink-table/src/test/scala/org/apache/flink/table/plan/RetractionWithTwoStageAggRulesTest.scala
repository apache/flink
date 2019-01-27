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

package org.apache.flink.table.plan

import org.apache.flink.api.common.time.Time
import org.apache.flink.api.scala._
import org.apache.flink.table.api.{AggPhaseEnforcer, TableConfigOptions}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.util.{StreamTableTestUtil, TableTestBase}

import org.junit.{Before, Test}

class RetractionWithTwoStageAggRulesTest extends TableTestBase {

  private var util: StreamTableTestUtil = _

  @Before
  def before(): Unit = {
    util = streamTestUtil()
    util.tableEnv.getConfig
      .withIdleStateRetentionTime(Time.hours(1), Time.hours(2))
    util.tableEnv.getConfig.getConf
      .setLong(TableConfigOptions.SQL_EXEC_MINIBATCH_ALLOW_LATENCY, 1000L)
    util.tableEnv.getConfig.getConf
      .setLong(TableConfigOptions.SQL_EXEC_MINIBATCH_SIZE, 3)
    util.tableEnv.getConfig.getConf.setString(
      TableConfigOptions.SQL_OPTIMIZER_AGG_PHASE_ENFORCER, AggPhaseEnforcer.TWO_PHASE.toString)
  }

  // one level unbounded groupBy
  @Test
  def testGroupBy(): Unit = {
    val table = util.addTable[(String, Int)]('word, 'number)

    val resultTable = table
      .groupBy('word)
      .select('number.count)

    util.verifyPlanAndTrait(resultTable)
  }

  // two level unbounded groupBy
  @Test
  def testTwoGroupBy(): Unit = {
    val table = util.addTable[(String, Int)]('word, 'number)

    val resultTable = table
      .groupBy('word)
      .select('word, 'number.count as 'count)
      .groupBy('count)
      .select('count, 'count.count as 'frequency)

    util.verifyPlanAndTrait(resultTable)
  }

  // test binaryNode
  @Test
  def testBinaryNode(): Unit = {
    val lTable = util.addTable[(String, Int)]('word, 'number)
    val rTable = util.addTable[(String, Long)]('word_r, 'count_r)

    val resultTable = lTable
      .groupBy('word)
      .select('word, 'number.count as 'count)
      .unionAll(rTable)
      .groupBy('count)
      .select('count, 'count.count as 'frequency)

    util.verifyPlanAndTrait(resultTable)
  }
}
