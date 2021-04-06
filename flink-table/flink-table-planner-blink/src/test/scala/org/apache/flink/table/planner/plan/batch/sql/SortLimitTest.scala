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

package org.apache.flink.table.planner.plan.batch.sql

import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.config.ExecutionConfigOptions
import org.apache.flink.table.planner.plan.rules.physical.batch.BatchPhysicalSortRule.TABLE_EXEC_RANGE_SORT_ENABLED
import org.apache.flink.table.planner.utils.TableTestBase

import org.junit.Test

class SortLimitTest extends TableTestBase {

  private val util = batchTestUtil()
  util.addTableSource[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
  util.tableEnv.getConfig.getConfiguration.setInteger(
    ExecutionConfigOptions.TABLE_EXEC_SORT_DEFAULT_LIMIT, 200)

  @Test
  def testNonRangeSortWithoutOffset(): Unit = {
    util.tableEnv.getConfig.getConfiguration.setBoolean(TABLE_EXEC_RANGE_SORT_ENABLED, false)
    util.verifyExecPlan("SELECT * FROM MyTable ORDER BY a DESC LIMIT 5")
  }

  @Test
  def testNonRangeSortWithLimit0(): Unit = {
    util.tableEnv.getConfig.getConfiguration.setBoolean(TABLE_EXEC_RANGE_SORT_ENABLED, false)
    util.verifyExecPlan("SELECT * FROM MyTable ORDER BY a DESC LIMIT 0")
  }

  @Test
  def testNonRangeSortOnlyWithOffset(): Unit = {
    util.tableEnv.getConfig.getConfiguration.setBoolean(TABLE_EXEC_RANGE_SORT_ENABLED, false)
    util.verifyExecPlan("SELECT * FROM MyTable ORDER BY a DESC OFFSET 10 ROWS")
  }

  @Test
  def testNoneRangeSortWithOffsetLimit(): Unit = {
    util.tableEnv.getConfig.getConfiguration.setBoolean(TABLE_EXEC_RANGE_SORT_ENABLED, false)
    util.verifyExecPlan("SELECT * FROM MyTable ORDER BY a DESC, b LIMIT 10 OFFSET 1")
  }

  @Test
  def testNoneRangeSortWithOffsetLimit0(): Unit = {
    util.tableEnv.getConfig.getConfiguration.setBoolean(TABLE_EXEC_RANGE_SORT_ENABLED, false)
    util.verifyExecPlan("SELECT * FROM MyTable ORDER BY a DESC, b LIMIT 0 OFFSET 1")
  }

  @Test
  def testRangeSortOnWithoutOffset(): Unit = {
    util.tableEnv.getConfig.getConfiguration.setBoolean(TABLE_EXEC_RANGE_SORT_ENABLED, true)
    util.verifyExecPlan("SELECT * FROM MyTable ORDER BY a DESC LIMIT 5")
  }

  @Test
  def testRangeSortOnWithLimit0(): Unit = {
    util.tableEnv.getConfig.getConfiguration.setBoolean(TABLE_EXEC_RANGE_SORT_ENABLED, true)
    util.verifyExecPlan("SELECT * FROM MyTable ORDER BY a DESC LIMIT 0")
  }

  @Test
  def testRangeSortOnlyWithOffset(): Unit = {
    util.tableEnv.getConfig.getConfiguration.setBoolean(TABLE_EXEC_RANGE_SORT_ENABLED, true)
    util.verifyExecPlan("SELECT * FROM MyTable ORDER BY a DESC OFFSET 10 ROWS")
  }

  @Test
  def testRangeSortWithOffsetLimit(): Unit = {
    util.tableEnv.getConfig.getConfiguration.setBoolean(TABLE_EXEC_RANGE_SORT_ENABLED, true)
    util.verifyExecPlan("SELECT * FROM MyTable ORDER BY a DESC, b LIMIT 10 OFFSET 1")
  }

  @Test
  def testRangeSortWithOffsetLimit0(): Unit = {
    util.tableEnv.getConfig.getConfiguration.setBoolean(TABLE_EXEC_RANGE_SORT_ENABLED, true)
    util.verifyExecPlan("SELECT * FROM MyTable ORDER BY a DESC, b LIMIT 0 OFFSET 1")
  }
}
