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

class SortTest extends TableTestBase {

  private val util = batchTestUtil()
  util.addTableSource[(Int, Long, String)]("MyTable", 'a, 'b, 'c)

  @Test
  def testNonRangeSortOnSingleFieldWithoutForceLimit(): Unit = {
    util.tableEnv.getConfig.set(TABLE_EXEC_RANGE_SORT_ENABLED, Boolean.box(false))
    util.tableEnv.getConfig
      .set(ExecutionConfigOptions.TABLE_EXEC_SORT_DEFAULT_LIMIT, Integer.valueOf(-1))
    util.verifyExecPlan("SELECT * FROM MyTable ORDER BY a DESC")
  }

  @Test
  def testNonRangeSortOnMultiFieldsWithoutForceLimit(): Unit = {
    util.tableEnv.getConfig.set(TABLE_EXEC_RANGE_SORT_ENABLED, Boolean.box(false))
    util.tableEnv.getConfig
      .set(ExecutionConfigOptions.TABLE_EXEC_SORT_DEFAULT_LIMIT, Integer.valueOf(-1))
    util.verifyExecPlan("SELECT * FROM MyTable ORDER BY a DESC, b")
  }

  @Test
  def testNonRangeSortWithForceLimit(): Unit = {
    util.tableEnv.getConfig.set(TABLE_EXEC_RANGE_SORT_ENABLED, Boolean.box(false))
    util.tableEnv.getConfig
      .set(ExecutionConfigOptions.TABLE_EXEC_SORT_DEFAULT_LIMIT, Integer.valueOf(200))
    util.verifyExecPlan("SELECT * FROM MyTable ORDER BY a DESC")
  }

  @Test
  def testRangeSortWithoutForceLimit(): Unit = {
    util.tableEnv.getConfig.set(TABLE_EXEC_RANGE_SORT_ENABLED, Boolean.box(true))
    util.tableEnv.getConfig
      .set(ExecutionConfigOptions.TABLE_EXEC_SORT_DEFAULT_LIMIT, Integer.valueOf(-1))
    // exec node does not support range sort yet, so we verify rel plan here
    util.verifyRelPlan("SELECT * FROM MyTable ORDER BY a DESC")
  }

  @Test
  def testRangeSortWithForceLimit(): Unit = {
    util.tableEnv.getConfig.set(TABLE_EXEC_RANGE_SORT_ENABLED, Boolean.box(true))
    util.tableEnv.getConfig
      .set(ExecutionConfigOptions.TABLE_EXEC_SORT_DEFAULT_LIMIT, Integer.valueOf(200))
    // exec node does not support range sort yet, so we verify rel plan here
    util.verifyRelPlan("SELECT * FROM MyTable ORDER BY a DESC")
  }
}
