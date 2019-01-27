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
import org.apache.flink.table.api.{TableConfig, TableConfigOptions}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.util.{BatchTableTestUtil, TableTestBase}
import org.junit.{Before, Test}

class SortTest extends TableTestBase {

  val util: BatchTableTestUtil = batchTestUtil()
  val tableConfig: TableConfig = util.tableEnv.getConfig

  @Before
  def setup(): Unit = {
    tableConfig.getConf.setBoolean(TableConfigOptions.SQL_EXEC_SORT_RANGE_ENABLED, true)
    util.addTable[(Int, Long, Long)]("MyTable", 'a, 'b, 'c)
  }

  @Test
  def testSingleFieldSort(): Unit = {
    val sqlQuery = "SELECT a, c FROM MyTable ORDER BY a DESC"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testSortWithForcedSinglePartition(): Unit = {
    tableConfig.getConf.setBoolean(TableConfigOptions.SQL_EXEC_SORT_RANGE_ENABLED, false)
    tableConfig.getConf.setInteger(TableConfigOptions.SQL_EXEC_SORT_DEFAULT_LIMIT, -1)

    val sqlQuery = "SELECT a, c FROM MyTable ORDER BY a DESC"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testSortWithForcedSinglePartitionAndLimit(): Unit = {
    val oldSortEnable = tableConfig.getConf.getBoolean(
      TableConfigOptions.SQL_EXEC_SORT_RANGE_ENABLED)
    val oldLimitValue = tableConfig.getConf.getInteger(
      TableConfigOptions.SQL_EXEC_SORT_DEFAULT_LIMIT)
    tableConfig.getConf.setBoolean(TableConfigOptions.SQL_EXEC_SORT_RANGE_ENABLED, false)
    tableConfig.getConf.setInteger(TableConfigOptions.SQL_EXEC_SORT_DEFAULT_LIMIT, 200)

    val sqlQuery = "SELECT a, c FROM MyTable ORDER BY a DESC"
    util.verifyPlan(sqlQuery)
    tableConfig.getConf.setBoolean(TableConfigOptions.SQL_EXEC_SORT_RANGE_ENABLED, oldSortEnable)
    tableConfig.getConf.setInteger(TableConfigOptions.SQL_EXEC_SORT_DEFAULT_LIMIT, oldLimitValue)
  }

  @Test
  def testMultiFieldSort(): Unit = {
    val sqlQuery = "SELECT a, b FROM MyTable ORDER BY a DESC, b"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testSortWithSameFieldGroupBy(): Unit = {
    val sqlQuery = "SELECT a, b, sum(c) FROM MyTable GROUP by a, b ORDER BY a, b"
    util.verifyPlan(sqlQuery)
  }
}
