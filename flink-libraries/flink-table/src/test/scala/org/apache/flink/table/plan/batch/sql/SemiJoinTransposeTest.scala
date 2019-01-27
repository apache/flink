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

import org.apache.flink.table.api.TableConfigOptions
import org.apache.flink.table.plan.stats.TableStats
import org.apache.flink.table.runtime.utils.CommonTestData
import org.apache.flink.table.util.TableTestBase
import org.junit.{Before, Test}

class SemiJoinTransposeTest extends TableTestBase {
  private val util = batchTestUtil()

  @Before
  def setup(): Unit = {
    util.addTable("T1", CommonTestData.get3Source(Array("f1", "f2", "f3")))
    util.addTable("T2", CommonTestData.get3Source(Array("f4", "f5", "f6")))
    util.addTable("T3", CommonTestData.get3Source(Array("f7", "f8", "f9")))
    util.tableEnv.alterTableStats("T1", Some(TableStats(10000L)))
    util.tableEnv.alterTableStats("T2", Some(TableStats(20000L)))
    util.tableEnv.alterTableStats("T3", Some(TableStats(30000L)))
    util.tableEnv.getConfig.getConf.setBoolean(
      TableConfigOptions.SQL_OPTIMIZER_JOIN_REORDER_ENABLED, true)
  }

  @Test
  def testSemiJoinTranspose(): Unit = {
    val sqlQuery = "SELECT f1, f5, f9 FROM T1, T2, T3 WHERE T2.f4 IN " +
      "(SELECT f7 FROM T3 WHERE f8 < 10) AND T1.f3 = T2.f6 AND T2.f4 = T3.f7 AND T2.f6 like 'abc'"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testAntiJoinTranspose(): Unit = {
    val sqlQuery = "SELECT f1, f5, f9 FROM T1, T2, T3 WHERE T2.f4 NOT IN " +
      "(SELECT f7 FROM T3 WHERE f8 < 10) AND T1.f3 = T2.f6 AND T2.f4 = T3.f7 AND T2.f6 like 'abc'"
    util.verifyPlan(sqlQuery)
  }
}
