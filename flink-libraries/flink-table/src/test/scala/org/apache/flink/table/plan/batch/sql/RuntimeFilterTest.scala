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
import org.apache.flink.table.plan.rules.physical.batch.runtimefilter.InsertRuntimeFilterRule
import org.apache.flink.table.plan.stats.{ColumnStats, TableStats}
import org.apache.flink.table.runtime.utils.CommonTestData
import org.apache.flink.table.util.TableTestBase
import org.junit.{Before, Test}

import scala.collection.JavaConversions._

class RuntimeFilterTest extends TableTestBase {

  private val util = batchTestUtil()

  @Before
  def before(): Unit = {
    util.getTableEnv.getConfig.getConf.setBoolean(
      TableConfigOptions.SQL_OPTIMIZER_JOIN_REORDER_ENABLED, true)
    util.tableEnv.getConfig.getConf.setBoolean(
      TableConfigOptions.SQL_EXEC_RUNTIME_FILTER_ENABLED, true)
    util.addTable("x", CommonTestData.get3Source(Array("a", "b", "c")))
    util.addTable("y", CommonTestData.get3Source(Array("a", "b", "c")))

    util.tableEnv.alterTableStats("x", Some(TableStats(10000000L, Map[String, ColumnStats](
      "a" -> columnStats(5L), "b" -> columnStats(10000000L), "c" -> columnStats(10L)))))
    util.tableEnv.alterTableStats("y", Some(TableStats(1000000000L, Map[String, ColumnStats](
      "a" -> columnStats(100L), "b" -> columnStats(1000000000L), "c" -> columnStats(10L)))))
    util.disableBroadcastHashJoin()
    InsertRuntimeFilterRule.resetBroadcastIdCounter()
  }

  private def columnStats(ndv: Long): ColumnStats = ColumnStats(ndv, 0L, 1D, 1, 100000000, 1)

  @Test
  def testRf(): Unit = {
    val query = "SELECT * FROM x, y WHERE x.a = y.a"
    util.verifyPlan(query)
  }

  @Test
  def testMultiFields(): Unit = {
    val query = "SELECT * FROM x, y WHERE x.a = y.a and x.b = y.b"
    util.verifyPlan(query)
  }

  @Test
  def testRemove(): Unit = {
    val query = "SELECT * FROM x, y WHERE x.a = y.a and x.b = y.b and x.c = y.c"
    util.verifyPlan(query)
  }

  @Test
  def testPushDownRf(): Unit = {
    val query = "SELECT * FROM x, (select b, count(*) from y group by b) z WHERE x.b = z.b"
    util.verifyPlan(query)
  }

  @Test
  def testPushDownRfBuilder(): Unit = {
    val query = "SELECT * FROM (SELECT y.b FROM x, y WHERE x.b = y.b) z, y WHERE y.b = z.b"
    util.verifyPlan(query)
  }

  @Test
  def testPushDownRfBuilder2(): Unit = {
    // TODO: why ndv(x.b) is not equal to y.b?
    util.tableEnv.getConfig.getConf.setDouble(
      TableConfigOptions.SQL_EXEC_RUNTIME_FILTER_BUILDER_PUSH_DOWN_RATIO_MAX, 1.8)
    val query = "SELECT * FROM (SELECT x.b FROM x, y WHERE x.b = y.b) z, y WHERE y.b = z.b"
    util.verifyPlan(query)
  }
}
