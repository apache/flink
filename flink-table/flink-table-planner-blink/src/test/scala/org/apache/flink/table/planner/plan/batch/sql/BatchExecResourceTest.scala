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

import org.apache.flink.api.common.typeinfo.{TypeInformation, Types}
import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.api.config.ExecutionConfigOptions
import org.apache.flink.table.delegation.Planner
import org.apache.flink.table.plan.stats.TableStats
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode
import org.apache.flink.table.planner.plan.stats.FlinkStatistic
import org.apache.flink.table.planner.utils.{TableConfigUtils, TableTestBase, TableTestUtil}

import org.junit.{Assert, Before, Test}

class BatchExecResourceTest extends TableTestBase {

  private val util = batchTestUtil()

  @Before
  def before(): Unit = {
    val table3Stats = new TableStats(5000000)
    util.addInputFormatTableSource("table3",
      Array[TypeInformation[_]](Types.INT, Types.LONG, Types.STRING),
      Array("a", "b", "c"), FlinkStatistic.builder().tableStats(table3Stats).build())
    BatchExecResourceTest.setResourceConfig(util.getTableEnv.getConfig)
  }

  @Test
  def testSourceParallelismNoneInfer(): Unit = {
    util.getPlanner.getExecEnv.setParallelism(17)
    util.getTableEnv.getConfig.getConfiguration.setString(
      ExecutionConfigOptions.TABLE_EXEC_RESOURCE_INFER_MODE,
      TableConfigUtils.InferMode.NONE.toString
    )
    val sql = "SELECT * FROM table3"
    assertSourceParallelism(sql, 17)
  }

  @Test
  def testSourceParallelism(): Unit = {
    util.getTableEnv.getConfig.getConfiguration.setString(
      ExecutionConfigOptions.TABLE_EXEC_RESOURCE_INFER_MODE,
      TableConfigUtils.InferMode.ONLY_SOURCE.toString
    )
    val sql = "SELECT * FROM table3"
    assertSourceParallelism(sql, 5)
  }

  @Test
  def testSourceParallelismMaxNum(): Unit = {
    util.getTableEnv.getConfig.getConfiguration.setString(
      ExecutionConfigOptions.TABLE_EXEC_RESOURCE_INFER_MODE,
      TableConfigUtils.InferMode.ONLY_SOURCE.toString
    )
    util.getTableEnv.getConfig.getConfiguration.setInteger(
      ExecutionConfigOptions.TABLE_EXEC_RESOURCE_INFER_SOURCE_PARALLELISM_MAX,
      2
    )
    val sql = "SELECT * FROM table3"
    assertSourceParallelism(sql, 2)
  }

  private def assertSourceParallelism(sql: String, parallelism: Int): Unit = {
    val table = util.tableEnv.sqlQuery(sql)
    val relNode = util.getPlanner.optimize(TableTestUtil.toRelNode(table))
    val execNode = util.getPlanner.translateToExecNodePlan(Seq(relNode)).get(0)
    val transform = execNode.asInstanceOf[ExecNode[Planner, _]].translateToPlan(util.getPlanner)
    Assert.assertEquals(parallelism, transform.getParallelism)
  }
}

object BatchExecResourceTest {

  def setResourceConfig(tableConfig: TableConfig): Unit = {
    tableConfig.getConfiguration.setInteger(
      ExecutionConfigOptions.TABLE_EXEC_RESOURCE_INFER_SOURCE_PARALLELISM_MAX,
      1000)
    tableConfig.getConfiguration.setLong(
      ExecutionConfigOptions.TABLE_EXEC_RESOURCE_INFER_ROWS_PER_PARTITION,
      1000000
    )
  }
}
