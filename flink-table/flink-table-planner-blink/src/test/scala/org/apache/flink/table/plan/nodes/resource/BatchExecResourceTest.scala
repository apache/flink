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

package org.apache.flink.table.plan.nodes.resource

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.api.{TableConfig, TableConfigOptions, Types}
import org.apache.flink.table.plan.nodes.resource.batch.parallelism.NodeResourceConfig
import org.apache.flink.table.plan.stats.{FlinkStatistic, TableStats}
import org.apache.flink.table.util.TableTestBase

import org.junit.{Before, Test}
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

import java.util.{Arrays => JArrays, Collection => JCollection}

@RunWith(classOf[Parameterized])
class BatchExecResourceTest(inferMode: String) extends TableTestBase {

  private val util = batchTestUtil()

  @Before
  def before(): Unit = {
    util.getTableEnv.getConfig.getConf.setString(
      TableConfigOptions.SQL_RESOURCE_INFER_MODE,
      inferMode
    )
    val table3Stats = new TableStats(5000000)
    util.addTableSource("table3",
      Array[TypeInformation[_]](Types.INT, Types.LONG, Types.STRING),
      Array("a", "b", "c"), FlinkStatistic.builder().tableStats(table3Stats).build())
    val table5Stats = new TableStats(8000000)
    util.addTableSource("Table5",
      Array[TypeInformation[_]](Types.INT, Types.LONG, Types.INT, Types.STRING, Types.LONG),
      Array("d", "e", "f", "g", "h"), FlinkStatistic.builder().tableStats(table5Stats).build())
    BatchExecResourceTest.setResourceConfig(util.getTableEnv.getConfig)
  }

  @Test
  def testSourcePartitionMaxNum(): Unit = {
    util.getTableEnv.getConfig.getConf.setInteger(
      TableConfigOptions.SQL_RESOURCE_INFER_SOURCE_PARALLELISM_MAX,
      2
    )
    val sqlQuery = "SELECT * FROM table3"
    util.verifyResource(sqlQuery)
  }

  @Test
  def testSortLimit(): Unit = {
    val sqlQuery = "SELECT sum(a) as sum_a, c FROM table3 group by c order by c limit 2"
    util.verifyResource(sqlQuery)
  }

  @Test
  def testConfigSourceParallelism(): Unit = {
    util.getTableEnv.getConfig.getConf.setInteger(
      TableConfigOptions.SQL_RESOURCE_SOURCE_PARALLELISM, 100)
    val sqlQuery = "SELECT sum(a) as sum_a, c FROM table3 group by c order by c limit 2"
    util.verifyResource(sqlQuery)
  }

  @Test
  // TODO check when range partition added.
  def testRangePartition(): Unit ={
    util.getTableEnv.getConfig.getConf.setBoolean(
      TableConfigOptions.SQL_EXEC_SORT_RANGE_ENABLED,
      true)
    val sqlQuery = "SELECT * FROM Table5 where d < 100 order by e"
    util.verifyResource(sqlQuery)
  }

  @Test
  def testUnionQuery(): Unit = {
    val statsOfTable4 = new TableStats(100L)
    util.addTableSource("table4",
      Array[TypeInformation[_]](Types.INT, Types.LONG, Types.STRING),
      Array("a", "b", "c"),
      FlinkStatistic.builder().tableStats(statsOfTable4).build())

    val sqlQuery = "SELECT sum(a) as sum_a, g FROM " +
        "(SELECT a, b, c FROM table3 UNION ALL SELECT a, b, c FROM table4), Table5 " +
        "WHERE b = e group by g"
    util.verifyResource(sqlQuery)
  }
}

object BatchExecResourceTest {

  @Parameterized.Parameters(name = "{0}")
  def parameters(): JCollection[String] = JArrays.asList(
    NodeResourceConfig.InferMode.NONE.toString,
    NodeResourceConfig.InferMode.ONLY_SOURCE.toString)

  def setResourceConfig(tableConfig: TableConfig): Unit = {
    tableConfig.getConf.setInteger(
      TableConfigOptions.SQL_RESOURCE_DEFAULT_PARALLELISM,
      18)
    tableConfig.getConf.setInteger(
      TableConfigOptions.SQL_RESOURCE_INFER_SOURCE_PARALLELISM_MAX,
      1000)
    tableConfig.getConf.setLong(
      TableConfigOptions.SQL_RESOURCE_INFER_ROWS_PER_PARTITION,
      1000000
    )
  }
}

