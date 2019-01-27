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

package org.apache.flink.table.resource.batch.parallelism.autoconf

import org.apache.flink.table.api.{TableConfigOptions, TableSchema}
import org.apache.flink.table.resource.batch.BatchExecResourceTest
import org.apache.flink.table.tpc.{STATS_MODE, TpcHSchemaProvider, TpchTableStatsProvider}
import org.apache.flink.table.util.{BatchTableTestUtil, NodeResourceUtil, TableTestBase}

import org.junit.{Before, Test}

class BatchExecResourceAdjustTest extends TableTestBase {

  private var util: BatchTableTestUtil = _

  @Before
  def before(): Unit = {
    util = batchTestUtil()
    util.getTableEnv.getConfig.setSubsectionOptimization(false)
    util.getTableEnv.getConfig.getConf.setString(
      TableConfigOptions.SQL_RESOURCE_INFER_MODE,
      NodeResourceUtil.InferMode.ALL.toString
    )
    BatchExecResourceTest.setResourceConfig(util.getTableEnv.getConfig)
  }

  @Test
  def testAdjustAccordingToFewCPU(): Unit = {
    setAdjustResource(util, 100)
    testResource(util)
  }

  @Test
  def testAdjustAccordingToMuchResource(): Unit = {
    setAdjustResource(util, Double.MaxValue)
    testResource(util)
  }

  private def testResource(util: BatchTableTestUtil): Unit = {
    val customerSchema = TpcHSchemaProvider.getSchema("customer")
    val colStatsOfCustomer =
      TpchTableStatsProvider.getTableStatsMap(1000, STATS_MODE.FULL).get("customer")
    util.addTableSource("customer",
      new TableSchema(customerSchema.getFieldNames,
        customerSchema.getFieldTypes),
      false, colStatsOfCustomer.get)

    val ordersSchema = TpcHSchemaProvider.getSchema("orders")
    val colStastOfOrders =
      TpchTableStatsProvider.getTableStatsMap(1000, STATS_MODE.FULL).get("orders")
    util.addTableSource("orders",
      new TableSchema(ordersSchema.getFieldNames,
        ordersSchema.getFieldTypes),
      false, colStastOfOrders.get)
    val lineitemSchema = TpcHSchemaProvider.getSchema("lineitem")
    val colStatsOfLineitem =
      TpchTableStatsProvider.getTableStatsMap(1000, STATS_MODE.FULL).get("lineitem")
    util.addTableSource("lineitem",
      new TableSchema(lineitemSchema.getFieldNames,
        lineitemSchema.getFieldTypes),
      false, colStatsOfLineitem.get)

    val sqlQuery = "select c.c_name, sum(l.l_quantity)" +
        " from customer c, orders o, lineitem l" +
        " where o.o_orderkey in ( " +
        " select l_orderkey from lineitem group by l_orderkey having" +
        " sum(l_quantity) > 300)" +
        " and c.c_custkey = o.o_custkey and o.o_orderkey = l.l_orderkey" +
        " group by c.c_name"
    util.verifyResource(sqlQuery)
  }

  private def setAdjustResource(
      util: BatchTableTestUtil,
      cpu: Double): Unit = {
    util.getTableEnv.getConfig.getConf.setDouble(
      TableConfigOptions.SQL_RESOURCE_RUNNING_UNIT_CPU_TOTAL,
      cpu
    )
  }
}
