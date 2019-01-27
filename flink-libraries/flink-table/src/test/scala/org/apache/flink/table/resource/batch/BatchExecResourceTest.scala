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

package org.apache.flink.table.resource.batch

import java.util.{Arrays => JArrays, Collection => JCollection}
import org.apache.flink.table.api.{TableConfig, TableConfigOptions, TableSchema}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.types.DataTypes
import org.apache.flink.table.plan.stats.{ColumnStats, TableStats}
import org.apache.flink.table.sinks.csv.CsvTableSink
import org.apache.flink.table.tpc.{STATS_MODE, TpcHSchemaProvider, TpchTableStatsProvider}
import org.apache.flink.table.util.{NodeResourceUtil, TableTestBase}

import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.{Before, Test}

import _root_.scala.collection.JavaConversions._

@RunWith(classOf[Parameterized])
class BatchExecResourceTest(inferMode: String) extends TableTestBase {

  private val util = batchTestUtil()

  @Before
  def before(): Unit = {
    util.getTableEnv.getConfig.setSubsectionOptimization(false)
    util.getTableEnv.getConfig.getConf.setString(
      TableConfigOptions.SQL_RESOURCE_INFER_MODE,
      inferMode
    )
    val tableSchema1 = new TableSchema(
      Array("a", "b", "c"),
      Array(
        DataTypes.INT,
        DataTypes.LONG,
        DataTypes.STRING))
    util.addTableSource("SmallTable3", tableSchema1)
    val tableSchema2 = new TableSchema(
      Array("d", "e", "f", "g", "h"),
      Array(
        DataTypes.INT,
        DataTypes.LONG,
        DataTypes.INT,
        DataTypes.STRING,
        DataTypes.LONG))
    util.addTableSource("Table5", tableSchema2)
    BatchExecResourceTest.setResourceConfig(util.getTableEnv.getConfig)
  }

  @Test
  def testSourcePartitionMaxNum(): Unit = {
    util.getTableEnv.getConfig.getConf.setInteger(
      TableConfigOptions.SQL_RESOURCE_INFER_SOURCE_PARALLELISM_MAX,
      300
    )
    val sqlQuery = "SELECT * FROM SmallTable3"
    util.verifyResource(sqlQuery)
  }

  @Test
  def testSortLimit(): Unit = {
    val sqlQuery = "SELECT sum(a) as sum_a, c FROM SmallTable3 group by c order by c limit 2"
    util.verifyResource(sqlQuery)
  }

  @Test
  def testConfigSourceParallelism(): Unit = {
    util.getTableEnv.getConfig.getConf.setInteger(
      TableConfigOptions.SQL_RESOURCE_SOURCE_PARALLELISM, 100)
    val sqlQuery = "SELECT sum(a) as sum_a, c FROM SmallTable3 group by c order by c limit 2"
    util.verifyResource(sqlQuery)
  }

  @Test
  def testRangePartition(): Unit ={
    util.getTableEnv.getConfig.getConf.setBoolean(
      TableConfigOptions.SQL_EXEC_SORT_RANGE_ENABLED,
      true)
    val sqlQuery = "SELECT * FROM Table5 where d < 100 order by e"
    util.verifyResource(sqlQuery)
  }

  @Test
  def testUnionQuery(): Unit = {
    val table3Schema = new TableSchema(
      Array("a", "b", "c"),
      Array(
        DataTypes.INT,
        DataTypes.LONG,
        DataTypes.STRING))

    val colStatsOfTable3 = TableStats(100L, Map[java.lang.String, ColumnStats](
      "a" -> ColumnStats(3L, 1L, 10000D * NodeResourceUtil.SIZE_IN_MB, 8, 5, -5),
      "b" -> ColumnStats(5L, 0L, 10000D * NodeResourceUtil.SIZE_IN_MB, 32, 6.1D, 0D),
      "c" -> ColumnStats(5L, 0L, 10000D * NodeResourceUtil.SIZE_IN_MB, 32, 6.1D, 0D)))
    util.addTableSource("Table3", table3Schema, true, colStatsOfTable3)

    val sqlQuery = "SELECT sum(a) as sum_a, g FROM " +
        "(SELECT a, b, c FROM SmallTable3 UNION ALL SELECT a, b, c FROM Table3), Table5 " +
        "WHERE b = e group by g"
    util.verifyResource(sqlQuery)
  }

  @Test
  def testSubsectionOptimization(): Unit = {
    util.getTableEnv.getConfig.setSubsectionOptimization(true)
    val query = "SELECT SUM(a) AS sum_a, c FROM SmallTable3 GROUP BY c "
    val table = util.getTableEnv.sqlQuery(query)
    val result1 = table.select('sum_a.sum as 'total_sum)
    val result2 = table.select('sum_a.min as 'total_min)
    result1.writeToSink(new CsvTableSink("/tmp/1"))
    result2.writeToSink(new CsvTableSink("/tmp/2"))
    util.verifyResourceWithSubsectionOptimization()
  }

  @Test
  def testAggregateWithJoin(): Unit = {
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

  @Test
  def testLimitPushDown(): Unit = {
    val lineitemSchema = TpcHSchemaProvider.getSchema("lineitem")
    val colStatsOfLineitem =
      TpchTableStatsProvider.getTableStatsMap(1000, STATS_MODE.FULL).get("lineitem")
    util.addTableSource("lineitem",
      new TableSchema(lineitemSchema.getFieldNames,
        lineitemSchema.getFieldTypes),
      true, colStatsOfLineitem.get)

    val sqlQuery = "select * from lineitem limit 1"
    util.verifyResource(sqlQuery)
  }

  @Test
  def testEnvParallelism(): Unit ={
    util.getTableEnv.getConfig.getConf.setInteger(
      TableConfigOptions.SQL_RESOURCE_DEFAULT_PARALLELISM,
      -1)
    util.tableEnv.streamEnv.setParallelism(73)
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

}

object BatchExecResourceTest {

  @Parameterized.Parameters(name = "{0}")
  def parameters(): JCollection[String] = JArrays.asList(
    NodeResourceUtil.InferMode.NONE.toString,
    NodeResourceUtil.InferMode.ONLY_SOURCE.toString,
    NodeResourceUtil.InferMode.ALL.toString)

  def setResourceConfig(tableConfig: TableConfig): Unit = {
    tableConfig.getConf.setInteger(
      TableConfigOptions.SQL_RESOURCE_DEFAULT_PARALLELISM,
      18)
    tableConfig.getConf.setLong(
      TableConfigOptions.SQL_RESOURCE_INFER_ROWS_PER_PARTITION,
      2L)
    tableConfig.getConf.setInteger(
      TableConfigOptions.SQL_RESOURCE_INFER_SOURCE_MB_PER_PARTITION,
      50000)
    tableConfig.getConf.setInteger(
      TableConfigOptions.SQL_RESOURCE_INFER_SOURCE_PARALLELISM_MAX,
      1000)
    tableConfig.getConf.setInteger(
      TableConfigOptions.SQL_RESOURCE_INFER_OPERATOR_PARALLELISM_MAX,
      800)
    tableConfig.getConf.setInteger(
      NodeResourceUtil.SQL_RESOURCE_INFER_OPERATOR_PARALLELISM_MIN,
      20
    )
    tableConfig.getConf.setDouble(
      TableConfigOptions.SQL_RESOURCE_DEFAULT_CPU,
      0.3
    )
    tableConfig.getConf.setInteger(
      TableConfigOptions.SQL_RESOURCE_SOURCE_DEFAULT_MEM,
      52
    )
    tableConfig.getConf.setInteger(
      TableConfigOptions.SQL_RESOURCE_SOURCE_DIRECT_MEM,
      24
    )
    tableConfig.getConf.setInteger(
      TableConfigOptions.SQL_RESOURCE_DEFAULT_DIRECT_MEM,
      35
    )
    tableConfig.getConf.setInteger(
      TableConfigOptions.SQL_RESOURCE_DEFAULT_MEM,
      46
    )
    tableConfig.getConf.setInteger(
      TableConfigOptions.SQL_RESOURCE_HASH_AGG_TABLE_MEM,
      33
    )
    tableConfig.getConf.setInteger(
      TableConfigOptions.SQL_RESOURCE_HASH_AGG_TABLE_PREFER_MEM,
      64
    )
    tableConfig.getConf.setInteger(
      TableConfigOptions.SQL_RESOURCE_HASH_AGG_TABLE_MAX_MEM,
      128
    )
    tableConfig.getConf.setInteger(
      TableConfigOptions.SQL_RESOURCE_HASH_JOIN_TABLE_MEM,
      43
    )
    tableConfig.getConf.setInteger(
      TableConfigOptions.SQL_RESOURCE_HASH_JOIN_TABLE_PREFER_MEM,
      47
    )
    tableConfig.getConf.setInteger(
      TableConfigOptions.SQL_RESOURCE_HASH_JOIN_TABLE_MAX_MEM,
      128
    )
    tableConfig.getConf.setInteger(
      TableConfigOptions.SQL_RESOURCE_SORT_BUFFER_MEM,
      53
    )
    tableConfig.getConf.setInteger(
      TableConfigOptions.SQL_RESOURCE_SORT_BUFFER_PREFER_MEM,
      57
    )
    tableConfig.getConf.setInteger(
      TableConfigOptions.SQL_RESOURCE_SORT_BUFFER_MAX_MEM,
      57
    )
    tableConfig.getConf.setLong(
      TableConfigOptions.SQL_RESOURCE_INFER_ROWS_PER_PARTITION,
      1000000
    )
    tableConfig.getConf.setDouble(
      NodeResourceUtil.SQL_RESOURCE_INFER_MEM_RESERVE_PREFER_DISCOUNT,
      0.5
    )
    tableConfig.getConf.setInteger(
      TableConfigOptions.SQL_RESOURCE_INFER_OPERATOR_MEM_MAX,
      470
    )
    tableConfig.getConf.setInteger(
      NodeResourceUtil.SQL_RESOURCE_INFER_OPERATOR_MEMORY_MIN,
      32
    )
  }
}
