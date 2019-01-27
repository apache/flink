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

import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.api.{TableConfigOptions, TableSchema}
import org.apache.flink.table.api.types.{DataType, DataTypes}
import org.apache.flink.table.plan.stats.{ColumnStats, TableStats}
import org.apache.flink.table.sources.BatchTableSource
import org.apache.flink.table.util.{TableSchemaUtil, TableTestBase}
import org.apache.flink.types.Row
import org.junit.{Before, Test}

import scala.collection.JavaConversions._

/**
  * Test for testing aggregate type based on cbo
  */
class AggregateTest extends TableTestBase {

  private val util = batchTestUtil()

  @Before
  def before(): Unit = {
    val tableSchema = new TableSchema(
      Array("customerId", "productId", "colLarge"),
      Array(
        DataTypes.LONG,
        DataTypes.INT,
        DataTypes.STRING))
    val colStats = Map[String, ColumnStats](
      "customerId" -> ColumnStats(10000000L, 1L, 8D, 8, 5, -5),
      "productId" -> ColumnStats(50000L, 0L, 4D, 32, 6.1D, 0D),
      "colLarge" -> ColumnStats(80000L, 0L, 1024D, 32, 6.1D, 0D))
    val table = new BatchTableSource[Row] {
      override def getReturnType: DataType =
        DataTypes.createRowType(
          tableSchema.getTypes.asInstanceOf[Array[DataType]],
          tableSchema.getColumnNames)

      override def getTableStats: TableStats = TableStats(10000000L, colStats)

      override def getBoundedStream(streamEnv: StreamExecutionEnvironment): DataStream[Row] = null

      /** Returns the table schema of the table source */
      override def getTableSchema = TableSchemaUtil.fromDataType(getReturnType)

      override def explainSource(): String = ""
    }
    util.addTable("t1", table)
    // sets the table memory size of hashAgg operator to 1MB
    util.getTableEnv.getConfig.getConf
      .setInteger(TableConfigOptions.SQL_RESOURCE_HASH_AGG_TABLE_MEM, 1)
  }

  @Test
  def testHashAggregateWithSmallData(): Unit = {
    // HashTable is small than threshold, so hashAggregate is chosen
    val sqlQuery = "select count(colLarge) from t1 group by productId"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testHashAggregateWithHighAggRatio(): Unit = {
    // Although hashTable is bigger than threshold, but ndv/inputRowCount is smaller than threshold,
    // so HashAgg is chosen
    val sqlQuery = "select max(productId) from t1 group by colLarge"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testSortAggregateWithLowAggRatio(): Unit = {
    // colLarge typed String is not fixed length in BinaryRow,
    // ndv/inputRowCount is bigger than threshold, so SortAgg with one phase is chosen
    val sqlQuery = "select max(colLarge) from t1 group by customerId"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testSortAggregateWithUnFixedLengthAggCall(): Unit = {
    // the result type of max(colLarge) is not fixed length, so hashAggregate cannot be chosen
    val sqlQuery = "select max(colLarge) from t1"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testAggregateWithoutGroupKeys(): Unit = {
    // there is no groupBy keys, so the costs of the hashAgg and sortAgg are equal,
    // sortAgg is chosen because of the VocannoPlanner's default strategy
    val sqlQuery = "select count(colLarge) from t1"
    util.verifyPlan(sqlQuery)
  }

}
