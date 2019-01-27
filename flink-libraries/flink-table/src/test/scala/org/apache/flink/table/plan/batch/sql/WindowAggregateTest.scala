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

import java.sql.Timestamp

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.api.TableSchema
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.types.{DataType, DataTypes}
import org.apache.flink.table.functions.aggregate.CountAggFunction
import org.apache.flink.table.plan.stats.{ColumnStats, TableStats}
import org.apache.flink.table.sources.BatchTableSource
import org.apache.flink.table.util.{TableSchemaUtil, TableTestBase}
import org.apache.flink.types.Row
import org.junit.{Before, Test}

import scala.collection.JavaConversions._

class WindowAggregateTest extends TableTestBase {

  private val util = batchTestUtil()

  @Before
  def before(): Unit = {
    // for udagg
    util.getTableEnv.registerFunction("countFun", new CountAggFunction())

    // common case
    util.addTable[(Int, Timestamp, Int, Long)]("MyTable", 'a, 'b, 'c, 'd)

    // for local-global window agg
    val colStats = Map[String, ColumnStats](
      "ts" -> ColumnStats(9000000L, 1L, 8D, 8, null, null),
      "customerId" -> ColumnStats(10000000L, 1L, 8D, 8, 5, -5),
      "productId" -> ColumnStats(500L, 0L, 4D, 32, 6.1D, 0D),
      "colLarge" -> ColumnStats(800000L, 0L, 1024D, 32, 6.1D, 0D))
    val tableSchema = new TableSchema(
      Array("ts", "customerId", "productId", "colLarge"),
      Array(DataTypes.TIMESTAMP, DataTypes.LONG, DataTypes.INT, DataTypes.STRING))
    val table = new BatchTableSource[Row] {
      override def getReturnType: DataType = DataTypes.createRowType(
          tableSchema.getTypes.asInstanceOf[Array[DataType]], tableSchema.getColumnNames)

      override def getTableStats: TableStats = TableStats(10000000L, colStats)

      override def getBoundedStream(
          streamEnv: StreamExecutionEnvironment): DataStream[Row] = null

      /** Returns the table schema of the table source */
      override def getTableSchema = TableSchemaUtil.fromDataType(getReturnType)

      override def explainSource(): String = ""
    }
    util.addTable("t1", table)
  }


  @Test
  def testNoGroupingSlidingWindow(): Unit = {
    val sqlQuery = "SELECT SUM(a), " +
        "HOP_START(b, INTERVAL '3' SECOND, INTERVAL '3' SECOND), " +
        "HOP_END(b, INTERVAL '3' SECOND, INTERVAL '3' SECOND)" +
        "FROM MyTable " +
        "GROUP BY HOP(b, INTERVAL '3' SECOND, INTERVAL '3' SECOND)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testNoGroupingTumblingWindow(): Unit = {
    val sqlQuery = "SELECT avg(c), sum(a) FROM MyTable " +
        "GROUP BY TUMBLE(b, INTERVAL '3' SECOND)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testOnePhaseTumblingWindowSortAgg(): Unit = {
    val sqlQuery = "select max(colLarge) from t1 " +
        "group by customerId, TUMBLE(ts, INTERVAL '3' SECOND)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testOnePhaseTumblingWindowHashAgg(): Unit = {
    val sqlQuery = "select count(colLarge) from t1 " +
        "group by customerId, TUMBLE(ts, INTERVAL '3' SECOND)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testOnePhaseSlidingWindowSortAgg(): Unit = {
    val sqlQuery = "select max(colLarge) from t1 " +
        "group by customerId, HOP(ts, INTERVAL '3' SECOND, INTERVAL '1' HOUR)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testOnePhaseSlidingWindowSortAgg2(): Unit = {
    val sqlQuery = "select max(colLarge) from t1 " +
        "group by productId, HOP(ts, INTERVAL '0.111' SECOND(1,3), INTERVAL '1' SECOND)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testTwoPhaseSlidingWindowSortAgg2(): Unit = {
    val sqlQuery = "select max(colLarge) from t1 " +
        "group by productId, HOP(ts, INTERVAL '0.1' SECOND(1,1), INTERVAL '1' SECOND)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testOnePhaseSlidingWindowSortAggWithPaneOptimization(): Unit = {
    val sqlQuery = "select count(colLarge) from t1 " +
        "group by customerId, HOP(ts, INTERVAL '3' SECOND, INTERVAL '1' HOUR)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testTwoPhaseTumblingWindowSortAgg(): Unit = {
    val sqlQuery = "SELECT avg(c), countFun(a) FROM MyTable " +
        "GROUP BY a, d, TUMBLE(b, INTERVAL '3' SECOND)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testTwoPhaseTumblingWindowHashAgg(): Unit = {
    val sqlQuery = "SELECT avg(c), count(a) FROM MyTable " +
        "GROUP BY a, d, TUMBLE(b, INTERVAL '3' SECOND)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testTwoPhaseSlidingWindowSortAgg(): Unit = {
    val sqlQuery = "SELECT countFun(c) FROM MyTable " +
        " GROUP BY a, d, HOP(b, INTERVAL '3' SECOND, INTERVAL '1' HOUR)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testTwoPhaseSlidingWindowHashAgg(): Unit = {
    val sqlQuery = "select count(colLarge) from t1 " +
        "group by productId, HOP(ts, INTERVAL '3' SECOND, INTERVAL '1' HOUR)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testDecomposableAggFunctions(): Unit = {
    util.addTable[(Int, String, Long, Timestamp)]("MyTable1", 'a, 'b, 'c, 'rowtime)
    val sql =
      "SELECT " +
          "VAR_POP(c), VAR_SAMP(c), STDDEV_POP(c), STDDEV_SAMP(c), " +
          "TUMBLE_START(rowtime, INTERVAL '15' MINUTE), " +
          "TUMBLE_END(rowtime, INTERVAL '15' MINUTE) " +
          "FROM MyTable1 " +
          "GROUP BY TUMBLE(rowtime, INTERVAL '15' MINUTE)"
    util.verifyPlan(sql)
  }
}
