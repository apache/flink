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

package org.apache.flink.table.planner.plan.stream.sql

import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.internal.TableEnvironmentInternal
import org.apache.flink.table.data.TimestampData
import org.apache.flink.table.functions.TableFunction
import org.apache.flink.table.planner.plan.stream.sql.RelTimeIndicatorConverterTest.TableFunc
import org.apache.flink.table.planner.utils.TableTestBase
import org.apache.flink.table.types.logical.BigIntType

import org.junit.Test

import java.sql.Timestamp

/**
  * Tests for [[org.apache.flink.table.planner.calcite.RelTimeIndicatorConverter]].
  */
class RelTimeIndicatorConverterTest extends TableTestBase {

  private val util = streamTestUtil()
  util.addDataStream[(Long, Long, Int)](
    "MyTable", 'rowtime.rowtime, 'long, 'int, 'proctime.proctime)
  util.addDataStream[(Long, Long, Int)]("MyTable1", 'rowtime.rowtime, 'long, 'int)
  util.addDataStream[(Long, Int)]("MyTable2", 'long, 'int, 'proctime.proctime)

  @Test
  def testSimpleMaterialization(): Unit = {
    val sqlQuery =
      """
        |SELECT rowtime FROM
        |    (SELECT FLOOR(rowtime TO DAY) AS rowtime, long FROM MyTable WHERE long > 0) t
      """.stripMargin
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testSelectAll(): Unit = {
    util.verifyExecPlan("SELECT * FROM MyTable")
  }

  @Test
  def testFilteringOnRowtime(): Unit = {
    val sqlQuery =
      "SELECT rowtime FROM MyTable1 WHERE rowtime > CAST('1990-12-02 12:11:11' AS TIMESTAMP(3))"
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testGroupingOnRowtime(): Unit = {
    util.verifyExecPlan("SELECT COUNT(long) FROM MyTable GROUP BY rowtime")
  }

  @Test
  def testAggregationOnRowtime(): Unit = {
    util.verifyExecPlan("SELECT MIN(rowtime) FROM MyTable1 GROUP BY long")
  }


  @Test
  def testGroupingOnProctime(): Unit = {
    util.verifyExecPlan("SELECT COUNT(long) FROM MyTable2 GROUP BY proctime")
  }

  @Test
  def testAggregationOnProctime(): Unit = {
    util.verifyExecPlan("SELECT MIN(proctime) FROM MyTable2 GROUP BY long")
  }

  @Test
  def testTableFunction(): Unit = {
    util.addFunction("tableFunc", new TableFunc)
    val sqlQuery =
      """
        |SELECT rowtime, proctime, s
        |FROM MyTable, LATERAL TABLE(tableFunc(rowtime, proctime, '')) AS T(s)
      """.stripMargin
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testUnion(): Unit = {
    util.verifyExecPlan("SELECT rowtime FROM MyTable1 UNION ALL SELECT rowtime FROM MyTable1")
  }

  @Test
  def testWindow(): Unit = {
    val sqlQuery =
      """
        |SELECT TUMBLE_END(rowtime, INTERVAL '10' SECOND),
        |    long,
        |    SUM(`int`)
        |FROM MyTable1
        |    GROUP BY TUMBLE(rowtime, INTERVAL '10' SECOND), long
      """.stripMargin
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testWindow2(): Unit = {
    val sqlQuery =
      """
        |SELECT TUMBLE_END(rowtime, INTERVAL '0.1' SECOND) AS `rowtime`,
        |    `long`,
        |   SUM(`int`)
        |FROM MyTable1
        |   GROUP BY `long`, TUMBLE(rowtime, INTERVAL '0.1' SECOND)
        |
        """.stripMargin
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testMultiWindow(): Unit = {
    val sqlQuery =
      """
        |SELECT TUMBLE_END(newrowtime, INTERVAL '30' SECOND), long, sum(`int`) FROM (
        |    SELECT
        |        TUMBLE_ROWTIME(rowtime, INTERVAL '10' SECOND) AS newrowtime,
        |        long,
        |        sum(`int`) as `int`
        |    FROM MyTable1
        |        GROUP BY TUMBLE(rowtime, INTERVAL '10' SECOND), long
        |) t GROUP BY TUMBLE(newrowtime, INTERVAL '30' SECOND), long
      """.stripMargin
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testWindowWithAggregationOnRowtime(): Unit = {
    val sqlQuery =
      """
        |SELECT MIN(rowtime), long FROM MyTable1
        |GROUP BY long, TUMBLE(rowtime, INTERVAL '0.1' SECOND)
      """.stripMargin
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testWindowWithAggregationOnRowtimeWithHaving(): Unit = {
    val result =
      """
        |SELECT MIN(rowtime), long FROM MyTable1
        |GROUP BY long, TUMBLE(rowtime, INTERVAL '1' SECOND)
        |HAVING QUARTER(TUMBLE_END(rowtime, INTERVAL '1' SECOND)) = 1
      """.stripMargin
    util.verifyExecPlan(result)
  }

  @Test
  def testKeepProcessTimeAttrAfterSubGraphOptimize(): Unit = {
    val stmtSet = util.tableEnv.createStatementSet()
    val sql =
      """
        |SELECT
        |    long,
        |    SUM(`int`)
        |FROM MyTable2
        |    GROUP BY TUMBLE(proctime, INTERVAL '10' SECOND), long
      """.stripMargin

    val table = util.tableEnv.sqlQuery(sql)

    val appendSink1 = util.createAppendTableSink(
      Array("long", "sum"),
      Array(new BigIntType(), new BigIntType()))
    util.tableEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal(
      "appendSink1", appendSink1)
    stmtSet.addInsert("appendSink1", table)

    val appendSink2 = util.createAppendTableSink(
      Array("long", "sum"),
      Array(new BigIntType(), new BigIntType()))
    util.tableEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal(
      "appendSink2", appendSink2)
    stmtSet.addInsert("appendSink2", table)

    util.verifyExecPlan(stmtSet)
  }

  // TODO add temporal table join case
}

object RelTimeIndicatorConverterTest {

  class TableFunc extends TableFunction[String] {
    val t = new Timestamp(0L)

    def eval(time1: TimestampData, time2: Timestamp, string: String): Unit = {
      collect(time1.toString + time2.after(t) + string)
    }
  }

}
