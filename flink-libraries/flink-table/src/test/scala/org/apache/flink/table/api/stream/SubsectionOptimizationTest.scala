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

package org.apache.flink.table.api.stream

import org.apache.flink.api.scala._
import org.apache.flink.table.api.TableConfigOptions
import org.apache.flink.table.api.scala._
import org.apache.flink.table.runtime.utils.TemporalTableUtils.TestingTemporalTableSource
import org.apache.flink.table.sinks.csv.CsvTableSink
import org.apache.flink.table.util.{TableFunc1, TableTestBase}

import org.junit.{Before, Test}

class SubsectionOptimizationTest extends TableTestBase {

  private val util = streamTestUtil()

  @Before
  def setup(): Unit = {
    util.addTable[(Int, Long, String)]("SmallTable3", 'a, 'b, 'c)
    util.addTable[(Int, Double, Int)](
      "MyTable", 'a, 'b, 'c, 'proctime.proctime, 'rowtime.rowtime)
    util.tableEnv.registerTableSource("TemporalSource", new TestingTemporalTableSource)

    util.tableEnv.getConfig.setSubsectionOptimization(true)
    util.tableEnv.getConfig.getConf.setBoolean(
      TableConfigOptions.SQL_OPTIMIZER_SUBSECTION_UNIONALL_AS_BREAKPOINT_DISABLED, true)
  }

  @Test
  def testMultiSinksWithUDTF(): Unit = {
    util.tableEnv.registerFunction("split", new TableFunc1)
    val view1 =
      """
        |SELECT  a, b - MOD(b, 300) AS b, c FROM SmallTable3
        |WHERE b >= UNIX_TIMESTAMP('${startTime}')
      """.stripMargin
    util.tableEnv.registerTable("view1", util.tableEnv.sqlQuery(view1))

    val view2 = "SELECT a, b, c1 AS c FROM view1, LATERAL TABLE(split(c)) AS T(c1) WHERE c <> '' "
    util.tableEnv.registerTable("view2", util.tableEnv.sqlQuery(view2))

    val view3 = "SELECT a, b, COUNT(DISTINCT c) AS total_c FROM view2 GROUP BY a, b"
    util.tableEnv.registerTable("view3", util.tableEnv.sqlQuery(view3))

    val table = util.tableEnv.sqlQuery(
      "SELECT a, total_c FROM view3 UNION ALL SELECT a, 0 AS total_c FROM view1")

    table.filter('a > 50).writeToSink(new CsvTableSink("file1"))
    table.filter('a < 50).writeToSink(new CsvTableSink("file2"))

    util.verifyPlan()
  }

  @Test
  def testMultiSinksWithWindow(): Unit = {
    val query1 =
      """
        |SELECT
        |    a,
        |    SUM (CAST (c AS DOUBLE)) AS sum_c,
        |    CAST(TUMBLE_END(rowtime, INTERVAL '15' SECOND) as INTEGER) AS `time`,
        |    CAST(TUMBLE_START(rowtime, INTERVAL '15' SECOND) as INTEGER) AS window_start,
        |    CAST(TUMBLE_END (rowtime, INTERVAL '15' SECOND) as INTEGER) AS window_end
        |FROM
        |    MyTable
        |GROUP BY
        |    TUMBLE (rowtime, INTERVAL '15' SECOND), a
      """.stripMargin

    val query2 =
      """
        |SELECT
        |    a,
        |    SUM (CAST (c AS DOUBLE)) AS sum_c,
        |    CAST(TUMBLE_END(rowtime, INTERVAL '15' SECOND) as INTEGER) AS `time`
        |FROM
        |    MyTable
        |GROUP BY
        |    TUMBLE (rowtime, INTERVAL '15' SECOND), a
      """.stripMargin

    util.tableEnv.sqlQuery(query1).writeToSink(new CsvTableSink("file1"))
    util.tableEnv.sqlQuery(query2).writeToSink(new CsvTableSink("file2"))

    util.verifyPlan()
  }

  @Test
  def testSingleSinkWithTemporalTableSource(): Unit = {
    val query =
      """
        |SELECT
        |    HOP_START(rowtime, INTERVAL '60' SECOND, INTERVAL '3' MINUTE),
        |    HOP_END(rowtime, INTERVAL '60' SECOND, INTERVAL '3' MINUTE),
        |    name1,
        |    name2,
        |    AVG(b) as avg_b
        |    FROM(
        |        SELECT
        |            t2.name as name1, t3.name as name2, t1.b, t1.rowtime
        |        FROM
        |            MyTable t1
        |        INNER join
        |            TemporalSource FOR SYSTEM_TIME AS OF PROCTIME() as t2
        |        ON t1.a = t2.id
        |        INNER JOIN
        |            TemporalSource FOR SYSTEM_TIME AS OF PROCTIME() as t3
        |        ON t1.c = t3.id
        |    ) d
        |    group by HOP(rowtime, INTERVAL '60' SECOND, INTERVAL '3' MINUTE), name1, name2
      """.stripMargin
    util.tableEnv.sqlQuery(query).writeToSink(new CsvTableSink("file1"))

    util.verifyPlan()
  }

}
