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
package org.apache.flink.table.plan.util

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{TableConfig, TableImpl}
import org.apache.flink.table.api.scala.{StreamTableEnvironment, _}
import org.apache.calcite.sql.SqlExplainLevel
import org.junit.Assert.assertEquals
import org.junit.Test

class FlinkRelOptUtilTest {

  @Test
  def testToString(): Unit = {
    val env  = StreamExecutionEnvironment.createLocalEnvironment()
    val tableEnv = StreamTableEnvironment.create(env, new TableConfig())

    val table = env.fromElements[(Int, Long, String)]().toTable(tableEnv, 'a, 'b, 'c)
    tableEnv.registerTable("MyTable", table)

    val sqlQuery =
      """
        |WITH t1 AS (SELECT a, c FROM MyTable WHERE b > 50),
        |     t2 AS (SELECT a * 2 AS a, c FROM MyTable WHERE b < 50)
        |
        |SELECT * FROM t1 JOIN t2 ON t1.a = t2.a
      """.stripMargin
    val result = tableEnv.sqlQuery(sqlQuery)
    val rel = result.asInstanceOf[TableImpl].getRelNode

    val expected1 =
      """
        |LogicalProject(a=[$0], c=[$1], a0=[$2], c0=[$3])
        |+- LogicalJoin(condition=[=($0, $2)], joinType=[inner])
        |   :- LogicalProject(a=[$0], c=[$2])
        |   :  +- LogicalFilter(condition=[>($1, 50)])
        |   :     +- LogicalTableScan(table=[[MyTable]])
        |   +- LogicalProject(a=[*($0, 2)], c=[$2])
        |      +- LogicalFilter(condition=[<($1, 50)])
        |         +- LogicalTableScan(table=[[MyTable]])
      """.stripMargin
    assertEquals(expected1.trim, FlinkRelOptUtil.toString(rel).trim)

    val expected2 =
      """
        |LogicalProject
        |+- LogicalJoin
        |   :- LogicalProject
        |   :  +- LogicalFilter
        |   :     +- LogicalTableScan
        |   +- LogicalProject
        |      +- LogicalFilter
        |         +- LogicalTableScan
      """.stripMargin
    assertEquals(expected2.trim, FlinkRelOptUtil.toString(rel, SqlExplainLevel.NO_ATTRIBUTES).trim)
  }

}
