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

package org.apache.flink.table.api

import org.apache.calcite.plan.RelOptUtil
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.junit.{Rule, Test}
import org.junit.Assert.assertEquals
import org.junit.rules.ExpectedException

class TableEnvironmentTest {

  // used for accurate exception information checking.
  val expectedException: ExpectedException = ExpectedException.none()

  @Rule
  def thrown: ExpectedException = expectedException

  val env = new StreamExecutionEnvironment(new LocalStreamEnvironment())
  val tableEnv: scala.StreamTableEnvironment = StreamTableEnvironment.create(env)

  @Test
  def testScanNonExistTable(): Unit = {
    thrown.expect(classOf[TableException])
    thrown.expectMessage("Table 'MyTable' was not found")
    tableEnv.scan("MyTable")
  }

  @Test
  def testRegisterDataStream(): Unit = {
    val table = env.fromElements[(Int, Long, String, Boolean)]().toTable(tableEnv, 'a, 'b, 'c, 'd)
    tableEnv.registerTable("MyTable", table)
    val scanTable = tableEnv.scan("MyTable")
    val actual = RelOptUtil.toString(scanTable.asInstanceOf[TableImpl].getRelNode)
    val expected = "LogicalTableScan(table=[[MyTable]])\n"
    assertEquals(expected, actual)

    // register on a conflict name
    thrown.expect(classOf[TableException])
    thrown.expectMessage("Table 'MyTable' already exists")
    tableEnv.registerDataStream("MyTable", env.fromElements[(Int, Long)]())
  }

  @Test
  def testSimpleQuery(): Unit = {
    val table = env.fromElements[(Int, Long, String, Boolean)]().toTable(tableEnv, 'a, 'b, 'c, 'd)
    tableEnv.registerTable("MyTable", table)
    val queryTable = tableEnv.sqlQuery("SELECT a, c, d FROM MyTable")
    val actual = RelOptUtil.toString(queryTable.asInstanceOf[TableImpl].getRelNode)
    val expected = "LogicalProject(a=[$0], c=[$2], d=[$3])\n" +
      "  LogicalTableScan(table=[[MyTable]])\n"
    assertEquals(expected, actual)
  }
}
