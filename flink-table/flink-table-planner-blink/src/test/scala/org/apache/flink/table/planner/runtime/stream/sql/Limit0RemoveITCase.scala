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

package org.apache.flink.table.planner.runtime.stream.sql

import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.api.internal.TableEnvironmentInternal
import org.apache.flink.table.planner.runtime.utils.{StreamingTestBase, TestSinkUtil, TestingAppendTableSink, TestingRetractTableSink}

import org.junit.Assert.assertEquals
import org.junit.Test

class Limit0RemoveITCase extends StreamingTestBase() {

  @Test
  def testSimpleLimitRemove(): Unit = {
    val ds = env.fromCollection(Seq(1, 2, 3, 4, 5, 6))
    val table = ds.toTable(tEnv, 'a)
    tEnv.registerTable("MyTable", table)

    val sql = "SELECT * FROM MyTable LIMIT 0"

    val result = tEnv.sqlQuery(sql)
    val sink = TestSinkUtil.configureSink(result, new TestingAppendTableSink())
    tEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal("MySink", sink)
    execInsertTableAndWaitResult(result, "MySink")

    assertEquals(0, sink.getAppendResults.size)
  }

  @Test
  def testLimitRemoveWithOrderBy(): Unit = {
    val ds = env.fromCollection(Seq(1, 2, 3, 4, 5, 6))
    val table = ds.toTable(tEnv, 'a)
    tEnv.registerTable("MyTable", table)

    val sql = "SELECT * FROM MyTable ORDER BY a LIMIT 0"

    val result = tEnv.sqlQuery(sql)
    val sink = TestSinkUtil.configureSink(result, new TestingAppendTableSink())
    tEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal("MySink", sink)
    execInsertTableAndWaitResult(result, "MySink")

    assertEquals(0, sink.getAppendResults.size)
  }

  @Test
  def testLimitRemoveWithSelect(): Unit = {
    val ds = env.fromCollection(Seq(1, 2, 3, 4, 5, 6))
    val table = ds.toTable(tEnv, 'a)
    tEnv.registerTable("MyTable", table)

    val sql = "select a2 from (select cast(a as int) a2 from MyTable limit 0)"

    val result = tEnv.sqlQuery(sql)
    val sink = TestSinkUtil.configureSink(result, new TestingAppendTableSink())
    tEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal("MySink", sink)
    execInsertTableAndWaitResult(result, "MySink")

    assertEquals(0, sink.getAppendResults.size)
  }

  @Test
  def testLimitRemoveWithIn(): Unit = {
    val ds1 = env.fromCollection(Seq(1, 2, 3, 4, 5, 6))
    val table1 = ds1.toTable(tEnv, 'a)
    tEnv.registerTable("MyTable1", table1)

    val ds2 = env.fromCollection(Seq(1, 2, 3))
    val table2 = ds2.toTable(tEnv, 'a)
    tEnv.registerTable("MyTable2", table2)

    val sql = "SELECT * FROM MyTable1 WHERE a IN (SELECT a FROM MyTable2 LIMIT 0)"

    val result = tEnv.sqlQuery(sql)
    val sink = TestSinkUtil.configureSink(result, new TestingAppendTableSink())
    tEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal("MySink", sink)
    execInsertTableAndWaitResult(result, "MySink")

    assertEquals(0, sink.getAppendResults.size)
  }

  @Test
  def testLimitRemoveWithNotIn(): Unit = {
    val ds1 = env.fromCollection(Seq(1, 2, 3, 4, 5, 6))
    val table1 = ds1.toTable(tEnv, 'a)
    tEnv.registerTable("MyTable1", table1)

    val ds2 = env.fromCollection(Seq(1, 2, 3))
    val table2 = ds2.toTable(tEnv, 'a)
    tEnv.registerTable("MyTable2", table2)

    val sql = "SELECT * FROM MyTable1 WHERE a NOT IN (SELECT a FROM MyTable2 LIMIT 0)"

    val result = tEnv.sqlQuery(sql)
    val sink = TestSinkUtil.configureSink(result, new TestingAppendTableSink())
    tEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal("MySink", sink)
    execInsertTableAndWaitResult(result, "MySink")

    val expected = Seq("1", "2", "3", "4", "5", "6")
    assertEquals(expected, sink.getAppendResults.sorted)
  }

  @Test
  def testLimitRemoveWithExists(): Unit = {
    val ds1 = env.fromCollection(Seq(1, 2, 3, 4, 5, 6))
    val table1 = ds1.toTable(tEnv, 'a)
    tEnv.registerTable("MyTable1", table1)

    val ds2 = env.fromCollection(Seq(1, 2, 3))
    val table2 = ds2.toTable(tEnv, 'a)
    tEnv.registerTable("MyTable2", table2)

    val sql = "SELECT * FROM MyTable1 WHERE EXISTS (SELECT a FROM MyTable2 LIMIT 0)"

    val result = tEnv.sqlQuery(sql)
    val sink = TestSinkUtil.configureSink(result, new TestingRetractTableSink())
    tEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal("MySink", sink)
    execInsertTableAndWaitResult(result, "MySink")

    assertEquals(0, sink.getRawResults.size)
  }

  @Test
  def testLimitRemoveWithNotExists(): Unit = {
    val ds1 = env.fromCollection(Seq(1, 2, 3, 4, 5, 6))
    val table1 = ds1.toTable(tEnv, 'a)
    tEnv.registerTable("MyTable1", table1)

    val ds2 = env.fromCollection(Seq(1, 2, 3))
    val table2 = ds2.toTable(tEnv, 'a)
    tEnv.registerTable("MyTable2", table2)

    val sql = "SELECT * FROM MyTable1 WHERE NOT EXISTS (SELECT a FROM MyTable2 LIMIT 0)"

    val result = tEnv.sqlQuery(sql)
    val sink = TestSinkUtil.configureSink(result, new TestingRetractTableSink())
    tEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal("MySink", sink)
    execInsertTableAndWaitResult(result, "MySink")

    val expected = Seq("1", "2", "3", "4", "5", "6")
    assertEquals(expected, sink.getRetractResults.sorted)
  }

  @Test
  def testLimitRemoveWithJoin(): Unit = {
    val ds1 = env.fromCollection(Seq(1, 2, 3, 4, 5, 6))
    val table1 = ds1.toTable(tEnv, 'a1)
    tEnv.registerTable("MyTable1", table1)

    val ds2 = env.fromCollection(Seq(1, 2, 3))
    val table2 = ds2.toTable(tEnv, 'a2)
    tEnv.registerTable("MyTable2", table2)

    val sql = "SELECT a1 FROM MyTable1 INNER JOIN (SELECT a2 FROM MyTable2 LIMIT 0) ON true"

    val result = tEnv.sqlQuery(sql)
    val sink = TestSinkUtil.configureSink(result, new TestingAppendTableSink())
    tEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal("MySink", sink)
    execInsertTableAndWaitResult(result, "MySink")

    assertEquals(0, sink.getAppendResults.size)
  }
}
