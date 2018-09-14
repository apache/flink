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

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.table.runtime.utils.CommonTestData
import org.apache.flink.table.utils.TableTestBase
import org.junit.Test

/**
  * Test for inserting into tables from external catalog.
  */
class ExternalCatalogInsertTest extends TableTestBase {
  private val tableBatchEnv = TableEnvironment.getTableEnvironment(
    ExecutionEnvironment.getExecutionEnvironment)
  private val tableStreamEnv = TableEnvironment.getTableEnvironment(
    StreamExecutionEnvironment.getExecutionEnvironment)

  @Test
  def testBatchTableApi(): Unit = {
    tableBatchEnv.registerExternalCatalog(
      "test",
      CommonTestData.getInMemoryTestCatalog(isStreaming = false))

    val table1 = tableBatchEnv.scan("test", "db1", "tb1")
    val table2 = tableBatchEnv.scan("test", "db2", "tb2")
    table2.select('d * 2, 'e, 'g.upperCase())
      .unionAll(table1.select('a * 2, 'b, 'c.upperCase()))
      .insertInto("test.db3.tb3")
  }

  @Test
  def testBatchSQL(): Unit = {
    tableBatchEnv.registerExternalCatalog(
      "test",
      CommonTestData.getInMemoryTestCatalog(isStreaming = false))

    val sqlInsert = "INSERT INTO `test.db3.tb3` SELECT d * 2, e, g FROM test.db2.tb2 WHERE d < 3 " +
      "UNION ALL (SELECT a * 2, b, c FROM test.db1.tb1)"

    tableBatchEnv.sqlUpdate(sqlInsert)
  }

  @Test
  def testStreamTableApi(): Unit = {
    var tableEnv = tableStreamEnv

    tableEnv.registerExternalCatalog(
      "test",
      CommonTestData.getInMemoryTestCatalog(isStreaming = true))

    val table1 = tableEnv.scan("test", "db1", "tb1")
    val table2 = tableEnv.scan("test", "db2", "tb2")

    table2.where("d < 3")
      .select('d * 2, 'e, 'g.upperCase())
      .unionAll(table1.select('a * 2, 'b, 'c.upperCase()))
      .insertInto("test.db3.tb3")
  }

  @Test
  def testStreamSQL(): Unit = {
    var tableEnv = tableStreamEnv

    tableEnv.registerExternalCatalog(
      "test",
      CommonTestData.getInMemoryTestCatalog(isStreaming = true))

    val sqlInsert = "INSERT INTO `test.db3.tb3` SELECT d * 2, e, g FROM test.db2.tb2 WHERE d < 3 " +
      "UNION ALL (SELECT a * 2, b, c FROM test.db1.tb1)"

    tableEnv.sqlUpdate(sqlInsert)
  }

  @Test
  def testTopLevelTable(): Unit = {
    var tableEnv = tableBatchEnv

    tableEnv.registerExternalCatalog(
      "test",
      CommonTestData.getInMemoryTestCatalog(isStreaming = false))

    val table1 = tableEnv.scan("test", "tb1")
    val table2 = tableEnv.scan("test", "db2", "tb2")
    table2.select('d * 2, 'e, 'g.upperCase())
      .unionAll(table1.select('a * 2, 'b, 'c.upperCase()))
      .insertInto("test.tb3")
  }
}
