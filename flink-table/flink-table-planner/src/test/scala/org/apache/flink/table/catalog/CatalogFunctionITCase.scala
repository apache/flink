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

package org.apache.flink.table.catalog

import java.util

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala.{BatchTableEnvironment, StreamTableEnvironment}
import org.junit.{Before, Test}
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

import org.hamcrest.CoreMatchers.containsString
import org.hamcrest.MatcherAssert.assertThat


/** Test cases for catalog function. */
@RunWith(classOf[Parameterized])
class CatalogFunctionITCase(isStreaming: Boolean) {

  private val batchExec: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
  private var batchEnv: BatchTableEnvironment = _
  private val streamExec: StreamExecutionEnvironment = StreamExecutionEnvironment
    .getExecutionEnvironment
  private var streamEnv: StreamTableEnvironment = _

  @Before
  def before(): Unit = {
    batchExec.setParallelism(4)
    streamExec.setParallelism(4)
    batchEnv = BatchTableEnvironment.create(batchExec)
    streamEnv = StreamTableEnvironment.create(streamExec)
  }

  def tableEnv: TableEnvironment = {
    if (isStreaming) {
      streamEnv
    } else {
      batchEnv
    }
  }

  @Test
  def testCreateFunction(): Unit = {
    val ddl1 =
      """
        |create temporary function f1
        |  as 'org.apache.flink.function.TestFunction'
      """.stripMargin
    tableEnv.sqlUpdate(ddl1)
    assert(tableEnv.listFunctions().contains("f1"))

    tableEnv.sqlUpdate("DROP TEMPORARY FUNCTION IF EXISTS catalog1.database1.f1")
    assert(tableEnv.listFunctions().contains("f1"))
  }

  @Test
  def testCreateSystemFunction(): Unit = {
    val ddl1 =
      """
        |create temporary system function f2
        |  as 'org.apache.flink.function.TestFunction'
      """.stripMargin
    tableEnv.sqlUpdate(ddl1)
    assert(tableEnv.listFunctions().contains("f2"))

    tableEnv.sqlUpdate("DROP TEMPORARY SYSTEM FUNCTION IF EXISTS catalog1.database1.f2")
    assert(tableEnv.listFunctions().contains("f2"))
  }

  @Test
  def testCreateFunctionWithFullPath(): Unit = {
    val ddl1 =
      """
        |create temporary function default_catalog.default_database.f2
        |  as 'org.apache.flink.function.TestFunction'
      """.stripMargin
    tableEnv.sqlUpdate(ddl1)
    assert(tableEnv.listFunctions().contains("f2"))

    tableEnv.sqlUpdate("DROP TEMPORARY FUNCTION IF EXISTS default_catalog.default_database.f2")
    assert(!tableEnv.listFunctions().contains("f2"))
  }

  @Test
  def testCreateFunctionWithLanguage(): Unit = {
    val ddl1 =
      """
        |create temporary function default_catalog.default_database.f2
        |  as 'org.apache.flink.function.TestFunction' language 'SCALA'
      """.stripMargin
    tableEnv.sqlUpdate(ddl1)
    assert(tableEnv.listFunctions().contains("f2"))

    tableEnv.sqlUpdate("DROP TEMPORARY FUNCTION IF EXISTS default_catalog.default_database.f2")
    assert(!tableEnv.listFunctions().contains("f2"))
  }

  @Test
  def testCreateFunctionNotExists(): Unit = {
    val ddl3 =
    """
      |create temporary function if not exists catalog1.database1.f3
      |  as 'org.apache.flink.function.TestFunction'
    """.stripMargin
    tableEnv.sqlUpdate(ddl3)

    val ddl4 =
      """
        |create temporary function catalog1.database1.f3
        |  as 'org.apache.flink.function.TestFunction'
    """.stripMargin

    try {
      tableEnv.sqlUpdate(ddl4)
    } catch {
      case e: Exception => {
        assertThat(e.getMessage(), containsString("Catalog catalog1 does not exist."));
      }
    }
  }

  @Test
  def testDropFunctionNonExists(): Unit = {
    tableEnv.sqlUpdate("DROP TEMPORARY FUNCTION IF EXISTS catalog1.database1.f4")

    try {
      tableEnv.sqlUpdate("DROP TEMPORARY FUNCTION catalog1.database1.f4")
    } catch {
      case e: Exception => {
        assertThat(e.getMessage(), containsString("Catalog catalog1 does not exist."));
      }
    }
  }
}

object CatalogFunctionITCase {
  @Parameterized.Parameters(name = "{0}")
  def parameters(): java.util.Collection[Boolean] = {
    util.Arrays.asList(true, false)
  }
}
