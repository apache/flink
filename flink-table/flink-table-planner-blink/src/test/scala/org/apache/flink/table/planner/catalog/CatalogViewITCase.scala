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

package org.apache.flink.table.planner.catalog

import java.util
import org.apache.flink.table.api.config.{ExecutionConfigOptions, TableConfigOptions}
import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}
import org.apache.flink.table.api.internal.TableEnvironmentImpl
import org.apache.flink.table.planner.factories.utils.TestCollectionTableFactory
import org.apache.flink.table.planner.runtime.utils.TableEnvUtil
import org.apache.flink.test.util.AbstractTestBase
import org.apache.flink.types.Row

import org.junit.Assert.assertEquals
import org.junit.{Before, Rule, Test}
import org.junit.rules.ExpectedException
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

import scala.collection.JavaConversions._

/** Test cases for view related DDLs. */
@RunWith(classOf[Parameterized])
class CatalogViewITCase(isStreamingMode: Boolean) extends AbstractTestBase {
  //~ Instance fields --------------------------------------------------------

  private val settings = if (isStreamingMode) {
    EnvironmentSettings.newInstance().inStreamingMode().build()
  } else {
    EnvironmentSettings.newInstance().inBatchMode().build()
  }

  private val tableEnv: TableEnvironment = TableEnvironmentImpl.create(settings)

  var _expectedEx: ExpectedException = ExpectedException.none

  @Rule
  def expectedEx: ExpectedException = _expectedEx

  @Before
  def before(): Unit = {
    tableEnv.getConfig.getConfiguration.setBoolean(
      TableConfigOptions.TABLE_DYNAMIC_TABLE_OPTIONS_ENABLED,
      true)

    tableEnv.getConfig
      .getConfiguration
      .setInteger(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 1)
    TestCollectionTableFactory.reset()
  }

  //~ Tools ------------------------------------------------------------------

  implicit def rowOrdering: Ordering[Row] = Ordering.by((r : Row) => {
    val builder = new StringBuilder
    0 until r.getArity foreach(idx => builder.append(r.getField(idx)))
    builder.toString()
  })

  def toRow(args: Any*):Row = {
    val row = new Row(args.length)
    0 until args.length foreach {
      i => row.setField(i, args(i))
    }
    row
  }

  @Test
  def testCreateViewIfNotExistsTwice(): Unit = {
    val sourceData = List(
      toRow(1, "1000", 2),
      toRow(2, "1", 3),
      toRow(3, "2000", 4),
      toRow(1, "2", 2),
      toRow(2, "3000", 3))

    TestCollectionTableFactory.initData(sourceData)

    val sourceDDL =
      """
        |CREATE TABLE T1(
        |  a int,
        |  b varchar,
        |  c int
        |) with (
        |  'connector' = 'COLLECTION'
        |)
      """.stripMargin

    val sinkDDL =
      """
        |CREATE TABLE T2(
        |  a int,
        |  b varchar,
        |  c int
        |) with (
        |  'connector' = 'COLLECTION'
        |)
      """.stripMargin

    val viewWith3ColumnDDL =
      """
        |CREATE VIEW IF NOT EXISTS T3(d, e, f) AS SELECT a, b, c FROM T1
      """.stripMargin

    val viewWith2ColumnDDL =
      """
        |CREATE VIEW IF NOT EXISTS T3(d, e) AS SELECT a, b FROM T1
      """.stripMargin

    val query = "SELECT d, e, f FROM T3"

    tableEnv.executeSql(sourceDDL)
    tableEnv.executeSql(sinkDDL)
    tableEnv.executeSql(viewWith3ColumnDDL)
    tableEnv.executeSql(viewWith2ColumnDDL)

    val result = tableEnv.sqlQuery(query)
    TableEnvUtil.execInsertTableAndWaitResult(result, "T2")
    assertEquals(sourceData.sorted, TestCollectionTableFactory.RESULT.sorted)
  }

  @Test
  def testCreateViewWithoutFieldListAndWithStar(): Unit = {
    val sourceData = List(
      toRow(1, "1000", 2),
      toRow(2, "1", 3),
      toRow(3, "2000", 4),
      toRow(1, "2", 2),
      toRow(2, "3000", 3))

    TestCollectionTableFactory.initData(sourceData)

    val sourceDDL =
      """
        |CREATE TABLE T1(
        |  a int,
        |  b varchar,
        |  c int
        |) with (
        |  'connector' = 'COLLECTION'
        |)
      """.stripMargin

    val sinkDDL =
      """
        |CREATE TABLE T2(
        |  a int,
        |  b varchar,
        |  c int
        |) with (
        |  'connector' = 'COLLECTION'
        |)
      """.stripMargin

    val viewDDL =
      """
        |CREATE VIEW IF NOT EXISTS T3 AS SELECT * FROM T1
      """.stripMargin

    val query = "SELECT * FROM T3"

    tableEnv.executeSql(sourceDDL)
    tableEnv.executeSql(sinkDDL)
    tableEnv.executeSql(viewDDL)

    val result = tableEnv.sqlQuery(query)
    TableEnvUtil.execInsertTableAndWaitResult(result, "T2")
    assertEquals(sourceData.sorted, TestCollectionTableFactory.RESULT.sorted)
  }

  @Test
  def testCreateTemporaryView(): Unit = {
    val sourceData = List(
      toRow(1, "1000", 2),
      toRow(2, "1", 3),
      toRow(3, "2000", 4),
      toRow(1, "2", 2),
      toRow(2, "3000", 3))

    TestCollectionTableFactory.initData(sourceData)

    val sourceDDL =
      """
        |CREATE TABLE T1(
        |  a int,
        |  b varchar,
        |  c int
        |) with (
        |  'connector' = 'COLLECTION'
        |)
      """.stripMargin

    val sinkDDL =
      """
        |CREATE TABLE T2(
        |  a int,
        |  b varchar,
        |  c int
        |) with (
        |  'connector' = 'COLLECTION'
        |)
      """.stripMargin

    val viewDDL =
      """
        |CREATE TEMPORARY VIEW T3(d, e, f) AS SELECT a, b, c FROM T1
      """.stripMargin

    val query = "SELECT d, e, f FROM T3"

    tableEnv.executeSql(sourceDDL)
    tableEnv.executeSql(sinkDDL)
    tableEnv.executeSql(viewDDL)

    val result = tableEnv.sqlQuery(query)
    TableEnvUtil.execInsertTableAndWaitResult(result, "T2")
    assertEquals(sourceData.sorted, TestCollectionTableFactory.RESULT.sorted)
  }

  @Test
  def testTemporaryViewMaskPermanentViewWithSameName(): Unit = {
    val sourceData = List(
      toRow(1, "1000", 2),
      toRow(2, "1", 3),
      toRow(3, "2000", 4),
      toRow(1, "2", 2),
      toRow(2, "3000", 3))

    val sourceDDL =
      """
        |CREATE TABLE T1(
        |  a int,
        |  b varchar,
        |  c int
        |) with (
        |  'connector' = 'COLLECTION'
        |)
      """.stripMargin

    val sinkDDL =
      """
        |CREATE TABLE T2(
        |  a int,
        |  b varchar,
        |  c int
        |) with (
        |  'connector' = 'COLLECTION'
        |)
      """.stripMargin

    val permanentView =
      """
        |CREATE VIEW IF NOT EXISTS T3 AS SELECT a, b, c FROM T1
      """.stripMargin

    val permanentViewData = List(
      toRow(1, "1000", 2),
      toRow(2, "1", 3),
      toRow(3, "2000", 4),
      toRow(1, "2", 2),
      toRow(2, "3000", 3))

    val temporaryView =
      """
        |CREATE TEMPORARY VIEW IF NOT EXISTS T3 AS SELECT a, b, c+1 FROM T1
      """.stripMargin

    val temporaryViewData = List(
      toRow(1, "1000", 3),
      toRow(2, "1", 4),
      toRow(3, "2000", 5),
      toRow(1, "2", 3),
      toRow(2, "3000", 4))

    tableEnv.executeSql(sourceDDL)
    tableEnv.executeSql(sinkDDL)
    tableEnv.executeSql(permanentView)
    tableEnv.executeSql(temporaryView)

    TestCollectionTableFactory.initData(sourceData)

    val query = "SELECT * FROM T3"

    TableEnvUtil.execInsertTableAndWaitResult(tableEnv.sqlQuery(query), "T2")
    // temporary view T3 masks permanent view T3
    assertEquals(temporaryViewData.sorted, TestCollectionTableFactory.RESULT.sorted)

    TestCollectionTableFactory.reset()
    TestCollectionTableFactory.initData(sourceData)

    val dropTemporaryView =
      """
        |DROP TEMPORARY VIEW IF EXISTS T3
      """.stripMargin
    tableEnv.executeSql(dropTemporaryView)
    TableEnvUtil.execInsertTableAndWaitResult(tableEnv.sqlQuery(query), "T2")
    // now we only have permanent view T3
    assertEquals(permanentViewData.sorted, TestCollectionTableFactory.RESULT.sorted)
  }
}

object CatalogViewITCase {
  @Parameterized.Parameters(name = "{0}")
  def parameters(): java.util.Collection[Boolean] = {
    util.Arrays.asList(true, false)
  }
}

