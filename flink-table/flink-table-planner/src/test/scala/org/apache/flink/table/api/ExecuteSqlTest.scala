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

import com.google.common.collect.Lists
import org.apache.flink.types.Row
import org.apache.flink.util.{CollectionUtil, TestLogger}
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.{Assert, Rule, Test}

import _root_.java.lang.{Boolean => JBoolean}
import _root_.java.util

/**
 * Test SQL statements which executed by [[TableEnvironment#executeSql()]]
 */
@RunWith(classOf[Parameterized])
class ExecuteSqlTest(isStreaming: Boolean) extends TestLogger {

  private val _tempFolder = new TemporaryFolder()

  @Rule
  def tempFolder: TemporaryFolder = _tempFolder

  private val settings = if (isStreaming) {
    EnvironmentSettings.newInstance().inStreamingMode().build()
  } else {
    EnvironmentSettings.newInstance().inBatchMode().build()
  }

  var tEnv: TableEnvironment = TableEnvironment.create(settings)

  @Test
  def testShowColumns(): Unit = {
    initTableAndView()
    // Tests `SHOW COLUMNS FROM TABLE`.
    showAllColumnsFromTable()
    showColumnsWithLikeClauseFromTable()
    showColumnsWithNotLikeClauseFromTable()

    // Tests `SHOW COLUMNS FROM VIEW`.
    showAllColumnsFromView()
    showColumnsWithLikeClauseFromView()
    showColumnsWithNotLikeClauseFromView()
  }

  private def initTableAndView(): Unit = {
    val createClause: String =
      s"""
         |CREATE TABLE IF NOT EXISTS orders (
         | `user` BIGINT NOT NULl,
         | `product` VARCHAR(32),
         | `amount` INT,
         | PRIMARY KEY(`user`) NOT ENFORCED
         |) """.stripMargin
    var createWithClause: String =
      s"""
         |with (
         | 'connector' = 'datagen'
         |)""".stripMargin
    if (!isStreaming) {
      val sinkPath = _tempFolder.newFolder().toString
      createWithClause =
        s"""
           |with (
           |  'connector' = 'filesystem',
           |  'path' = '$sinkPath',
           |  'format' = 'testcsv'
           |)""".stripMargin
    }
    tEnv.executeSql(createClause + createWithClause)
    tEnv.executeSql("create view orders_view as select * from orders")
  }

  private def showAllColumnsFromTable(): Unit = {

    val expectedResultRows: util.List[Row] = Lists.newArrayList(
      Row.of("user", "BIGINT", new JBoolean(false), "PRI(user)", null, null),
      Row.of("product", "VARCHAR(32)", new JBoolean(true), null, null, null),
      Row.of("amount", "INT", new JBoolean(true), null, null, null))
    val resultsWithFrom: util.List[Row] = CollectionUtil.iteratorToList(
      tEnv
        .executeSql("show columns from orders")
        .collect()
    )
    val resultsWithIn: util.List[Row] = CollectionUtil.iteratorToList(
      tEnv
        .executeSql("show columns in orders")
        .collect()
    )
    Assert.assertEquals(expectedResultRows, resultsWithFrom)
    Assert.assertEquals(expectedResultRows, resultsWithIn)
  }

  private def showColumnsWithLikeClauseFromTable(): Unit = {

    val expectedResultRows: util.List[Row] = Lists.newArrayList(
      Row.of("user", "BIGINT", new JBoolean(false), "PRI(user)", null, null),
      Row.of("product", "VARCHAR(32)", new JBoolean(true), null, null, null))
    val resultsWithFrom: util.List[Row] = CollectionUtil.iteratorToList(
      tEnv
        .executeSql("show columns from orders like '%_r%'")
        .collect()
    )
    val resultsWithIn: util.List[Row] = CollectionUtil.iteratorToList(
      tEnv
        .executeSql("show columns in orders like '%_r%'")
        .collect()
    )
    Assert.assertEquals(expectedResultRows, resultsWithFrom)
    Assert.assertEquals(expectedResultRows, resultsWithIn)
  }

  private def showColumnsWithNotLikeClauseFromTable(): Unit = {

    val expectedResultRows: util.List[Row] = Lists.newArrayList(
      Row.of("amount", "INT", new JBoolean(true), null, null, null))
    val resultsWithFrom: util.List[Row] = CollectionUtil.iteratorToList(
      tEnv
        .executeSql("show columns from orders not like '%_r%'")
        .collect()
    )
    val resultsWithIn: util.List[Row] = CollectionUtil.iteratorToList(
      tEnv
        .executeSql("show columns in orders not like '%_r%'")
        .collect()
    )
    Assert.assertEquals(expectedResultRows, resultsWithFrom)
    Assert.assertEquals(expectedResultRows, resultsWithIn)
  }

  private def showAllColumnsFromView(): Unit = {

    val expectedResultRows: util.List[Row] = Lists.newArrayList(
      Row.of("user", "BIGINT", new JBoolean(false), null, null, null),
      Row.of("product", "VARCHAR(32)", new JBoolean(true), null, null, null),
      Row.of("amount", "INT", new JBoolean(true), null, null, null))
    val resultsWithFrom: util.List[Row] = CollectionUtil.iteratorToList(
      tEnv
        .executeSql("show columns from orders_view")
        .collect()
    )
    val resultsWithIn: util.List[Row] = CollectionUtil.iteratorToList(
      tEnv.
        executeSql("show columns in orders_view")
        .collect()
    )
    Assert.assertEquals(expectedResultRows, resultsWithFrom)
    Assert.assertEquals(expectedResultRows, resultsWithIn)
  }

  private def showColumnsWithLikeClauseFromView(): Unit = {

    val expectedResultRows: util.List[Row] = Lists.newArrayList(
      Row.of("user", "BIGINT", new JBoolean(false), null, null, null),
      Row.of("product", "VARCHAR(32)", new JBoolean(true), null, null, null))
    val resultsWithFrom: util.List[Row] = CollectionUtil.iteratorToList(
      tEnv
        .executeSql("show columns from orders_view like '%_r%'")
        .collect()
    )
    val resultsWithIn: util.List[Row] = CollectionUtil.iteratorToList(
      tEnv
        .executeSql("show columns in orders_view like '%_r%'")
        .collect()
    )
    Assert.assertEquals(expectedResultRows, resultsWithFrom)
    Assert.assertEquals(expectedResultRows, resultsWithIn)
  }

  private def showColumnsWithNotLikeClauseFromView(): Unit = {

    val expectedResultRows: util.List[Row] = Lists.newArrayList(
      Row.of("amount", "INT", new JBoolean(true), null, null, null))
    val resultsWithFrom: util.List[Row] = CollectionUtil.iteratorToList(
      tEnv
        .executeSql("show columns from orders_view not like '%_r%'")
        .collect()
    )
    val resultsWithIn: util.List[Row] = CollectionUtil.iteratorToList(
      tEnv
        .executeSql("show columns in orders_view not like '%_r%'")
        .collect()
    )
    Assert.assertEquals(expectedResultRows, resultsWithFrom)
    Assert.assertEquals(expectedResultRows, resultsWithIn)
  }

}

object ExecuteSqlTest {
  @Parameterized.Parameters(name = "isStream={0}")
  def parameters(): util.Collection[JBoolean] = {
    util.Arrays.asList(true, false)
  }
}
