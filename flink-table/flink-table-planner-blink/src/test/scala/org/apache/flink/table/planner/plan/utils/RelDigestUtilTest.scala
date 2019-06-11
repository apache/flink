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
package org.apache.flink.table.planner.plan.utils

import org.apache.flink.api.common.typeinfo.BasicTypeInfo.{DOUBLE_TYPE_INFO, INT_TYPE_INFO, STRING_TYPE_INFO}
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.table.api.internal.TableEnvironmentImpl
import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}
import org.apache.flink.table.planner.runtime.utils.BatchTableEnvUtil
import org.apache.flink.table.planner.runtime.utils.BatchTestBase.row
import org.apache.flink.table.planner.utils.TableTestUtil

import org.junit.Assert.assertEquals
import org.junit.{Before, Test}

import scala.collection.Seq

class RelDigestUtilTest {

  var tableEnv: TableEnvironment = _

  @Before
  def before(): Unit = {
    val settings = EnvironmentSettings.newInstance().useBlinkPlanner().build()
    val tEnv = TableEnvironmentImpl.create(settings)
    BatchTableEnvUtil.registerCollection(
      tEnv,
      "MyTable",
      Seq(row("Mike", 1, 12.3, "Smith")),
      new RowTypeInfo(STRING_TYPE_INFO, INT_TYPE_INFO, DOUBLE_TYPE_INFO, STRING_TYPE_INFO),
      "first, id, score, last")
    tableEnv = tEnv
  }

  @Test
  def testGetDigestWithDynamicFunction(): Unit = {
    val table = tableEnv.sqlQuery(
      """
        |(SELECT id AS random FROM MyTable ORDER BY rand() LIMIT 1)
        |INTERSECT
        |(SELECT id AS random FROM MyTable ORDER BY rand() LIMIT 1)
        |INTERSECT
        |(SELECT id AS random FROM MyTable ORDER BY rand() LIMIT 1)
      """.stripMargin)
    val rel = TableTestUtil.toRelNode(table)
    val expected = TableTestUtil.readFromResource("/digest/testGetDigestWithDynamicFunction.out")
    assertEquals(expected, RelDigestUtil.getDigest(rel))
  }

  @Test
  def testGetDigestWithDynamicFunctionView(): Unit = {
    val view = tableEnv.sqlQuery("SELECT id AS random FROM MyTable ORDER BY rand() LIMIT 1")
    tableEnv.registerTable("MyView", view)
    val table = tableEnv.sqlQuery(
      """
        |(SELECT * FROM MyView)
        |INTERSECT
        |(SELECT * FROM MyView)
        |INTERSECT
        |(SELECT * FROM MyView)
      """.stripMargin)
    val rel = TableTestUtil.toRelNode(table).accept(new ExpandTableScanShuttle())
    val expected = TableTestUtil.readFromResource(
      "/digest/testGetDigestWithDynamicFunctionView.out")
    assertEquals(expected, RelDigestUtil.getDigest(rel))
  }

}
