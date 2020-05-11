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

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.api.internal.TableEnvironmentImpl
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment
import org.apache.flink.table.planner.utils.TestTableSourceSinks
import org.apache.flink.types.Row
import org.apache.flink.util.TestLogger

import org.apache.flink.shaded.guava18.com.google.common.collect.Lists

import org.hamcrest.Matchers.containsString
import org.junit.Assert.{assertEquals, assertTrue}
import org.junit.rules.{ExpectedException, TemporaryFolder}
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.{Before, Rule, Test}

import _root_.java.util

@RunWith(classOf[Parameterized])
class TableITCase(tableEnvName: String, isStreaming: Boolean) extends TestLogger {

  // used for accurate exception information checking.
  val expectedException: ExpectedException = ExpectedException.none()

  @Rule
  def thrown: ExpectedException = expectedException

  private val _tempFolder = new TemporaryFolder()

  @Rule
  def tempFolder: TemporaryFolder = _tempFolder

  var tEnv: TableEnvironment = _

  private val settings = if (isStreaming) {
    EnvironmentSettings.newInstance().inStreamingMode().build()
  } else {
    EnvironmentSettings.newInstance().inBatchMode().build()
  }

  @Before
  def setup(): Unit = {
    tableEnvName match {
      case "TableEnvironment" =>
        tEnv = TableEnvironmentImpl.create(settings)
      case "StreamTableEnvironment" =>
        tEnv = StreamTableEnvironment.create(
          StreamExecutionEnvironment.getExecutionEnvironment, settings)
      case _ => throw new UnsupportedOperationException("unsupported tableEnvName: " + tableEnvName)
    }
    TestTableSourceSinks.createPersonCsvTemporaryTable(tEnv, "MyTable")
  }

  @Test
  def testExecute(): Unit = {
    val query =
      """
        |select id, concat(concat(`first`, ' '), `last`) as `full name`
        |from MyTable where mod(id, 2) = 0
      """.stripMargin
    val table = tEnv.sqlQuery(query)
    val tableResult = table.execute()
    assertTrue(tableResult.getJobClient.isPresent)
    assertEquals(ResultKind.SUCCESS_WITH_CONTENT, tableResult.getResultKind)
    assertEquals(
      TableSchema.builder()
        .field("id", DataTypes.INT())
        .field("full name", DataTypes.STRING())
        .build(),
      tableResult.getTableSchema)
    val expected = util.Arrays.asList(
      Row.of(Integer.valueOf(2), "Bob Taylor"),
      Row.of(Integer.valueOf(4), "Peter Smith"),
      Row.of(Integer.valueOf(6), "Sally Miller"),
      Row.of(Integer.valueOf(8), "Kelly Williams"))
    val actual = Lists.newArrayList(tableResult.collect())
    actual.sort(new util.Comparator[Row]() {
      override def compare(o1: Row, o2: Row): Int = {
        o1.getField(0).asInstanceOf[Int].compareTo(o2.getField(0).asInstanceOf[Int])
      }
    })
    assertEquals(expected, actual)
  }

  @Test
  def testExecuteWithUpdateChanges(): Unit = {
    if (!isStreaming) {
      return
    }
    // TODO Once FLINK-16998 is finished, all kinds of changes will be supported.
    thrown.expect(classOf[TableException])
    thrown.expectMessage(containsString(
      "AppendStreamTableSink doesn't support consuming update changes"))
    tEnv.sqlQuery("select count(*) from MyTable").execute()
  }

}

object TableITCase {
  @Parameterized.Parameters(name = "{0}:isStream={1}")
  def parameters(): util.Collection[Array[_]] = {
    util.Arrays.asList(
      Array("TableEnvironment", true),
      Array("TableEnvironment", false),
      Array("StreamTableEnvironment", true)
    )
  }
}
