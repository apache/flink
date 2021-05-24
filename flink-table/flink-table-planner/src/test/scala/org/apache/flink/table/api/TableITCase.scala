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
import org.apache.flink.table.api.TableEnvironmentITCase.getPersonCsvTableSource
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment
import org.apache.flink.table.api.internal.{TableEnvironmentImpl, TableEnvironmentInternal}
import org.apache.flink.table.catalog.{Column, ResolvedSchema}
import org.apache.flink.types.{Row, RowKind}
import org.apache.flink.util.CollectionUtil

import org.junit.Assert.{assertEquals, assertTrue}
import org.junit.rules.{ExpectedException, TemporaryFolder}
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.{Before, Rule, Test}

import java.lang.{Long => JLong}
import java.util

@RunWith(classOf[Parameterized])
class TableITCase(tableEnvName: String) {

  // used for accurate exception information checking.
  val expectedException: ExpectedException = ExpectedException.none()

  @Rule
  def thrown: ExpectedException = expectedException

  private val _tempFolder = new TemporaryFolder()

  @Rule
  def tempFolder: TemporaryFolder = _tempFolder

  var tEnv: TableEnvironment = _

  private val settings =
    EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build()

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
    tEnv.asInstanceOf[TableEnvironmentInternal].registerTableSourceInternal(
      "MyTable", getPersonCsvTableSource)
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
      ResolvedSchema.of(
        Column.physical("id", DataTypes.INT()),
        Column.physical("full name", DataTypes.STRING())),
      tableResult.getResolvedSchema)
    val expected = util.Arrays.asList(
      Row.of(Integer.valueOf(2), "Bob Taylor"),
      Row.of(Integer.valueOf(4), "Peter Smith"),
      Row.of(Integer.valueOf(6), "Sally Miller"),
      Row.of(Integer.valueOf(8), "Kelly Williams"))
    val actual = CollectionUtil.iteratorToList(tableResult.collect())
    actual.sort(new util.Comparator[Row]() {
      override def compare(o1: Row, o2: Row): Int = {
        o1.getField(0).asInstanceOf[Int].compareTo(o2.getField(0).asInstanceOf[Int])
      }
    })
    assertEquals(expected, actual)
  }

  @Test
  def testExecuteWithUpdateChanges(): Unit = {
    val tableResult = tEnv.sqlQuery("select count(*) as c from MyTable").execute()
    assertTrue(tableResult.getJobClient.isPresent)
    assertEquals(ResultKind.SUCCESS_WITH_CONTENT, tableResult.getResultKind)
    assertEquals(
      ResolvedSchema.of(Column.physical("c", DataTypes.BIGINT().notNull())),
      tableResult.getResolvedSchema)
    val expected = util.Arrays.asList(
      Row.ofKind(RowKind.INSERT, JLong.valueOf(1)),
      Row.ofKind(RowKind.DELETE, JLong.valueOf(1)),
      Row.ofKind(RowKind.INSERT, JLong.valueOf(2)),
      Row.ofKind(RowKind.DELETE, JLong.valueOf(2)),
      Row.ofKind(RowKind.INSERT, JLong.valueOf(3)),
      Row.ofKind(RowKind.DELETE, JLong.valueOf(3)),
      Row.ofKind(RowKind.INSERT, JLong.valueOf(4)),
      Row.ofKind(RowKind.DELETE, JLong.valueOf(4)),
      Row.ofKind(RowKind.INSERT, JLong.valueOf(5)),
      Row.ofKind(RowKind.DELETE, JLong.valueOf(5)),
      Row.ofKind(RowKind.INSERT, JLong.valueOf(6)),
      Row.ofKind(RowKind.DELETE, JLong.valueOf(6)),
      Row.ofKind(RowKind.INSERT, JLong.valueOf(7)),
      Row.ofKind(RowKind.DELETE, JLong.valueOf(7)),
      Row.ofKind(RowKind.INSERT, JLong.valueOf(8))
    )
    val actual = CollectionUtil.iteratorToList(tableResult.collect())
    assertEquals(expected, actual)
  }

}

object TableITCase {
  @Parameterized.Parameters(name = "{0}")
  def parameters(): util.Collection[Array[_]] = {
    util.Arrays.asList(
      Array("TableEnvironment"),
      Array("StreamTableEnvironment")
    )
  }
}
