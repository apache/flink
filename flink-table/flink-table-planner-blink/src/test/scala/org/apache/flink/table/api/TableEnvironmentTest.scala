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

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeinfo.Types.STRING
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.internal.TableEnvironmentImpl
import org.apache.flink.table.api.scala.{StreamTableEnvironment, _}
import org.apache.flink.table.planner.delegation.PlannerBase
import org.apache.flink.table.planner.sinks.{CollectRowTableSink, CollectTableSink}
import org.apache.flink.table.planner.utils.{TableTestUtil, TestTableSources}
import org.apache.flink.table.types.utils.TypeConversions.fromDataTypeToLegacyInfo
import org.apache.flink.types.Row
import org.apache.flink.util.AbstractID

import org.apache.calcite.plan.RelOptUtil
import org.apache.calcite.sql.SqlExplainLevel
import org.junit.Assert.assertEquals
import org.junit.rules.ExpectedException
import org.junit.{Rule, Test}

class TableEnvironmentTest {

  // used for accurate exception information checking.
  val expectedException: ExpectedException = ExpectedException.none()

  @Rule
  def thrown: ExpectedException = expectedException

  val env = new StreamExecutionEnvironment(new LocalStreamEnvironment())
  val tableEnv = StreamTableEnvironment.create(env, TableTestUtil.STREAM_SETTING)

  @Test
  def testScanNonExistTable(): Unit = {
    thrown.expect(classOf[ValidationException])
    thrown.expectMessage("Table `MyTable` was not found")
    tableEnv.scan("MyTable")
  }

  @Test
  def testRegisterDataStream(): Unit = {
    val table = env.fromElements[(Int, Long, String, Boolean)]().toTable(tableEnv, 'a, 'b, 'c, 'd)
    tableEnv.registerTable("MyTable", table)
    val scanTable = tableEnv.scan("MyTable")
    val relNode = TableTestUtil.toRelNode(scanTable)
    val actual = RelOptUtil.toString(relNode)
    val expected = "LogicalTableScan(table=[[default_catalog, default_database, MyTable]])\n"
    assertEquals(expected, actual)

    // register on a conflict name
    thrown.expect(classOf[ValidationException])
    thrown.expectMessage(
      "Temporary table `default_catalog`.`default_database`.`MyTable` already exists")
    tableEnv.registerDataStream("MyTable", env.fromElements[(Int, Long)]())
  }

  @Test
  def testSimpleQuery(): Unit = {
    val table = env.fromElements[(Int, Long, String, Boolean)]().toTable(tableEnv, 'a, 'b, 'c, 'd)
    tableEnv.registerTable("MyTable", table)
    val queryTable = tableEnv.sqlQuery("SELECT a, c, d FROM MyTable")
    val relNode = TableTestUtil.toRelNode(queryTable)
    val actual = RelOptUtil.toString(relNode, SqlExplainLevel.NO_ATTRIBUTES)
    val expected = "LogicalProject\n" +
      "  LogicalTableScan\n"
    assertEquals(expected, actual)
  }

  @Test
  def testExecuteTwiceUsingSameTableEnv(): Unit = {
    val settings = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build()
    val tEnv = TableEnvironmentImpl.create(settings)
    tEnv.registerTableSource("MyTable", TestTableSources.getPersonCsvTableSource)
    registerTableSink(tEnv, Array("first"), Array(STRING), "MySink1")
    registerTableSink(tEnv, Array("last"), Array(STRING), "MySink2")

    val table1 = tEnv.sqlQuery("select first from MyTable")
    tEnv.insertInto(table1, "MySink1")
    val res1 = tEnv.execute("test1")
    assertEquals(1, res1.getAllAccumulatorResults.size())

    val table2 = tEnv.sqlQuery("select last from MyTable")
    tEnv.insertInto(table2, "MySink2")
    val res2 = tEnv.execute("test2")
    assertEquals(1, res2.getAllAccumulatorResults.size())
  }

  private def registerTableSink(
      tEnv: TableEnvironment,
      fieldNames: Array[String],
      fieldTypes: Array[TypeInformation[_]],
      tableName: String): Unit = {
    val sink = new CollectRowTableSink()
      .configure(fieldNames, fieldTypes)
      .asInstanceOf[CollectTableSink[Row]]
    val id = new AbstractID().toString
    val typeSerializer = fromDataTypeToLegacyInfo(sink.getConsumedDataType)
      .asInstanceOf[TypeInformation[Row]]
      .createSerializer(tEnv.asInstanceOf[TableEnvironmentImpl]
        .getPlanner.asInstanceOf[PlannerBase].getExecEnv.getConfig)
    sink.init(typeSerializer.asInstanceOf[TypeSerializer[Row]], id)
    tEnv.registerTableSink(tableName, sink)
  }

}
