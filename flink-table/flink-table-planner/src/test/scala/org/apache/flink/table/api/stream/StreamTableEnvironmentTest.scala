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

package org.apache.flink.table.api.stream

import org.apache.flink.api.java.tuple.{Tuple5 => JTuple5}
import org.apache.flink.api.java.typeutils.TupleTypeInfo
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.{StreamExecutionEnvironment => JStreamExecEnv}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.java.internal.{StreamTableEnvironmentImpl => JStreamTableEnvironmentImpl}
import org.apache.flink.table.api.java.{StreamTableEnvironment => JStreamTableEnv}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{TableConfig, Types, ValidationException}
import org.apache.flink.table.catalog.{CatalogManager, FunctionCatalog, GenericInMemoryCatalog}
import org.apache.flink.table.executor.StreamExecutor
import org.apache.flink.table.planner.StreamPlanner
import org.apache.flink.table.runtime.utils.StreamTestData
import org.apache.flink.table.utils.TableTestBase
import org.apache.flink.table.utils.TableTestUtil.{binaryNode, streamTableNode, term, unaryNode}
import org.apache.flink.types.Row
import org.junit.Test
import org.mockito.Mockito.{mock, when}
import java.lang.{Integer => JInt, Long => JLong}

import org.apache.flink.table.module.ModuleManager

class StreamTableEnvironmentTest extends TableTestBase {

  @Test
  def testSqlWithoutRegistering(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]("tableName", 'a, 'b, 'c)

    val sqlTable = util.tableEnv.sqlQuery(s"SELECT a, b, c FROM $table WHERE b > 12")

    val expected = unaryNode(
      "DataStreamCalc",
      streamTableNode(table),
      term("select", "a, b, c"),
      term("where", ">(b, 12)"))

    util.verifyTable(sqlTable, expected)

    val table2 = util.addTable[(Long, Int, String)]('d, 'e, 'f)

    val sqlTable2 = util.tableEnv.sqlQuery(s"SELECT d, e, f FROM $table2 " +
        s"UNION ALL SELECT a, b, c FROM $table")

    val expected2 = binaryNode(
      "DataStreamUnion",
      streamTableNode(table2),
      streamTableNode(table),
      term("all", "true"),
      term("union all", "d, e, f"))

    util.verifyTable(sqlTable2, expected2)
  }

  @Test
  def testToAppendSinkOnUpdatingTable(): Unit = {
    expectedException.expect(classOf[ValidationException])
    expectedException.expectMessage("Table is not an append-only table. Use the toRetractStream()" +
      " in order to handle add and retract messages.")

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env)

    val t = StreamTestData.get3TupleDataStream(env).toTable(tEnv, 'id, 'num, 'text)

    t.groupBy('text)
      .select('text, 'id.count, 'num.sum)
      .toAppendStream[Row]

    // must fail because table is not append-only
    env.execute()
  }

  @Test
  def testProctimeAttributeWithAtomicInput(): Unit = {
    val util = streamTestUtil()
    // cannot replace an attribute with proctime
    util.addTable[String]('s, 'pt.proctime)
  }

  @Test
  def testReplacingRowtimeAttributeWithAtomicInput(): Unit = {
    val util = streamTestUtil()
    util.addTable[Long]('rt.rowtime)
  }

  @Test
  def testAppendedRowtimeAttributeWithAtomicInput(): Unit = {
    val util = streamTestUtil()
    util.addTable[String]('s, 'rt.rowtime)
  }

  @Test
  def testRowtimeAndProctimeAttributeWithAtomicInput1(): Unit = {
    val util = streamTestUtil()
    util.addTable[String]('s, 'rt.rowtime, 'pt.proctime)
  }

  @Test
  def testRowtimeAndProctimeAttributeWithAtomicInput2(): Unit = {
    val util = streamTestUtil()
    util.addTable[String]('s, 'pt.proctime, 'rt.rowtime)
  }

  @Test
  def testRowtimeAndProctimeAttributeWithAtomicInput3(): Unit = {
    val util = streamTestUtil()
    util.addTable[Long]('rt.rowtime, 'pt.proctime)
  }

  @Test
  def testProctimeAttribute(): Unit = {
    val util = streamTestUtil()
    // cannot replace an attribute with proctime
    util.addTable[(Long, Int, String, Int, Long)]('a, 'b, 'c, 'd, 'e, 'pt.proctime)
  }

  @Test
  def testReplacedRowtimeAttribute(): Unit = {
    val util = streamTestUtil()
    util.addTable[(Long, Int, String, Int, Long)]('rt.rowtime, 'b, 'c, 'd, 'e)
  }

  @Test
  def testAppendedRowtimeAttribute(): Unit = {
    val util = streamTestUtil()
    util.addTable[(Long, Int, String, Int, Long)]('a, 'b, 'c, 'd, 'e, 'rt.rowtime)
  }

  @Test
  def testRowtimeAndProctimeAttribute1(): Unit = {
    val util = streamTestUtil()
    util.addTable[(Long, Int, String, Int, Long)]('a, 'b, 'c, 'd, 'e, 'rt.rowtime, 'pt.proctime)
  }

  @Test
  def testRowtimeAndProctimeAttribute2(): Unit = {
    val util = streamTestUtil()
    util.addTable[(Long, Int, String, Int, Long)]('a, 'b, 'c, 'd, 'e, 'pt.proctime, 'rt.rowtime)
  }

  @Test
  def testRowtimeAndProctimeAttribute3(): Unit = {
    val util = streamTestUtil()
    util.addTable[(Long, Int, String, Int, Long)]('rt.rowtime, 'b, 'c, 'd, 'e, 'pt.proctime)
  }

  @Test
  def testProctimeAttributeParsed(): Unit = {
    val (jTEnv, ds) = prepareSchemaExpressionParser
    jTEnv.fromDataStream(ds, "a, b, c, d, e, pt.proctime")
  }

  @Test
  def testReplacingRowtimeAttributeParsed(): Unit = {
    val (jTEnv, ds) = prepareSchemaExpressionParser
    jTEnv.fromDataStream(ds, "a.rowtime, b, c, d, e")
  }

  @Test
  def testAppedingRowtimeAttributeParsed(): Unit = {
    val (jTEnv, ds) = prepareSchemaExpressionParser
    jTEnv.fromDataStream(ds, "a, b, c, d, e, rt.rowtime")
  }

  @Test
  def testRowtimeAndProctimeAttributeParsed1(): Unit = {
    val (jTEnv, ds) = prepareSchemaExpressionParser
    jTEnv.fromDataStream(ds, "a, b, c, d, e, pt.proctime, rt.rowtime")
  }

  @Test
  def testRowtimeAndProctimeAttributeParsed2(): Unit = {
    val (jTEnv, ds) = prepareSchemaExpressionParser
    jTEnv.fromDataStream(ds, "rt.rowtime, b, c, d, e, pt.proctime")
  }

  private def prepareSchemaExpressionParser:
    (JStreamTableEnv, DataStream[JTuple5[JLong, JInt, String, JInt, JLong]]) = {

    val jStreamExecEnv = mock(classOf[JStreamExecEnv])
    when(jStreamExecEnv.getStreamTimeCharacteristic).thenReturn(TimeCharacteristic.EventTime)
    val config = new TableConfig
    val manager: CatalogManager = new CatalogManager(
      "default_catalog",
      new GenericInMemoryCatalog("default_catalog", "default_database"))
    val moduleManager: ModuleManager = new ModuleManager
    val executor: StreamExecutor = new StreamExecutor(jStreamExecEnv)
    val functionCatalog = new FunctionCatalog(config, manager, moduleManager)
    val streamPlanner = new StreamPlanner(executor, config, functionCatalog, manager)
    val jTEnv = new JStreamTableEnvironmentImpl(
      manager,
      moduleManager,
      functionCatalog,
      config,
      jStreamExecEnv,
      streamPlanner,
      executor,
      true)

    val sType = new TupleTypeInfo(Types.LONG, Types.INT, Types.STRING, Types.INT, Types.LONG)
      .asInstanceOf[TupleTypeInfo[JTuple5[JLong, JInt, String, JInt, JLong]]]
    val ds = mock(classOf[DataStream[JTuple5[JLong, JInt, String, JInt, JLong]]])
    when(ds.getType).thenReturn(sType)

    (jTEnv, ds)
  }

}
