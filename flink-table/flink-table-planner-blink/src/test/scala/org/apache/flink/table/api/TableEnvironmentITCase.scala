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
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment => ScalaStreamExecutionEnvironment}
import org.apache.flink.table.api.internal.TableEnvironmentImpl
import org.apache.flink.table.api.java.StreamTableEnvironment
import org.apache.flink.table.api.scala.{StreamTableEnvironment => ScalaStreamTableEnvironment, _}
import org.apache.flink.table.planner.runtime.utils.TestingAppendSink
import org.apache.flink.table.planner.utils.TableTestUtil.{readFromResource, replaceStageId}
import org.apache.flink.table.planner.utils.TestTableSources.getPersonCsvTableSource
import org.apache.flink.table.sinks.CsvTableSink
import org.apache.flink.types.Row
import org.apache.flink.util.FileUtils

import org.junit.Assert.{assertEquals, assertFalse, assertTrue}
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.{Assert, Before, Rule, Test}

import _root_.java.io.File
import _root_.java.util

import org.apache.flink.table.planner.factories.utils.TestCollectionTableFactory

import _root_.scala.collection.mutable


@RunWith(classOf[Parameterized])
class TableEnvironmentITCase(tableEnvName: String, isStreaming: Boolean) {

  private val _tempFolder = new TemporaryFolder()

  @Rule
  def tempFolder: TemporaryFolder = _tempFolder

  var tEnv: TableEnvironment = _

  private val settings = if (isStreaming) {
    EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
  } else {
    EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build()
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
    tEnv.registerTableSource("MyTable", getPersonCsvTableSource)
  }

  @Test
  def testExecuteTwiceUsingSameTableEnv(): Unit = {
    val sink1Path = registerCsvTableSink(tEnv, Array("first"), Array(STRING), "MySink1")
    val sink2Path = registerCsvTableSink(tEnv, Array("last"), Array(STRING), "MySink2")
    checkEmptyFile(sink1Path)
    checkEmptyFile(sink2Path)

    val table1 = tEnv.sqlQuery("select first from MyTable")
    tEnv.insertInto(table1, "MySink1")
    tEnv.execute("test1")
    assertFirstValues(sink1Path)
    checkEmptyFile(sink2Path)

    // delete first csv file
    new File(sink1Path).delete()
    assertFalse(new File(sink1Path).exists())

    val table2 = tEnv.sqlQuery("select last from MyTable")
    tEnv.insertInto(table2, "MySink2")
    tEnv.execute("test2")
    assertFalse(new File(sink1Path).exists())
    assertLastValues(sink2Path)
  }

  @Test
  def testExplainAndExecuteSingleSink(): Unit = {
    val sinkPath = registerCsvTableSink(tEnv, Array("first"), Array(STRING), "MySink1")

    val table1 = tEnv.sqlQuery("select first from MyTable")
    tEnv.insertInto(table1, "MySink1")

    tEnv.explain(false)
    tEnv.execute("test1")
    assertFirstValues(sinkPath)
  }

  @Test
  def testExplainAndExecuteMultipleSink(): Unit = {
    val sink1Path = registerCsvTableSink(tEnv, Array("first"), Array(STRING), "MySink1")
    val sink2Path = registerCsvTableSink(tEnv, Array("first"), Array(STRING), "MySink2")

    val table1 = tEnv.sqlQuery("select first from MyTable")
    tEnv.insertInto(table1, "MySink1")
    val table2 = tEnv.sqlQuery("select last from MyTable")
    tEnv.insertInto(table2, "MySink2")

    tEnv.explain(false)
    tEnv.execute("test1")
    assertFirstValues(sink1Path)
    assertLastValues(sink2Path)
  }

  @Test
  def testExplainTwice(): Unit = {
    registerCsvTableSink(tEnv, Array("first"), Array(STRING), "MySink1")
    registerCsvTableSink(tEnv, Array("first"), Array(STRING), "MySink2")

    val table1 = tEnv.sqlQuery("select first from MyTable")
    tEnv.insertInto(table1, "MySink1")
    val table2 = tEnv.sqlQuery("select last from MyTable")
    tEnv.insertInto(table2, "MySink2")

    val result1 = tEnv.explain(false)
    val result2 = tEnv.explain(false)
    assertEquals(replaceStageId(result1), replaceStageId(result2))
  }

  @Test
  def testSqlUpdateAndToDataStream(): Unit = {
    if (!tableEnvName.equals("StreamTableEnvironment")) {
      return
    }
    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment
    val streamTableEnv = StreamTableEnvironment.create(streamEnv, settings)
    streamTableEnv.registerTableSource("MyTable", getPersonCsvTableSource)
    val sink1Path = registerCsvTableSink(streamTableEnv, Array("first"), Array(STRING), "MySink1")
    checkEmptyFile(sink1Path)

    streamTableEnv.sqlUpdate("insert into MySink1 select first from MyTable")

    val table = streamTableEnv.sqlQuery("select last from MyTable where id > 0")
    val resultSet = streamTableEnv.toAppendStream(table, classOf[Row])
    val sink = new TestingAppendSink
    resultSet.addSink(sink)

    val explain = streamTableEnv.explain(false)
    assertEquals(
      replaceStageId(readFromResource("/explain/testSqlUpdateAndToDataStream.out")),
      replaceStageId(explain))

    streamTableEnv.execute("test1")
    assertFirstValues(sink1Path)

    // the DataStream program is not executed
    assertFalse(sink.isInitialized)

    deleteFile(sink1Path)

    streamEnv.execute("test2")
    assertEquals(getExpectedLastValues.sorted, sink.getAppendResults.sorted)
    // the table program is not executed again
    assertFileNotExist(sink1Path)
  }

  @Test
  def testToDataStreamAndSqlUpdate(): Unit = {
    if (!tableEnvName.equals("StreamTableEnvironment")) {
      return
    }
    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment
    val streamTableEnv = StreamTableEnvironment.create(streamEnv, settings)
    streamTableEnv.registerTableSource("MyTable", getPersonCsvTableSource)
    val sink1Path = registerCsvTableSink(streamTableEnv, Array("first"), Array(STRING), "MySink1")
    checkEmptyFile(sink1Path)

    val table = streamTableEnv.sqlQuery("select last from MyTable where id > 0")
    val resultSet = streamTableEnv.toAppendStream(table, classOf[Row])
    val sink = new TestingAppendSink
    resultSet.addSink(sink)

    streamTableEnv.sqlUpdate("insert into MySink1 select first from MyTable")

    val explain = streamTableEnv.explain(false)
    assertEquals(
      replaceStageId(readFromResource("/explain/testSqlUpdateAndToDataStream.out")),
      replaceStageId(explain))

    streamEnv.execute("test2")
    // the table program is not executed
    checkEmptyFile(sink1Path)
    assertEquals(getExpectedLastValues.sorted, sink.getAppendResults.sorted)

    streamTableEnv.execute("test1")
    assertFirstValues(sink1Path)
    // the DataStream program is not executed again because the result in sink is not changed
    assertEquals(getExpectedLastValues.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testFromToDataStreamAndSqlUpdate(): Unit = {
    if (!tableEnvName.equals("StreamTableEnvironment")) {
      return
    }
    val streamEnv = ScalaStreamExecutionEnvironment.getExecutionEnvironment
    val streamTableEnv = ScalaStreamTableEnvironment.create(streamEnv, settings)
    val t = streamEnv.fromCollection(getPersonData)
      .toTable(streamTableEnv, 'first, 'id, 'score, 'last)
    streamTableEnv.registerTable("MyTable", t)
    val sink1Path = registerCsvTableSink(streamTableEnv, Array("first"), Array(STRING), "MySink1")
    checkEmptyFile(sink1Path)

    val table = streamTableEnv.sqlQuery("select last from MyTable where id > 0")
    val resultSet = streamTableEnv.toAppendStream[Row](table)
    val sink = new TestingAppendSink
    resultSet.addSink(sink)

    streamTableEnv.sqlUpdate("insert into MySink1 select first from MyTable")

    val explain = streamTableEnv.explain(false)
    assertEquals(
      replaceStageId(readFromResource("/explain/testFromToDataStreamAndSqlUpdate.out")),
      replaceStageId(explain))

    streamEnv.execute("test2")
    // the table program is not executed
    checkEmptyFile(sink1Path)
    assertEquals(getExpectedLastValues.sorted, sink.getAppendResults.sorted)

    streamTableEnv.execute("test1")
    assertFirstValues(sink1Path)
    // the DataStream program is not executed again because the result in sink is not changed
    assertEquals(getExpectedLastValues.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testClearOperation(): Unit = {
    TestCollectionTableFactory.reset()
    val tableEnv = TableEnvironmentImpl.create(settings)
    tableEnv.sqlUpdate("create table dest1(x map<int,bigint>) with('connector' = 'COLLECTION')")
    tableEnv.sqlUpdate("create table dest2(x int) with('connector' = 'COLLECTION')")
    tableEnv.sqlUpdate("create table src(x int) with('connector' = 'COLLECTION')")

    try {
      // it would fail due to query and sink type mismatch
      tableEnv.sqlUpdate("insert into dest1 select count(*) from src")
      tableEnv.execute("insert dest1")
      Assert.fail("insert is expected to fail due to type mismatch")
    } catch {
      case _: Exception => //expected
    }

    tableEnv.sqlUpdate("drop table dest1")
    tableEnv.sqlUpdate("insert into dest2 select x from src")
    tableEnv.execute("insert dest2")
  }

  private def registerCsvTableSink(
      tEnv: TableEnvironment,
      fieldNames: Array[String],
      fieldTypes: Array[TypeInformation[_]],
      tableName: String): String = {
    val resultFile = _tempFolder.newFile()
    val path = resultFile.getAbsolutePath

    val configuredSink = new CsvTableSink(path, ",", 1, WriteMode.OVERWRITE)
      .configure(fieldNames, fieldTypes)
    tEnv.registerTableSink(tableName, configuredSink)

    path
  }

  def getPersonData: List[(String, Int, Double, String)] = {
    val data = new mutable.MutableList[(String, Int, Double, String)]
    data.+=(("Mike", 1, 12.3, "Smith"))
    data.+=(("Bob", 2, 45.6, "Taylor"))
    data.+=(("Sam", 3, 7.89, "Miller"))
    data.+=(("Peter", 4, 0.12, "Smith"))
    data.+=(("Liz", 5, 34.5, "Williams"))
    data.+=(("Sally", 6, 6.78, "Miller"))
    data.+=(("Alice", 7, 90.1, "Smith"))
    data.+=(("Kelly", 8, 2.34, "Williams"))
    data.toList
  }

  private def assertFirstValues(csvFilePath: String): Unit = {
    val expected = List("Mike", "Bob", "Sam", "Peter", "Liz", "Sally", "Alice", "Kelly")
    val actual = FileUtils.readFileUtf8(new File(csvFilePath)).split("\n").toList
    assertEquals(expected.sorted, actual.sorted)
  }

  private def assertLastValues(csvFilePath: String): Unit = {
    val actual = FileUtils.readFileUtf8(new File(csvFilePath)).split("\n").toList
    assertEquals(getExpectedLastValues.sorted, actual.sorted)
  }

  private def getExpectedLastValues: List[String] = {
    List("Smith", "Taylor", "Miller", "Smith", "Williams", "Miller", "Smith", "Williams")
  }

  private def checkEmptyFile(csvFilePath: String): Unit = {
    assertTrue(FileUtils.readFileUtf8(new File(csvFilePath)).isEmpty)
  }

  private def deleteFile(path: String): Unit = {
    new File(path).delete()
    assertFalse(new File(path).exists())
  }

  private def assertFileNotExist(path: String): Unit = {
    assertFalse(new File(path).exists())
  }
}

object TableEnvironmentITCase {
  @Parameterized.Parameters(name = "{0}:isStream={1}")
  def parameters(): util.Collection[Array[_]] = {
    util.Arrays.asList(
      Array("TableEnvironment", true),
      Array("TableEnvironment", false),
      Array("StreamTableEnvironment", true)
    )
  }
}
