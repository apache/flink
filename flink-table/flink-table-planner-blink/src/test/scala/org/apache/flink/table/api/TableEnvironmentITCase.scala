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

import org.apache.flink.api.common.typeinfo.Types.STRING
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment => ScalaStreamExecutionEnvironment}
import org.apache.flink.table.api.internal.TableEnvironmentImpl
import org.apache.flink.table.api.java.StreamTableEnvironment
import org.apache.flink.table.api.scala.{StreamTableEnvironment => ScalaStreamTableEnvironment, _}
import org.apache.flink.table.planner.factories.utils.TestCollectionTableFactory
import org.apache.flink.table.planner.runtime.utils.TestingAppendSink
import org.apache.flink.table.planner.utils.TableTestUtil.{readFromResource, replaceStageId}
import org.apache.flink.table.planner.utils.TestTableSourceSinks
import org.apache.flink.types.Row
import org.apache.flink.util.{FileUtils, TestLogger}

import org.junit.Assert.{assertEquals, assertFalse, assertTrue}
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.{Assert, Before, Rule, Test}

import _root_.java.io.{File, FileFilter}
import _root_.java.lang.{Long => JLong}
import _root_.java.util

import _root_.scala.collection.mutable

@RunWith(classOf[Parameterized])
class TableEnvironmentITCase(tableEnvName: String, isStreaming: Boolean) extends TestLogger {

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
  def testExecuteTwiceUsingSameTableEnv(): Unit = {
    val sink1Path = TestTableSourceSinks.createCsvTemporarySinkTable(
      tEnv, new TableSchema(Array("first"), Array(STRING)), "MySink1")

    val sink2Path = TestTableSourceSinks.createCsvTemporarySinkTable(
      tEnv, new TableSchema(Array("first"), Array(STRING)), "MySink2")


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
    val sinkPath = TestTableSourceSinks.createCsvTemporarySinkTable(
      tEnv, new TableSchema(Array("first"), Array(STRING)), "MySink1")

    val table1 = tEnv.sqlQuery("select first from MyTable")
    tEnv.insertInto(table1, "MySink1")

    tEnv.explain(false)
    tEnv.execute("test1")
    assertFirstValues(sinkPath)
  }

  @Test
  def testExplainAndExecuteMultipleSink(): Unit = {
    val sink1Path = TestTableSourceSinks.createCsvTemporarySinkTable(
      tEnv, new TableSchema(Array("first"), Array(STRING)), "MySink1")

    val sink2Path = TestTableSourceSinks.createCsvTemporarySinkTable(
      tEnv, new TableSchema(Array("first"), Array(STRING)), "MySink2")

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
    val sink1Path = TestTableSourceSinks.createCsvTemporarySinkTable(
      tEnv, new TableSchema(Array("first"), Array(STRING)), "MySink1")

    val sink2Path = TestTableSourceSinks.createCsvTemporarySinkTable(
      tEnv, new TableSchema(Array("first"), Array(STRING)), "MySink2")

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
    TestTableSourceSinks.createPersonCsvTemporaryTable(streamTableEnv, "MyTable")
    val sink1Path = TestTableSourceSinks.createCsvTemporarySinkTable(
      streamTableEnv, new TableSchema(Array("first"), Array(STRING)), "MySink1")
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
    TestTableSourceSinks.createPersonCsvTemporaryTable(streamTableEnv, "MyTable")
    val sink1Path = TestTableSourceSinks.createCsvTemporarySinkTable(
      streamTableEnv, new TableSchema(Array("first"), Array(STRING)), "MySink1")
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
    val sink1Path = TestTableSourceSinks.createCsvTemporarySinkTable(
      streamTableEnv, new TableSchema(Array("first"), Array(STRING)), "MySink1")
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
  def testExecuteSqlWithInsertInto(): Unit = {
    val sinkPath = TestTableSourceSinks.createCsvTemporarySinkTable(
      tEnv, new TableSchema(Array("first"), Array(STRING)), "MySink1")
    checkEmptyFile(sinkPath)
    val tableResult = tEnv.executeSql("insert into MySink1 select first from MyTable")
    checkInsertTableResult(tableResult)
    // wait job finished
    tableResult.getJobClient.get()
      .getJobExecutionResult(Thread.currentThread().getContextClassLoader)
      .get()
    assertFirstValues(sinkPath)
  }

  @Test
  def testExecuteSqlWithInsertOverwrite(): Unit = {
    if(isStreaming) {
      // Streaming mode not support overwrite for FileSystemTableSink.
      return
    }

    val sinkPath = _tempFolder.newFolder().toString
    tEnv.sqlUpdate(
      s"""
         |create table MySink (
         |  first string
         |) with (
         |  'connector' = 'filesystem',
         |  'path' = '$sinkPath',
         |  'format' = 'testcsv'
         |)
       """.stripMargin
    )

    val tableResult1 = tEnv.executeSql("insert overwrite MySink select first from MyTable")
    checkInsertTableResult(tableResult1)
    // wait job finished
    tableResult1.getJobClient.get()
      .getJobExecutionResult(Thread.currentThread().getContextClassLoader)
      .get()
    assertFirstValues(sinkPath)

    val tableResult2 =  tEnv.executeSql("insert overwrite MySink select first from MyTable")
    checkInsertTableResult(tableResult2)
    // wait job finished
    tableResult2.getJobClient.get()
      .getJobExecutionResult(Thread.currentThread().getContextClassLoader)
      .get()
    assertFirstValues(sinkPath)
  }

  @Test
  def testExecuteSqlAndSqlUpdate(): Unit = {
    val sink1Path = TestTableSourceSinks.createCsvTemporarySinkTable(
      tEnv, new TableSchema(Array("first"), Array(STRING)), "MySink1")
    val sink2Path = TestTableSourceSinks.createCsvTemporarySinkTable(
      tEnv, new TableSchema(Array("last"), Array(STRING)), "MySink2")
    checkEmptyFile(sink1Path)
    checkEmptyFile(sink2Path)

    val tableResult = tEnv.executeSql("insert into MySink1 select first from MyTable")
    checkInsertTableResult(tableResult)
    // wait job finished
    tableResult.getJobClient.get()
      .getJobExecutionResult(Thread.currentThread().getContextClassLoader)
      .get()

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
  def testExecuteSqlAndToDataStream(): Unit = {
    if (!tableEnvName.equals("StreamTableEnvironment")) {
      return
    }
    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment
    val streamTableEnv = StreamTableEnvironment.create(streamEnv, settings)
    TestTableSourceSinks.createPersonCsvTemporaryTable(streamTableEnv, "MyTable")
    val sink1Path = TestTableSourceSinks.createCsvTemporarySinkTable(
      streamTableEnv, new TableSchema(Array("first"), Array(STRING)), "MySink1")
    checkEmptyFile(sink1Path)

    val table = streamTableEnv.sqlQuery("select last from MyTable where id > 0")
    val resultSet = streamTableEnv.toAppendStream(table, classOf[Row])
    val sink = new TestingAppendSink
    resultSet.addSink(sink)

    val tableResult = streamTableEnv.executeSql("insert into MySink1 select first from MyTable")
    checkInsertTableResult(tableResult)
    // wait job finished
    tableResult.getJobClient.get()
      .getJobExecutionResult(Thread.currentThread().getContextClassLoader)
      .get()
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
  def testExecuteInsert(): Unit = {
    val sinkPath = TestTableSourceSinks.createCsvTemporarySinkTable(
      tEnv, new TableSchema(Array("first"), Array(STRING)), "MySink")
    checkEmptyFile(sinkPath)
    val table = tEnv.sqlQuery("select first from MyTable")
    val tableResult = table.executeInsert("MySink")
    checkInsertTableResult(tableResult)
    // wait job finished
    tableResult.getJobClient.get()
      .getJobExecutionResult(Thread.currentThread().getContextClassLoader)
      .get()
    assertFirstValues(sinkPath)
  }

  @Test
  def testExecuteInsertOverwrite(): Unit = {
    if(isStreaming) {
      // Streaming mode not support overwrite for FileSystemTableSink.
      return
    }
    val sinkPath = _tempFolder.newFolder().toString
    tEnv.sqlUpdate(
      s"""
         |create table MySink (
         |  first string
         |) with (
         |  'connector' = 'filesystem',
         |  'path' = '$sinkPath',
         |  'format' = 'testcsv'
         |)
       """.stripMargin
    )
    val tableResult1 = tEnv.sqlQuery("select first from MyTable").executeInsert("MySink", true)
    checkInsertTableResult(tableResult1)
    // wait job finished
    tableResult1.getJobClient.get()
      .getJobExecutionResult(Thread.currentThread().getContextClassLoader)
      .get()
    assertFirstValues(sinkPath)

    val tableResult2 = tEnv.sqlQuery("select first from MyTable").executeInsert("MySink", true)
    checkInsertTableResult(tableResult2)
    // wait job finished
    tableResult2.getJobClient.get()
      .getJobExecutionResult(Thread.currentThread().getContextClassLoader)
      .get()
    assertFirstValues(sinkPath)
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
    val actual = readFile(csvFilePath)
    assertEquals(expected.sorted, actual.sorted)
  }

  private def assertLastValues(csvFilePath: String): Unit = {
    val actual = readFile(csvFilePath)
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

  private def checkInsertTableResult(tableResult: TableResult): Unit = {
    assertTrue(tableResult.getJobClient.isPresent)
    assertEquals(ResultKind.SUCCESS_WITH_CONTENT, tableResult.getResultKind)
    val it = tableResult.collect()
    assertTrue(it.hasNext)
    assertEquals(Row.of(JLong.valueOf(-1L)), it.next())
    assertFalse(it.hasNext)
  }

  private def readFile(csvFilePath: String): List[String] = {
    val file = new File(csvFilePath)
    if (file.isDirectory) {
      file.listFiles(new FileFilter() {
        override def accept(f: File): Boolean = f.isFile
      }).map(FileUtils.readFileUtf8).flatMap(_.split("\n")).toList
    } else {
      FileUtils.readFileUtf8(file).split("\n").toList
    }
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
