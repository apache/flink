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
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.internal.TableEnvironmentImpl
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.planner.utils.{TableTestUtil, TestTableSources}
import org.apache.flink.table.sinks.CsvTableSink
import org.apache.flink.util.FileUtils

import org.junit.Assert.{assertEquals, assertFalse, assertTrue}
import org.junit.rules.TemporaryFolder
import org.junit.{Rule, Test}

import _root_.java.io.File


class TableEnvironmentITCase {

  private val _tempFolder = new TemporaryFolder()

  @Rule
  def tempFolder: TemporaryFolder = _tempFolder

  @Test
  def testExecuteTwiceUsingSameTableEnvOnBatch(): Unit = {
    val settings = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build()
    testExecuteTwiceUsingSameTableEnv(TableEnvironmentImpl.create(settings))
  }

  @Test
  def testExecuteTwiceUsingSameTableEnvOnStream(): Unit = {
    // test TableEnvironment
    val settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    testExecuteTwiceUsingSameTableEnv(TableEnvironmentImpl.create(settings))

    // test StreamTableEnvironment
    testExecuteTwiceUsingSameTableEnv(StreamTableEnvironment.create(
      StreamExecutionEnvironment.getExecutionEnvironment, settings))
  }

  private def testExecuteTwiceUsingSameTableEnv(tEnv: TableEnvironment): Unit = {
    tEnv.registerTableSource("MyTable", TestTableSources.getPersonCsvTableSource)
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
  def testExplainAndExecuteTableEnvOnBatch(): Unit = {
    val settings = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build()
    testExplainAndExecuteSingleSink(TableEnvironmentImpl.create(settings))
    testExplainAndExecuteMultipleSink(TableEnvironmentImpl.create(settings))
  }

  @Test
  def testExplainAndExecuteTableEnvOnStream(): Unit = {
    val settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    testExplainAndExecuteSingleSink(TableEnvironmentImpl.create(settings))
    testExplainAndExecuteMultipleSink(TableEnvironmentImpl.create(settings))
  }

  private def testExplainAndExecuteSingleSink(tEnv: TableEnvironment): Unit = {
    tEnv.registerTableSource("MyTable", TestTableSources.getPersonCsvTableSource)
    val sinkPath = registerCsvTableSink(tEnv, Array("first"), Array(STRING), "MySink1")

    val table1 = tEnv.sqlQuery("select first from MyTable")
    tEnv.insertInto(table1, "MySink1")

    tEnv.explain(false)
    tEnv.execute("test1")
    assertFirstValues(sinkPath)
  }

  private def testExplainAndExecuteMultipleSink(tEnv: TableEnvironment): Unit = {
    tEnv.registerTableSource("MyTable", TestTableSources.getPersonCsvTableSource)
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
  def testExplainTwiceOnBatch(): Unit = {
    val settings = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build()
    testExplainTwice(TableEnvironmentImpl.create(settings))
  }

  @Test
  def testExplainTwiceOnStream(): Unit = {
    val settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    testExplainTwice(TableEnvironmentImpl.create(settings))
  }

  private def testExplainTwice(tEnv: TableEnvironment): Unit = {
    tEnv.registerTableSource("MyTable", TestTableSources.getPersonCsvTableSource)
    registerCsvTableSink(tEnv, Array("first"), Array(STRING), "MySink1")
    registerCsvTableSink(tEnv, Array("first"), Array(STRING), "MySink2")

    val table1 = tEnv.sqlQuery("select first from MyTable")
    tEnv.insertInto(table1, "MySink1")
    val table2 = tEnv.sqlQuery("select last from MyTable")
    tEnv.insertInto(table2, "MySink2")

    val result1 = tEnv.explain(false)
    val result2 = tEnv.explain(false)
    assertEquals(TableTestUtil.replaceStageId(result1), TableTestUtil.replaceStageId(result2))
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

  private def assertFirstValues(csvFilePath: String): Unit = {
    val expected = List("Mike", "Bob", "Sam", "Peter", "Liz", "Sally", "Alice", "Kelly")
    val actual = FileUtils.readFileUtf8(new File(csvFilePath)).split("\n").toList
    assertEquals(expected.sorted, actual.sorted)
  }

  private def assertLastValues(csvFilePath: String): Unit = {
    val expected = List(
      "Smith", "Taylor", "Miller", "Smith", "Williams", "Miller", "Smith", "Williams")
    val actual = FileUtils.readFileUtf8(new File(csvFilePath)).split("\n").toList
    assertEquals(expected.sorted, actual.sorted)
  }

  private def checkEmptyFile(csvFilePath: String): Unit = {
    assertTrue(FileUtils.readFileUtf8(new File(csvFilePath)).isEmpty)
  }
}
