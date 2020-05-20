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
import org.apache.flink.table.api.TableEnvironmentITCase.getPersonCsvTableSource
import org.apache.flink.table.api.internal.{TableEnvironmentImpl, TableEnvironmentInternal}
import org.apache.flink.table.api.java.StreamTableEnvironment
import org.apache.flink.table.api.scala.{StreamTableEnvironment => ScalaStreamTableEnvironment}
import org.apache.flink.table.runtime.utils.StreamITCase
import org.apache.flink.table.sinks.CsvTableSink
import org.apache.flink.table.sources.CsvTableSource
import org.apache.flink.table.utils.TableTestUtil.{readFromResource, replaceStageId}
import org.apache.flink.table.utils.{TestTableSourceWithTime, TestingOverwritableTableSink}
import org.apache.flink.types.Row
import org.apache.flink.util.FileUtils
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists
import org.hamcrest.Matchers.containsString
import org.junit.Assert.{assertEquals, assertFalse, assertTrue}
import org.junit.rules.{ExpectedException, TemporaryFolder}
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.{After, Before, Rule, Test}
import _root_.java.io.{File, FileOutputStream, OutputStreamWriter}
import _root_.java.lang.{Long => JLong}
import _root_.java.util

import _root_.scala.collection.mutable


@RunWith(classOf[Parameterized])
class TableEnvironmentITCase(tableEnvName: String) {

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

  @After
  def teardown(): Unit = {
    StreamITCase.clear
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
    streamTableEnv.asInstanceOf[TableEnvironmentInternal].registerTableSourceInternal(
      "MyTable", getPersonCsvTableSource)
    val sink1Path = registerCsvTableSink(streamTableEnv, Array("first"), Array(STRING), "MySink1")
    checkEmptyFile(sink1Path)
    StreamITCase.clear

    streamTableEnv.sqlUpdate("insert into MySink1 select first from MyTable")

    val table = streamTableEnv.sqlQuery("select last from MyTable where id > 0")
    val resultSet = streamTableEnv.toAppendStream(table, classOf[Row])
    resultSet.addSink(new StreamITCase.StringSink[Row])

    val explain = streamTableEnv.explain(false)
    assertEquals(
      replaceStageId(readFromResource("testSqlUpdateAndToDataStream.out")),
      replaceStageId(explain))

    streamTableEnv.execute("test1")
    assertFirstValues(sink1Path)

    // the DataStream program is not executed
    assertTrue(StreamITCase.testResults.isEmpty)

    deleteFile(sink1Path)

    streamEnv.execute("test2")
    assertEquals(getExpectedLastValues.sorted, StreamITCase.testResults.sorted)
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
    streamTableEnv.asInstanceOf[TableEnvironmentInternal].registerTableSourceInternal(
      "MyTable", getPersonCsvTableSource)
    val sink1Path = registerCsvTableSink(streamTableEnv, Array("first"), Array(STRING), "MySink1")
    checkEmptyFile(sink1Path)
    StreamITCase.clear

    val table = streamTableEnv.sqlQuery("select last from MyTable where id > 0")
    val resultSet = streamTableEnv.toAppendStream(table, classOf[Row])
    resultSet.addSink(new StreamITCase.StringSink[Row])

    streamTableEnv.sqlUpdate("insert into MySink1 select first from MyTable")

    val explain = streamTableEnv.explain(false)
    assertEquals(
      replaceStageId(readFromResource("testSqlUpdateAndToDataStream.out")),
      replaceStageId(explain))

    streamEnv.execute("test2")
    // the table program is not executed
    checkEmptyFile(sink1Path)
    assertEquals(getExpectedLastValues.sorted, StreamITCase.testResults.sorted)
    StreamITCase.testResults.clear()

    streamTableEnv.execute("test1")
    assertFirstValues(sink1Path)
    // the DataStream program is not executed again
    assertTrue(StreamITCase.testResults.isEmpty)
  }

  @Test
  def testFromToDataStreamAndSqlUpdate(): Unit = {
    if (!tableEnvName.equals("StreamTableEnvironment")) {
      return
    }
    val streamEnv = ScalaStreamExecutionEnvironment.getExecutionEnvironment
    val streamTableEnv = ScalaStreamTableEnvironment.create(streamEnv, settings)
    streamTableEnv.asInstanceOf[TableEnvironmentInternal].registerTableSourceInternal(
      "MyTable", getPersonCsvTableSource)
    val sink1Path = registerCsvTableSink(streamTableEnv, Array("first"), Array(STRING), "MySink1")
    checkEmptyFile(sink1Path)
    StreamITCase.clear

    val table = streamTableEnv.sqlQuery("select last from MyTable where id > 0")
    val resultSet = streamTableEnv.toAppendStream[Row](table)
    resultSet.addSink(new StreamITCase.StringSink[Row])

    streamTableEnv.sqlUpdate("insert into MySink1 select first from MyTable")

    val explain = streamTableEnv.explain(false)
    assertEquals(
      replaceStageId(readFromResource("testFromToDataStreamAndSqlUpdate.out")),
      replaceStageId(explain).replaceAll("Scan\\(id=\\[\\d+\\], ", "Scan("))

    streamEnv.execute("test2")
    // the table program is not executed
    checkEmptyFile(sink1Path)
    assertEquals(getExpectedLastValues.sorted, StreamITCase.testResults.sorted)
    StreamITCase.testResults.clear()

    streamTableEnv.execute("test1")
    assertFirstValues(sink1Path)
    // the DataStream program is not executed again
    assertTrue(StreamITCase.testResults.isEmpty)
  }

  @Test
  def testExecuteSqlWithInsertInto(): Unit = {
    val sinkPath = registerCsvTableSink(tEnv, Array("first"), Array(STRING), "MySink1")
    checkEmptyFile(sinkPath)
    val tableResult = tEnv.executeSql("insert into MySink1 select first from MyTable")
    checkInsertTableResult(tableResult, "default_catalog.default_database.MySink1")
    // wait job finished
    tableResult.getJobClient.get()
      .getJobExecutionResult(Thread.currentThread().getContextClassLoader)
      .get()
    assertFirstValues(sinkPath)
  }

  @Test
  def testExecuteSqlWithInsertOverwrite(): Unit = {
    val resultFile = _tempFolder.newFile()
    val sinkPath = resultFile.getAbsolutePath
    val configuredSink = new TestingOverwritableTableSink(sinkPath)
      .configure(Array("first"), Array(STRING))
    tEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal("MySink", configuredSink)

    checkEmptyFile(sinkPath)
    val tableResult1 = tEnv.executeSql("insert overwrite MySink select first from MyTable")
    checkInsertTableResult(tableResult1, "default_catalog.default_database.MySink")
    // wait job finished
    tableResult1.getJobClient.get()
      .getJobExecutionResult(Thread.currentThread().getContextClassLoader)
      .get()
    assertFirstValues(sinkPath)

    val tableResult2 = tEnv.executeSql("insert overwrite MySink select first from MyTable")
    checkInsertTableResult(tableResult2, "default_catalog.default_database.MySink")
    // wait job finished
    tableResult2.getJobClient.get()
      .getJobExecutionResult(Thread.currentThread().getContextClassLoader)
      .get()
    assertFirstValues(sinkPath)
  }

  @Test
  def testExecuteSqlAndSqlUpdate(): Unit = {
    val sink1Path = registerCsvTableSink(tEnv, Array("first"), Array(STRING), "MySink1")
    val sink2Path = registerCsvTableSink(tEnv, Array("last"), Array(STRING), "MySink2")
    checkEmptyFile(sink1Path)
    checkEmptyFile(sink2Path)

    val tableResult = tEnv.executeSql("insert into MySink1 select first from MyTable")
    checkInsertTableResult(tableResult, "default_catalog.default_database.MySink1")
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
    streamTableEnv.asInstanceOf[TableEnvironmentInternal].registerTableSourceInternal(
      "MyTable", getPersonCsvTableSource)
    val sink1Path = registerCsvTableSink(streamTableEnv, Array("first"), Array(STRING), "MySink1")
    checkEmptyFile(sink1Path)
    StreamITCase.clear

    val table = streamTableEnv.sqlQuery("select last from MyTable where id > 0")
    val resultSet = streamTableEnv.toAppendStream(table, classOf[Row])
    resultSet.addSink(new StreamITCase.StringSink[Row])

    val tableResult = streamTableEnv.executeSql("insert into MySink1 select first from MyTable")
    checkInsertTableResult(tableResult, "default_catalog.default_database.MySink1")
    // wait job finished
    tableResult.getJobClient.get()
      .getJobExecutionResult(Thread.currentThread().getContextClassLoader)
      .get()
    assertFirstValues(sink1Path)

    // the DataStream program is not executed
    assertTrue(StreamITCase.testResults.isEmpty)

    deleteFile(sink1Path)

    streamEnv.execute("test2")
    assertEquals(getExpectedLastValues.sorted, StreamITCase.testResults.sorted)
    // the table program is not executed again
    assertFileNotExist(sink1Path)
  }

  @Test
  def testExecuteInsert(): Unit = {
    val sinkPath = registerCsvTableSink(tEnv, Array("first"), Array(STRING), "MySink")
    checkEmptyFile(sinkPath)
    val table = tEnv.sqlQuery("select first from MyTable")
    val tableResult = table.executeInsert("MySink")
    checkInsertTableResult(tableResult, "default_catalog.default_database.MySink")
    // wait job finished
    tableResult.getJobClient.get()
      .getJobExecutionResult(Thread.currentThread().getContextClassLoader)
      .get()
    assertFirstValues(sinkPath)
  }

  @Test
  def testExecuteInsertOverwrite(): Unit = {
    val resultFile = _tempFolder.newFile()
    val sinkPath = resultFile.getAbsolutePath
    val configuredSink = new TestingOverwritableTableSink(sinkPath)
      .configure(Array("first"), Array(STRING))
    tEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal("MySink", configuredSink)

    checkEmptyFile(sinkPath)
    val tableResult1 = tEnv.sqlQuery("select first from MyTable").executeInsert("MySink", true)
    checkInsertTableResult(tableResult1, "default_catalog.default_database.MySink")
    // wait job finished
    tableResult1.getJobClient.get()
      .getJobExecutionResult(Thread.currentThread().getContextClassLoader)
      .get()
    assertFirstValues(sinkPath)

    val tableResult2 = tEnv.sqlQuery("select first from MyTable").executeInsert("MySink", true)
    checkInsertTableResult(tableResult2, "default_catalog.default_database.MySink")
    // wait job finished
    tableResult2.getJobClient.get()
      .getJobExecutionResult(Thread.currentThread().getContextClassLoader)
      .get()
    assertFirstValues(sinkPath)
  }

  @Test
  def testStatementSet(): Unit = {
    val sink1Path = registerCsvTableSink(tEnv, Array("first"), Array(STRING), "MySink1")
    val sink2Path = registerCsvTableSink(tEnv, Array("last"), Array(STRING), "MySink2")

    val stmtSet = tEnv.createStatementSet()
    stmtSet.addInsert("MySink1", tEnv.sqlQuery("select first from MyTable"))
    stmtSet.addInsertSql("insert into MySink2 select last from MyTable")

    val actual = stmtSet.explain()
    val expected = readFromResource("testStatementSet0.out")
    assertEquals(replaceStageId(expected), replaceStageId(actual))

    val tableResult = stmtSet.execute()
    checkInsertTableResult(
      tableResult,
      "default_catalog.default_database.MySink1",
      "default_catalog.default_database.MySink2")
    // wait job finished
    tableResult.getJobClient.get()
      .getJobExecutionResult(Thread.currentThread().getContextClassLoader)
      .get()

    assertFirstValues(sink1Path)
    assertLastValues(sink2Path)
  }

  @Test
  def testStatementSetWithOverwrite(): Unit = {
    val sink1Path = _tempFolder.newFile().getAbsolutePath
    val configuredSink1 = new TestingOverwritableTableSink(sink1Path)
      .configure(Array("first"), Array(STRING))
    tEnv.asInstanceOf[TableEnvironmentInternal]
      .registerTableSinkInternal("MySink1", configuredSink1)
    checkEmptyFile(sink1Path)

    val sink2Path = _tempFolder.newFile().getAbsolutePath
    val configuredSink2 = new TestingOverwritableTableSink(sink2Path)
      .configure(Array("last"), Array(STRING))
    tEnv.asInstanceOf[TableEnvironmentInternal]
      .registerTableSinkInternal("MySink2", configuredSink2)
    checkEmptyFile(sink2Path)

    val stmtSet = tEnv.createStatementSet()
    stmtSet.addInsert("MySink1", tEnv.sqlQuery("select first from MyTable"), true)
      .addInsertSql("insert overwrite MySink2 select last from MyTable")

    val tableResult1 = stmtSet.execute()
    checkInsertTableResult(
      tableResult1,
      "default_catalog.default_database.MySink1",
      "default_catalog.default_database.MySink2")
    // wait job finished
    tableResult1.getJobClient.get()
      .getJobExecutionResult(Thread.currentThread().getContextClassLoader)
      .get()

    assertFirstValues(sink1Path)
    assertLastValues(sink2Path)

    // execute again using same StatementSet instance
    stmtSet.addInsert("MySink1", tEnv.sqlQuery("select first from MyTable"), true)
      .addInsertSql("insert overwrite MySink2 select last from MyTable")

    val tableResult2 = stmtSet.execute()
    checkInsertTableResult(
      tableResult2,
      "default_catalog.default_database.MySink1",
      "default_catalog.default_database.MySink2")
    // wait job finished
    tableResult2.getJobClient.get()
      .getJobExecutionResult(Thread.currentThread().getContextClassLoader)
      .get()

    assertFirstValues(sink1Path)
    assertLastValues(sink2Path)
  }

  @Test
  def testExecuteSelect(): Unit = {
    val query =
      """
        |select id, concat(concat(`first`, ' '), `last`) as `full name`
        |from MyTable where mod(id, 2) = 0
      """.stripMargin
    val tableResult = tEnv.executeSql(query)
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
  def testExecuteSelectWithUpdateChanges(): Unit = {
    // TODO Once FLINK-16998 is finished, all kinds of changes will be supported.
    thrown.expect(classOf[TableException])
    thrown.expectMessage(containsString(
      "AppendStreamTableSink requires that Table has only insert changes."))
    tEnv.executeSql("select count(*) from MyTable")
  }

  @Test
  def testExecuteSelectWithTimeAttribute(): Unit = {
    val data = Seq("Mary")
    val schema = new TableSchema(Array("name", "pt"), Array(Types.STRING, Types.SQL_TIMESTAMP()))
    val sourceType = Types.STRING
    val tableSource = new TestTableSourceWithTime(schema, sourceType, data, null, "pt")
    tEnv.asInstanceOf[TableEnvironmentInternal].registerTableSourceInternal("T", tableSource)

    val tableResult = tEnv.executeSql("select * from T")
    assertTrue(tableResult.getJobClient.isPresent)
    assertEquals(ResultKind.SUCCESS_WITH_CONTENT, tableResult.getResultKind)
    assertEquals(
      TableSchema.builder()
        .field("name", DataTypes.STRING())
        .field("pt", Types.SQL_TIMESTAMP())
        .build(),
      tableResult.getTableSchema)
    val it = tableResult.collect()
    assertTrue(it.hasNext)
    val row = it.next()
    assertEquals(2, row.getArity)
    assertEquals("Mary", row.getField(0))
    assertFalse(it.hasNext)
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
    tEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal(tableName, configuredSink)

    path
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

  private def checkInsertTableResult(tableResult: TableResult, fieldNames: String*): Unit = {
    assertTrue(tableResult.getJobClient.isPresent)
    assertEquals(ResultKind.SUCCESS_WITH_CONTENT, tableResult.getResultKind)
    assertEquals(
      util.Arrays.asList(fieldNames: _*),
      util.Arrays.asList(tableResult.getTableSchema.getFieldNames: _*))
    val it = tableResult.collect()
    assertTrue(it.hasNext)
    val affectedRowCounts = fieldNames.map(_ => JLong.valueOf(-1L))
    assertEquals(Row.of(affectedRowCounts: _*), it.next())
    assertFalse(it.hasNext)
  }
}

object TableEnvironmentITCase {
  @Parameterized.Parameters(name = "{0}")
  def parameters(): util.Collection[Array[_]] = {
    util.Arrays.asList(
      Array("TableEnvironment"),
      Array("StreamTableEnvironment")
    )
  }

  def getPersonCsvTableSource: CsvTableSource = {
    val header = "First#Id#Score#Last"
    val csvRecords = getPersonData.map {
      case (first, id, score, last) => s"$first#$id#$score#$last"
    }

    val tempFilePath = writeToTempFile(
      header + "$" + csvRecords.mkString("$"),
      "csv-test",
      "tmp")
    CsvTableSource.builder()
      .path(tempFilePath)
      .field("first", Types.STRING)
      .field("id", Types.INT)
      .field("score", Types.DOUBLE)
      .field("last", Types.STRING)
      .fieldDelimiter("#")
      .lineDelimiter("$")
      .ignoreFirstLine()
      .commentPrefix("%")
      .build()
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

  private def writeToTempFile(
      contents: String,
      filePrefix: String,
      fileSuffix: String,
      charset: String = "UTF-8"): String = {
    val tempFile = File.createTempFile(filePrefix, fileSuffix)
    tempFile.deleteOnExit()
    val tmpWriter = new OutputStreamWriter(new FileOutputStream(tempFile), charset)
    tmpWriter.write(contents)
    tmpWriter.close()
    tempFile.getAbsolutePath
  }
}
