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

package org.apache.flink.table.factories

import java.util

import org.apache.flink.api.common.typeinfo.BasicTypeInfo.{INT_TYPE_INFO, LONG_TYPE_INFO, STRING_TYPE_INFO}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.types.DataTypes
import org.apache.flink.table.api.{RichTableSchema, TableEnvironment}
import org.apache.flink.table.dataformat.BinaryString.fromString
import org.apache.flink.table.descriptors.{ConnectorDescriptorValidator, FormatDescriptorValidator}
import org.apache.flink.table.factories.csv.CsvTableFactory
import org.apache.flink.table.factories.orc.OrcTableFactory
import org.apache.flink.table.factories.parquet.ParquetTableFactory
import org.apache.flink.table.runtime.batch.sql.BatchTestBase.binaryRow
import org.apache.flink.table.typeutils.BaseRowTypeInfo
import org.apache.flink.table.util.TableProperties
import org.apache.flink.test.util.TestBaseUtils
import org.junit.rules.TemporaryFolder
import org.junit.{After, Before, Rule, Test}

import scala.collection.JavaConversions._
import scala.collection.Seq

class FileSystemTableFactoryITCase {

  val dataType = new BaseRowTypeInfo(
    INT_TYPE_INFO, LONG_TYPE_INFO, STRING_TYPE_INFO)

  val data = Seq(
    binaryRow(dataType, 1, 1L, fromString("Hi")),
    binaryRow(dataType, 2, 2L, fromString("Hello")),
    binaryRow(dataType, 3, 2L, fromString("Hello world")),
    binaryRow(dataType, 4, 3L, fromString("Hello world, how are you?"))
  )

  val data1 = List(
    (1, 1L, "Hi"),
    (2, 2L, "Hello"),
    (3, 2L, "Hello world"),
    (4, 3L, "Hello world, how are you?")
  )

  private[this] val csvProps = Map(
    ConnectorDescriptorValidator.CONNECTOR_TYPE -> "CSV",
    "updatemode" -> "append", // key should be lowercase
    "parallelism" -> "1"
  )

  var expectedResult: String = _

  val _tempFolder = new TemporaryFolder()
  private var resultPath: String = null

  @Rule
  def tempFolder = _tempFolder

  private[this] def lookUpProps(updateMode: String): java.util.Map[String, String] = {
    val ret = new util.HashMap[String, String]()
    ret.putAll(csvProps)
    ret.put("updatemode", updateMode)
    ret.put("path", getPath())
    ret
  }

  private[this] def getCsvProps(
    name: String,
    updateMode: String = "append"): java.util.Map[String, String] = {
    val tableProperties = new TableProperties
    val richSchema = new RichTableSchema(
      "a,b,c".split(","),
      Seq(DataTypes.INT, DataTypes.LONG, DataTypes.STRING).toArray)
    tableProperties.putTableNameIntoProperties(name)
    tableProperties.putSchemaIntoProperties(richSchema)
    tableProperties.putProperties(lookUpProps(updateMode))
    tableProperties.toKeyLowerCase.toMap
  }
  val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
  val streamTableEnv = TableEnvironment.getTableEnvironment(streamEnv)
  val batchEnv = TableEnvironment.getBatchTableEnvironment(streamEnv)

  private[this] def getPath(): String = {
    resultPath
  }

  @Before
  def before(): Unit = {
    resultPath = tempFolder.newFile().toURI.toString
    batchEnv.registerCollection("sTable", data, dataType, 'a, 'b, 'c)
    val table = streamEnv.fromCollection(data1).toTable(streamTableEnv, 'a, 'b, 'c)
    streamTableEnv.registerTable("sTable", table)
  }

  @After
  def after(): Unit = {
    if (expectedResult != null) {
      TestBaseUtils.compareResultsByLinesInMemory(expectedResult, resultPath)
    }
  }

  @Test
  def testStreamAppendCsvSink(): Unit = {
    val tableSink = TableFactoryService
      .find(classOf[CsvTableFactory], lookUpProps("append"))
        .createStreamTableSink(getCsvProps("csvTableSink"))
    streamTableEnv.sqlQuery("select a, b, c from sTable").writeToSink(tableSink)
    streamEnv.execute()
    expectedResult = "1,1,Hi\n" +
      "2,2,Hello\n" +
      "3,2,Hello world\n" +
      "4,3,Hello world, how are you?"
  }

  @Test
  def testStreamUpsertCsvSink(): Unit = {
    val tableSink = TableFactoryService
      .find(classOf[CsvTableFactory], lookUpProps("upsert"))
      .createStreamTableSink(getCsvProps("csvTableSink", updateMode = "upsert"))
    streamTableEnv.sqlQuery("select a, b, c from sTable").writeToSink(tableSink)
    streamEnv.execute()
    expectedResult = "Add,1,1,Hi\n" +
      "Add,2,2,Hello\n" +
      "Add,3,2,Hello world\n" +
      "Add,4,3,Hello world, how are you?"
  }

  @Test
  def testStreamRetractCsvSink(): Unit = {
    val tableSink = TableFactoryService
      .find(classOf[CsvTableFactory], lookUpProps("retract"))
      .createStreamTableSink(getCsvProps("csvTableSink", updateMode = "retract"))
    streamTableEnv.sqlQuery("select a, b, c from sTable").writeToSink(tableSink)
    streamEnv.execute()
    expectedResult = "True,1,1,Hi\n" +
      "True,2,2,Hello\n" +
      "True,3,2,Hello world\n" +
      "True,4,3,Hello world, how are you?"
  }

  @Test
  def testBatchWithUpsertCsvSink(): Unit = {
    val tableSink = TableFactoryService
      .find(classOf[CsvTableFactory], lookUpProps("upsert"))
      .createBatchCompatibleTableSink(getCsvProps("csvTableSink", updateMode = "upsert"))
    batchEnv.sqlQuery("select a, b, c from sTable").writeToSink(tableSink)
    batchEnv.execute()
    expectedResult = "Add,1,1,Hi\n" +
      "Add,2,2,Hello\n" +
      "Add,3,2,Hello world\n" +
      "Add,4,3,Hello world, how are you?"
  }

  @Test
  def testBatchWithRetractCsvSink(): Unit = {
    val tableSink = TableFactoryService
      .find(classOf[CsvTableFactory], lookUpProps("retract"))
      .createBatchCompatibleTableSink(getCsvProps("csvTableSink", updateMode = "retract"))
    batchEnv.sqlQuery("select a, b, c from sTable").writeToSink(tableSink)
    batchEnv.execute()
    expectedResult = "True,1,1,Hi\n" +
      "True,2,2,Hello\n" +
      "True,3,2,Hello world\n" +
      "True,4,3,Hello world, how are you?"
  }

  @Test
  def testFindParquetSinkTableFactory(): Unit = {
    val props = Map(
      ConnectorDescriptorValidator.CONNECTOR_TYPE -> "filesystem",
      FormatDescriptorValidator.FORMAT_TYPE -> "PARQUET"
    )
    val tableSink = TableFactoryService.find(classOf[ParquetTableFactory], props)
  }

  @Test
  def testFindORCSinkTableFactory(): Unit = {
    val props = Map(
      ConnectorDescriptorValidator.CONNECTOR_TYPE -> "filesystem",
      FormatDescriptorValidator.FORMAT_TYPE -> "ORC"
    )
    val tableSink = TableFactoryService.find(classOf[OrcTableFactory], props)
  }
}
