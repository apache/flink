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

package org.apache.flink.table.planner.runtime.batch.sql

import org.apache.flink.api.common.typeinfo.BasicTypeInfo.INT_TYPE_INFO
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.datastream.{DataStream, DataStreamSink}
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.table.api.config.ExecutionConfigOptions
import org.apache.flink.table.api.{SqlDialect, TableEnvironment, TableException, TableSchema, ValidationException}
import org.apache.flink.table.catalog.{CatalogTableImpl, ObjectPath}
import org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_TYPE
import org.apache.flink.table.descriptors.DescriptorProperties
import org.apache.flink.table.descriptors.Schema.SCHEMA
import org.apache.flink.table.factories.{TableSinkFactory, TableSourceFactory}
import org.apache.flink.table.planner.runtime.batch.sql.PartitionableSinkITCase._
import org.apache.flink.table.planner.runtime.utils.BatchTestBase
import org.apache.flink.table.planner.runtime.utils.BatchTestBase.row
import org.apache.flink.table.planner.runtime.utils.TestData._
import org.apache.flink.table.sinks.{PartitionableTableSink, StreamTableSink, TableSink}
import org.apache.flink.table.sources.{StreamTableSource, TableSource}
import org.apache.flink.table.types.logical.{BigIntType, IntType, VarCharType}
import org.apache.flink.types.Row

import org.junit.Assert._
import org.junit.rules.ExpectedException
import org.junit.{Before, Rule, Test}

import java.util
import java.util.{function, ArrayList => JArrayList, LinkedList => JLinkedList, List => JList, Map => JMap}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.Seq

/**
  * Test cases for [[org.apache.flink.table.sinks.PartitionableTableSink]].
  */
class PartitionableSinkITCase extends BatchTestBase {

  private val _expectedException = ExpectedException.none
  private val type4 = new RowTypeInfo(INT_TYPE_INFO, INT_TYPE_INFO, INT_TYPE_INFO)

  @Rule
  def expectedEx: ExpectedException = _expectedException

  @Before
  override def before(): Unit = {
    super.before()
    env.setParallelism(3)
    tEnv.getConfig
      .getConfiguration
      .setInteger(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 3)
    registerCollection("nonSortTable", testData, type3, "a, b, c", dataNullables)
    registerCollection("sortTable", testData1, type3, "a, b, c", dataNullables)
    PartitionableSinkITCase.init()
  }

  @Test
  def testInsertWithOutPartitionGrouping(): Unit = {
    registerTableSink()
    tEnv.sqlUpdate("insert into sinkTable select a, max(b), c"
      + " from nonSortTable group by a, c")
    tEnv.execute("testJob")
    assertEquals(List("1,5,Hi",
      "1,5,Hi01",
      "1,5,Hi02"),
      RESULT1.sorted)
    assert(RESULT2.isEmpty)
    assertEquals(List("2,1,Hello world01",
      "2,1,Hello world02",
      "2,1,Hello world03",
      "2,1,Hello world04",
      "2,2,Hello world, how are you?",
      "3,1,Hello world",
      "3,2,Hello",
      "3,2,Hello01",
      "3,2,Hello02",
      "3,2,Hello03",
      "3,2,Hello04"),
      RESULT3.sorted)
  }

  @Test
  def testInsertWithPartitionGrouping(): Unit = {
    registerTableSink()
    tEnv.sqlUpdate("insert into sinkTable select a, b, c from sortTable")
    tEnv.execute("testJob")
    assertEquals(List("1,1,Hello world",
      "1,1,Hello world, how are you?"),
      RESULT1.toList)
    assertEquals(List("4,4,你好，陌生人",
      "4,4,你好，陌生人，我是",
      "4,4,你好，陌生人，我是中国人",
      "4,4,你好，陌生人，我是中国人，你来自哪里？"),
      RESULT2.toList)
    assertEquals(List("2,2,Hi",
      "2,2,Hello",
      "3,3,I'm fine, thank",
      "3,3,I'm fine, thank you",
      "3,3,I'm fine, thank you, and you?"),
      RESULT3.toList)
  }

  @Test
  def testInsertWithStaticPartitions(): Unit = {
    registerTableSink()
    tEnv.sqlUpdate("insert into sinkTable partition(a=1) select b, c from sortTable")
    tEnv.execute("testJob")
    assertEquals(List("1,2,Hi",
      "1,1,Hello world",
      "1,2,Hello",
      "1,1,Hello world, how are you?",
      "1,3,I'm fine, thank",
      "1,3,I'm fine, thank you",
      "1,3,I'm fine, thank you, and you?",
      "1,4,你好，陌生人",
      "1,4,你好，陌生人，我是",
      "1,4,你好，陌生人，我是中国人",
      "1,4,你好，陌生人，我是中国人，你来自哪里？"),
      RESULT1.toList)
    assert(RESULT2.isEmpty)
    assert(RESULT3.isEmpty)
  }

  @Test
  def testInsertWithStaticAndDynamicPartitions(): Unit = {
    registerTableSink(partitionColumns = Array("a", "b"))
    tEnv.sqlUpdate("insert into sinkTable partition(a=1) select b, c from sortTable")
    tEnv.execute("testJob")
    assertEquals(List("1,1,Hello world", "1,1,Hello world, how are you?"), RESULT1.toList)
    assertEquals(List(
      "1,4,你好，陌生人",
      "1,4,你好，陌生人，我是",
      "1,4,你好，陌生人，我是中国人",
      "1,4,你好，陌生人，我是中国人，你来自哪里？"),
      RESULT2.toList)
    assertEquals(List(
      "1,2,Hi",
      "1,2,Hello",
      "1,3,I'm fine, thank",
      "1,3,I'm fine, thank you",
      "1,3,I'm fine, thank you, and you?"),
      RESULT3.toList)
  }

  @Test
  def testStaticPartitionNotInPartitionFields(): Unit = {
    expectedEx.expect(classOf[ValidationException])
    registerTableSink(tableName = "sinkTable2", rowType = type4,
      partitionColumns = Array("a", "b"))
    tEnv.sqlUpdate("insert into sinkTable2 partition(c=1) select a, b from sinkTable2")
    tEnv.execute("testJob")
  }

  @Test
  def testInsertStaticPartitionOnNonPartitionedSink(): Unit = {
    expectedEx.expect(classOf[TableException])
    registerTableSink(tableName = "sinkTable2", rowType = type4, partitionColumns = Array())
    tEnv.sqlUpdate("insert into sinkTable2 partition(c=1) select a, b from sinkTable2")
    tEnv.execute("testJob")
  }

  private def registerTableSink(
      tableName: String = "sinkTable",
      rowType: RowTypeInfo = type3,
      grouping: Boolean = true,
      partitionColumns: Array[String] = Array[String]("a")): Unit = {
    PartitionableSinkITCase.registerTableSink(tEnv, tableName, rowType, grouping, partitionColumns)
  }
}

object PartitionableSinkITCase {
  val RESULT1 = new JLinkedList[String]()
  val RESULT2 = new JLinkedList[String]()
  val RESULT3 = new JLinkedList[String]()
  val RESULT_QUEUE: JList[JLinkedList[String]] = new JArrayList[JLinkedList[String]]()

  def init(): Unit = {
    RESULT1.clear()
    RESULT2.clear()
    RESULT3.clear()
    RESULT_QUEUE.clear()
    RESULT_QUEUE.add(RESULT1)
    RESULT_QUEUE.add(RESULT2)
    RESULT_QUEUE.add(RESULT3)
  }

  /**
    * Sink function of unsafe memory.
    */
  class UnsafeMemorySinkFunction(outputType: TypeInformation[Row])
    extends RichSinkFunction[Row] {
    private var resultSet: JLinkedList[String] = _

    override def open(param: Configuration): Unit = {
      val taskId = getRuntimeContext.getIndexOfThisSubtask
      resultSet = RESULT_QUEUE.get(taskId)
    }

    @throws[Exception]
    override def invoke(row: Row): Unit = {
      resultSet.add(row.toString)
    }
  }

  val fieldNames = Array("a", "b", "c")
  val dataType = Array(new IntType(), new BigIntType(), new VarCharType(VarCharType.MAX_LENGTH))
  val dataNullables = Array(true, true, true)

  val testData = Seq(
    row(3, 2L, "Hello03"),
    row(1, 5L, "Hi"),
    row(1, 5L, "Hi01"),
    row(1, 5L, "Hi02"),
    row(3, 2L, "Hello"),
    row(3, 2L, "Hello01"),
    row(2, 1L, "Hello world03"),
    row(3, 2L, "Hello02"),
    row(3, 2L, "Hello04"),
    row(3, 1L, "Hello world"),
    row(2, 1L, "Hello world01"),
    row(2, 1L, "Hello world02"),
    row(2, 1L, "Hello world04"),
    row(2, 2L, "Hello world, how are you?")
  )

  val testData1 = Seq(
    row(2, 2L, "Hi"),
    row(1, 1L, "Hello world"),
    row(2, 2L, "Hello"),
    row(1, 1L, "Hello world, how are you?"),
    row(3, 3L, "I'm fine, thank"),
    row(3, 3L, "I'm fine, thank you"),
    row(3, 3L, "I'm fine, thank you, and you?"),
    row(4, 4L, "你好，陌生人"),
    row(4, 4L, "你好，陌生人，我是"),
    row(4, 4L, "你好，陌生人，我是中国人"),
    row(4, 4L, "你好，陌生人，我是中国人，你来自哪里？")
  )

  def registerTableSink(
      tEnv: TableEnvironment,
      tableName: String,
      rowType: RowTypeInfo,
      grouping: Boolean,
      partitionColumns: Array[String]): Unit = {
    val properties = new DescriptorProperties()
    properties.putString("supports-grouping", grouping.toString)
    properties.putString(CONNECTOR_TYPE, "TestPartitionableSink")
    partitionColumns.zipWithIndex.foreach { case (part, i) =>
      properties.putString("partition-column." + i, part)
    }

    val table = new CatalogTableImpl(
      new TableSchema(Array("a", "b", "c"), rowType.getFieldTypes),
      util.Arrays.asList[String](partitionColumns: _*),
      properties.asMap(),
      ""
    )
    tEnv.getCatalog(tEnv.getCurrentCatalog).get()
        .createTable(new ObjectPath(tEnv.getCurrentDatabase, tableName), table, false)
  }
}

private class TestSink(
    rowType: RowTypeInfo,
    supportsGrouping: Boolean,
    partitionColumns: Array[String])
    extends StreamTableSink[Row]
        with PartitionableTableSink {
  private var staticPartitions: JMap[String, String] = _

  override def setStaticPartition(partitions: JMap[String, String]): Unit =
    this.staticPartitions = partitions

  override def configure(fieldNames: Array[String],
      fieldTypes: Array[TypeInformation[_]]): TableSink[Row] = this

  override def configurePartitionGrouping(s: Boolean): Boolean = {
    supportsGrouping
  }

  override def getTableSchema: TableSchema = {
    new TableSchema(Array("a", "b", "c"), rowType.getFieldTypes)
  }

  override def getOutputType: RowTypeInfo = rowType

  override def consumeDataStream(dataStream: DataStream[Row]): DataStreamSink[_] = {
    dataStream.addSink(new UnsafeMemorySinkFunction(rowType))
        .setParallelism(dataStream.getParallelism)
  }

  def getStaticPartitions: JMap[String, String] = {
    staticPartitions
  }
}

class TestPartitionableSinkFactory extends TableSinkFactory[Row] with TableSourceFactory[Row] {

  override def requiredContext(): util.Map[String, String] = {
    val context = new util.HashMap[String, String]()
    context.put(CONNECTOR_TYPE, "TestPartitionableSink")
    context
  }

  override def supportedProperties(): util.List[String] = {
    val supported = new util.ArrayList[String]()
    supported.add("*")
    supported
  }

  override def createTableSink(properties: util.Map[String, String]): TableSink[Row] = {
    val dp = new DescriptorProperties()
    dp.putProperties(properties)

    val schema = dp.getTableSchema(SCHEMA)
    val supportsGrouping = dp.getBoolean("supports-grouping")
    val partitionColumns = dp.getArray("partition-column", new function.Function[String, String] {
      override def apply(t: String): String = dp.getString(t)
    })
    new TestSink(
      schema.toRowType.asInstanceOf[RowTypeInfo],
      supportsGrouping,
      partitionColumns.asScala.toArray[String])
  }

  /**
    * Remove it after FLINK-14387.
    */
  override def createTableSource(properties: JMap[String, String]): TableSource[Row] = {
    val dp = new DescriptorProperties()
    dp.putProperties(properties)

    new StreamTableSource[Row] {
      override def getTableSchema: TableSchema = {
        dp.getTableSchema(SCHEMA)
      }

      override def getDataStream(execEnv: StreamExecutionEnvironment): DataStream[Row] = {
        throw new RuntimeException
      }
    }
  }
}

