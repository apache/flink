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

package org.apache.flink.table.runtime.batch.sql

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.io.RichOutputFormat
import org.apache.flink.api.common.typeinfo.BasicTypeInfo.{INT_TYPE_INFO, LONG_TYPE_INFO, STRING_TYPE_INFO}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.DataSet
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration
import org.apache.flink.table.api.scala.BatchTableEnvironment
import org.apache.flink.table.api.{DataTypes, SqlDialect, TableSchema, ValidationException}
import org.apache.flink.table.factories.utils.TestCollectionTableFactory.TestCollectionInputFormat
import org.apache.flink.table.runtime.batch.sql.PartitionableSinkITCase.{RESULT1, RESULT2, RESULT3, _}
import org.apache.flink.table.sinks.{BatchTableSink, PartitionableTableSink, TableSink}
import org.apache.flink.table.sources.BatchTableSource
import org.apache.flink.table.types.logical.{BigIntType, IntType, VarCharType}
import org.apache.flink.types.Row

import org.junit.Assert.assertEquals
import org.junit.rules.ExpectedException
import org.junit.{Before, Rule, Test}

import java.util.{ArrayList => JArrayList, LinkedList => JLinkedList, List => JList, Map => JMap}
import org.apache.flink.api.java

import scala.collection.JavaConversions._
import scala.collection.Seq

class PartitionableSinkITCase {
  private val batchExec: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
  private var tEnv: BatchTableEnvironment = _
  private val type3 = new RowTypeInfo(INT_TYPE_INFO, LONG_TYPE_INFO, STRING_TYPE_INFO)
  private val type4 = new RowTypeInfo(INT_TYPE_INFO, INT_TYPE_INFO, INT_TYPE_INFO)

  private val _expectedException = ExpectedException.none

  @Rule
  def expectedEx: ExpectedException = _expectedException

  @Before
  def before(): Unit = {
    batchExec.setParallelism(3)
    tEnv = BatchTableEnvironment.create(batchExec)
    tEnv.getConfig.setSqlDialect(SqlDialect.HIVE)
    registerTableSource("nonSortTable", testData.toList)
    registerTableSource("sortTable", testData1.toList)
    PartitionableSinkITCase.init()
  }

  def registerTableSource(name: String, data: List[Row]): Unit = {
    val tableSchema = TableSchema.builder()
      .field("a", DataTypes.INT())
      .field("b", DataTypes.BIGINT())
      .field("c", DataTypes.STRING())
      .build()
    tEnv.registerTableSource(name, new CollectionTableSource(data, 100, tableSchema))
  }

  @Test
  def testInsertWithOutPartitionGrouping(): Unit = {
    registerTableSink(grouping = false)
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
    val testSink = registerTableSink()
    tEnv.sqlUpdate("insert into sinkTable partition(a=1) select b, c from sortTable")
    tEnv.execute("testJob")
    // this sink should have been set up with static partitions
    assertEquals(testSink.getStaticPartitions.toMap, Map("a" -> "1"))
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
    val testSink = registerTableSink(partitionColumns = Array("a", "b"))
    tEnv.sqlUpdate("insert into sinkTable partition(a=1) select b, c from sortTable")
    tEnv.execute("testJob")
    // this sink should have been set up with static partitions
    assertEquals(testSink.getStaticPartitions.toMap, Map("a" -> "1"))
    assertEquals(List("1,3,I'm fine, thank",
      "1,3,I'm fine, thank you",
      "1,3,I'm fine, thank you, and you?"),
      RESULT1.toList)
    assertEquals(List("1,2,Hi",
      "1,2,Hello"),
      RESULT2.toList)
    assertEquals(List("1,1,Hello world",
      "1,1,Hello world, how are you?",
      "1,4,你好，陌生人",
      "1,4,你好，陌生人，我是",
      "1,4,你好，陌生人，我是中国人",
      "1,4,你好，陌生人，我是中国人，你来自哪里？"),
      RESULT3.toList)
  }

  @Test
  def testDynamicPartitionInFrontOfStaticPartition(): Unit = {
    expectedEx.expect(classOf[ValidationException])
    expectedEx.expectMessage("Static partition column b "
      + "should appear before dynamic partition a")
    registerTableSink(partitionColumns = Array("a", "b"))
    tEnv.sqlUpdate("insert into sinkTable partition(b=1) select a, c from sortTable")
    tEnv.execute("testJob")
  }

  @Test
  def testStaticPartitionNotInPartitionFields(): Unit = {
    expectedEx.expect(classOf[ValidationException])
    expectedEx.expectMessage("Static partition column c " +
      "should be in the partition fields list [a, b].")
    registerTableSink(tableName = "sinkTable2", rowType = type4,
      partitionColumns = Array("a", "b"))
    tEnv.sqlUpdate("insert into sinkTable2 partition(c=1) select a, b from sinkTable2")
    tEnv.execute("testJob")
  }

  @Test
  def testInsertStaticPartitionOnNonPartitionedSink(): Unit = {
    expectedEx.expect(classOf[ValidationException])
    expectedEx.expectMessage(
      "Can't insert static partitions into a non-partitioned table sink.")
    registerTableSink(tableName = "sinkTable2", rowType = type4, partitionColumns = Array())
    tEnv.sqlUpdate("insert into sinkTable2 partition(c=1) select a, b from sinkTable2")
    tEnv.execute("testJob")
  }

  private def registerTableSink(
      tableName: String = "sinkTable",
      rowType: RowTypeInfo = type3,
      grouping: Boolean = true,
      partitionColumns: Array[String] = Array[String]("a")): TestSink = {
    val testSink = new TestSink(rowType, grouping, partitionColumns)
    tEnv.registerTableSink(tableName, testSink)
    testSink
  }

  private class TestSink(rowType: RowTypeInfo,
      supportsGrouping: Boolean,
      partitionColumns: Array[String])
    extends BatchTableSink[Row]
      with PartitionableTableSink {
    private var staticPartitions: JMap[String, String] = _

    override def getPartitionFieldNames: JList[String] = partitionColumns.toList

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

    def getStaticPartitions: JMap[String, String] = {
      staticPartitions
    }

    override def emitDataSet(dataSet: DataSet[Row]): Unit = {
      dataSet.map(new MapFunction[Row, String] {
        override def map(value: Row): String = value.toString
      }).output(new CollectionOutputFormat)
        .setParallelism(dataSet.getExecutionEnvironment.getParallelism)
    }
  }

  /**
    * Table source of collection.
    */
  class CollectionTableSource(
    val data: List[Row],
    val emitIntervalMs: Long,
    val schema: TableSchema)
    extends BatchTableSource[Row] {

    private val rowType: TypeInformation[Row] = schema.toRowType

    override def getReturnType: TypeInformation[Row] = rowType

    override def getTableSchema: TableSchema = {
      schema
    }

    override def getDataSet(execEnv: java.ExecutionEnvironment): DataSet[Row] = {
      execEnv.createInput(new TestCollectionInputFormat[Row](emitIntervalMs,
        data, rowType.createSerializer(new ExecutionConfig)), rowType)
    }
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

  /** OutputFormat that writes data to a collection. **/
  class CollectionOutputFormat extends RichOutputFormat[String] {
    private var resultSet: JLinkedList[String] = _

    override def configure(parameters: Configuration): Unit = {}

    override def open(taskNumber: Int, numTasks: Int): Unit = {
      resultSet = RESULT_QUEUE.get(taskNumber)
    }

    override def writeRecord(record: String): Unit = {
      resultSet.add(record)
    }

    override def close(): Unit = {}
  }

  val fieldNames = Array("a", "b", "c")
  val dataType = Array(new IntType(), new BigIntType(), new VarCharType(VarCharType.MAX_LENGTH))
  val dataNullables = Array(false, false, false)

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

  def row(args: Any*):Row = {
    val row = new Row(args.length)
    0 until args.length foreach {
      i => row.setField(i, args(i))
    }
    row
  }
}
