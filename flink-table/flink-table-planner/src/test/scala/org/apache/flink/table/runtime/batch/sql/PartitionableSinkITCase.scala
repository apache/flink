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

import java.util.{LinkedList => JLinkedList, Map => JMap}

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.io.RichOutputFormat
import org.apache.flink.api.common.typeinfo.BasicTypeInfo.{INT_TYPE_INFO, LONG_TYPE_INFO, STRING_TYPE_INFO}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java
import org.apache.flink.api.java.DataSet
import org.apache.flink.api.java.operators.DataSink
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration
import org.apache.flink.table.api.internal.TableEnvironmentInternal
import org.apache.flink.table.api.bridge.scala.BatchTableEnvironment
import org.apache.flink.table.api.{DataTypes, TableSchema}
import org.apache.flink.table.factories.utils.TestCollectionTableFactory.TestCollectionInputFormat
import org.apache.flink.table.runtime.batch.sql.PartitionableSinkITCase._
import org.apache.flink.table.sinks.{BatchTableSink, PartitionableTableSink, TableSink}
import org.apache.flink.table.sources.BatchTableSource
import org.apache.flink.table.types.logical.{BigIntType, IntType, VarCharType}
import org.apache.flink.test.util.AbstractTestBase
import org.apache.flink.types.Row
import org.junit.Assert.assertEquals
import org.junit.rules.ExpectedException
import org.junit.{Before, Rule, Test}

import scala.collection.JavaConversions._
import scala.collection.Seq

class PartitionableSinkITCase extends AbstractTestBase {

  private val batchExec: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
  private var tEnv: BatchTableEnvironment = _
  private val type3 = new RowTypeInfo(INT_TYPE_INFO, LONG_TYPE_INFO, STRING_TYPE_INFO)
  private val type4 = new RowTypeInfo(INT_TYPE_INFO, INT_TYPE_INFO, INT_TYPE_INFO)

  private val _expectedException = ExpectedException.none

  @Rule
  def expectedEx: ExpectedException = _expectedException

  @Before
  def before(): Unit = {
    batchExec.setParallelism(1)
    tEnv = BatchTableEnvironment.create(batchExec)
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
    tEnv.asInstanceOf[TableEnvironmentInternal].registerTableSourceInternal(
      name, new CollectionTableSource(data, 100, tableSchema))
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
      RESULT.toList)
  }

  @Test
  def testStaticPartitionNotInPartitionFields(): Unit = {
    expectedEx.expect(classOf[RuntimeException])
    registerTableSink(tableName = "sinkTable2", rowType = type4,
      partitionColumns = Array("a", "b"))
    tEnv.sqlUpdate("insert into sinkTable2 partition(c=1) select a, b from sinkTable2")
    tEnv.execute("testJob")
  }

  @Test
  def testInsertStaticPartitionOnNonPartitionedSink(): Unit = {
    expectedEx.expect(classOf[RuntimeException])
    registerTableSink(tableName = "sinkTable2", rowType = type4, partitionColumns = Array())
    tEnv.sqlUpdate("insert into sinkTable2 partition(c=1) select a, b from sinkTable2")
    tEnv.execute("testJob")
  }

  private def registerTableSink(
      tableName: String = "sinkTable",
      rowType: RowTypeInfo = type3,
      partitionColumns: Array[String] = Array[String]("a")): TestSink = {
    val testSink = new TestSink(rowType, partitionColumns)
    tEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal(tableName, testSink)
    testSink
  }

  private class TestSink(rowType: RowTypeInfo, partitionColumns: Array[String])
    extends BatchTableSink[Row]
      with PartitionableTableSink {
    private var staticPartitions: JMap[String, String] = _

    override def setStaticPartition(partitions: JMap[String, String]): Unit = {
      partitions.foreach { case (part, v) =>
        if (!partitionColumns.contains(part)) {
          throw new RuntimeException
        }
      }
      this.staticPartitions = partitions
    }

    override def configure(fieldNames: Array[String],
      fieldTypes: Array[TypeInformation[_]]): TableSink[Row] = this

    override def configurePartitionGrouping(s: Boolean): Boolean = {
      false
    }

    override def getTableSchema: TableSchema = {
      new TableSchema(Array("a", "b", "c"), rowType.getFieldTypes)
    }

    override def getOutputType: RowTypeInfo = rowType

    def getStaticPartitions: JMap[String, String] = {
      staticPartitions
    }

    override def consumeDataSet(dataSet: DataSet[Row]): DataSink[_] = {
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
  val RESULT = new JLinkedList[String]()

  def init(): Unit = {
    RESULT.clear()
  }

  /** OutputFormat that writes data to a collection. **/
  class CollectionOutputFormat extends RichOutputFormat[String] {
    override def configure(parameters: Configuration): Unit = {}

    override def open(taskNumber: Int, numTasks: Int): Unit = {}

    override def writeRecord(record: String): Unit = {
      RESULT.add(record)
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
