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
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.configuration.Configuration
import org.apache.flink.sql.parser.impl.FlinkSqlParserImpl
import org.apache.flink.sql.parser.validate.FlinkSqlConformance
import org.apache.flink.streaming.api.datastream.{DataStream, DataStreamSink}
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.table.api.{ExecutionConfigOptions, TableConfig, TableException, TableSchema}
import org.apache.flink.table.calcite.CalciteConfig
import org.apache.flink.table.runtime.batch.sql.PartitionableSinkITCase._
import org.apache.flink.table.runtime.utils.BatchTestBase
import org.apache.flink.table.runtime.utils.BatchTestBase.row
import org.apache.flink.table.runtime.utils.TestData._
import org.apache.flink.table.sinks.{PartitionableTableSink, StreamTableSink, TableSink}
import org.apache.flink.table.types.logical.{BigIntType, IntType, VarCharType}
import org.apache.flink.types.Row

import org.apache.calcite.config.Lex
import org.apache.calcite.sql.parser.SqlParser
import org.junit.Assert._
import org.junit.{Before, Test}

import java.util.concurrent.LinkedBlockingQueue
import java.util.{LinkedList => JLinkedList, List => JList, Map => JMap}

import scala.collection.JavaConversions._
import scala.collection.Seq

/**
  * Test cases for [[org.apache.flink.table.sinks.PartitionableTableSink]].
  */
class PartitionableSinkITCase extends BatchTestBase {

  @Before
  override def before(): Unit = {
    super.before()
    env.setParallelism(3)
    tEnv.getConfig
      .getConfiguration
      .setInteger(ExecutionConfigOptions.SQL_RESOURCE_DEFAULT_PARALLELISM, 3)
    registerCollection("nonSortTable", testData, type3, "a, b, c", dataNullables)
    registerCollection("sortTable", testData1, type3, "a, b, c", dataNullables)
    PartitionableSinkITCase.init()
  }

  override def getTableConfig: TableConfig = {
    val parserConfig = SqlParser.configBuilder
      .setParserFactory(FlinkSqlParserImpl.FACTORY)
      .setConformance(FlinkSqlConformance.HIVE) // set up hive dialect
      .setLex(Lex.JAVA)
      .setIdentifierMaxLength(256).build
    val plannerConfig = CalciteConfig.createBuilder(CalciteConfig.DEFAULT)
      .replaceSqlParserConfig(parserConfig)
    val tableConfig = new TableConfig
    tableConfig.setPlannerConfig(plannerConfig.build())
    tableConfig
  }

  @Test
  def testInsertWithOutPartitionGrouping(): Unit = {
    registerTableSink(grouping = false)
    tEnv.sqlUpdate("insert into sinkTable select a, max(b), c"
      + " from nonSortTable group by a, c")
    tEnv.execute("testJob")
    val resultSet = List(RESULT1, RESULT2, RESULT3)
    assert(resultSet.exists(l => l.size() == 3))
    resultSet.filter(l => l.size() == 3).foreach{ list =>
      assert(list.forall(r => r.getField(0).toString == "1"))
    }
  }

  @Test
  def testInsertWithPartitionGrouping(): Unit = {
    registerTableSink(grouping = true)
    tEnv.sqlUpdate("insert into sinkTable select a, b, c from sortTable")
    tEnv.execute("testJob")
    val resultSet = List(RESULT1, RESULT2, RESULT3)
    resultSet.foreach(l => assertSortedByFirstNField(l, 1))
    assertEquals(resultSet.map(l => collectDistinctGroupCount(l, 2)).sum, 4)
  }

  @Test
  def testInsertWithStaticPartitions(): Unit = {
    val testSink = registerTableSink(grouping = true)
    tEnv.sqlUpdate("insert into sinkTable partition(a=1) select b, c from sortTable")
    tEnv.execute("testJob")
    // this sink should have been set up with static partitions
    assertEquals(testSink.getStaticPartitions.toMap, Map("a" -> "1"))
    val resultSet = List(RESULT1, RESULT2, RESULT3)
    val result = resultSet.filter(l => l.size() == 11)
    assert(result.size == 1)
    result.get(0).forall(r => r.getField(0).asInstanceOf[Int] == 1)
  }

  @Test
  def testInsertWithStaticAndDynamicPartitions(): Unit = {
    val testSink = registerTableSink(grouping = true, partitionColumns = Array("a", "b"))
    tEnv.sqlUpdate("insert into sinkTable partition(a=1) select b, c from sortTable")
    tEnv.execute("testJob")
    // this sink should have been set up with static partitions
    assertEquals(testSink.getStaticPartitions.toMap, Map("a" -> "1"))
    val resultSet = List(RESULT1, RESULT2, RESULT3)
    resultSet.foreach(l => assertSortedByFirstNField(l, 2))
    assertEquals(resultSet.map(l => collectDistinctGroupCount(l, 2)).sum, 4)
  }

  @Test(expected = classOf[TableException])
  def testDynamicPartitionInFrontOfStaticPartition(): Unit = {
    // Static partition column b should appear before dynamic partition a
    registerTableSink(grouping = true, partitionColumns = Array("a", "b"))
    tEnv.sqlUpdate("insert into sinkTable partition(b=1) select a, c from sortTable")
    tEnv.execute("testJob")
  }

  private def assertSortedByFirstNField(r: JLinkedList[Row], n: Int): Unit = {
    val firstFields = r.map { r =>
      val builder: StringBuilder = new StringBuilder
      0 until n foreach(i => builder.append(r.getField(i)))
      Integer.parseInt(builder.toString())
    }
    assertArrayEquals(firstFields.toArray, firstFields.sorted.toArray)
  }

  /**
    * Collect the group cnt with the specified number of key fields.
    * @param r the rows to group by
    * @param n the number of grouping fields
    * @return the group count of the rows
    */
  private def collectDistinctGroupCount(r: JLinkedList[Row], n: Int): Int = {
    val groupSet = scala.collection.mutable.SortedSet[Int]()
    r.foreach { r =>
      val builder: StringBuilder = new StringBuilder
      0 until n foreach(i => builder.append(r.getField(i)))
      groupSet += Integer.parseInt(builder.toString())
    }
    groupSet.size
  }

  private def registerTableSink(grouping: Boolean,
      partitionColumns: Array[String] = Array[String]("a")): TestSink = {
    val testSink = new TestSink(grouping, partitionColumns)
    tEnv.registerTableSink("sinkTable", testSink)
    testSink
  }

  private class TestSink(supportsGrouping: Boolean, partitionColumns: Array[String])
    extends StreamTableSink[Row]
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
      new TableSchema(Array("a", "b", "c"), type3.getFieldTypes)
    }

    override def getOutputType: RowTypeInfo = type3

    override def emitDataStream(dataStream: DataStream[Row]): Unit = {
      dataStream.addSink(new UnsafeMemorySinkFunction(type3))
        .setParallelism(dataStream.getParallelism)
    }

    override def consumeDataStream(dataStream: DataStream[Row]): DataStreamSink[_] = {
      dataStream.addSink(new UnsafeMemorySinkFunction(type3))
        .setParallelism(dataStream.getParallelism)
    }

    def getStaticPartitions: JMap[String, String] = {
      staticPartitions
    }
  }
}

object PartitionableSinkITCase {
  val RESULT1 = new JLinkedList[Row]()
  val RESULT2 = new JLinkedList[Row]()
  val RESULT3 = new JLinkedList[Row]()
  val RESULT_QUEUE: LinkedBlockingQueue[JLinkedList[Row]] =
    new LinkedBlockingQueue[JLinkedList[Row]]()

  def init(): Unit = {
    RESULT1.clear()
    RESULT2.clear()
    RESULT3.clear()
    RESULT_QUEUE.clear()
    RESULT_QUEUE.put(RESULT1)
    RESULT_QUEUE.put(RESULT2)
    RESULT_QUEUE.put(RESULT3)
  }

  /**
    * Sink function of unsafe memory.
    */
  class UnsafeMemorySinkFunction(outputType: TypeInformation[Row])
    extends RichSinkFunction[Row] {
    private var serializer: TypeSerializer[Row] = _
    private var resultSet: JLinkedList[Row] = _

    override def open(param: Configuration): Unit = {
      serializer = outputType.createSerializer(new ExecutionConfig)
      resultSet = RESULT_QUEUE.poll()
    }

    @throws[Exception]
    override def invoke(row: Row): Unit = {
      resultSet.add(serializer.copy(row))
    }
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
}
