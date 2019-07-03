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
import org.apache.flink.streaming.api.datastream.{DataStream, DataStreamSink}
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.table.api.{TableConfigOptions, TableSchema}
import org.apache.flink.table.runtime.batch.sql.PartitionableSinkITCase._
import org.apache.flink.table.runtime.utils.BatchTestBase
import org.apache.flink.table.runtime.utils.BatchTestBase.row
import org.apache.flink.table.runtime.utils.TestData._
import org.apache.flink.table.sinks.{PartitionableTableSink, StreamTableSink, TableSink}
import org.apache.flink.table.types.logical.{BigIntType, IntType, VarCharType}
import org.apache.flink.types.Row

import org.junit.{Before, Test}

import java.util.concurrent.LinkedBlockingQueue
import java.util.{HashMap => JHashMap, LinkedList => JLinkedList, List => JList, Map => JMap}

import scala.collection.JavaConversions._
import scala.collection.Seq

import org.junit.Assert._

/**
  * Test cases for [[org.apache.flink.table.sinks.PartitionableTableSink]].
  */
class PartitionableSinkITCase extends BatchTestBase {

  @Before
  def before(): Unit = {
    env.setParallelism(3)
    tEnv.getConfig.getConf.setInteger(TableConfigOptions.SQL_RESOURCE_DEFAULT_PARALLELISM, 3)
    registerCollection("nonSortTable", data, type3, "a, b, c", dataNullables)
    registerCollection("sortTable", data1, type3, "a, b, c", dataNullables)
    PartitionableSinkITCase.init()
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
    resultSet.foreach(l => assertSortedByFirstField(l))
  }

  private def assertSortedByFirstField(r: JLinkedList[Row]): Unit = {
    val firstFields = r.map(r => r.getField(0).asInstanceOf[Int])
    assertArrayEquals(firstFields.toArray, firstFields.sorted.toArray)
  }

  private def registerTableSink(grouping: Boolean): Unit = {
    tEnv.registerTableSink("sinkTable", new TestSink(grouping))
  }

  private class TestSink(supportsGrouping: Boolean)
    extends StreamTableSink[Row]
    with PartitionableTableSink {

    override def getPartitionFieldNames: JList[String] = List("a")

    override def setStaticPartition(partitions: JMap[String, String]): Unit =
      new JHashMap[String, String]()

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

  val data = Seq(
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

  val data1 = Seq(
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
