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

package org.apache.flink.table.plan.nodes.resource

import org.apache.flink.api.common.io.OutputFormat
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.datastream.{DataStream, DataStreamSink}
import org.apache.flink.streaming.api.environment
import org.apache.flink.streaming.api.operators.StreamOperatorFactory
import org.apache.flink.streaming.api.transformations.{SinkTransformation, SourceTransformation}
import org.apache.flink.table.api.{ExecutionConfigOptions, TableConfig, TableSchema, Types}
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.plan.stats.{FlinkStatistic, TableStats}
import org.apache.flink.table.sinks.{AppendStreamTableSink, StreamTableSink, TableSink}
import org.apache.flink.table.sources.StreamTableSource
import org.apache.flink.table.types.TypeInfoLogicalTypeConverter
import org.apache.flink.table.typeutils.BaseRowTypeInfo
import org.apache.flink.table.util.{TableTestBase, TableTestUtil, TestingTableEnvironment}

import org.junit.Assert.assertEquals
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.{Before, Test}
import org.mockito.Mockito.{mock, when}

import java.util

@RunWith(classOf[Parameterized])
class ExecNodeResourceTest(isBatch: Boolean) extends TableTestBase {

  private var testUtil: TableTestUtil = _

  @Before
  def before(): Unit = {
    testUtil = if(isBatch) batchTestUtil() else streamTestUtil()
    val table3Stats = new TableStats(5000000)
    val table3Source = new MockTableSource(isBatch,
      new TableSchema(Array("a", "b", "c"),
      Array[TypeInformation[_]](Types.INT, Types.LONG, Types.STRING)))
    testUtil.addTableSource(
      "table3", table3Source, FlinkStatistic.builder().tableStats(table3Stats).build())
    val table5Stats = new TableStats(8000000)
    val table5Source = new MockTableSource(isBatch,
      new TableSchema(Array("d", "e", "f", "g", "h"),
      Array[TypeInformation[_]](Types.INT, Types.LONG, Types.INT, Types.STRING, Types.LONG)))
    testUtil.addTableSource(
      "table5", table5Source, FlinkStatistic.builder().tableStats(table5Stats).build())
    ExecNodeResourceTest.setResourceConfig(testUtil.tableEnv.getConfig)
  }

  @Test
  def testSourcePartitionMaxNum(): Unit = {
    val sqlQuery = "SELECT * FROM table3"
    testUtil.verifyResource(sqlQuery)
  }

  @Test
  def testSortLimit(): Unit = {
    val sqlQuery = "SELECT sum(a) as sum_a, c FROM table3 group by c order by c limit 2"
    testUtil.verifyResource(sqlQuery)
  }

  @Test
  def testConfigSourceParallelism(): Unit = {
    testUtil.tableEnv.getConfig.getConfiguration.setInteger(
      ExecutionConfigOptions.SQL_RESOURCE_SOURCE_PARALLELISM, 100)
    val sqlQuery = "SELECT sum(a) as sum_a, c FROM table3 group by c order by c limit 2"
    testUtil.verifyResource(sqlQuery)
  }

  @Test
  def testUnionQuery(): Unit = {
    val statsOfTable4 = new TableStats(100L)
    val table4Source = new MockTableSource(isBatch,
      new TableSchema(Array("a", "b", "c"),
        Array[TypeInformation[_]](Types.INT, Types.LONG, Types.STRING)))
    testUtil.addTableSource(
      "table4", table4Source, FlinkStatistic.builder().tableStats(statsOfTable4).build())

    val sqlQuery = "SELECT sum(a) as sum_a, g FROM " +
        "(SELECT a, b, c FROM table3 UNION ALL SELECT a, b, c FROM table4), table5 " +
        "WHERE b = e group by g"
    testUtil.verifyResource(sqlQuery)
  }

  @Test
  def testSinkSelfParallelism(): Unit = {
    val sqlQuery = "SELECT * FROM table3"
    val table = testUtil.tableEnv.sqlQuery(sqlQuery)
    val tableSink = new MockTableSink(new TableSchema(Array("a", "b", "c"),
      Array[TypeInformation[_]](Types.INT, Types.LONG, Types.STRING)), -1)

    testUtil.writeToSink(table, tableSink, "sink")
    testUtil.tableEnv.asInstanceOf[TestingTableEnvironment].translate()
    assertEquals(17, tableSink.getSinkTransformation.getParallelism)
  }

  @Test
  def testSinkConfigParallelism(): Unit = {
    testUtil.tableEnv.getConfig.getConfiguration.setInteger(
      ExecutionConfigOptions.SQL_RESOURCE_SINK_PARALLELISM,
      25
    )
    val sqlQuery = "SELECT * FROM table3"
    val table = testUtil.tableEnv.sqlQuery(sqlQuery)
    val tableSink = new MockTableSink(new TableSchema(Array("a", "b", "c"),
      Array[TypeInformation[_]](Types.INT, Types.LONG, Types.STRING)), -1)

    testUtil.writeToSink(table, tableSink, "sink")
    testUtil.tableEnv.asInstanceOf[TestingTableEnvironment].translate()
    assertEquals(25, tableSink.getSinkTransformation.getParallelism)
  }

  @Test
  def testSinkConfigParallelismWhenMax1(): Unit = {
    testUtil.tableEnv.getConfig.getConfiguration.setInteger(
      ExecutionConfigOptions.SQL_RESOURCE_SINK_PARALLELISM,
      25
    )
    val sqlQuery = "SELECT * FROM table3"
    val table = testUtil.tableEnv.sqlQuery(sqlQuery)
    val tableSink = new MockTableSink(new TableSchema(Array("a", "b", "c"),
      Array[TypeInformation[_]](Types.INT, Types.LONG, Types.STRING)), 23)

    testUtil.writeToSink(table, tableSink, "sink")
    testUtil.tableEnv.asInstanceOf[TestingTableEnvironment].translate()
    assertEquals(17, tableSink.getSinkTransformation.getParallelism)
  }

  @Test
  def testSinkConfigParallelismWhenMax2(): Unit = {
    testUtil.tableEnv.getConfig.getConfiguration.setInteger(
      ExecutionConfigOptions.SQL_RESOURCE_SINK_PARALLELISM,
      25
    )
    val sqlQuery = "SELECT * FROM table3"
    val table = testUtil.tableEnv.sqlQuery(sqlQuery)
    val tableSink = new MockTableSink(new TableSchema(Array("a", "b", "c"),
      Array[TypeInformation[_]](Types.INT, Types.LONG, Types.STRING)), 25)

    testUtil.writeToSink(table, tableSink, "sink")
    testUtil.tableEnv.asInstanceOf[TestingTableEnvironment].translate()
    assertEquals(25, tableSink.getSinkTransformation.getParallelism)
  }
}

object ExecNodeResourceTest {

  @Parameterized.Parameters(name = "isBatch={0}")
  def parameters(): util.Collection[Array[Any]] = {
    util.Arrays.asList(
      Array(true),
      Array(false)
    )
  }

  def setResourceConfig(tableConfig: TableConfig): Unit = {
    tableConfig.getConfiguration.setInteger(
      ExecutionConfigOptions.SQL_RESOURCE_DEFAULT_PARALLELISM,
      18)
  }
}

/**
  * Batch/Stream [[org.apache.flink.table.sources.TableSource]] for resource testing.
  */
class MockTableSource(isBatch: Boolean, schema: TableSchema)
    extends StreamTableSource[BaseRow] {

  override def isBounded: Boolean = isBatch

  override def getDataStream(
      execEnv: environment.StreamExecutionEnvironment): DataStream[BaseRow] = {
    val transformation = mock(classOf[SourceTransformation[BaseRow]])
    when(transformation.getMaxParallelism).thenReturn(-1)
    val bs = mock(classOf[DataStream[BaseRow]])
    when(bs.getTransformation).thenReturn(transformation)
    when(transformation.getOutputType).thenReturn(getReturnType)
    val factory = mock(classOf[StreamOperatorFactory[BaseRow]])
    when(factory.isStreamSource).thenReturn(!isBatch)
    when(transformation.getOperatorFactory).thenReturn(factory)
    bs
  }

  override def getReturnType: TypeInformation[BaseRow] = {
    val LogicalTypes = schema.getFieldTypes.map(
      TypeInfoLogicalTypeConverter.fromTypeInfoToLogicalType)
    new BaseRowTypeInfo(LogicalTypes, schema.getFieldNames)
  }

  override def getTableSchema: TableSchema = schema
}

/**
  * Batch/Stream [[org.apache.flink.table.sinks.TableSink]] for resource testing.
  */
class MockTableSink(schema: TableSchema, maxParallelism: Int) extends StreamTableSink[BaseRow]
with AppendStreamTableSink[BaseRow] {

  private var sinkTransformation:SinkTransformation[BaseRow] = _

  override def emitDataStream(dataStream: DataStream[BaseRow]) = ???

  override def consumeDataStream(dataStream: DataStream[BaseRow]): DataStreamSink[_] = {
    val outputFormat = mock(classOf[OutputFormat[BaseRow]])
    val ret = dataStream.writeUsingOutputFormat(outputFormat).name("collect")
    ret.getTransformation.setParallelism(17)
    if (maxParallelism > 0) {
      ret.getTransformation.setMaxParallelism(maxParallelism)
    }
    sinkTransformation = ret.getTransformation
    ret
  }

  override def configure(fieldNames: Array[String], fieldTypes: Array[TypeInformation[_]]):
  TableSink[BaseRow] = {
    this
  }

  override def getOutputType: TypeInformation[BaseRow] = {
    val LogicalTypes = schema.getFieldTypes.map(
      TypeInfoLogicalTypeConverter.fromTypeInfoToLogicalType)
    new BaseRowTypeInfo(LogicalTypes, schema.getFieldNames)
  }

  override def getFieldNames: Array[String] = schema.getFieldNames

  /**
    * @deprecated Use the field types of { @link #getTableSchema()} instead.
    */
  override def getFieldTypes: Array[TypeInformation[_]] = schema.getFieldTypes

  def getSinkTransformation: SinkTransformation[BaseRow] = sinkTransformation
}

