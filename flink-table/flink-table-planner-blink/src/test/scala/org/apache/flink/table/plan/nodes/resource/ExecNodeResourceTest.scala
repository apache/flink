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
import org.apache.flink.table.plan.nodes.resource.NodeResourceConfig.InferMode
import org.apache.flink.table.util.{TableTestBase, TableTestUtil}
import org.apache.flink.streaming.api.datastream.{DataStream, DataStreamSink}
import org.apache.flink.streaming.api.environment
import org.apache.flink.streaming.api.operators.StreamOperatorFactory
import org.apache.flink.streaming.api.transformations.{SinkTransformation, SourceTransformation}
import org.apache.flink.table.api.{TableConfig, TableConfigOptions, TableSchema, Types}
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.plan.schema.TableSourceTable
import org.apache.flink.table.plan.stats.{FlinkStatistic, TableStats}
import org.apache.flink.table.sinks.{AppendStreamTableSink, StreamTableSink, TableSink}
import org.apache.flink.table.sources.StreamTableSource
import org.apache.flink.table.types.TypeInfoLogicalTypeConverter
import org.apache.flink.table.typeutils.BaseRowTypeInfo

import org.junit.Assert.assertEquals
import org.junit.{Before, Test}
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.mockito.Mockito.{mock, when}

import java.util

@RunWith(classOf[Parameterized])
class ExecNodeResourceTest(isBatch: Boolean,
    inferMode: NodeResourceConfig.InferMode) extends TableTestBase {

  private var testUtil: TableTestUtil = _

  @Before
  def before(): Unit = {
    testUtil = if(isBatch) batchTestUtil() else streamTestUtil()
    testUtil.getTableEnv.getConfig.getConf.setString(
      TableConfigOptions.SQL_RESOURCE_INFER_MODE,
      inferMode.toString
    )
    val table3Stats = new TableStats(5000000)
    val table3Source = new MockTableSource(isBatch,
      new TableSchema(Array("a", "b", "c"),
      Array[TypeInformation[_]](Types.INT, Types.LONG, Types.STRING)))
    testUtil.getTableEnv.registerTableInternal("table3",
      new TableSourceTable[BaseRow](table3Source,
        !isBatch,
        FlinkStatistic.builder().tableStats(table3Stats).build()))
    val table5Stats = new TableStats(8000000)
    val table5Source = new MockTableSource(isBatch,
      new TableSchema(Array("d", "e", "f", "g", "h"),
      Array[TypeInformation[_]](Types.INT, Types.LONG, Types.INT, Types.STRING, Types.LONG)))
    testUtil.getTableEnv.registerTableInternal("table5",
      new TableSourceTable[BaseRow](table5Source,
      !isBatch,
      FlinkStatistic.builder().tableStats(table5Stats).build()))
    ExecNodeResourceTest.setResourceConfig(testUtil.getTableEnv.getConfig)
  }

  @Test
  def testSourcePartitionMaxNum(): Unit = {
    testUtil.getTableEnv.getConfig.getConf.setInteger(
      TableConfigOptions.SQL_RESOURCE_INFER_SOURCE_PARALLELISM_MAX,
      2
    )
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
    testUtil.getTableEnv.getConfig.getConf.setInteger(
      TableConfigOptions.SQL_RESOURCE_SOURCE_PARALLELISM, 100)
    val sqlQuery = "SELECT sum(a) as sum_a, c FROM table3 group by c order by c limit 2"
    testUtil.verifyResource(sqlQuery)
  }

  @Test
  // TODO check when range partition added.
  def testRangePartition(): Unit ={
    testUtil.getTableEnv.getConfig.getConf.setBoolean(
      TableConfigOptions.SQL_EXEC_SORT_RANGE_ENABLED,
      true)
    val sqlQuery = "SELECT * FROM table5 where d < 100 order by e"
    testUtil.verifyResource(sqlQuery)
  }

  @Test
  def testUnionQuery(): Unit = {
    val statsOfTable4 = new TableStats(100L)
    testUtil.addTableSource("table4",
      Array[TypeInformation[_]](Types.INT, Types.LONG, Types.STRING),
      Array("a", "b", "c"),
      FlinkStatistic.builder().tableStats(statsOfTable4).build())

    val sqlQuery = "SELECT sum(a) as sum_a, g FROM " +
        "(SELECT a, b, c FROM table3 UNION ALL SELECT a, b, c FROM table4), table5 " +
        "WHERE b = e group by g"
    testUtil.verifyResource(sqlQuery)
  }

  @Test
  def testSinkSelfParallelism(): Unit = {
    val sqlQuery = "SELECT * FROM table3"
    val table = testUtil.getTableEnv.sqlQuery(sqlQuery)
    val tableSink = new MockTableSink(new TableSchema(Array("a", "b", "c"),
      Array[TypeInformation[_]](Types.INT, Types.LONG, Types.STRING)), -1)

    testUtil.getTableEnv.writeToSink(table, tableSink, "sink")
    testUtil.getTableEnv.generateStreamGraph()
    assertEquals(17, tableSink.getSinkTransformation.getParallelism)
  }

  @Test
  def testSinkConfigParallelism(): Unit = {
    testUtil.getTableEnv.getConfig.getConf.setInteger(
      TableConfigOptions.SQL_RESOURCE_SINK_PARALLELISM,
      25
    )
    val sqlQuery = "SELECT * FROM table3"
    val table = testUtil.getTableEnv.sqlQuery(sqlQuery)
    val tableSink = new MockTableSink(new TableSchema(Array("a", "b", "c"),
      Array[TypeInformation[_]](Types.INT, Types.LONG, Types.STRING)), -1)

    testUtil.getTableEnv.writeToSink(table, tableSink, "sink")
    testUtil.getTableEnv.generateStreamGraph()
    assertEquals(25, tableSink.getSinkTransformation.getParallelism)
  }

  @Test
  def testSinkConfigParallelismWhenMax1(): Unit = {
    testUtil.getTableEnv.getConfig.getConf.setInteger(
      TableConfigOptions.SQL_RESOURCE_SINK_PARALLELISM,
      25
    )
    val sqlQuery = "SELECT * FROM table3"
    val table = testUtil.getTableEnv.sqlQuery(sqlQuery)
    val tableSink = new MockTableSink(new TableSchema(Array("a", "b", "c"),
      Array[TypeInformation[_]](Types.INT, Types.LONG, Types.STRING)), 23)

    testUtil.getTableEnv.writeToSink(table, tableSink, "sink")
    testUtil.getTableEnv.generateStreamGraph()
    assertEquals(17, tableSink.getSinkTransformation.getParallelism)
  }

  @Test
  def testSinkConfigParallelismWhenMax2(): Unit = {
    testUtil.getTableEnv.getConfig.getConf.setInteger(
      TableConfigOptions.SQL_RESOURCE_SINK_PARALLELISM,
      25
    )
    val sqlQuery = "SELECT * FROM table3"
    val table = testUtil.getTableEnv.sqlQuery(sqlQuery)
    val tableSink = new MockTableSink(new TableSchema(Array("a", "b", "c"),
      Array[TypeInformation[_]](Types.INT, Types.LONG, Types.STRING)), 25)

    testUtil.getTableEnv.writeToSink(table, tableSink, "sink")
    testUtil.getTableEnv.generateStreamGraph()
    assertEquals(25, tableSink.getSinkTransformation.getParallelism)
  }
}

object ExecNodeResourceTest {

  @Parameterized.Parameters(name = "isBatch={0}, {1}")
  def parameters(): util.Collection[Array[Any]] = {
    util.Arrays.asList(
      Array(true, InferMode.NONE),
      Array(true, InferMode.ONLY_SOURCE),
      Array(false, InferMode.NONE),
      Array(false, InferMode.ONLY_SOURCE)
    )
  }

  def setResourceConfig(tableConfig: TableConfig): Unit = {
    tableConfig.getConf.setInteger(
      TableConfigOptions.SQL_RESOURCE_DEFAULT_PARALLELISM,
      18)
    tableConfig.getConf.setInteger(
      TableConfigOptions.SQL_RESOURCE_INFER_SOURCE_PARALLELISM_MAX,
      1000)
    tableConfig.getConf.setLong(
      TableConfigOptions.SQL_RESOURCE_INFER_ROWS_PER_PARTITION,
      1000000
    )
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

