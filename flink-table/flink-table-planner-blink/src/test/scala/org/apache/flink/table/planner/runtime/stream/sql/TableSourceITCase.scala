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

package org.apache.flink.table.planner.runtime.stream.sql

import org.apache.flink.api.scala._
import org.apache.flink.core.testutils.FlinkMatchers.containsCause
import org.apache.flink.table.api.TableException
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.planner.factories.TestValuesTableFactory
import org.apache.flink.table.planner.runtime.utils.{StreamingTestBase, TestData, TestingAppendSink, TestingRetractSink}
import org.apache.flink.table.planner.utils._
import org.apache.flink.table.runtime.functions.scalar.SourceWatermarkFunction
import org.apache.flink.table.utils.LegacyRowResource
import org.apache.flink.types.Row
import org.junit.Assert._
import org.junit.{Before, Rule, Test}

class TableSourceITCase extends StreamingTestBase {

  @Rule
  def usesLegacyRows: LegacyRowResource = LegacyRowResource.INSTANCE

  @Before
  override def before(): Unit = {
    super.before()
    val myTableDataId = TestValuesTableFactory.registerData(TestData.smallData3)
    tEnv.executeSql(
      s"""
         |CREATE TABLE MyTable (
         |  `a` INT,
         |  `b` BIGINT,
         |  `c` STRING
         |) WITH (
         |  'connector' = 'values',
         |  'data-id' = '$myTableDataId',
         |  'bounded' = 'false'
         |)
         |""".stripMargin)

    val filterableTableDataId = TestValuesTableFactory.registerData(
      TestLegacyFilterableTableSource.defaultRows)
    tEnv.executeSql(
      s"""
         |CREATE TABLE FilterableTable (
         |  name STRING,
         |  id BIGINT,
         |  amount INT,
         |  price DOUBLE
         |) WITH (
         |  'connector' = 'values',
         |  'filterable-fields' = 'amount',
         |  'data-id' = '$filterableTableDataId',
         |  'bounded' = 'false'
         |)
         |""".stripMargin)

    val metadataTableDataId = TestValuesTableFactory.registerData(TestData.smallData5)
    tEnv.executeSql(
      s"""
         |CREATE TABLE MetadataTable (
         |  `a` INT,
         |  `other_metadata` INT METADATA FROM 'metadata_3',
         |  `b` BIGINT,
         |  `metadata_1` INT METADATA,
         |  `computed` AS `metadata_1` * 2,
         |  `metadata_2` STRING METADATA
         |) WITH (
         |  'connector' = 'values',
         |  'data-id' = '$metadataTableDataId',
         |  'bounded' = 'false',
         |  'readable-metadata' = 'metadata_1:INT, metadata_2:STRING, metadata_3:BIGINT'
         |)
         |""".stripMargin)
    val nestedTableDataId = TestValuesTableFactory.registerData(TestData.deepNestedRow)
    tEnv.executeSql(
      s"""
         |CREATE TABLE NestedTable (
         |  id BIGINT,
         |  deepNested ROW<
         |     nested1 ROW<name STRING, `value.` INT>,
         |     `nested2.` ROW<num INT, flag BOOLEAN>>,
         |  nested ROW<name STRING, `value` INT>,
         |  name STRING,
         |  nestedItem ROW<deepArray ROW<`value` INT> ARRAY, deepMap MAP<STRING, INT>>,
         |  lower_name AS LOWER(name)
         |) WITH (
         |  'connector' = 'values',
         |  'nested-projection-supported' = 'true',
         |  'data-id' = '$nestedTableDataId',
         |  'bounded' = 'true'
         |)
         |""".stripMargin
    )
  }

  @Test
  def testSimpleProject(): Unit = {
    val result = tEnv.sqlQuery("SELECT a, c FROM MyTable").toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = Seq(
      "1,Hi",
      "2,Hello",
      "3,Hello world")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testProjectWithoutInputRef(): Unit = {
    val result = tEnv.sqlQuery("SELECT COUNT(*) FROM MyTable").toRetractStream[Row]
    val sink = new TestingRetractSink()
    result.addSink(sink).setParallelism(result.parallelism)
    env.execute()

    val expected = Seq("3")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testNestedProject(): Unit = {
    val query =
      """
        |SELECT id,
        |    deepNested.nested1.name AS nestedName,
        |    nested.`value` AS nestedValue,
        |    deepNested.`nested2.`.flag AS nestedFlag,
        |    deepNested.`nested2.`.num + deepNested.nested1.`value.` AS nestedNum,
        |    lower_name
        |FROM NestedTable
      """.stripMargin

    val result = tEnv.sqlQuery(query).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = Seq(
      "1,Sarah,10000,true,1100,mary",
      "2,Rob,20000,false,2200,bob",
      "3,Mike,30000,true,3300,liz")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testNestedProjectWithItem(): Unit = {
    val query =
      """
        |SELECT nestedItem.deepArray[nestedItem.deepMap['Monday']] FROM  NestedTable
        |""".stripMargin
    val result = tEnv.sqlQuery(query).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = Seq("1", "1", "1")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }


  @Test
  def testTableSourceWithFilterable(): Unit = {
    val query = "SELECT id, amount, name FROM FilterableTable WHERE amount > 4 AND price < 9"
    val result = tEnv.sqlQuery(query).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = Seq("5,5,Record_5", "6,6,Record_6", "7,7,Record_7", "8,8,Record_8")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testTableSourceWithFunctionFilterable(): Unit = {
    val query = "SELECT id, amount, name FROM FilterableTable " +
      "WHERE amount > 4 AND price < 9 AND upper(name) = 'RECORD_5'"
    val result = tEnv.sqlQuery(query).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = Seq("5,5,Record_5")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testInputFormatSource(): Unit = {
    val dataId = TestValuesTableFactory.registerData(TestData.smallData3)
    tEnv.executeSql(
      s"""
         |CREATE TABLE MyInputFormatTable (
         |  `a` INT,
         |  `b` BIGINT,
         |  `c` STRING
         |) WITH (
         |  'connector' = 'values',
         |  'data-id' = '$dataId',
         |  'runtime-source' = 'InputFormat'
         |)
         |""".stripMargin
    )

    val result = tEnv.sqlQuery("SELECT a, c FROM MyInputFormatTable").toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = Seq("1,Hi", "2,Hello", "3,Hello world")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testAllDataTypes(): Unit = {
    val dataId = TestValuesTableFactory.registerData(TestData.fullDataTypesData)
    tEnv.executeSql(
      s"""
         |CREATE TABLE T (
         |  `a` BOOLEAN,
         |  `b` TINYINT,
         |  `c` SMALLINT,
         |  `d` INT,
         |  `e` BIGINT,
         |  `f` FLOAT,
         |  `g` DOUBLE,
         |  `h` DECIMAL(5, 2),
         |  `i` VARCHAR(5),
         |  `j` CHAR(5),
         |  `k` DATE,
         |  `l` TIME(0),
         |  `m` TIMESTAMP(9),
         |  `n` TIMESTAMP(9) WITH LOCAL TIME ZONE,
         |  `o` ARRAY<BIGINT>,
         |  `p` ROW<f1 BIGINT, f2 STRING, f3 DOUBLE>,
         |  `q` MAP<STRING, INT>
         |) WITH (
         |  'connector' = 'values',
         |  'data-id' = '$dataId'
         |)
         |""".stripMargin
    )

    val result = tEnv.sqlQuery("SELECT * FROM T").toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = Seq(
      "true,127,32767,2147483647,9223372036854775807,-1.123,-1.123,5.10,1,1,1969-01-01," +
        "00:00:00.123,1969-01-01T00:00:00.123456789,1969-01-01T00:00:00.123456789Z," +
        "[1, 2, 3],1,a,2.3,{k1=1}",
      "false,-128,-32768,-2147483648,-9223372036854775808,3.4,3.4,6.10,12,12,1970-09-30," +
        "01:01:01.123,1970-09-30T01:01:01.123456,1970-09-30T01:01:01.123456Z," +
        "[4, 5],null,b,4.56,{k4=4, k2=2}",
      "true,0,0,0,0,0.12,0.12,7.10,123,123,1990-12-24," +
        "08:10:24.123,1990-12-24T08:10:24.123,1990-12-24T08:10:24.123Z," +
        "[6, null, 7],3,null,7.86,{k3=null}",
      "false,5,4,123,1234,1.2345,1.2345,8.12,1234,1234,2020-05-01," +
        "23:23:23,2020-05-01T23:23:23,2020-05-01T23:23:23Z," +
        "[8],4,c,null,{null=3}",
      "null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null"
    )
    assertEquals(expected.sorted.mkString("\n"), sink.getAppendResults.sorted.mkString("\n"))
  }

  @Test
  def testSimpleMetadataAccess(): Unit = {
    val result = tEnv.sqlQuery("SELECT `a`, `b`, `metadata_2` FROM MetadataTable")
      .toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = Seq(
      "1,1,Hallo",
      "2,2,Hallo Welt",
      "2,3,Hallo Welt wie")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testComplexMetadataAccess(): Unit = {
    val result = tEnv.sqlQuery(
        "SELECT `a`, `other_metadata`, `b`, `metadata_2`, `computed` FROM MetadataTable")
      .toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()
    // (a, b, metadata_1, computed, metadata_2, other_metadata)
    // (1, 1L, 0, 0, "Hallo", 1L)
    // (2, 2L, 1, 2, "Hallo Welt", 2L)
    // (2, 3L, 2, 4, "Hallo Welt wie", 1L)
    val expected = Seq(
      "1,1,1,Hallo,0",
      "2,2,2,Hallo Welt,2",
      "2,1,3,Hallo Welt wie,4")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testNestedProjectionWithMetadataAccess(): Unit = {
    val query =
      """
        |SELECT id,
        |    deepNested.nested1.name AS nestedName,
        |    nested.`value` AS nestedValue,
        |    deepNested.`nested2.`.flag AS nestedFlag,
        |    deepNested.`nested2.`.num + deepNested.nested1.`value.` AS nestedNum,
        |    LOWER(name) as lowerName
        |FROM NestedTable
      """.stripMargin

    val result = tEnv.sqlQuery(query).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = Seq(
      "1,Sarah,10000,true,1100,mary",
      "2,Rob,20000,false,2200,bob",
      "3,Mike,30000,true,3300,liz")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testSourceWatermarkInDDL(): Unit = {
    val dataId = TestValuesTableFactory.registerData(TestData.data3WithTimestamp)
    tEnv.executeSql(
      s"""
         |CREATE TABLE tableWithWatermark (
         |  `a` INT,
         |  `b` BIGINT,
         |  `c` STRING,
         |  `ts` TIMESTAMP(3),
         |  WATERMARK FOR ts AS SOURCE_WATERMARK()
         |) WITH (
         |  'connector' = 'values',
         |  'data-id' = '$dataId',
         |  'bounded' = 'false'
         |)
         |""".stripMargin)


    try {
      tEnv.executeSql("SELECT * FROM tableWithWatermark").await()
      fail("should fail")
    } catch {
      case t: Throwable =>
        assertThat(t, containsCause(new TableException(SourceWatermarkFunction.ERROR_MESSAGE)))
    }
  }

  @Test
  def testSourceWatermarkInQuery(): Unit = {
    try {
      tEnv.executeSql("SELECT *, SOURCE_WATERMARK() FROM MyTable").print()
      fail("should fail")
    } catch {
      case t: Throwable =>
        assertThat(t, containsCause(new TableException(SourceWatermarkFunction.ERROR_MESSAGE)))
    }
  }
}
