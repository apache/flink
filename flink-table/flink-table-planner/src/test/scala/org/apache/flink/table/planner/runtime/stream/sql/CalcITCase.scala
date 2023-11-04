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

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.table.api.{TableDescriptor, _}
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.api.config.ExecutionConfigOptions
import org.apache.flink.table.api.config.ExecutionConfigOptions.LegacyCastBehaviour
import org.apache.flink.table.api.internal.TableEnvironmentInternal
import org.apache.flink.table.catalog.CatalogDatabaseImpl
import org.apache.flink.table.data.{GenericRowData, MapData, RowData}
import org.apache.flink.table.planner.factories.TestValuesTableFactory
import org.apache.flink.table.planner.runtime.utils._
import org.apache.flink.table.planner.runtime.utils.BatchTestBase.row
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo
import org.apache.flink.table.runtime.typeutils.MapDataSerializerTest.CustomMapData
import org.apache.flink.table.types.logical.{BigIntType, BooleanType, IntType, VarCharType}
import org.apache.flink.table.utils.LegacyRowResource
import org.apache.flink.test.util.TestBaseUtils
import org.apache.flink.types.Row
import org.apache.flink.util.CollectionUtil

import org.junit._
import org.junit.Assert._

import java.time.Instant
import java.util

import scala.collection.JavaConversions._

class CalcITCase extends StreamingTestBase {

  @Rule
  def usesLegacyRows: LegacyRowResource = LegacyRowResource.INSTANCE

  @Test
  def testSelectWithLegacyCastIntToDate(): Unit = {
    tEnv.getConfig.getConfiguration
      .set(ExecutionConfigOptions.TABLE_EXEC_LEGACY_CAST_BEHAVIOUR, LegacyCastBehaviour.ENABLED)

    val result = tEnv
      .sqlQuery("SELECT CASE WHEN true THEN CAST(2 AS INT) ELSE CAST('2017-12-11' AS DATE) END")
      .toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = List("1970-01-03")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testCastNumericToBooleanInCondition(): Unit = {
    val sqlQuery =
      s"""
         | SELECT * FROM MyTableRow WHERE b = CAST(1 AS BOOLEAN)
         | UNION ALL
         | SELECT * FROM MyTableRow WHERE b = CAST(0 AS BOOLEAN)
         |""".stripMargin

    val rowData1: GenericRowData = new GenericRowData(2)
    rowData1.setField(0, 1)
    rowData1.setField(1, true)

    val rowData2: GenericRowData = new GenericRowData(2)
    rowData2.setField(0, 2)
    rowData2.setField(1, false)

    val data = List(rowData1, rowData2)

    implicit val dataType: TypeInformation[GenericRowData] =
      InternalTypeInfo
        .ofFields(new IntType(), new BooleanType())
        .asInstanceOf[TypeInformation[GenericRowData]]

    val ds = env.fromCollection(data)

    val t = ds.toTable(tEnv, 'a, 'b)
    tEnv.createTemporaryView("MyTableRow", t)

    val outputType = InternalTypeInfo.ofFields(new IntType(), new BooleanType())

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[RowData]
    val sink = new TestingAppendRowDataSink(outputType)
    result.addSink(sink)
    env.execute()

    val expected = List(
      "+I(1,true)",
      "+I(2,false)"
    )
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testGenericRowAndRowData(): Unit = {
    val sqlQuery = "SELECT * FROM MyTableRow"

    val rowData: GenericRowData = new GenericRowData(3)
    rowData.setField(0, 1)
    rowData.setField(1, 1)
    rowData.setField(2, 1L)

    val data = List(rowData)

    implicit val tpe: TypeInformation[GenericRowData] =
      InternalTypeInfo
        .ofFields(new IntType(), new IntType(), new BigIntType())
        .asInstanceOf[TypeInformation[GenericRowData]]

    val ds = env.fromCollection(data)

    val t = ds.toTable(tEnv, 'a, 'b, 'c)
    tEnv.createTemporaryView("MyTableRow", t)

    val outputType = InternalTypeInfo.ofFields(new IntType(), new IntType(), new BigIntType())

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[RowData]
    val sink = new TestingAppendRowDataSink(outputType)
    result.addSink(sink)
    env.execute()

    val expected = List("+I(1,1,1)")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testRowAndRowData(): Unit = {
    val sqlQuery = "SELECT * FROM MyTableRow WHERE c < 3"

    val data = List(
      Row.of("Hello", "Worlds", Int.box(1)),
      Row.of("Hello", "Hiden", Int.box(5)),
      Row.of("Hello again", "Worlds", Int.box(2)))

    implicit val tpe: TypeInformation[Row] = new RowTypeInfo(Types.STRING, Types.STRING, Types.INT)

    val ds = env.fromCollection(data)

    val t = ds.toTable(tEnv, 'a, 'b, 'c)
    tEnv.createTemporaryView("MyTableRow", t)

    val outputType =
      InternalTypeInfo.ofFields(VarCharType.STRING_TYPE, VarCharType.STRING_TYPE, new IntType())

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[RowData]
    val sink = new TestingAppendRowDataSink(outputType)
    result.addSink(sink)
    env.execute()

    val expected = List("+I(Hello,Worlds,1)", "+I(Hello again,Worlds,2)")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testGenericRowAndRow(): Unit = {
    val sqlQuery = "SELECT * FROM MyTableRow"

    val rowData: GenericRowData = new GenericRowData(3)
    rowData.setField(0, 1)
    rowData.setField(1, 1)
    rowData.setField(2, 1L)

    val data = List(rowData)

    implicit val tpe: TypeInformation[GenericRowData] =
      InternalTypeInfo
        .ofFields(new IntType(), new IntType(), new BigIntType())
        .asInstanceOf[TypeInformation[GenericRowData]]

    val ds = env.fromCollection(data)

    val t = ds.toTable(tEnv, 'a, 'b, 'c)
    tEnv.createTemporaryView("MyTableRow", t)

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = List("1,1,1")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testRowAndRow(): Unit = {
    val sqlQuery = "SELECT * FROM MyTableRow WHERE c < 3"

    val data = List(
      Row.of("Hello", "Worlds", Int.box(1)),
      Row.of("Hello", "Hiden", Int.box(5)),
      Row.of("Hello again", "Worlds", Int.box(2)))

    implicit val tpe: TypeInformation[Row] = new RowTypeInfo(Types.STRING, Types.STRING, Types.INT)

    val ds = env.fromCollection(data)

    val t = ds.toTable(tEnv, 'a, 'b, 'c)
    tEnv.createTemporaryView("MyTableRow", t)

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = List("Hello,Worlds,1", "Hello again,Worlds,2")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testPrimitiveMapType(): Unit = {
    val sqlQuery = "SELECT MAP[b, 30, 10, a] FROM MyTableRow"

    val t = env
      .fromCollection(TestData.smallTupleData3)
      .toTable(tEnv, 'a, 'b, 'c)
    tEnv.createTemporaryView("MyTableRow", t)

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = List("{1=30, 10=1}", "{2=30, 10=2}", "{2=30, 10=3}")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testNonPrimitiveMapType(): Unit = {
    val sqlQuery = "SELECT MAP[a, c] FROM MyTableRow"

    val t = env
      .fromCollection(TestData.smallTupleData3)
      .toTable(tEnv, 'a, 'b, 'c)
    tEnv.createTemporaryView("MyTableRow", t)

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = List("{1=Hi}", "{2=Hello}", "{3=Hello world}")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testSelectStarFromNestedTable(): Unit = {
    val sqlQuery = "SELECT * FROM MyTable"

    val table = tEnv.fromDataStream(
      env.fromCollection(
        Seq(
          ((0, 0), "0"),
          ((1, 1), "1"),
          ((2, 2), "2")
        )),
      '_1,
      '_2)
    tEnv.createTemporaryView("MyTable", table)

    val result = tEnv.sqlQuery(sqlQuery)
    val sink = TestSinkUtil.configureSink(result, new TestingAppendTableSink())
    tEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal("MySink", sink)
    table.executeInsert("MySink").await()

    val expected = List("0,0,0", "1,1,1", "2,2,2")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testIn(): Unit = {
    val sqlQuery = "SELECT * FROM MyTable WHERE b in (1,3,4,5,6)"

    val t = env
      .fromCollection(TestData.tupleData3)
      .toTable(tEnv, 'a, 'b, 'c)
    tEnv.createTemporaryView("MyTable", t)

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = List(
      "1,1,Hi",
      "4,3,Hello world, how are you?",
      "5,3,I am fine.",
      "6,3,Luke Skywalker",
      "7,4,Comment#1",
      "8,4,Comment#2",
      "9,4,Comment#3",
      "10,4,Comment#4",
      "11,5,Comment#5",
      "12,5,Comment#6",
      "13,5,Comment#7",
      "14,5,Comment#8",
      "15,5,Comment#9",
      "16,6,Comment#10",
      "17,6,Comment#11",
      "18,6,Comment#12",
      "19,6,Comment#13",
      "20,6,Comment#14",
      "21,6,Comment#15"
    )
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testNotIn(): Unit = {
    val sqlQuery = "SELECT * FROM MyTable WHERE b not in (1,3,4,5,6)"

    val t = env
      .fromCollection(TestData.tupleData3)
      .toTable(tEnv, 'a, 'b, 'c)
    tEnv.createTemporaryView("MyTable", t)

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = List("2,2,Hello", "3,2,Hello world")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testLongProjectionList(): Unit = {

    val t = env
      .fromCollection(TestData.smallTupleData3)
      .toTable(tEnv, 'a, 'b, 'c)
    tEnv.createTemporaryView("MyTable", t)

    val selectList = Stream
      .range(3, 200)
      .map(i => s"CASE WHEN a IS NOT NULL AND a > $i THEN 0 WHEN a < 0 THEN 0 ELSE $i END")
      .mkString(",")
    val sqlQuery = s"select $selectList from MyTable"

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = Stream.range(3, 200).map(_.toString).mkString(",")
    assertEquals(sink.getAppendResults.size, TestData.smallTupleData3.size)
    sink.getAppendResults.foreach(result => assertEquals(expected, result))
  }

  @Test
  def testIfFunction(): Unit = {
    val testDataId = TestValuesTableFactory.registerData(TestData.data1)
    val ddl =
      s"""
         |CREATE TABLE t (
         |  a int,
         |  b varchar,
         |  c int
         |) WITH (
         |  'connector' = 'values',
         |  'data-id' = '$testDataId',
         |  'bounded' = 'true'
         |)
         |""".stripMargin
    tEnv.executeSql(ddl)
    val expected = List("false,1", "false,2", "false,3", "true,4", "true,5", "true,6")
    val actual = tEnv
      .executeSql("SELECT IF(a > 3, 'true', 'false'), a from t")
      .collect()
      .map(r => r.toString)
      .toList
    assertEquals(expected.sorted, actual.sorted)
  }

  @Test
  def testSourceWithCustomInternalData(): Unit = {

    def createMapData(k: Long, v: Long): MapData = {
      val mapData = new util.HashMap[Long, Long]()
      mapData.put(k, v)
      new CustomMapData(mapData)
    }

    val rowData1: GenericRowData = new GenericRowData(2)
    rowData1.setField(0, 1L)
    rowData1.setField(1, createMapData(1L, 2L))
    val rowData2: GenericRowData = new GenericRowData(2)
    rowData2.setField(0, 2L)
    rowData2.setField(1, createMapData(4L, 5L))
    val values = List(rowData1, rowData2)

    val myTableDataId = TestValuesTableFactory.registerRowData(values)

    val ddl =
      s"""
         |CREATE TABLE CustomTable (
         |  a bigint,
         |  b map<bigint, bigint>
         |) WITH (
         |  'connector' = 'values',
         |  'data-id' = '$myTableDataId',
         |  'register-internal-data' = 'true',
         |  'bounded' = 'true'
         |)
       """.stripMargin

    env.getConfig.disableObjectReuse()
    tEnv.executeSql(ddl)
    val result = tEnv.executeSql("select a, b from CustomTable")

    val expected = List("1,{1=2}", "2,{4=5}")
    val actual = CollectionUtil.iteratorToList(result.collect()).map(r => r.toString)
    assertEquals(expected.sorted, actual.sorted)
  }

  @Test
  def testSimpleProject(): Unit = {
    val myTableDataId = TestValuesTableFactory.registerData(TestData.smallData3)
    val ddl =
      s"""
         |CREATE TABLE SimpleTable (
         |  a int,
         |  b bigint,
         |  c string
         |) WITH (
         |  'connector' = 'values',
         |  'data-id' = '$myTableDataId',
         |  'bounded' = 'true'
         |)
       """.stripMargin
    tEnv.executeSql(ddl)

    val result = tEnv.sqlQuery("select a, c from SimpleTable").toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = List("1,Hi", "2,Hello", "3,Hello world")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testNestedProject(): Unit = {
    val data = Seq(
      row(1, row(row("HI", 11), row(111, true)), row("hi", 1111), "tom"),
      row(2, row(row("HELLO", 22), row(222, false)), row("hello", 2222), "mary"),
      row(3, row(row("HELLO WORLD", 33), row(333, true)), row("hello world", 3333), "benji")
    )
    val myTableDataId = TestValuesTableFactory.registerData(data)
    val ddl =
      s"""
         |CREATE TABLE NestedTable (
         |  id int,
         |  deepNested row<nested1 row<name string, `value` int>,
         |                 nested2 row<num int, flag boolean>>,
         |  nested row<name string, `value` int>,
         |  name string
         |) WITH (
         |  'connector' = 'values',
         |  'nested-projection-supported' = 'false',
         |  'data-id' = '$myTableDataId',
         |  'bounded' = 'true'
         |)
       """.stripMargin
    tEnv.executeSql(ddl)

    val sqlQuery =
      """
        |select id,
        |    deepNested.nested1.name AS nestedName,
        |    nested.`value` AS nestedValue,
        |    deepNested.nested2.flag AS nestedFlag,
        |    deepNested.nested2.num AS nestedNum
        |from NestedTable
        |""".stripMargin
    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected =
      List("1,HI,1111,true,111", "2,HELLO,2222,false,222", "3,HELLO WORLD,3333,true,333")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testDecimalArrayWithDifferentPrecision(): Unit = {
    val sqlQuery = "SELECT ARRAY[0.12, 0.5, 0.99]"

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = List("[0.12, 0.50, 0.99]")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testDecimalMapWithDifferentPrecision(): Unit = {
    val sqlQuery = "SELECT Map['a', 0.12, 'b', 0.5]"

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = List("{a=0.12, b=0.50}")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testCurrentWatermark(): Unit = {
    val rows = Seq(
      row(1, Instant.ofEpochSecond(644326662L)),
      row(2, Instant.ofEpochSecond(1622466300L)),
      row(3, Instant.ofEpochSecond(1622466300L))
    )
    val tableId = TestValuesTableFactory.registerData(rows)

    // We need a fixed timezone to make sure this test can run on machines across the world
    tEnv.getConfig.set("table.local-time-zone", "Europe/Berlin")

    tEnv.executeSql(s"""
                       |CREATE TABLE T (
                       |  id INT,
                       |  ts TIMESTAMP_LTZ(3),
                       |  WATERMARK FOR ts AS ts
                       |) WITH (
                       |  'connector' = 'values',
                       |  'data-id' = '$tableId',
                       |  'bounded' = 'true'
                       |)
       """.stripMargin)

    // Table API
    val result1 = tEnv
      .from("T")
      .select($("id"), currentWatermark($("ts")))
      .execute()
      .collect()
      .toList
    TestBaseUtils.compareResultAsText(
      result1,
      """1,null
        |2,1990-06-02T11:37:42Z
        |3,2021-05-31T13:05:00Z
        |""".stripMargin)

    // SQL
    val result2 = tEnv
      .sqlQuery("SELECT id, CURRENT_WATERMARK(ts) FROM T")
      .execute()
      .collect()
      .toList
    TestBaseUtils.compareResultAsText(
      result2,
      """1,null
        |2,1990-06-02T11:37:42Z
        |3,2021-05-31T13:05:00Z
        |""".stripMargin)

    val result3 = tEnv
      .sqlQuery(
        """
          |SELECT id FROM T WHERE CURRENT_WATERMARK(ts) IS NULL OR ts > CURRENT_WATERMARK(ts)
          |""".stripMargin)
      .execute()
      .collect()
      .toList
    TestBaseUtils.compareResultAsText(
      result3,
      """1
        |2
        |""".stripMargin)

    val result4 = tEnv
      .sqlQuery("""
                  |SELECT
                  |  TUMBLE_END(ts, INTERVAL '1' SECOND),
                  |  CURRENT_WATERMARK(ts)
                  |FROM T
                  |GROUP BY
                  |  TUMBLE(ts, INTERVAL '1' SECOND),
                  |  CURRENT_WATERMARK(ts)
                  |""".stripMargin)
      .execute()
      .collect()
      .toList
    TestBaseUtils.compareResultAsText(
      result4,
      """1990-06-02T13:37:43,null
        |2021-05-31T15:05:01,1990-06-02T11:37:42Z
        |2021-05-31T15:05:01,2021-05-31T13:05:00Z
        |""".stripMargin
    )
  }

  @Test
  def testCurrentWatermarkForNonRowtimeAttribute(): Unit = {
    val tableId = TestValuesTableFactory.registerData(Seq())
    tEnv.executeSql(s"""
                       |CREATE TABLE T (
                       |  ts TIMESTAMP_LTZ(3)
                       |) WITH (
                       |  'connector' = 'values',
                       |  'data-id' = '$tableId',
                       |  'bounded' = 'true'
                       |)
       """.stripMargin)

    try {
      tEnv.sqlQuery("SELECT CURRENT_WATERMARK(ts) FROM T")
      fail("CURRENT_WATERMARK for a non-rowtime attribute should have failed.");
    } catch {
      case e: Exception =>
        assertEquals(
          "SQL validation failed. Invalid function call:\n" +
            "CURRENT_WATERMARK(TIMESTAMP_LTZ(3))",
          e.getMessage)
    }
  }

  @Test
  def testCurrentWatermarkWithoutAnyAttribute(): Unit = {
    val tableId = TestValuesTableFactory.registerData(Seq())
    tEnv.executeSql(s"""
                       |CREATE TABLE T (
                       |  ts TIMESTAMP_LTZ(3)
                       |) WITH (
                       |  'connector' = 'values',
                       |  'data-id' = '$tableId',
                       |  'bounded' = 'true'
                       |)
       """.stripMargin)

    try {
      tEnv.sqlQuery("SELECT ts, CURRENT_WATERMARK() FROM T")
      fail("CURRENT_WATERMARK without any attribute should have failed.");
    } catch {
      case e: Exception =>
        assertEquals(
          "SQL validation failed. From line 1, column 12 to line 1, column 30: No match found for function signature CURRENT_WATERMARK().\n" +
            "Supported signatures are:\n" +
            "CURRENT_WATERMARK(<TIMESTAMP_WITHOUT_TIME_ZONE *ROWTIME*>)\n" +
            "CURRENT_WATERMARK(<TIMESTAMP_WITH_LOCAL_TIME_ZONE *ROWTIME*>)",
          e.getMessage
        )
    }
  }

  @Test
  def testCreateTemporaryTableFromDescriptor(): Unit = {
    val rows = Seq(row(42))
    val tableId = TestValuesTableFactory.registerData(rows)

    tEnv.createTemporaryTable(
      "T",
      TableDescriptor
        .forConnector("values")
        .schema(
          Schema
            .newBuilder()
            .column("f0", DataTypes.INT())
            .build())
        .option("data-id", tableId)
        .option("bounded", "true")
        .build()
    )

    val result = tEnv.sqlQuery("SELECT * FROM T").execute().collect().toList
    TestBaseUtils.compareResultAsText(result, "42")
  }

  @Test
  def testSearch(): Unit = {
    val stream = env.fromElements("HC809", "H389N     ")
    tEnv.createTemporaryView(
      "SimpleTable",
      stream,
      Schema.newBuilder().column("f0", DataTypes.STRING()).build())

    val sql =
      """
        |SELECT upper(f0) from SimpleTable where upper(f0) in (
        |'CTNBSmokeSensor',
        |'H388N',
        |'H389N     ',
        |'GHL-IRD',
        |'JY-BF-20YN',
        |'HC809',
        |'DH-9908N-AEP',
        |'DH-9908N'
        |)
        |""".stripMargin
    val result = tEnv.sqlQuery(sql).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()
    val expected =
      List("HC809", "H389N     ")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testMultipleCoalesces(): Unit = {
    val result = tEnv
      .sqlQuery(
        "SELECT COALESCE(1),\n" +
          "COALESCE(1, 2),\n" +
          "COALESCE(cast(NULL as int), 2),\n" +
          "COALESCE(1, cast(NULL as int)),\n" +
          "COALESCE(cast(NULL as int), cast(NULL as int), 3),\n" +
          "COALESCE(4, cast(NULL as int), cast(NULL as int), cast(NULL as int)),\n" +
          "COALESCE('1'),\n" +
          "COALESCE('1', '23'),\n" +
          "COALESCE(cast(NULL as varchar), '2'),\n" +
          "COALESCE('1', cast(NULL as varchar)),\n" +
          "COALESCE(cast(NULL as varchar), cast(NULL as varchar), '3'),\n" +
          "COALESCE('4', cast(NULL as varchar), cast(NULL as varchar), cast(NULL as varchar)),\n" +
          "COALESCE(1.0),\n" +
          "COALESCE(1.0, 2),\n" +
          "COALESCE(cast(NULL as double), 2.0),\n" +
          "COALESCE(cast(NULL as double), 2.0, 3.0),\n" +
          "COALESCE(2.0, cast(NULL as double), 3.0),\n" +
          "COALESCE(cast(NULL as double), cast(NULL as double))")
      .execute()
      .collect()
      .toList
    TestBaseUtils.compareResultAsText(result, "1,1,2,1,3,4,1,1,2,1,3,4,1.0,1.0,2.0,2.0,2.0,null")
  }

  @Test
  def testCurrentDatabase(): Unit = {
    val result1 = tEnv.sqlQuery("SELECT CURRENT_DATABASE()").execute().collect().toList
    assertEquals(Seq(row(tEnv.getCurrentDatabase)), result1)

    // switch to another database
    tEnv
      .getCatalog(tEnv.getCurrentCatalog)
      .get()
      .createDatabase(
        "db1",
        new CatalogDatabaseImpl(new util.HashMap[String, String](), "db1"),
        false)
    tEnv.useDatabase("db1")
    val result2 = tEnv.sqlQuery("SELECT CURRENT_DATABASE()").execute().collect().toList
    assertEquals(Seq(row(tEnv.getCurrentDatabase)), result2)
  }

  @Test
  def testLikeWithConditionContainsDoubleQuotationMark(): Unit = {
    val rows = Seq(row(42, "abc"), row(2, "cbc\"ddd"))
    val dataId = TestValuesTableFactory.registerData(rows)

    val ddl =
      s"""
         |CREATE TABLE MyTable (
         |  a int,
         |  b string
         |) WITH (
         |  'connector' = 'values',
         |  'data-id' = '$dataId',
         |  'bounded' = 'true'
         |)
       """.stripMargin
    tEnv.executeSql(ddl)

    val result = tEnv
      .sqlQuery("""
                  | SELECT * FROM MyTable WHERE b LIKE '%"%'
                  |""".stripMargin)
      .toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = List("2,cbc\"ddd")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testNonMergeableRandCall(): Unit = {
    val t = env
      .fromCollection(TestData.smallTupleData3)
      .toTable(tEnv, 'a, 'b, 'c)
    tEnv.createTemporaryView("SimpleTable", t)

    val result = tEnv
      .sqlQuery(s"""
                   |SELECT b - a FROM (
                   |  SELECT r + 5 AS a, r + 7 AS b FROM (
                   |    SELECT RAND() AS r FROM SimpleTable
                   |  ) t1
                   |) t2
                   |""".stripMargin)
      .toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = List("2.0", "2.0", "2.0")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }
}
