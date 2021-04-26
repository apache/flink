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
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.api.internal.TableEnvironmentInternal
import org.apache.flink.table.data.{GenericRowData, MapData, RowData}
import org.apache.flink.table.planner.factories.TestValuesTableFactory
import org.apache.flink.table.planner.runtime.utils.BatchTestBase.row
import org.apache.flink.table.planner.runtime.utils._
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo
import org.apache.flink.table.runtime.typeutils.MapDataSerializerTest.CustomMapData
import org.apache.flink.table.types.logical.{BigIntType, IntType, VarCharType}
import org.apache.flink.table.utils.LegacyRowResource
import org.apache.flink.types.Row
import org.apache.flink.util.CollectionUtil

import java.util
import org.junit.Assert._
import org.junit._
import scala.collection.JavaConversions._
import scala.collection.Seq

class CalcITCase extends StreamingTestBase {

  @Rule
  def usesLegacyRows: LegacyRowResource = LegacyRowResource.INSTANCE

  @Test
  def testGenericRowAndRowData(): Unit = {
    val sqlQuery = "SELECT * FROM MyTableRow"

    val rowData: GenericRowData = new GenericRowData(3)
    rowData.setField(0, 1)
    rowData.setField(1, 1)
    rowData.setField(2, 1L)

    val data = List(rowData)

    implicit val tpe: TypeInformation[GenericRowData] =
      InternalTypeInfo.ofFields(
        new IntType(),
        new IntType(),
        new BigIntType()).asInstanceOf[TypeInformation[GenericRowData]]

    val ds = env.fromCollection(data)

    val t = ds.toTable(tEnv, 'a, 'b, 'c)
    tEnv.registerTable("MyTableRow", t)

    val outputType = InternalTypeInfo.ofFields(
      new IntType(),
      new IntType(),
      new BigIntType())

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

    implicit val tpe: TypeInformation[Row] = new RowTypeInfo(
      Types.STRING,
      Types.STRING,
      Types.INT)

    val ds = env.fromCollection(data)

    val t = ds.toTable(tEnv, 'a, 'b, 'c)
    tEnv.registerTable("MyTableRow", t)

    val outputType = InternalTypeInfo.ofFields(
      new VarCharType(VarCharType.MAX_LENGTH),
      new VarCharType(VarCharType.MAX_LENGTH),
      new IntType())

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[RowData]
    val sink = new TestingAppendRowDataSink(outputType)
    result.addSink(sink)
    env.execute()

    val expected = List("+I(Hello,Worlds,1)","+I(Hello again,Worlds,2)")
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
      InternalTypeInfo.ofFields(
        new IntType(),
        new IntType(),
        new BigIntType()).asInstanceOf[TypeInformation[GenericRowData]]

    val ds = env.fromCollection(data)

    val t = ds.toTable(tEnv, 'a, 'b, 'c)
    tEnv.registerTable("MyTableRow", t)

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

    implicit val tpe: TypeInformation[Row] = new RowTypeInfo(
      Types.STRING,
      Types.STRING,
      Types.INT)

    val ds = env.fromCollection(data)

    val t = ds.toTable(tEnv, 'a, 'b, 'c)
    tEnv.registerTable("MyTableRow", t)

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = List("Hello,Worlds,1","Hello again,Worlds,2")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testPrimitiveMapType(): Unit = {
    val sqlQuery = "SELECT MAP[b, 30, 10, a] FROM MyTableRow"

    val t = env.fromCollection(TestData.smallTupleData3)
            .toTable(tEnv, 'a, 'b, 'c)
    tEnv.registerTable("MyTableRow", t)

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = List(
      "{1=30, 10=1}",
      "{2=30, 10=2}",
      "{2=30, 10=3}")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testNonPrimitiveMapType(): Unit = {
    val sqlQuery = "SELECT MAP[a, c] FROM MyTableRow"

    val t = env.fromCollection(TestData.smallTupleData3)
            .toTable(tEnv, 'a, 'b, 'c)
    tEnv.registerTable("MyTableRow", t)

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = List(
      "{1=Hi}",
      "{2=Hello}",
      "{3=Hello world}")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testSelectStarFromNestedTable(): Unit = {
    val sqlQuery = "SELECT * FROM MyTable"

    val table = tEnv.fromDataStream(env.fromCollection(Seq(
      ((0, 0), "0"),
      ((1, 1), "1"),
      ((2, 2), "2")
    )), '_1, '_2)
    tEnv.registerTable("MyTable", table)

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

    val t = env.fromCollection(TestData.tupleData3)
      .toTable(tEnv, 'a, 'b, 'c)
    tEnv.registerTable("MyTable", t)

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = List(
      "1,1,Hi", "4,3,Hello world, how are you?", "5,3,I am fine.", "6,3,Luke Skywalker",
      "7,4,Comment#1", "8,4,Comment#2", "9,4,Comment#3", "10,4,Comment#4", "11,5,Comment#5",
      "12,5,Comment#6", "13,5,Comment#7", "14,5,Comment#8", "15,5,Comment#9", "16,6,Comment#10",
      "17,6,Comment#11", "18,6,Comment#12", "19,6,Comment#13", "20,6,Comment#14", "21,6,Comment#15")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testNotIn(): Unit = {
    val sqlQuery = "SELECT * FROM MyTable WHERE b not in (1,3,4,5,6)"

    val t = env.fromCollection(TestData.tupleData3)
      .toTable(tEnv, 'a, 'b, 'c)
    tEnv.registerTable("MyTable", t)

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = List("2,2,Hello", "3,2,Hello world")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testLongProjectionList(): Unit = {

    val t = env.fromCollection(TestData.smallTupleData3)
      .toTable(tEnv, 'a, 'b, 'c)
    tEnv.createTemporaryView("MyTable", t)

    val selectList = Stream.range(3, 200)
      .map(i => s"CASE WHEN a IS NOT NULL AND a > $i THEN 0 WHEN a < 0 THEN 0 ELSE $i END")
      .mkString(",")
    val sqlQuery = s"select $selectList from MyTable"

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = Stream.range(3, 200).map(_.toString).mkString(",")
    assertEquals(sink.getAppendResults.size, TestData.smallTupleData3.size)
    sink.getAppendResults.foreach( result =>
      assertEquals(expected, result)
    )
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
    val result = tEnv.executeSql( "select a, b from CustomTable")

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

    val result = tEnv.sqlQuery( "select a, c from SimpleTable").toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = List("1,Hi","2,Hello", "3,Hello world")
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
      List("1,HI,1111,true,111","2,HELLO,2222,false,222", "3,HELLO WORLD,3333,true,333")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }
}
