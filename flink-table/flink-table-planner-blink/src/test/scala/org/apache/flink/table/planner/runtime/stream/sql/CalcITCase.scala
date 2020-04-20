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
import org.apache.flink.table.api.scala._
import org.apache.flink.table.dataformat.{BaseRow, GenericRow}
import org.apache.flink.table.planner.runtime.utils.{StreamingTestBase, TestData, TestSinkUtil, TestingAppendBaseRowSink, TestingAppendSink, TestingAppendTableSink}
import org.apache.flink.table.runtime.typeutils.BaseRowTypeInfo
import org.apache.flink.table.types.logical.{BigIntType, IntType, VarCharType}
import org.apache.flink.types.Row

import org.junit.Assert._
import org.junit._

class CalcITCase extends StreamingTestBase {

  @Test
  def testGenericRowAndBaseRow(): Unit = {
    val sqlQuery = "SELECT * FROM MyTableRow"

    val rowData: GenericRow = new GenericRow(3)
    rowData.setInt(0, 1)
    rowData.setInt(1, 1)
    rowData.setLong(2, 1L)

    val data = List(rowData)

    implicit val tpe: TypeInformation[GenericRow] =
      new BaseRowTypeInfo(
        new IntType(),
        new IntType(),
        new BigIntType()).asInstanceOf[TypeInformation[GenericRow]]

    val ds = env.fromCollection(data)

    val t = ds.toTable(tEnv, 'a, 'b, 'c)
    tEnv.registerTable("MyTableRow", t)

    val outputType = new BaseRowTypeInfo(
      new IntType(),
      new IntType(),
      new BigIntType())

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[BaseRow]
    val sink = new TestingAppendBaseRowSink(outputType)
    result.addSink(sink)
    env.execute()

    val expected = List("0|1,1,1")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testRowAndBaseRow(): Unit = {
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

    val outputType = new BaseRowTypeInfo(
      new VarCharType(VarCharType.MAX_LENGTH),
      new VarCharType(VarCharType.MAX_LENGTH),
      new IntType())

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[BaseRow]
    val sink = new TestingAppendBaseRowSink(outputType)
    result.addSink(sink)
    env.execute()

    val expected = List("0|Hello,Worlds,1","0|Hello again,Worlds,2")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testGenericRowAndRow(): Unit = {
    val sqlQuery = "SELECT * FROM MyTableRow"

    val rowData: GenericRow = new GenericRow(3)
    rowData.setInt(0, 1)
    rowData.setInt(1, 1)
    rowData.setLong(2, 1L)

    val data = List(rowData)

    implicit val tpe: TypeInformation[GenericRow] =
      new BaseRowTypeInfo(
        new IntType(),
        new IntType(),
        new BigIntType()).asInstanceOf[TypeInformation[GenericRow]]

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
    )))
    tEnv.registerTable("MyTable", table)

    val result = tEnv.sqlQuery(sqlQuery)
    val sink = TestSinkUtil.configureSink(result, new TestingAppendTableSink())
    tEnv.registerTableSink("MySink", sink)
    tEnv.insertInto("MySink", result)
    tEnv.execute("test")

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
}
