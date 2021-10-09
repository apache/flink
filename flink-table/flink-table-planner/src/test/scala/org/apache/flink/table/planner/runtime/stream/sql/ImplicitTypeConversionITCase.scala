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
import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.data.{DecimalDataUtils, GenericRowData, RowData, StringData, TimestampData}
import org.apache.flink.table.planner.codegen.CodeGenException
import org.apache.flink.table.planner.runtime.utils.{StreamingTestBase, TestingAppendRowDataSink}
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo
import org.apache.flink.table.types.logical._
import org.apache.flink.table.utils.LegacyRowResource

import org.junit.Assert.assertEquals
import org.junit.{Rule, Test}

import java.time.{LocalDate, LocalDateTime, LocalTime}

class ImplicitTypeConversionITCase extends StreamingTestBase {

  @Rule
  def usesLegacyRows: LegacyRowResource = LegacyRowResource.INSTANCE

  private def testSingleTableSqlQueryWithOutputType(
      sqlQuery: String, outputType: InternalTypeInfo[RowData]): List[String] = {
    val rowData: GenericRowData = new GenericRowData(14)
    rowData.setField(0, 1.toByte)
    rowData.setField(1, 1.toShort)
    rowData.setField(2, 1)
    rowData.setField(3, 1.toLong)
    rowData.setField(4, DecimalDataUtils.castFrom(1, 1, 0))
    rowData.setField(5, 1.toFloat)
    rowData.setField(6, 1.toDouble)
    val date: Int = LocalDate.parse("2001-01-01").toEpochDay.toInt
    rowData.setField(7, date)
    val time: Int = (LocalTime.parse("00:00:00").toNanoOfDay / 1000000L).toInt
    rowData.setField(8, time)
    val timestamp = TimestampData.fromLocalDateTime(LocalDateTime.parse("2001-01-01T00:00:00"))
    rowData.setField(9, timestamp)
    rowData.setField(10, StringData.fromString("1"))
    rowData.setField(11, StringData.fromString("2001-01-01"))
    rowData.setField(12, StringData.fromString("00:00:00"))
    rowData.setField(13, StringData.fromString("2001-01-01 00:00:00"))
    val data = List(rowData)

    implicit val tpe: TypeInformation[GenericRowData] =
      InternalTypeInfo.ofFields(
        new TinyIntType(),
        new SmallIntType(),
        new IntType(),
        new BigIntType(),
        new DecimalType(1, 0),
        new FloatType(),
        new DoubleType(),
        new DateType(),
        new TimeType(),
        new TimestampType(),
        new VarCharType(),
        new VarCharType(),
        new VarCharType(),
        new VarCharType()).asInstanceOf[TypeInformation[GenericRowData]]

    val ds = env.fromCollection(data)

    val table = ds.toTable(
      tEnv,
      'field_tinyint, 'field_smallint, 'field_int, 'field_bigint, 'field_decimal,
      'field_float, 'field_double,
      'field_date, 'field_time, 'field_timestamp,
      'field_varchar_equals_numeric,
      'field_varchar_equals_date,
      'field_varchar_equals_time,
      'field_varchar_equals_timestamp)
    tEnv.registerTable("TestTable", table)

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[RowData]
    val sink = new TestingAppendRowDataSink(outputType)
    result.addSink(sink)
    env.execute()

    sink.getAppendResults
  }

  @Test
  def testTinyIntAndSmallIntConversionInFilter(): Unit ={
    val sqlQuery =
      "SELECT field_tinyint, field_smallint FROM TestTable WHERE field_tinyint = field_smallint"

    val outputType = InternalTypeInfo.ofFields(
      new TinyIntType(),
      new SmallIntType())

    val expected = List("+I(1,1)")

    val actualResult = testSingleTableSqlQueryWithOutputType(sqlQuery, outputType)
    assertEquals(expected.sorted, actualResult.sorted)
  }


  @Test
  def testTinyIntAndIntConversionInFilter(): Unit ={
    val sqlQuery =
      "SELECT field_tinyint, field_int FROM TestTable WHERE field_tinyint = field_int"

    val outputType = InternalTypeInfo.ofFields(
      new TinyIntType(),
      new IntType())

    val expected = List("+I(1,1)")

    val actualResult = testSingleTableSqlQueryWithOutputType(sqlQuery, outputType)
    assertEquals(expected.sorted, actualResult.sorted)
  }

  @Test
  def testTinyIntAndBigIntConversionInFilter(): Unit ={
    val sqlQuery =
      "SELECT field_tinyint, field_bigint FROM TestTable WHERE field_tinyint = field_bigint"

    val outputType = InternalTypeInfo.ofFields(
      new TinyIntType(),
      new BigIntType())

    val expected = List("+I(1,1)")

    val actualResult = testSingleTableSqlQueryWithOutputType(sqlQuery, outputType)
    assertEquals(expected.sorted, actualResult.sorted)
  }

  @Test
  def testTinyIntAndDecimalConversionInFilter(): Unit ={
    val sqlQuery =
      "SELECT field_tinyint, field_decimal FROM TestTable WHERE field_tinyint = field_decimal"

    val outputType = InternalTypeInfo.ofFields(
      new TinyIntType(),
      new DecimalType())

    val expected = List("+I(1,1)")

    val actualResult = testSingleTableSqlQueryWithOutputType(sqlQuery, outputType)
    assertEquals(expected.sorted, actualResult.sorted)
  }

  @Test
  def testTinyIntAndFloatConversionInFilter(): Unit ={
    val sqlQuery =
      "SELECT field_tinyint, field_float FROM TestTable WHERE field_tinyint = field_float"

    val outputType = InternalTypeInfo.ofFields(
      new TinyIntType(),
      new FloatType())

    val expected = List("+I(1,1.0)")

    val actualResult = testSingleTableSqlQueryWithOutputType(sqlQuery, outputType)
    assertEquals(expected.sorted, actualResult.sorted)
  }

  @Test
  def testTinyIntAndDoubleConversionInFilter(): Unit ={
    val sqlQuery =
      "SELECT field_tinyint, field_double FROM TestTable WHERE field_tinyint = field_double"

    val outputType = InternalTypeInfo.ofFields(
      new TinyIntType(),
      new DoubleType())

    val expected = List("+I(1,1.0)")

    val actualResult = testSingleTableSqlQueryWithOutputType(sqlQuery, outputType)
    assertEquals(expected.sorted, actualResult.sorted)
  }

  @Test
  def testSmallIntAndIntConversionInFilter(): Unit ={
    val sqlQuery =
      "SELECT field_smallint, field_int FROM TestTable WHERE field_smallint = field_int"

    val outputType = InternalTypeInfo.ofFields(
      new SmallIntType(),
      new IntType())

    val expected = List("+I(1,1)")

    val actualResult = testSingleTableSqlQueryWithOutputType(sqlQuery, outputType)
    assertEquals(expected.sorted, actualResult.sorted)
  }

  @Test
  def testSmallIntAndBigIntConversionInFilter(): Unit ={
    val sqlQuery =
      "SELECT field_smallint, field_bigint FROM TestTable WHERE field_smallint = field_bigint"

    val outputType = InternalTypeInfo.ofFields(
      new SmallIntType(),
      new BigIntType())

    val expected = List("+I(1,1)")

    val actualResult = testSingleTableSqlQueryWithOutputType(sqlQuery, outputType)
    assertEquals(expected.sorted, actualResult.sorted)
  }

  @Test
  def testSmallIntAndDecimalConversionInFilter(): Unit ={
    val sqlQuery =
      "SELECT field_smallint, field_decimal FROM TestTable WHERE field_smallint = field_decimal"

    val outputType = InternalTypeInfo.ofFields(
      new SmallIntType(),
      new DecimalType())

    val expected = List("+I(1,1)")

    val actualResult = testSingleTableSqlQueryWithOutputType(sqlQuery, outputType)
    assertEquals(expected.sorted, actualResult.sorted)
  }

  @Test
  def testSmallIntAndFloatConversionInFilter(): Unit ={
    val sqlQuery =
      "SELECT field_smallint, field_float FROM TestTable WHERE field_smallint = field_float"

    val outputType = InternalTypeInfo.ofFields(
      new SmallIntType(),
      new FloatType())

    val expected = List("+I(1,1.0)")

    val actualResult = testSingleTableSqlQueryWithOutputType(sqlQuery, outputType)
    assertEquals(expected.sorted, actualResult.sorted)
  }

  @Test
  def testSmallIntAndDoubleConversionInFilter(): Unit ={
    val sqlQuery =
      "SELECT field_smallint, field_double FROM TestTable WHERE field_smallint = field_double"

    val outputType = InternalTypeInfo.ofFields(
      new SmallIntType(),
      new DoubleType())

    val expected = List("+I(1,1.0)")

    val actualResult = testSingleTableSqlQueryWithOutputType(sqlQuery, outputType)
    assertEquals(expected.sorted, actualResult.sorted)
  }

  @Test
  def testIntAndBigIntConversionInFilter(): Unit ={
    val sqlQuery =
      "SELECT field_int, field_bigint FROM TestTable WHERE field_int = field_bigint"

    val outputType = InternalTypeInfo.ofFields(
      new IntType(),
      new BigIntType())

    val expected = List("+I(1,1)")

    val actualResult = testSingleTableSqlQueryWithOutputType(sqlQuery, outputType)
    assertEquals(expected.sorted, actualResult.sorted)
  }

  @Test
  def testIntAndDecimalConversionInFilter(): Unit ={
    val sqlQuery =
      "SELECT field_int, field_decimal FROM TestTable WHERE field_int = field_decimal"

    val outputType = InternalTypeInfo.ofFields(
      new IntType(),
      new DecimalType())

    val expected = List("+I(1,1)")

    val actualResult = testSingleTableSqlQueryWithOutputType(sqlQuery, outputType)
    assertEquals(expected.sorted, actualResult.sorted)
  }

  @Test
  def testIntAndFloatConversionInFilter(): Unit ={
    val sqlQuery =
      "SELECT field_int, field_float FROM TestTable WHERE field_int = field_float"

    val outputType = InternalTypeInfo.ofFields(
      new IntType(),
      new FloatType())

    val expected = List("+I(1,1.0)")

    val actualResult = testSingleTableSqlQueryWithOutputType(sqlQuery, outputType)
    assertEquals(expected.sorted, actualResult.sorted)
  }

  @Test
  def testIntAndDoubleConversionInFilter(): Unit ={
    val sqlQuery =
      "SELECT field_int, field_double FROM TestTable WHERE field_int = field_double"

    val outputType = InternalTypeInfo.ofFields(
      new IntType(),
      new DoubleType())

    val expected = List("+I(1,1.0)")

    val actualResult = testSingleTableSqlQueryWithOutputType(sqlQuery, outputType)
    assertEquals(expected.sorted, actualResult.sorted)
  }

  @Test
  def testBigIntAndDecimalConversionInFilter(): Unit ={
    val sqlQuery =
      "SELECT field_bigint, field_decimal FROM TestTable WHERE field_bigint = field_decimal"

    val outputType = InternalTypeInfo.ofFields(
      new BigIntType(),
      new DecimalType())

    val expected = List("+I(1,1)")

    val actualResult = testSingleTableSqlQueryWithOutputType(sqlQuery, outputType)
    assertEquals(expected.sorted, actualResult.sorted)
  }

  @Test
  def testBigIntAndFloatConversionInFilter(): Unit ={
    val sqlQuery =
      "SELECT field_bigint, field_float FROM TestTable WHERE field_bigint = field_float"

    val outputType = InternalTypeInfo.ofFields(
      new BigIntType(),
      new FloatType())

    val expected = List("+I(1,1.0)")

    val actualResult = testSingleTableSqlQueryWithOutputType(sqlQuery, outputType)
    assertEquals(expected.sorted, actualResult.sorted)
  }

  @Test
  def testBigIntAndDoubleConversionInFilter(): Unit ={
    val sqlQuery =
      "SELECT field_bigint, field_double FROM TestTable WHERE field_bigint = field_double"

    val outputType = InternalTypeInfo.ofFields(
      new BigIntType(),
      new DoubleType())

    val expected = List("+I(1,1.0)")

    val actualResult = testSingleTableSqlQueryWithOutputType(sqlQuery, outputType)
    assertEquals(expected.sorted, actualResult.sorted)
  }

  @Test
  def testDecimalAndFloatConversionInFilter(): Unit ={
    val sqlQuery =
      "SELECT field_decimal, field_float FROM TestTable WHERE field_decimal = field_float"

    val outputType = InternalTypeInfo.ofFields(
      new DecimalType(),
      new FloatType())

    val expected = List("+I(1,1.0)")

    val actualResult = testSingleTableSqlQueryWithOutputType(sqlQuery, outputType)
    assertEquals(expected.sorted, actualResult.sorted)
  }

  @Test
  def testDecimalAndDoubleConversionInFilter(): Unit ={
    val sqlQuery =
      "SELECT field_decimal, field_double FROM TestTable WHERE field_decimal = field_double"

    val outputType = InternalTypeInfo.ofFields(
      new DecimalType(),
      new DoubleType())

    val expected = List("+I(1,1.0)")

    val actualResult = testSingleTableSqlQueryWithOutputType(sqlQuery, outputType)
    assertEquals(expected.sorted, actualResult.sorted)
  }

  @Test
  def testFloatAndDoubleConversionInFilter(): Unit ={
    val sqlQuery =
      "SELECT field_float, field_double FROM TestTable WHERE field_float = field_double"

    val outputType = InternalTypeInfo.ofFields(
      new FloatType(),
      new DoubleType())

    val expected = List("+I(1.0,1.0)")

    val actualResult = testSingleTableSqlQueryWithOutputType(sqlQuery, outputType)
    assertEquals(expected.sorted, actualResult.sorted)
  }


  @Test
  def testDateAndVarCharConversionInFilter(): Unit = {
    val sqlQuery =
      "SELECT field_date, field_varchar_equals_date FROM TestTable " +
        "WHERE field_date = field_varchar_equals_date"

    val outputType = InternalTypeInfo.ofFields(
      new DateType(),
      new VarCharType())

    val expected = List("+I(11323,2001-01-01)")

    val actualResult = testSingleTableSqlQueryWithOutputType(sqlQuery, outputType)
    assertEquals(expected.sorted, actualResult.sorted)
  }

  @Test
  def testTimeAndVarCharConversionInFilter(): Unit = {
    val sqlQuery =
      "SELECT field_time, field_varchar_equals_time FROM TestTable " +
        "WHERE field_time = field_varchar_equals_time"

    val outputType = InternalTypeInfo.ofFields(
      new TimeType(),
      new VarCharType())

    val expected = List("+I(0,00:00:00)")

    val actualResult = testSingleTableSqlQueryWithOutputType(sqlQuery, outputType)
    assertEquals(expected.sorted, actualResult.sorted)
  }

  @Test
  def testTimestampAndVarCharConversionInFilter(): Unit = {
    val sqlQuery =
      "SELECT field_timestamp, field_varchar_equals_timestamp FROM TestTable " +
        "WHERE field_timestamp = field_varchar_equals_timestamp"

    val outputType = InternalTypeInfo.ofFields(
      new TimestampType(),
      new VarCharType())

    val expected = List("+I(2001-01-01T00:00,2001-01-01 00:00:00)")

    val actualResult = testSingleTableSqlQueryWithOutputType(sqlQuery, outputType)
    assertEquals(expected.sorted, actualResult.sorted)
  }

  private def getFilterAndProjectionExceptionMessage(types: List[String]): String = {
    s"implicit type conversion between " +
      s"${types(0)}" +
      s" and " +
      s"${types(1)}" +
      s" is not supported now"
  }

  private def testSingleTableInvalidImplicitConversionTypes(
      sqlQuery: String,
      outputType: InternalTypeInfo[RowData],
      types: List[String]): Unit = {
    expectedException.expect(classOf[CodeGenException])
    expectedException.expectMessage(
      getFilterAndProjectionExceptionMessage(types))
    testSingleTableSqlQueryWithOutputType(sqlQuery, outputType)
  }

  @Test
  def testInvalidTinyIntAndVarCharConversionInFilter(): Unit = {
    val sqlQuery =
      "SELECT field_tinyint, field_varchar_equals_numeric FROM TestTable " +
        "WHERE field_tinyint = field_varchar_equals_numeric"

    val outputType = InternalTypeInfo.ofFields(
      new TinyIntType(),
      new VarCharType())

    testSingleTableInvalidImplicitConversionTypes(
      sqlQuery, outputType, List("TINYINT", "VARCHAR"))
  }

  @Test
  def testInvalidSmallIntAndVarCharConversionInFilter(): Unit = {
    val sqlQuery =
      "SELECT field_smallint, field_varchar_equals_numeric FROM TestTable " +
        "WHERE field_smallint = field_varchar_equals_numeric"

    val outputType = InternalTypeInfo.ofFields(
      new SmallIntType(),
      new VarCharType())

    testSingleTableInvalidImplicitConversionTypes(
      sqlQuery, outputType, List("SMALLINT", "VARCHAR"))
  }

  @Test
  def testInvalidIntAndVarCharConversionInFilter(): Unit = {
    val sqlQuery =
      "SELECT field_int, field_varchar_equals_numeric FROM TestTable " +
        "WHERE field_int = field_varchar_equals_numeric"

    val outputType = InternalTypeInfo.ofFields(
      new IntType(),
      new VarCharType())

    testSingleTableInvalidImplicitConversionTypes(
      sqlQuery, outputType, List("INTEGER", "VARCHAR"))
  }

  @Test
  def testInvalidBigIntAndVarCharConversionInFilter(): Unit = {
    val sqlQuery =
      "SELECT field_bigint, field_varchar_equals_numeric FROM TestTable " +
        "WHERE field_bigint = field_varchar_equals_numeric"

    val outputType = InternalTypeInfo.ofFields(
      new BigIntType(),
      new VarCharType())

    testSingleTableInvalidImplicitConversionTypes(
      sqlQuery, outputType, List("BIGINT", "VARCHAR"))
  }

  @Test
  def testInvalidDecimalAndVarCharConversionInFilter(): Unit = {
    val sqlQuery =
      "SELECT field_decimal, field_varchar_equals_numeric FROM TestTable " +
        "WHERE field_decimal = field_varchar_equals_numeric"

    val outputType = InternalTypeInfo.ofFields(
      new DecimalType(),
      new VarCharType())

    testSingleTableInvalidImplicitConversionTypes(
      sqlQuery, outputType, List("DECIMAL", "VARCHAR"))
  }

  @Test
  def testInvalidFloatAndVarCharConversionInFilter(): Unit = {
    val sqlQuery =
      "SELECT field_float, field_varchar_equals_numeric FROM TestTable " +
        "WHERE field_float = field_varchar_equals_numeric"

    val outputType = InternalTypeInfo.ofFields(
      new FloatType(),
      new VarCharType())

    testSingleTableInvalidImplicitConversionTypes(
      sqlQuery, outputType, List("FLOAT", "VARCHAR"))
  }

  @Test
  def testInvalidDoubleAndVarCharConversionInFilter(): Unit = {
    val sqlQuery =
      "SELECT field_double, field_varchar_equals_numeric FROM TestTable " +
        "WHERE field_double = field_varchar_equals_numeric"

    val outputType = InternalTypeInfo.ofFields(
      new DoubleType(),
      new VarCharType())

    testSingleTableInvalidImplicitConversionTypes(
      sqlQuery, outputType, List("DOUBLE", "VARCHAR"))
  }

  @Test
  def testFloatAndDoubleConversionInProjection(): Unit ={
    val sqlQuery =
      "SELECT field_float, field_double, field_float = field_double FROM TestTable"

    val outputType = InternalTypeInfo.ofFields(
      new FloatType(),
      new DoubleType(),
      new BooleanType())

    val expected = List("+I(1.0,1.0,true)")

    val actualResult = testSingleTableSqlQueryWithOutputType(sqlQuery, outputType)
    assertEquals(expected.sorted, actualResult.sorted)
  }


  @Test
  def testDateAndVarCharConversionInProjection(): Unit = {
    val sqlQuery =
      "SELECT field_date, field_varchar_equals_date, " +
        "field_date = field_varchar_equals_date FROM TestTable"

    val outputType = InternalTypeInfo.ofFields(
      new DateType(),
      new VarCharType(),
      new BooleanType())

    val expected = List("+I(11323,2001-01-01,true)")

    val actualResult = testSingleTableSqlQueryWithOutputType(sqlQuery, outputType)
    assertEquals(expected.sorted, actualResult.sorted)
  }

  @Test
  def testInvalidDecimalAndVarCharConversionInProjection(): Unit = {
    val sqlQuery =
      "SELECT field_decimal, field_varchar_equals_numeric, " +
        "field_decimal = field_varchar_equals_numeric FROM TestTable"
    val outputType = InternalTypeInfo.ofFields(
      new DecimalType(),
      new VarCharType(),
      new BooleanType())

    testSingleTableInvalidImplicitConversionTypes(
      sqlQuery, outputType, List("DECIMAL", "VARCHAR"))
  }

  def registerTableA(): Unit = {
    val rowDataA: GenericRowData = new GenericRowData(6)
    rowDataA.setField(0, 1)
    rowDataA.setField(1, 1)
    rowDataA.setField(2, 1)
    val date: Int = LocalDate.parse("2001-01-01").toEpochDay.toInt
    rowDataA.setField(3, date)
    val time: Int = (LocalTime.parse("00:00:00").toNanoOfDay / 1000000L).toInt
    rowDataA.setField(4, time)
    val timestamp = TimestampData.fromLocalDateTime(LocalDateTime.parse("2001-01-01T00:00:00"))
    rowDataA.setField(5, timestamp)
    val dataA = List(rowDataA)

    implicit val tpeA: TypeInformation[GenericRowData] =
      InternalTypeInfo.ofFields(
        new IntType(),
        new IntType(),
        new IntType(),
        new DateType(),
        new TimeType(),
        new TimestampType()).asInstanceOf[TypeInformation[GenericRowData]]

    val dsA = env.fromCollection(dataA)

    val tableA = dsA.toTable(tEnv, 'a1, 'a2, 'a3, 'a4, 'a5, 'a6)
    tEnv.registerTable("A", tableA)
  }

  def registerTableB(): Unit = {
    val rowDataB: GenericRowData = new GenericRowData(6)
    rowDataB.setField(0, 1)
    rowDataB.setField(1, 1.toLong)
    rowDataB.setField(2, StringData.fromString("1"))
    rowDataB.setField(3, StringData.fromString("2001-01-01"))
    rowDataB.setField(4, StringData.fromString("00:00:00"))
    rowDataB.setField(5, StringData.fromString("2001-01-01 00:00:00"))
    val dataB = List(rowDataB)

    implicit val tpeB: TypeInformation[GenericRowData] =
      InternalTypeInfo.ofFields(
        new IntType(),
        new BigIntType(),
        new VarCharType(),
        new VarCharType(),
        new VarCharType(),
        new VarCharType()).asInstanceOf[TypeInformation[GenericRowData]]

    val dsB = env.fromCollection(dataB)

    val tableB = dsB.toTable(tEnv, 'b1, 'b2, 'b3, 'b4, 'b5, 'b6)
    tEnv.registerTable("B", tableB)
  }

  def testTwoTableJoinSqlQuery(
      sqlQuery: String, outputType: InternalTypeInfo[RowData]): Unit = {
    registerTableA()
    registerTableB()

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[RowData]
    val sink = new TestingAppendRowDataSink(outputType)
    result.addSink(sink)
    env.execute()

    val actualResult = sink.getAppendResults
    val expected = List("+I(1,1)")
    assertEquals(expected.sorted, actualResult.sorted)
  }

  @Test
  def testIntAndBigIntConversionInJoinOn(): Unit = {
    val sqlQuery =
      "SELECT a1, b1 from A join B on a2 = b2"

    val outputType = InternalTypeInfo.ofFields(
      new IntType(),
      new IntType())

    testTwoTableJoinSqlQuery(sqlQuery, outputType)
  }

  private def testTwoTableInvalidImplicitConversionTypes(
      sqlQuery: String,
      outputType: InternalTypeInfo[RowData],
      types: List[String]): Unit = {
      expectedException.expect(classOf[TableException])
      expectedException.expectMessage(
        getJoinOnExceptionMessage(types))
    testTwoTableJoinSqlQuery(sqlQuery, outputType)
  }

  private def getJoinOnExceptionMessage(types: List[String]): String = {
    s"implicit type conversion between " +
      s"${types(0)}" +
      s" and " +
      s"${types(1)}" +
      s" is not supported on join's condition now"
  }

  @Test
  def testInvalidIntAndVarCharConversionInJoinOn(): Unit = {
    val sqlQuery =
      "SELECT a1, b1 from A join B on a3 = b3"

    val outputType = InternalTypeInfo.ofFields(
      new IntType(),
      new IntType())

    testTwoTableInvalidImplicitConversionTypes(
      sqlQuery, outputType, List("INTEGER", "VARCHAR(1)"))
  }

  @Test
  def testInvalidDateAndVarCharConversionInJoinOn(): Unit ={
    val sqlQuery =
      "SELECT a1, b1 from A join B on a4 = b4"

    val outputType = InternalTypeInfo.ofFields(
      new IntType(),
      new IntType())

    testTwoTableInvalidImplicitConversionTypes(
      sqlQuery, outputType, List("DATE", "VARCHAR(1)"))
  }

  @Test
  def testInvalidTimeAndVarCharConversionInJoinOn(): Unit ={
    val sqlQuery =
      "SELECT a1, b1 from A join B on a5 = b5"

    val outputType = InternalTypeInfo.ofFields(
      new IntType(),
      new IntType())

    testTwoTableInvalidImplicitConversionTypes(
      sqlQuery, outputType, List("TIME(0)", "VARCHAR(1)"))
  }

  @Test
  def testInvalidTimestampAndVarCharConversionInJoinOn(): Unit ={
    val sqlQuery =
      "SELECT a1, b1 from A join B on a6 = b6"

    val outputType = InternalTypeInfo.ofFields(
      new IntType(),
      new IntType())

    testTwoTableInvalidImplicitConversionTypes(
      sqlQuery, outputType, List("TIMESTAMP(6)", "VARCHAR(1)"))
  }
}
