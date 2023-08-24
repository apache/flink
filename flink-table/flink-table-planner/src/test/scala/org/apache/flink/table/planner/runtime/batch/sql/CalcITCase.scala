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
package org.apache.flink.table.planner.runtime.batch.sql

import org.apache.flink.api.common.typeinfo.BasicTypeInfo.{INT_TYPE_INFO, LONG_TYPE_INFO, STRING_TYPE_INFO}
import org.apache.flink.api.common.typeinfo.LocalTimeTypeInfo.{LOCAL_DATE, LOCAL_DATE_TIME, LOCAL_TIME}
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO
import org.apache.flink.api.common.typeinfo.SqlTimeTypeInfo.{DATE, TIME, TIMESTAMP}
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.api.common.typeinfo.Types.INSTANT
import org.apache.flink.api.java.typeutils._
import org.apache.flink.api.scala._
import org.apache.flink.table.api.{DataTypes, TableSchema, ValidationException}
import org.apache.flink.table.api.config.ExecutionConfigOptions
import org.apache.flink.table.api.config.ExecutionConfigOptions.LegacyCastBehaviour
import org.apache.flink.table.catalog.CatalogDatabaseImpl
import org.apache.flink.table.data.{DecimalDataUtils, TimestampData}
import org.apache.flink.table.data.util.DataFormatConverters.LocalDateConverter
import org.apache.flink.table.planner.expressions.utils.{RichFunc1, RichFunc2, RichFunc3, SplitUDF}
import org.apache.flink.table.planner.factories.TestValuesTableFactory
import org.apache.flink.table.planner.plan.rules.physical.batch.BatchPhysicalSortRule
import org.apache.flink.table.planner.runtime.utils.{BatchTableEnvUtil, BatchTestBase, TestData, UserDefinedFunctionTestUtils}
import org.apache.flink.table.planner.runtime.utils.BatchTableEnvUtil.parseFieldNames
import org.apache.flink.table.planner.runtime.utils.BatchTestBase.row
import org.apache.flink.table.planner.runtime.utils.TestData._
import org.apache.flink.table.planner.runtime.utils.UserDefinedFunctionTestUtils._
import org.apache.flink.table.planner.utils.{DateTimeTestUtil, TestLegacyFilterableTableSource}
import org.apache.flink.table.planner.utils.DateTimeTestUtil._
import org.apache.flink.table.utils.DateTimeUtils.toLocalDateTime
import org.apache.flink.types.Row

import org.assertj.core.api.Assertions.{assertThat, assertThatThrownBy}
import org.junit.jupiter.api.{BeforeEach, Disabled, Test}

import java.nio.charset.StandardCharsets
import java.sql.{Date, Time, Timestamp}
import java.time._
import java.util

class CalcITCase extends BatchTestBase {

  @BeforeEach
  override def before(): Unit = {
    super.before()
    registerCollection("Table3", data3, type3, "a, b, c", nullablesOfData3)
    registerCollection("NullTable3", nullData3, type3, "a, b, c", nullablesOfData3)
    registerCollection("SmallTable3", smallData3, type3, "a, b, c", nullablesOfData3)
    registerCollection("testTable", buildInData, buildInType, "a,b,c,d,e,f,g,h,i,j")
  }

  @Test
  def testSelectWithLegacyCastIntToDate(): Unit = {
    tEnv.getConfig.getConfiguration
      .set(ExecutionConfigOptions.TABLE_EXEC_LEGACY_CAST_BEHAVIOUR, LegacyCastBehaviour.ENABLED)
    checkResult(
      "SELECT CASE WHEN true THEN CAST(2 AS INT) ELSE CAST('2017-12-11' AS DATE) END",
      Seq(row("1970-01-03")))
  }

  @Test
  def testSelectStar(): Unit = {
    checkResult("SELECT * FROM Table3 where a is not null", data3)
  }

  @Test
  def testSimpleSelectAll(): Unit = {
    checkResult("SELECT a, b, c FROM Table3", data3)
  }

  @Test
  def testManySelectWithFilter(): Unit = {
    val data = Seq(
      (true, 1, 2, 3, 4, 5, 6, 7),
      (false, 1, 2, 3, 4, 5, 6, 7)
    )
    BatchTableEnvUtil.registerCollection(tEnv, "MyT", data, "a, b, c, d, e, f, g, h")
    checkResult(
      """
        |SELECT
        |  a, b, c, d, e, f, g, h,
        |  a, b, c, g, d, e, f, h,
        |  h, g, f, e, d, c, b, a,
        |  h, f, e, d, g, c, b, a,
        |  c, a, b, g, f, e, h, d,
        |  a, b, c, d, e, f, g, h,
        |  a, b, c, g, d, e, f, h,
        |  h, g, f, e, d, c, b, a,
        |  h, f, e, d, g, c, b, a,
        |  c, a, b, g, f, e, h, d
        |FROM MyT WHERE a
      """.stripMargin,
      Seq(
        row(
          true, 1, 2, 3, 4, 5, 6, 7, true, 1, 2, 6, 3, 4, 5, 7, 7, 6, 5, 4, 3, 2, 1, true, 7, 5, 4,
          3, 6, 2, 1, true, 2, true, 1, 6, 5, 4, 7, 3, true, 1, 2, 3, 4, 5, 6, 7, true, 1, 2, 6, 3,
          4, 5, 7, 7, 6, 5, 4, 3, 2, 1, true, 7, 5, 4, 3, 6, 2, 1, true, 2, true, 1, 6, 5, 4, 7, 3
        ))
    )
  }

  @Test
  def testManySelect(): Unit = {
    registerCollection(
      "ProjectionTestTable",
      projectionTestData,
      projectionTestDataType,
      "a, b, c, d, e, f, g, h",
      nullablesOfProjectionTestData)
    checkResult(
      """
        |SELECT
        |  a, b, c, d, e, f, g, h,
        |  a, b, c, g, d, e, f, h,
        |  h, g, f, e, d, c, b, a,
        |  h, f, e, d, g, c, b, a,
        |  c, a, b, g, f, e, h, d,
        |  a, b, c, d, e, f, g, h,
        |  a, b, c, g, d, e, f, h,
        |  h, g, f, e, d, c, b, a,
        |  h, f, e, d, g, c, b, a,
        |  c, a, b, g, f, e, h, d
        |FROM ProjectionTestTable
      """.stripMargin,
      Seq(
        row(
          1,
          10,
          100,
          "1",
          "10",
          "100",
          1000,
          "1000",
          1,
          10,
          100,
          1000,
          "1",
          "10",
          "100",
          "1000",
          "1000",
          1000,
          "100",
          "10",
          "1",
          100,
          10,
          1,
          "1000",
          "100",
          "10",
          "1",
          1000,
          100,
          10,
          1,
          100,
          1,
          10,
          1000,
          "100",
          "10",
          "1000",
          "1",
          1,
          10,
          100,
          "1",
          "10",
          "100",
          1000,
          "1000",
          1,
          10,
          100,
          1000,
          "1",
          "10",
          "100",
          "1000",
          "1000",
          1000,
          "100",
          "10",
          "1",
          100,
          10,
          1,
          "1000",
          "100",
          "10",
          "1",
          1000,
          100,
          10,
          1,
          100,
          1,
          10,
          1000,
          "100",
          "10",
          "1000",
          "1"
        ),
        row(
          2,
          20,
          200,
          "2",
          "20",
          "200",
          2000,
          "2000",
          2,
          20,
          200,
          2000,
          "2",
          "20",
          "200",
          "2000",
          "2000",
          2000,
          "200",
          "20",
          "2",
          200,
          20,
          2,
          "2000",
          "200",
          "20",
          "2",
          2000,
          200,
          20,
          2,
          200,
          2,
          20,
          2000,
          "200",
          "20",
          "2000",
          "2",
          2,
          20,
          200,
          "2",
          "20",
          "200",
          2000,
          "2000",
          2,
          20,
          200,
          2000,
          "2",
          "20",
          "200",
          "2000",
          "2000",
          2000,
          "200",
          "20",
          "2",
          200,
          20,
          2,
          "2000",
          "200",
          "20",
          "2",
          2000,
          200,
          20,
          2,
          200,
          2,
          20,
          2000,
          "200",
          "20",
          "2000",
          "2"
        ),
        row(
          3,
          30,
          300,
          "3",
          "30",
          "300",
          3000,
          "3000",
          3,
          30,
          300,
          3000,
          "3",
          "30",
          "300",
          "3000",
          "3000",
          3000,
          "300",
          "30",
          "3",
          300,
          30,
          3,
          "3000",
          "300",
          "30",
          "3",
          3000,
          300,
          30,
          3,
          300,
          3,
          30,
          3000,
          "300",
          "30",
          "3000",
          "3",
          3,
          30,
          300,
          "3",
          "30",
          "300",
          3000,
          "3000",
          3,
          30,
          300,
          3000,
          "3",
          "30",
          "300",
          "3000",
          "3000",
          3000,
          "300",
          "30",
          "3",
          300,
          30,
          3,
          "3000",
          "300",
          "30",
          "3",
          3000,
          300,
          30,
          3,
          300,
          3,
          30,
          3000,
          "300",
          "30",
          "3000",
          "3"
        )
      )
    )
  }

  @Test
  def testSelectWithNaming(): Unit = {
    checkResult("SELECT `1-_./Ü`, b, c FROM (SELECT a as `1-_./Ü`, b, c FROM Table3)", data3)
  }

  @Test
  def testInvalidFields(): Unit = {
    assertThatThrownBy(() => checkResult("SELECT a, foo FROM Table3", data3))
      .isInstanceOf(classOf[ValidationException])
  }

  @Test
  def testAllRejectingFilter(): Unit = {
    checkResult("SELECT * FROM Table3 WHERE false", Seq())
  }

  @Test
  def testAllPassingFilter(): Unit = {
    checkResult("SELECT * FROM Table3 WHERE true", data3)
  }

  @Test
  def testFilterOnString(): Unit = {
    checkResult(
      "SELECT * FROM Table3 WHERE c LIKE '%world%'",
      Seq(
        row(3, 2L, "Hello world"),
        row(4, 3L, "Hello world, how are you?")
      ))

    val rows = Seq(row(3, "H.llo"), row(3, "Hello"))
    val dataId = TestValuesTableFactory.registerData(rows)

    val ddl =
      s"""
         |CREATE TABLE MyTable (
         |  a int,
         |  c string
         |) WITH (
         |  'connector' = 'values',
         |  'data-id' = '$dataId',
         |  'bounded' = 'true'
         |)
         |""".stripMargin
    tEnv.executeSql(ddl)

    checkResult(
      s"""
         |SELECT c FROM MyTable
         |  WHERE c LIKE 'H.llo'
         |""".stripMargin,
      Seq(row("H.llo"))
    )
    checkResult(
      s"""
         |SELECT c FROM MyTable
         |  WHERE c SIMILAR TO 'H.llo'
         |""".stripMargin,
      Seq(row("H.llo"), row("Hello"))
    )
    checkEmptyResult(s"""
                        |SELECT c FROM MyTable
                        |  WHERE c NOT SIMILAR TO 'H.llo'
                        |""".stripMargin)
  }

  @Test
  def testFilterOnInteger(): Unit = {
    checkResult(
      "SELECT * FROM Table3 WHERE MOD(a,2)=0",
      Seq(
        row(2, 2L, "Hello"),
        row(4, 3L, "Hello world, how are you?"),
        row(6, 3L, "Luke Skywalker"),
        row(8, 4L, "Comment#2"),
        row(10, 4L, "Comment#4"),
        row(12, 5L, "Comment#6"),
        row(14, 5L, "Comment#8"),
        row(16, 6L, "Comment#10"),
        row(18, 6L, "Comment#12"),
        row(20, 6L, "Comment#14")
      )
    )
  }

  @Test
  def testDisjunctivePredicate(): Unit = {
    checkResult(
      "SELECT * FROM Table3 WHERE a < 2 OR a > 20",
      Seq(
        row(1, 1L, "Hi"),
        row(21, 6L, "Comment#15")
      ))
  }

  @Test
  def testFilterWithAnd(): Unit = {
    checkResult(
      "SELECT * FROM Table3 WHERE MOD(a,2)<>0 AND MOD(b,2)=0",
      Seq(
        row(3, 2L, "Hello world"),
        row(7, 4L, "Comment#1"),
        row(9, 4L, "Comment#3"),
        row(17, 6L, "Comment#11"),
        row(19, 6L, "Comment#13"),
        row(21, 6L, "Comment#15")
      )
    )
  }

  @Test
  def testAdvancedDataTypes(): Unit = {
    val data = Seq(
      row(localDate("1984-07-12"), localTime("14:34:24"), localDateTime("1984-07-12 14:34:24")))
    registerCollection(
      "MyTable",
      data,
      new RowTypeInfo(LOCAL_DATE, LOCAL_TIME, LOCAL_DATE_TIME),
      "a, b, c")

    checkResult(
      "SELECT a, b, c, DATE '1984-07-12', TIME '14:34:24', " +
        "TIMESTAMP '1984-07-12 14:34:24' FROM MyTable",
      Seq(
        row(
          localDate("1984-07-12"),
          localTime("14:34:24"),
          localDateTime("1984-07-12 14:34:24"),
          localDate("1984-07-12"),
          localTime("14:34:24"),
          localDateTime("1984-07-12 14:34:24")
        ))
    )

    checkResult(
      "SELECT a, b, c, DATE '1984-07-12', TIME '14:34:24', " +
        "TIMESTAMP '1984-07-12 14:34:24' FROM MyTable " +
        "WHERE a = '1984-07-12' and b = '14:34:24' and c = '1984-07-12 14:34:24'",
      Seq(
        row(
          localDate("1984-07-12"),
          localTime("14:34:24"),
          localDateTime("1984-07-12 14:34:24"),
          localDate("1984-07-12"),
          localTime("14:34:24"),
          localDateTime("1984-07-12 14:34:24")
        ))
    )

    checkResult(
      "SELECT a, b, c, DATE '1984-07-12', TIME '14:34:24', " +
        "TIMESTAMP '1984-07-12 14:34:24' FROM MyTable " +
        "WHERE '1984-07-12' = a and '14:34:24' = b and '1984-07-12 14:34:24' = c",
      Seq(
        row(
          localDate("1984-07-12"),
          localTime("14:34:24"),
          localDateTime("1984-07-12 14:34:24"),
          localDate("1984-07-12"),
          localTime("14:34:24"),
          localDateTime("1984-07-12 14:34:24")
        ))
    )
  }

  @Test
  def testUserDefinedScalarFunction(): Unit = {
    registerFunction("hashCode", MyHashCode)
    val data = Seq(row("a"), row("b"), row("c"))
    registerCollection("MyTable", data, new RowTypeInfo(STRING_TYPE_INFO), "text")

    checkResult(
      "SELECT hashCode(text), hashCode('22') FROM MyTable",
      Seq(row(97, 1600), row(98, 1600), row(99, 1600)))
  }

  @Test
  def testDecimalReturnType(): Unit = {
    registerFunction("myNegative", MyNegative)
    checkResult(
      "SELECT myNegative(5.1)",
      Seq(row(new java.math.BigDecimal("-5.100000000000000000"))))
  }

  @Test
  def testUDFWithInternalClass(): Unit = {
    registerFunction("func", BinaryStringFunction)
    val data = Seq(row("a"), row("b"), row("c"))
    registerCollection("MyTable", data, new RowTypeInfo(STRING_TYPE_INFO), "text")

    checkResult("SELECT func(text) FROM MyTable", Seq(row("a"), row("b"), row("c")))
  }

  @Test
  def testTimestampSemantics(): Unit = {
    // If the timestamp literal '1969-07-20 16:17:39' is inserted in Washington D.C.
    // and then queried from Paris, it might be shown in the following ways based
    // on timestamp semantics:
    // TODO: Add ZonedDateTime/OffsetDateTime
    val new_york = ZoneId.of("America/New_York")
    val ldt = localDateTime("1969-07-20 16:17:39")
    val data = Seq(
      row(
        ldt,
        ldt.toInstant(new_york.getRules.getOffset(ldt))
      ))
    registerCollection("T", data, new RowTypeInfo(LOCAL_DATE_TIME, INSTANT), "a, b")

    val pairs = ZoneId.of("Europe/Paris")
    tEnv.getConfig.setLocalTimeZone(pairs)
    checkResult(
      "SELECT CAST(a AS VARCHAR), b, CAST(b AS VARCHAR) FROM T",
      Seq(row("1969-07-20 16:17:39.000", "1969-07-20T20:17:39Z", "1969-07-20 21:17:39.000"))
    )
  }

  @Test
  def testTimeUDF(): Unit = {
    val data = Seq(
      row(
        localDate("1984-07-12"),
        Date.valueOf("1984-07-12"),
        DateTimeTestUtil.localTime("08:03:09"),
        Time.valueOf("08:03:09"),
        localDateTime("2019-09-19 08:03:09"),
        Timestamp.valueOf("2019-09-19 08:03:09"),
        Timestamp.valueOf("2019-09-19 08:03:09").toInstant
      ))
    registerCollection(
      "MyTable",
      data,
      new RowTypeInfo(LOCAL_DATE, DATE, LOCAL_TIME, TIME, LOCAL_DATE_TIME, TIMESTAMP, INSTANT),
      "a, b, c, d, e, f, g")

    tEnv.registerFunction("dateFunc", DateFunction)
    tEnv.registerFunction("localDateFunc", LocalDateFunction)
    tEnv.registerFunction("timeFunc", TimeFunction)
    tEnv.registerFunction("localTimeFunc", LocalTimeFunction)
    tEnv.registerFunction("timestampFunc", TimestampFunction)
    tEnv.registerFunction("datetimeFunc", DateTimeFunction)
    tEnv.registerFunction("instantFunc", InstantFunction)

    val v1 = "1984-07-12"
    val v2 = "08:03:09"
    val v3 = "2019-09-19 08:03:09.0"
    val v4 = "2019-09-19T08:03:09"
    checkResult(
      "SELECT" +
        " dateFunc(a), localDateFunc(a), dateFunc(b), localDateFunc(b)," +
        " timeFunc(c), localTimeFunc(c), timeFunc(d), localTimeFunc(d)," +
        " timestampFunc(e), datetimeFunc(e), timestampFunc(f), datetimeFunc(f)," +
        " CAST(instantFunc(g) AS TIMESTAMP), instantFunc(g)" +
        " FROM MyTable",
      Seq(
        row(
          v1,
          v1,
          v1,
          v1,
          v2,
          v2,
          v2,
          v2,
          v3,
          v4,
          v3,
          v4,
          localDateTime("2019-09-19 08:03:09"),
          Timestamp.valueOf("2019-09-19 08:03:09").toInstant))
    )
  }

  @Test
  def testTimeUDFParametersImplicitCast(): Unit = {
    val data: Seq[Row] = Seq(
      row(
        localDateTime("2019-09-19 08:03:09.123"),
        Timestamp.valueOf("2019-09-19 08:03:09").toInstant,
        Timestamp.valueOf("2019-09-19 08:03:09.123").toInstant,
        Timestamp.valueOf("2019-09-19 08:03:09.123456").toInstant,
        Timestamp.valueOf("2019-09-19 08:03:09.123456789").toInstant,
        Timestamp.valueOf("2019-09-19 08:03:09.123").toInstant
      ))
    val dataId = TestValuesTableFactory.registerData(data)

    val ddl =
      s"""
         |CREATE TABLE MyTable (
         |  ntz TIMESTAMP(3),
         |  ltz0 TIMESTAMP_LTZ(0),
         |  ltz3 TIMESTAMP_LTZ(3),
         |  ltz6 TIMESTAMP_LTZ(6),
         |  ltz9 TIMESTAMP_LTZ(9),
         |  ltz_not_null TIMESTAMP_LTZ(3) NOT NULL
         |) WITH (
         |  'connector' = 'values',
         |  'data-id' = '$dataId',
         |  'bounded' = 'true'
         |)
         |""".stripMargin

    tEnv.executeSql(ddl)
    tEnv.createTemporaryFunction("timestampFunc", TimestampFunction)
    tEnv.createTemporaryFunction("datetimeFunc", DateTimeFunction)
    tEnv.createTemporaryFunction("instantFunc", InstantFunction)

    checkResult(
      "SELECT" +
        " timestampFunc(ntz), datetimeFunc(ntz), instantFunc(ntz)," +
        " timestampFunc(ltz0), datetimeFunc(ltz0), instantFunc(ltz0)," +
        " timestampFunc(ltz3), datetimeFunc(ltz3), instantFunc(ltz3)," +
        " timestampFunc(ltz6), datetimeFunc(ltz6), instantFunc(ltz6)," +
        " timestampFunc(ltz9), datetimeFunc(ltz9), instantFunc(ltz9)," +
        " timestampFunc(ltz_not_null), datetimeFunc(ltz_not_null), instantFunc(ltz_not_null)" +
        " FROM MyTable",
      Seq(
        row(
          // ntz
          "2019-09-19 08:03:09.123",
          "2019-09-19T08:03:09.123",
          Timestamp.valueOf("2019-09-19 08:03:09.123").toInstant,
          // ltz0
          "2019-09-19 08:03:09.0",
          "2019-09-19T08:03:09",
          Timestamp.valueOf("2019-09-19 08:03:09").toInstant,
          // ltz3
          "2019-09-19 08:03:09.123",
          "2019-09-19T08:03:09.123",
          Timestamp.valueOf("2019-09-19 08:03:09.123").toInstant,
          // ltz6
          "2019-09-19 08:03:09.123456",
          "2019-09-19T08:03:09.123456",
          Timestamp.valueOf("2019-09-19 08:03:09.123456").toInstant,
          // ltz6
          "2019-09-19 08:03:09.123456789",
          "2019-09-19T08:03:09.123456789",
          Timestamp.valueOf("2019-09-19 08:03:09.123456789").toInstant,
          // ltz_not_null
          "2019-09-19 08:03:09.123",
          "2019-09-19T08:03:09.123",
          Timestamp.valueOf("2019-09-19 08:03:09.123").toInstant
        ))
    )
  }

  @Test
  def testBinary(): Unit = {
    val data = Seq(row(1, 2, "hehe".getBytes(StandardCharsets.UTF_8)))
    registerCollection(
      "MyTable",
      data,
      new RowTypeInfo(INT_TYPE_INFO, INT_TYPE_INFO, BYTE_PRIMITIVE_ARRAY_TYPE_INFO),
      "a, b, c")

    checkResult("SELECT a, b, c FROM MyTable", data)
  }

  @Test
  def testUserDefinedScalarFunctionWithParameter(): Unit = {
    registerFunction("RichFunc2", new RichFunc2)
    UserDefinedFunctionTestUtils.setJobParameters(env, Map("string.value" -> "ABC"))

    checkResult(
      "SELECT c FROM SmallTable3 where RichFunc2(c)='ABC#Hello'",
      Seq(row("Hello"))
    )
  }

  @Test
  def testUserDefinedScalarFunctionWithDistributedCache(): Unit = {
    val words = "Hello\nWord"
    val filePath = UserDefinedFunctionTestUtils.writeCacheFile("test_words", words)
    env.registerCachedFile(filePath, "words")
    registerFunction("RichFunc3", new RichFunc3)

    checkResult(
      "SELECT c FROM SmallTable3 where RichFunc3(c)=true",
      Seq(row("Hello"))
    )
  }

  @Test
  def testMultipleUserDefinedScalarFunctions(): Unit = {
    registerFunction("RichFunc1", new RichFunc1)
    registerFunction("RichFunc2", new RichFunc2)
    UserDefinedFunctionTestUtils.setJobParameters(env, Map("string.value" -> "Abc"))

    checkResult(
      "SELECT c FROM SmallTable3 where RichFunc2(c)='Abc#Hello' or RichFunc1(a)=3 and b=2",
      Seq(row("Hello"), row("Hello world"))
    )
  }

  @Test
  def testExternalTypeFunc1(): Unit = {
    registerFunction("func1", RowFunc)
    registerFunction("rowToStr", RowToStrFunc)
    registerFunction("func2", ListFunc)
    registerFunction("func3", StringFunc)
    val data = Seq(row("a"), row("b"), row("c"))
    registerCollection("MyTable", data, new RowTypeInfo(STRING_TYPE_INFO), "text")

    checkResult(
      "SELECT rowToStr(func1(text)), func2(text), func3(text) FROM MyTable",
      Seq(
        row("a", util.Arrays.asList("a"), "a"),
        row("b", util.Arrays.asList("b"), "b"),
        row("c", util.Arrays.asList("c"), "c")
      )
    )
  }

  @Test
  def testExternalTypeFunc2(): Unit = {
    registerFunction("func1", RowFunc)
    registerFunction("rowToStr", RowToStrFunc)
    registerFunction("func2", ListFunc)
    registerFunction("func3", StringFunc)
    val data = Seq(row("a"), row("b"), row("c"))
    registerCollection("MyTable", data, new RowTypeInfo(STRING_TYPE_INFO), "text")

    // go to shuffler to serializer
    checkResult(
      "SELECT text, count(*), rowToStr(func1(text)), func2(text), func3(text) " +
        "FROM MyTable group by text",
      Seq(
        row("a", 1, "a", util.Arrays.asList("a"), "a"),
        row("b", 1, "b", util.Arrays.asList("b"), "b"),
        row("c", 1, "c", util.Arrays.asList("c"), "c")
      )
    )
  }

  @Test
  def testPojoField(): Unit = {
    val data = Seq(row(new MyPojo(5, 105)), row(new MyPojo(6, 11)), row(new MyPojo(7, 12)))
    registerCollection(
      "MyTable",
      data,
      new RowTypeInfo(TypeExtractor.createTypeInfo(classOf[MyPojo])),
      "a")

    checkResult(
      "SELECT a FROM MyTable",
      Seq(
        row(row(5, 105)),
        row(row(6, 11)),
        row(row(7, 12))
      ))
  }

  @Test
  def testPojoFieldUDF(): Unit = {
    val data = Seq(row(new MyPojo(5, 105)), row(new MyPojo(6, 11)), row(new MyPojo(7, 12)))
    registerCollection(
      "MyTable",
      data,
      new RowTypeInfo(TypeExtractor.createTypeInfo(classOf[MyPojo])),
      "a")

    // 1. external type for udf parameter
    registerFunction("pojoFunc", MyPojoFunc)
    registerFunction("toPojoFunc", MyToPojoFunc)
    checkResult("SELECT pojoFunc(a) FROM MyTable", Seq(row(105), row(11), row(12)))

    // 2. external type return in udf
    checkResult(
      "SELECT toPojoFunc(pojoFunc(a)) FROM MyTable",
      Seq(row(row(11, 11)), row(row(12, 12)), row(row(105, 105))))
  }

  // TODO
//  @Test
//  def testUDFWithGetResultTypeFromLiteral(): Unit = {
//    registerFunction("hashCode0", LiteralHashCode)
//    registerFunction("hashCode1", LiteralHashCode)
//    val data = Seq(row("a"), row("b"), row("c"))
//    tEnv.registerCollection("MyTable", data, new RowTypeInfo(STRING_TYPE_INFO), "text")
//    checkResult(
//      "SELECT hashCode0(text, 'int') FROM MyTable",
//      Seq(row(97), row(98), row(99)
//      ))
//
//    checkResult(
//      "SELECT hashCode1(text, 'string') FROM MyTable",
//      Seq(row("str97"), row("str98"), row("str99")
//      ))
//  }

  @Test
  def testInNonConstantValue(): Unit = {
    checkResult(
      "SELECT a FROM Table3 WHERE a IN (CAST(b AS INT), 21)",
      Seq(row(1), row(2), row(21)))
  }

  @Test
  def testInSmallValues(): Unit = {
    checkResult("SELECT a FROM Table3 WHERE a in (1, 2)", Seq(row(1), row(2)))

    checkResult("SELECT a FROM Table3 WHERE a in (1, 2, NULL)", Seq(row(1), row(2)))

    checkResult("SELECT a FROM Table3 WHERE a in (1, 2) and b = 2", Seq(row(2)))
  }

  @Test
  def testInLargeValues(): Unit = {
    checkResult(
      "SELECT a FROM Table3 WHERE a in (1, 2, 3, 4, 5)",
      Seq(row(1), row(2), row(3), row(4), row(5)))

    checkResult("SELECT a FROM Table3 WHERE a in (1, 2, 3, 4, 5) and b = 2", Seq(row(2), row(3)))

    checkResult("SELECT c FROM Table3 WHERE c in ('Hi', 'H2', 'H3', 'H4', 'H5')", Seq(row("Hi")))
  }

  @Test
  def testComplexInLargeValues(): Unit = {
    checkResult(
      "SELECT c FROM Table3 WHERE substring(c, 0, 2) in ('Hi', 'H2', 'H3', 'H4', 'H5')",
      Seq(row("Hi")))

    checkResult(
      "SELECT c FROM Table3 WHERE a = 1 and " +
        "(b = 1 or (c = 'Hello' and substring(c, 0, 2) in ('Hi', 'H2', 'H3', 'H4', 'H5')))",
      Seq(row("Hi")))

    checkResult(
      "SELECT c FROM Table3 WHERE a = 1 and " +
        "(b = 1 or (c = 'Hello' and (" +
        "substring(c, 0, 2) = 'Hi' or substring(c, 0, 2) = 'H2' or " +
        "substring(c, 0, 2) = 'H3' or substring(c, 0, 2) = 'H4' or " +
        "substring(c, 0, 2) = 'H5')))",
      Seq(row("Hi"))
    )
  }

  @Test
  def testNotInLargeValues(): Unit = {
    checkResult("SELECT a FROM SmallTable3 WHERE a not in (2, 3, 4, 5)", Seq(row(1)))

    checkResult(
      "SELECT a FROM SmallTable3 WHERE a not in (2, 3, 4, 5) or b = 2",
      Seq(row(1), row(2), row(3)))

    checkResult(
      "SELECT c FROM SmallTable3 WHERE c not in ('Hi', 'H2', 'H3', 'H4')",
      Seq(row("Hello"), row("Hello world")))
  }

  @Test
  def testComplexNotInLargeValues(): Unit = {
    checkResult(
      "SELECT c FROM SmallTable3 WHERE substring(c, 0, 2) not in ('Hi', 'H2', 'H3', 'H4', 'H5')",
      Seq(row("Hello"), row("Hello world")))

    checkResult(
      "SELECT c FROM SmallTable3 WHERE a = 1 or " +
        "(b = 1 and (c = 'Hello' or substring(c, 0, 2) not in ('Hi', 'H2', 'H3', 'H4', 'H5')))",
      Seq(row("Hi")))

    checkResult(
      "SELECT c FROM SmallTable3 WHERE a = 1 or " +
        "(b = 1 and (c = 'Hello' or (" +
        "substring(c, 0, 2) <> 'Hi' and substring(c, 0, 2) <> 'H2' and " +
        "substring(c, 0, 2) <> 'H3' and substring(c, 0, 2) <> 'H4' and " +
        "substring(c, 0, 2) <> 'H5')))",
      Seq(row("Hi"))
    )
  }

  @Test
  def testRowType(): Unit = {
    // literals
    checkResult(
      "SELECT ROW(1, 'Hi', true) FROM SmallTable3",
      Seq(
        row(row(1, "Hi", true)),
        row(row(1, "Hi", true)),
        row(row(1, "Hi", true))
      )
    )

    // primitive type
    checkResult(
      "SELECT ROW(1, a, b) FROM SmallTable3",
      Seq(
        row(row(1, 1, 1L)),
        row(row(1, 2, 2L)),
        row(row(1, 3, 2L))
      )
    )
  }

  @Test
  def testRowTypeWithDecimal(): Unit = {
    val d = DecimalDataUtils.castFrom(2.0002, 5, 4).toBigDecimal
    checkResult(
      "SELECT ROW(CAST(2.0002 AS DECIMAL(5, 4)), a, c) FROM SmallTable3",
      Seq(
        row(row(d, 1, "Hi")),
        row(row(d, 2, "Hello")),
        row(row(d, 3, "Hello world"))
      )
    )
  }

  @Test
  def testArrayType(): Unit = {
    // literals
    checkResult(
      "SELECT ARRAY['Hi', 'Hello', 'How are you'] FROM SmallTable3",
      Seq(
        row("[Hi, Hello, How are you]"),
        row("[Hi, Hello, How are you]"),
        row("[Hi, Hello, How are you]")
      )
    )

    // primitive type
    checkResult(
      "SELECT ARRAY[b, 30, 10, a] FROM SmallTable3",
      Seq(
        row("[1, 30, 10, 1]"),
        row("[2, 30, 10, 2]"),
        row("[2, 30, 10, 3]")
      )
    )

    // non-primitive type
    checkResult(
      "SELECT ARRAY['Test', c] FROM SmallTable3",
      Seq(
        row("[Test, Hi]"),
        row("[Test, Hello]"),
        row("[Test, Hello world]")
      )
    )
  }

  @Test
  def testMapType(): Unit = {
    // literals
    checkResult(
      "SELECT MAP[1, 'Hello', 2, 'Hi'] FROM SmallTable3",
      Seq(
        row("{1=Hello, 2=Hi}"),
        row("{1=Hello, 2=Hi}"),
        row("{1=Hello, 2=Hi}")
      )
    )

    // primitive type
    checkResult(
      "SELECT MAP[b, 30, 10, a] FROM SmallTable3",
      Seq(
        row("{1=30, 10=1}"),
        row("{2=30, 10=2}"),
        row("{2=30, 10=3}")
      )
    )

    // non-primitive type
    checkResult(
      "SELECT MAP[a, c] FROM SmallTable3",
      Seq(
        row("{1=Hi}"),
        row("{2=Hello}"),
        row("{3=Hello world}")
      )
    )
  }

  @Test
  def testMapTypeGroupBy(): Unit = {
    assertThatThrownBy(
      () =>
        checkResult("SELECT COUNT(*) FROM SmallTable3 GROUP BY MAP[1, 'Hello', 2, 'Hi']", Seq()))
      .hasMessage(
        "Type(MAP<INT NOT NULL, VARCHAR(5) NOT NULL> NOT NULL) is not an orderable data type, it is not supported as a ORDER_BY/GROUP_BY/JOIN_EQUAL field.")
  }

  @Test
  def testValueConstructor(): Unit = {
    val data = Seq(row("foo", 12, localDateTime("1984-07-12 14:34:24.001")))
    BatchTableEnvUtil.registerCollection(
      tEnv,
      "MyTable",
      data,
      new RowTypeInfo(Types.STRING, Types.INT, Types.LOCAL_DATE_TIME),
      Some(parseFieldNames("a, b, c")),
      None,
      None)

    val table = parseQuery(
      "SELECT ROW(a, b, c), ARRAY[12, b], MAP[a, c] FROM MyTable " +
        "WHERE (a, b, c) = ('foo', 12, TIMESTAMP '1984-07-12 14:34:24.001')")
    val result = executeQuery(table)

    val nestedRow = result.head.getField(0).asInstanceOf[Row]
    assertThat(data.head.getField(0)).isEqualTo(nestedRow.getField(0))
    assertThat(data.head.getField(1)).isEqualTo(nestedRow.getField(1))
    assertThat(data.head.getField(2)).isEqualTo(nestedRow.getField(2))

    val arr = result.head.getField(1).asInstanceOf[Array[Integer]]
    assertThat(12).isEqualTo(arr(0))
    assertThat(data.head.getField(1)).isEqualTo(arr(1))

    val hashMap = result.head.getField(2).asInstanceOf[util.HashMap[String, Timestamp]]
    assertThat(data.head.getField(2))
      .isEqualTo(hashMap.get(data.head.getField(0).asInstanceOf[String]))
  }

  @Test
  def testSelectStarFromNestedTable(): Unit = {

    val table = BatchTableEnvUtil.fromCollection(
      tEnv,
      Seq(
        ((0, 0), "0"),
        ((1, 1), "1"),
        ((2, 2), "2")
      ))
    tEnv.createTemporaryView("MyTable", table)

    checkResult(
      "SELECT * FROM MyTable",
      Seq(
        row(row(0, 0), "0"),
        row(row(1, 1), "1"),
        row(row(2, 2), "2")
      )
    )
  }

  @Test
  def testSelectStarFromNestedValues(): Unit = {
    val table = BatchTableEnvUtil.fromCollection(
      tEnv,
      Seq(
        (0L, "0"),
        (1L, "1"),
        (2L, "2")
      ),
      "a, b")
    tEnv.createTemporaryView("MyTable", table)

    checkResult(
      "select * from (select MAP[a,b], a from MyTable)",
      Seq(
        row("{0=0}", 0),
        row("{1=1}", 1),
        row("{2=2}", 2)
      )
    )

    checkResult(
      "select * from (select ROW(a, a), b from MyTable)",
      Seq(
        row(row(0, 0), "0"),
        row(row(1, 1), "1"),
        row(row(2, 2), "2")
      )
    )
  }

  @Test
  def testSelectStarFromNestedValues2(): Unit = {
    val table = BatchTableEnvUtil.fromCollection(
      tEnv,
      Seq(
        (0L, "0"),
        (1L, "1"),
        (2L, "2")
      ),
      "a, b")
    tEnv.createTemporaryView("MyTable", table)
    checkResult(
      "select * from (select ARRAY[a,cast(b as BIGINT)], a from MyTable)",
      Seq(
        row("[0, 0]", 0),
        row("[1, 1]", 1),
        row("[2, 2]", 2)
      )
    )
  }

  @Disabled // TODO support Unicode
  @Test
  def testFunctionWithUnicodeParameters(): Unit = {
    val data = List(
      ("a\u0001b", "c\"d", "e\"\u0004f"), // uses Java/Scala escaping
      ("x\u0001y", "y\"z", "z\"\u0004z")
    )

    val splitUDF0 = new SplitUDF(deterministic = true)
    val splitUDF1 = new SplitUDF(deterministic = false)

    registerFunction("splitUDF0", splitUDF0)
    registerFunction("splitUDF1", splitUDF1)

    val t1 = BatchTableEnvUtil.fromCollection(tEnv, data, "a, b, c")
    tEnv.createTemporaryView("T1", t1)
    // uses SQL escaping (be aware that even Scala multi-line strings parse backslash!)
    checkResult(
      s"""
         |SELECT
         |  splitUDF0(a, U&'${'\\'}0001', 0) AS a0,
         |  splitUDF1(a, U&'${'\\'}0001', 0) AS a1,
         |  splitUDF0(b, U&'"', 1) AS b0,
         |  splitUDF1(b, U&'"', 1) AS b1,
         |  splitUDF0(c, U&'${'\\'}${'\\'}"${'\\'}0004', 0) AS c0,
         |  splitUDF1(c, U&'${'\\'}"#0004' UESCAPE '#', 0) AS c1
         |FROM T1
         |""".stripMargin,
      Seq(row("a", "a", "d", "d", "e", "e"), row("x", "x", "z", "z", "z", "z"))
    )
  }

  @Test
  def testCastInWhere(): Unit = {
    checkResult(
      "SELECT CAST(a AS VARCHAR(10)) FROM Table3 WHERE CAST(a AS VARCHAR(10)) = '1'",
      Seq(row(1)))
  }

  @Test
  def testLike(): Unit = {
    checkResult("SELECT a FROM NullTable3 WHERE c LIKE '%llo%'", Seq(row(2), row(3), row(4)))

    checkResult(
      "SELECT a FROM NullTable3 WHERE CAST(a as VARCHAR(10)) LIKE CAST(b as VARCHAR(10))",
      Seq(row(1), row(2)))

    checkResult(
      "SELECT a FROM NullTable3 WHERE c NOT LIKE '%Comment%' AND c NOT LIKE '%Hello%'",
      Seq(row(1), row(5), row(6), row(null), row(null)))

    checkResult(
      "SELECT a FROM NullTable3 WHERE c LIKE 'Comment#%' and c LIKE '%2'",
      Seq(row(8), row(18)))

    checkResult("SELECT a FROM NullTable3 WHERE c LIKE 'Comment#12'", Seq(row(18)))

    checkResult("SELECT a FROM NullTable3 WHERE c LIKE '%omm%nt#12'", Seq(row(18)))
  }

  @Test
  def testLikeWithEscape(): Unit = {

    val rows = Seq(
      (1, "ha_ha"),
      (2, "ffhaha_hahaff"),
      (3, "aaffhaha_hahaffaa"),
      (4, "aaffhaaa_aahaffaa"),
      (5, "a%_ha")
    )

    BatchTableEnvUtil.registerCollection(tEnv, "MyT", rows, "a, b")

    checkResult("SELECT a FROM MyT WHERE b LIKE '%ha?_ha%' ESCAPE '?'", Seq(row(1), row(2), row(3)))

    checkResult("SELECT a FROM MyT WHERE b LIKE '%ha?_ha' ESCAPE '?'", Seq(row(1)))

    checkResult("SELECT a FROM MyT WHERE b LIKE 'ha?_ha%' ESCAPE '?'", Seq(row(1)))

    checkResult("SELECT a FROM MyT WHERE b LIKE 'ha?_ha' ESCAPE '?'", Seq(row(1)))

    checkResult("SELECT a FROM MyT WHERE b LIKE '%affh%ha?_ha%' ESCAPE '?'", Seq(row(3)))

    checkResult("SELECT a FROM MyT WHERE b LIKE 'a?%?_ha' ESCAPE '?'", Seq(row(5)))

    checkResult("SELECT a FROM MyT WHERE b LIKE 'h_?_ha' ESCAPE '?'", Seq(row(1)))
  }

  @Test
  def testChainLike(): Unit = {
    // special case to test CHAIN_PATTERN.
    checkResult(
      "SELECT a FROM NullTable3 WHERE c LIKE '% /sys/kvengine/KVServerRole/kvengine/kv_server%'",
      Seq())

    // special case to test CHAIN_PATTERN.
    checkResult("SELECT a FROM NullTable3 WHERE c LIKE '%Tuple%%'", Seq(row(null), row(null)))

    // special case to test CHAIN_PATTERN.
    checkResult(
      "SELECT a FROM NullTable3 WHERE c LIKE '%/order/inter/touch/backwayprice.do%%'",
      Seq())
  }

  @Test
  def testEqual(): Unit = {
    checkResult("SELECT a FROM Table3 WHERE c = 'Hi'", Seq(row(1)))

    checkResult("SELECT c FROM Table3 WHERE c <> 'Hello' AND b = 2", Seq(row("Hello world")))
  }

  @Test
  def testSubString(): Unit = {
    checkResult("SELECT SUBSTRING(c, 6, 13) FROM Table3 WHERE a = 6", Seq(row("Skywalker")))
  }

  @Test
  def testConcat(): Unit = {
    checkResult("SELECT CONCAT(c, '-haha') FROM Table3 WHERE a = 1", Seq(row("Hi-haha")))

    checkResult("SELECT CONCAT_WS('-x-', c, 'haha') FROM Table3 WHERE a = 1", Seq(row("Hi-x-haha")))
  }

  @Test
  def testStringAgg(): Unit = {
    checkResult("SELECT MIN(c) FROM NullTable3", Seq(row("Comment#1")))

    checkResult(
      "SELECT SUM(b) FROM NullTable3 WHERE c = 'NullTuple' OR c LIKE '%Hello world%' GROUP BY c",
      Seq(row(1998), row(2), row(3)))
  }

  @Test
  def testTruncate(): Unit = {
    checkResult("SELECT TRUNCATE(CAST(123.456 AS DOUBLE), 2)", Seq(row(123.45)))

    checkResult("SELECT TRUNCATE(CAST(123.456 AS DOUBLE))", Seq(row(123.0)))

    checkResult("SELECT TRUNCATE(CAST(123.456 AS FLOAT), 2)", Seq(row(123.45f)))

    checkResult("SELECT TRUNCATE(CAST(123.456 AS FLOAT))", Seq(row(123.0f)))

    checkResult("SELECT TRUNCATE(123, -1)", Seq(row(120)))

    checkResult("SELECT TRUNCATE(123, -2)", Seq(row(100)))

    checkResult(
      "SELECT TRUNCATE(CAST(123.456 AS DECIMAL(6, 3)), 2)",
      Seq(row(new java.math.BigDecimal("123.45"))))

    checkResult(
      "SELECT TRUNCATE(CAST(123.456 AS DECIMAL(6, 3)))",
      Seq(row(new java.math.BigDecimal("123"))))
  }

  @Test
  def testStringUdf(): Unit = {
    registerFunction("myFunc", MyStringFunc)
    checkResult("SELECT myFunc(c) FROM Table3 WHERE a = 1", Seq(row("Hihaha")))
  }

  @Test
  def testNestUdf(): Unit = {
    registerFunction("func", MyStringFunc)
    checkResult(
      "SELECT func(func(func(c))) FROM SmallTable3",
      Seq(row("Hello worldhahahahahaha"), row("Hellohahahahahaha"), row("Hihahahahahaha")))
  }

  @Test
  def testCurrentDate(): Unit = {
    // Execution in on Query should return the same value
    checkResult("SELECT CURRENT_DATE = CURRENT_DATE FROM testTable WHERE a = TRUE", Seq(row(true)))

    val d0 = LocalDateConverter.INSTANCE.toInternal(
      toLocalDateTime(System.currentTimeMillis()).toLocalDate)

    val table = parseQuery("SELECT CURRENT_DATE FROM testTable WHERE a = TRUE")
    val result = executeQuery(table)
    val d1 =
      LocalDateConverter.INSTANCE.toInternal(result.toList.head.getField(0).asInstanceOf[LocalDate])

    assertThat(d0 <= d1 && d1 - d0 <= 1).isTrue
  }

  @Test
  def testCurrentTimestamp(): Unit = {
    // Execution in on Query should return the same value
    checkResult(
      "SELECT CURRENT_TIMESTAMP = CURRENT_TIMESTAMP FROM testTable WHERE a = TRUE",
      Seq(row(true)))

    // CURRENT_TIMESTAMP should return the current timestamp
    val ts0 = System.currentTimeMillis()

    val table = parseQuery("SELECT CURRENT_TIMESTAMP FROM testTable WHERE a = TRUE")
    val result = executeQuery(table)
    val ts1 =
      TimestampData.fromInstant(result.toList.head.getField(0).asInstanceOf[Instant]).getMillisecond

    val ts2 = System.currentTimeMillis()

    assertThat(ts0 <= ts1 && ts1 <= ts2).isTrue
  }

  @Test
  def testCurrentTime(): Unit = {
    // Execution in on Query should return the same value
    checkResult("SELECT CURRENT_TIME = CURRENT_TIME FROM testTable WHERE a = TRUE", Seq(row(true)))
  }

  def testTimestampCompareWithDate(): Unit = {
    checkResult("SELECT j FROM testTable WHERE j < DATE '2017-11-11'", Seq(row(true)))
  }

  /**
   * TODO Support below string timestamp format to cast to timestamp: yyyy yyyy-[m]m yyyy-[m]m-[d]d
   * yyyy-[m]m-[d]d yyyy-[m]m-[d]d [h]h:[m]m:[s]s.[ms][ms][ms][us][us][us] yyyy-[m]m-[d]d
   * [h]h:[m]m:[s]s.[ms][ms][ms][us][us][us]Z yyyy-[m]m-[d]d
   * [h]h:[m]m:[s]s.[ms][ms][ms][us][us][us]-[h]h:[m]m yyyy-[m]m-[d]d
   * [h]h:[m]m:[s]s.[ms][ms][ms][us][us][us]+[h]h:[m]m
   * yyyy-[m]m-[d]dT[h]h:[m]m:[s]s.[ms][ms][ms][us][us][us]
   * yyyy-[m]m-[d]dT[h]h:[m]m:[s]s.[ms][ms][ms][us][us][us]Z
   * yyyy-[m]m-[d]dT[h]h:[m]m:[s]s.[ms][ms][ms][us][us][us]-[h]h:[m]m
   * yyyy-[m]m-[d]dT[h]h:[m]m:[s]s.[ms][ms][ms][us][us][us]+[h]h:[m]m
   * [h]h:[m]m:[s]s.[ms][ms][ms][us][us][us] [h]h:[m]m:[s]s.[ms][ms][ms][us][us][us]Z
   * [h]h:[m]m:[s]s.[ms][ms][ms][us][us][us]-[h]h:[m]m
   * [h]h:[m]m:[s]s.[ms][ms][ms][us][us][us]+[h]h:[m]m T[h]h:[m]m:[s]s.[ms][ms][ms][us][us][us]
   * T[h]h:[m]m:[s]s.[ms][ms][ms][us][us][us]Z T[h]h:[m]m:[s]s.[ms][ms][ms][us][us][us]-[h]h:[m]m
   * T[h]h:[m]m:[s]s.[ms][ms][ms][us][us][us]+[h]h:[m]m
   */
  @Test
  def testTimestampCompareWithDateString(): Unit = {
    // j 2015-05-20 10:00:00.887
    checkResult(
      "SELECT j FROM testTable WHERE j < '2017-11-11'",
      Seq(row(localDateTime("2015-05-20 10:00:00.887"))))
  }

  @Test
  def testDateCompareWithDateString(): Unit = {
    checkResult(
      "SELECT h FROM testTable WHERE h <= '2017-12-12'",
      Seq(
        row(localDate("2017-12-12")),
        row(localDate("2017-12-12"))
      ))
  }

  @Test
  def testDateEqualsWithDateString(): Unit = {
    checkResult(
      "SELECT h FROM testTable WHERE h = '2017-12-12'",
      Seq(
        row(localDate("2017-12-12")),
        row(localDate("2017-12-12"))
      ))
  }

  @Test
  def testDateFormat(): Unit = {
    // j 2015-05-20 10:00:00.887
    checkResult(
      "SELECT j, " +
        " DATE_FORMAT(j, 'yyyy/MM/dd HH:mm:ss')," +
        " DATE_FORMAT('2015-05-20 10:00:00.887', 'yyyy/MM/dd HH:mm:ss')" +
        " FROM testTable WHERE a = TRUE",
      Seq(
        row(localDateTime("2015-05-20 10:00:00.887"), "2015/05/20 10:00:00", "2015/05/20 10:00:00")
      )
    )
  }

  @Test
  def testYear(): Unit = {
    checkResult(
      "SELECT j, YEAR(j) FROM testTable WHERE a = TRUE",
      Seq(row(localDateTime("2015-05-20 10:00:00.887"), "2015")))
  }

  @Test
  def testQuarter(): Unit = {
    checkResult(
      "SELECT j, QUARTER(j) FROM testTable WHERE a = TRUE",
      Seq(row(localDateTime("2015-05-20 10:00:00.887"), "2")))
  }

  @Test
  def testMonth(): Unit = {
    checkResult(
      "SELECT j, MONTH(j) FROM testTable WHERE a = TRUE",
      Seq(row(localDateTime("2015-05-20 10:00:00.887"), "5")))
  }

  @Test
  def testWeek(): Unit = {
    checkResult(
      "SELECT j, WEEK(j) FROM testTable WHERE a = TRUE",
      Seq(row(localDateTime("2015-05-20 10:00:00.887"), "21")))
  }

  @Test
  def testDayOfYear(): Unit = {
    checkResult(
      "SELECT j, DAYOFYEAR(j) FROM testTable WHERE a = TRUE",
      Seq(row(localDateTime("2015-05-20 10:00:00.887"), "140")))
  }

  @Test
  def testDayOfMonth(): Unit = {
    checkResult(
      "SELECT j, DAYOFMONTH(j) FROM testTable WHERE a = TRUE",
      Seq(row(localDateTime("2015-05-20 10:00:00.887"), "20")))
  }

  @Test
  def testDayOfWeek(): Unit = {
    checkResult(
      "SELECT j, DAYOFWEEK(j) FROM testTable WHERE a = TRUE",
      Seq(row(localDateTime("2015-05-20 10:00:00.887"), "4")))
  }

  @Test
  def testHour(): Unit = {
    checkResult(
      "SELECT j, HOUR(j) FROM testTable WHERE a = TRUE",
      Seq(row(localDateTime("2015-05-20 10:00:00.887"), "10")))
  }

  @Test
  def testMinute(): Unit = {
    checkResult(
      "SELECT j, MINUTE(j) FROM testTable WHERE a = TRUE",
      Seq(row(localDateTime("2015-05-20 10:00:00.887"), "0")))
  }

  @Test
  def testSecond(): Unit = {
    checkResult(
      "SELECT j, SECOND(j) FROM testTable WHERE a = TRUE",
      Seq(row(localDateTime("2015-05-20 10:00:00.887"), "0")))
  }

  @Test
  def testToDate(): Unit = {
    checkResult(
      "SELECT" +
        " TO_DATE(CAST(null AS VARCHAR))," +
        " TO_DATE('2016-12-31')," +
        " TO_DATE('2016-12-31', 'yyyy-MM-dd')",
      Seq(row(null, localDate("2016-12-31"), localDate("2016-12-31")))
    )
  }

  @Test
  def testToTimestamp(): Unit = {
    checkResult(
      "SELECT" +
        " TO_TIMESTAMP(CAST(null AS VARCHAR))," +
        " TO_TIMESTAMP('2016-12-31 00:12:00')," +
        " TO_TIMESTAMP('2016-12-31', 'yyyy-MM-dd')",
      Seq(row(null, localDateTime("2016-12-31 00:12:00"), localDateTime("2016-12-31 00:00:00")))
    )
  }

  @Test
  def testCalcBinary(): Unit = {
    registerCollection(
      "BinaryT",
      nullData3.map(
        r =>
          row(
            r.getField(0),
            r.getField(1),
            r.getField(2).toString.getBytes(StandardCharsets.UTF_8))),
      new RowTypeInfo(INT_TYPE_INFO, LONG_TYPE_INFO, BYTE_PRIMITIVE_ARRAY_TYPE_INFO),
      "a, b, c",
      nullablesOfNullData3
    )
    checkResult(
      "select a, b, c from BinaryT where b < 1000",
      nullData3.map(
        r =>
          row(
            r.getField(0),
            r.getField(1),
            r.getField(2).toString.getBytes(StandardCharsets.UTF_8)))
    )
  }

  @Test
  def testOrderByBinary(): Unit = {
    registerCollection(
      "BinaryT",
      nullData3.map(
        r =>
          row(
            r.getField(0),
            r.getField(1),
            r.getField(2).toString.getBytes(StandardCharsets.UTF_8))),
      new RowTypeInfo(INT_TYPE_INFO, LONG_TYPE_INFO, BYTE_PRIMITIVE_ARRAY_TYPE_INFO),
      "a, b, c",
      nullablesOfNullData3
    )
    tableConfig.set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, Int.box(1))
    tableConfig.set(BatchPhysicalSortRule.TABLE_EXEC_RANGE_SORT_ENABLED, Boolean.box(true))

    assertThatThrownBy(
      () =>
        checkResult(
          "select * from BinaryT order by c",
          nullData3
            .sortBy((x: Row) => x.getField(2).asInstanceOf[String])
            .map(
              r =>
                row(
                  r.getField(0),
                  r.getField(1),
                  r.getField(2).toString.getBytes(StandardCharsets.UTF_8))),
          isSorted = true
        )).isInstanceOf(classOf[UnsupportedOperationException])
  }

  @Test
  def testGroupByBinary(): Unit = {
    registerCollection(
      "BinaryT2",
      nullData3.map(
        r =>
          row(
            r.getField(0),
            r.getField(1).toString.getBytes(StandardCharsets.UTF_8),
            r.getField(2))),
      new RowTypeInfo(INT_TYPE_INFO, BYTE_PRIMITIVE_ARRAY_TYPE_INFO, STRING_TYPE_INFO),
      "a, b, c",
      nullablesOfNullData3
    )
    checkResult(
      "select sum(sumA) from (select sum(a) as sumA, b, c from BinaryT2 group by c, b) group by b",
      Seq(row(1), row(111), row(15), row(34), row(5), row(65), row(null))
    )
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

    checkResult(
      "select a, c from SimpleTable",
      Seq(row(1, "Hi"), row(2, "Hello"), row(3, "Hello world"))
    )
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

    checkResult(
      """
        |select id,
        |    deepNested.nested1.name AS nestedName,
        |    nested.`value` AS nestedValue,
        |    deepNested.nested2.flag AS nestedFlag,
        |    deepNested.nested2.num AS nestedNum
        |from NestedTable
        |""".stripMargin,
      Seq(
        row(1, "HI", 1111, true, 111),
        row(2, "HELLO", 2222, false, 222),
        row(3, "HELLO WORLD", 3333, true, 333))
    )
  }

  @Test
  def testFloatIn(): Unit = {
    val source = tEnv.fromValues(
      DataTypes.ROW(
        DataTypes.FIELD("f0", DataTypes.FLOAT()),
        DataTypes.FIELD("f1", DataTypes.FLOAT()),
        DataTypes.FIELD("f2", DataTypes.FLOAT())),
      row(1.0f, 11.0f, 12.0f),
      row(2.0f, 21.0f, 22.0f),
      row(3.0f, 31.0f, 32.0f),
      row(4.0f, 41.0f, 42.0f),
      row(5.0f, 51.0f, 52.0f)
    )

    tEnv.createTemporaryView("myTable", source)

    val query = """
                  |select * from myTable where f0 in (1.0, 2.0, 3.0)
                  |""".stripMargin

    checkResult(
      query,
      Seq(row(1.0f, 11.0f, 12.0f), row(2.0f, 21.0f, 22.0f), row(3.0f, 31.0f, 32.0f))
    )
  }

  @Test
  def testSearch(): Unit = {
    val myTableDataId = TestValuesTableFactory.registerData(
      Seq(row("HC809"), row("H389N     "))
    )
    val ddl =
      s"""
         |CREATE TABLE SimpleTable (
         |  content STRING
         |) WITH (
         |  'connector' = 'values',
         |  'data-id' = '$myTableDataId',
         |  'bounded' = 'true'
         |)
         |""".stripMargin
    tEnv.executeSql(ddl)
    val sql =
      """
        |SELECT UPPER(content) from SimpleTable where UPPER(content) in (
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
    checkResult(
      sql,
      Seq(row("HC809"), row("H389N     "))
    )
  }

  @Test
  def testSearchWithNull(): Unit = {
    runQueryWithIn(
      """
        |'CTNBSmokeSensor',
        |'H389N     ',
        |'GHL-IRD',
        |'JY-BF-20YN',
        |'HC809',
        |'DH-9908N-AEP',
        |'DH-9908N',
        | null""".stripMargin
    )
  }

  @Test
  def testSearchWithNull2(): Unit = {
    runQueryWithIn(
      """
        | null,
        |'CTNBSmokeSensor',
        |'H389N     ',
        |'GHL-IRD',
        |'JY-BF-20YN',
        |'HC809',
        |'DH-9908N-AEP',
        |'DH-9908N'
        |""".stripMargin
    )
  }

  private def runQueryWithIn(inParameter: String): Unit = {
    val myTableDataId = TestValuesTableFactory.registerData(Seq(row("HC809"), row(null)))
    val ddl =
      s"""
         |CREATE TABLE SimpleTable (
         |  content String
         |) WITH (
         |  'connector' = 'values',
         |  'data-id' = '$myTableDataId',
         |  'bounded' = 'true'
         |)
         |""".stripMargin
    tEnv.executeSql(ddl)
    val sql =
      s"""
         |SELECT content from SimpleTable where UPPER(content) in (
         | $inParameter
         |)
         |""".stripMargin
    checkResult(
      sql,
      Seq(row("HC809"))
    )
  }

  @Test
  def testFilterPushDownWithInterval(): Unit = {
    val schema = TableSchema
      .builder()
      .field("a", DataTypes.TIMESTAMP)
      .field("b", DataTypes.TIMESTAMP)
      .build()

    val data = List(
      row(localDateTime("2021-03-30 10:00:00"), localDateTime("2021-03-30 14:59:59")),
      row(localDateTime("2021-03-30 10:00:00"), localDateTime("2021-03-30 15:00:00")),
      row(localDateTime("2021-03-30 10:00:00"), localDateTime("2021-03-30 15:00:01")),
      row(localDateTime("2021-03-30 10:00:00"), localDateTime("2023-03-30 09:59:59")),
      row(localDateTime("2021-03-30 10:00:00"), localDateTime("2023-03-30 10:00:00")),
      row(localDateTime("2021-03-30 10:00:00"), localDateTime("2023-03-30 10:00:01"))
    )

    TestLegacyFilterableTableSource.createTemporaryTable(
      tEnv,
      schema,
      "myTable",
      isBounded = true,
      data,
      Set("a", "b"))

    checkResult(
      "SELECT * FROM myTable WHERE TIMESTAMPADD(HOUR, 5, a) >= b",
      Seq(
        row(localDateTime("2021-03-30 10:00:00"), localDateTime("2021-03-30 14:59:59")),
        row(localDateTime("2021-03-30 10:00:00"), localDateTime("2021-03-30 15:00:00"))
      )
    )

    checkResult(
      "SELECT * FROM myTable WHERE TIMESTAMPADD(YEAR, 2, a) >= b",
      Seq(
        row(localDateTime("2021-03-30 10:00:00"), localDateTime("2021-03-30 14:59:59")),
        row(localDateTime("2021-03-30 10:00:00"), localDateTime("2021-03-30 15:00:00")),
        row(localDateTime("2021-03-30 10:00:00"), localDateTime("2021-03-30 15:00:01")),
        row(localDateTime("2021-03-30 10:00:00"), localDateTime("2023-03-30 09:59:59")),
        row(localDateTime("2021-03-30 10:00:00"), localDateTime("2023-03-30 10:00:00"))
      )
    )
  }

  @Test
  def testOrWithIsNullPredicate(): Unit = {
    checkResult(
      """
        |SELECT * FROM NullTable3 AS T
        |WHERE T.a = 1 OR T.a = 3 OR T.a IS NULL
        |""".stripMargin,
      Seq(
        row(1, 1L, "Hi"),
        row(3, 2L, "Hello world"),
        row(null, 999L, "NullTuple"),
        row(null, 999L, "NullTuple"))
    )

    checkResult(
      """
        |SELECT * FROM NullTable3 AS T
        |WHERE T.a IN (1, 3) OR T.a IS NULL
        |""".stripMargin,
      Seq(
        row(1, 1L, "Hi"),
        row(3, 2L, "Hello world"),
        row(null, 999L, "NullTuple"),
        row(null, 999L, "NullTuple"))
    )

    checkResult(
      """
        |SELECT * FROM NullTable3 AS T
        |WHERE T.a IN (1, 3) OR T.a IS NOT NULL
        |""".stripMargin,
      Seq(
        row(1, 1L, "Hi"),
        row(10, 4L, "Comment#4"),
        row(11, 5L, "Comment#5"),
        row(12, 5L, "Comment#6"),
        row(13, 5L, "Comment#7"),
        row(14, 5L, "Comment#8"),
        row(15, 5L, "Comment#9"),
        row(16, 6L, "Comment#10"),
        row(17, 6L, "Comment#11"),
        row(18, 6L, "Comment#12"),
        row(19, 6L, "Comment#13"),
        row(2, 2L, "Hello"),
        row(20, 6L, "Comment#14"),
        row(21, 6L, "Comment#15"),
        row(3, 2L, "Hello world"),
        row(4, 3L, "Hello world, how are you?"),
        row(5, 3L, "I am fine."),
        row(6, 3L, "Luke Skywalker"),
        row(7, 4L, "Comment#1"),
        row(8, 4L, "Comment#2"),
        row(9, 4L, "Comment#3")
      )
    )

    checkResult(
      """
        |SELECT * FROM NullTable3 AS T
        |WHERE T.a NOT IN (1, 2) OR T.a IS NULL
        |""".stripMargin,
      Seq(
        row(10, 4L, "Comment#4"),
        row(11, 5L, "Comment#5"),
        row(12, 5L, "Comment#6"),
        row(13, 5L, "Comment#7"),
        row(14, 5L, "Comment#8"),
        row(15, 5L, "Comment#9"),
        row(16, 6L, "Comment#10"),
        row(17, 6L, "Comment#11"),
        row(18, 6L, "Comment#12"),
        row(19, 6L, "Comment#13"),
        row(20, 6L, "Comment#14"),
        row(21, 6L, "Comment#15"),
        row(3, 2L, "Hello world"),
        row(4, 3L, "Hello world, how are you?"),
        row(5, 3L, "I am fine."),
        row(6, 3L, "Luke Skywalker"),
        row(7, 4L, "Comment#1"),
        row(8, 4L, "Comment#2"),
        row(9, 4L, "Comment#3"),
        row(null, 999L, "NullTuple"),
        row(null, 999L, "NullTuple")
      )
    )

    checkResult(
      """
        |SELECT * FROM NullTable3 AS T
        |WHERE T.a NOT IN (1, 2) OR T.a IS NOT NULL
        |""".stripMargin,
      Seq(
        row(1, 1L, "Hi"),
        row(10, 4L, "Comment#4"),
        row(11, 5L, "Comment#5"),
        row(12, 5L, "Comment#6"),
        row(13, 5L, "Comment#7"),
        row(14, 5L, "Comment#8"),
        row(15, 5L, "Comment#9"),
        row(16, 6L, "Comment#10"),
        row(17, 6L, "Comment#11"),
        row(18, 6L, "Comment#12"),
        row(19, 6L, "Comment#13"),
        row(20, 6L, "Comment#14"),
        row(21, 6L, "Comment#15"),
        row(2, 2L, "Hello"),
        row(3, 2L, "Hello world"),
        row(4, 3L, "Hello world, how are you?"),
        row(5, 3L, "I am fine."),
        row(6, 3L, "Luke Skywalker"),
        row(7, 4L, "Comment#1"),
        row(8, 4L, "Comment#2"),
        row(9, 4L, "Comment#3")
      )
    )

    // Test for Sarg.nullAs == RexUnknownAs.FALSE
    // taken from https://issues.apache.org/jira/browse/CALCITE-4446
    checkResult(
      """
        |SELECT * FROM NullTable3 AS T
        |WHERE T.a IS NOT NULL AND T.a IN (1, 3)
        |""".stripMargin,
      Seq(
        row(1, 1L, "Hi"),
        row(3, 2L, "Hello world")
      )
    )

    // Test for Sarg.nullAs == RexUnknownAs.UNKNOWN
    // taken from https://issues.apache.org/jira/browse/CALCITE-4446
    checkResult(
      """
        |SELECT * FROM NullTable3 AS T
        |WHERE T.a IN (1, 3)
        |""".stripMargin,
      Seq(
        row(1, 1L, "Hi"),
        row(3, 2L, "Hello world")
      )
    )
  }

  @Test
  def testOrWithIsNullInIf(): Unit = {
    val data = Seq(row("", "N"), row("X", "Y"), row(null, "Y"))
    registerCollection("MyTable", data, new RowTypeInfo(STRING_TYPE_INFO, STRING_TYPE_INFO), "a, b")

    checkResult("SELECT IF(a = '', 'a', 'b') FROM MyTable", Seq(row('a'), row('b'), row('b')))
    checkResult("SELECT IF(a IS NULL, 'a', 'b') FROM MyTable", Seq(row('b'), row('b'), row('a')))
    checkResult(
      "SELECT IF(a = '' OR a IS NULL, 'a', 'b') FROM MyTable",
      Seq(row('a'), row('b'), row('a')))
    checkResult(
      "SELECT IF(a IN ('', ' ') OR a IS NULL, 'a', 'b') FROM MyTable",
      Seq(row('a'), row('b'), row('a')))
    checkResult(
      "SELECT IF(a IN ('', ' ') OR a IS NOT NULL, 'a', 'b') FROM MyTable",
      Seq(row('a'), row('a'), row('b')))
    checkResult(
      "SELECT IF(a NOT IN ('', ' ') OR a IS NULL, 'a', 'b') FROM MyTable",
      Seq(row('b'), row('a'), row('a')))
    // Test for Sarg.nullAs == RexUnknownAs.FALSE
    // taken from https://issues.apache.org/jira/browse/CALCITE-4446
    checkResult(
      "SELECT IF(a NOT IN ('', ' ') AND a IS NOT NULL, 'a', 'b') FROM MyTable",
      Seq(row('b'), row('a'), row('b')))
    // Test for Sarg.nullAs == RexUnknownAs.UNKNOWN
    // taken from https://issues.apache.org/jira/browse/CALCITE-4446
    checkResult(
      "SELECT IF(a NOT IN ('', ' '), 'a', 'b') FROM MyTable",
      Seq(row('a'), row('b'), row('b')))
  }

  @Test
  def testFilterConditionWithCast(): Unit = {
    val dataId = TestValuesTableFactory.registerData(
      Seq(row(1, "true"), row(2, "false"), row(3, "invalid"), row(4, null)))
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

    checkResult("select a from MyTable where try_cast(b as boolean)", Seq(row(1)))
    checkResult(
      "select try_cast(b as boolean) from MyTable",
      Seq(row(true), row(false), row(null), row(null)))
  }

  @Test
  def testTimestampAdd(): Unit = {
    // we're not adding this test to ScalarFunctionsTest because that test
    // directly uses the generated code and does not check for expression types
    val dataId = TestValuesTableFactory.registerData(
      Seq(
        row(
          LocalDateTime.of(2021, 7, 15, 16, 50, 0, 123000000),
          LocalDateTime.of(2021, 7, 15, 16, 50, 0, 123456789),
          Instant.ofEpochMilli(1626339000123L),
          Instant.ofEpochSecond(1626339000, 123456789),
          LocalDate.of(2021, 7, 15),
          LocalTime.of(16, 50, 0, 123000000)
        )))
    val ddl =
      s"""
         |CREATE TABLE MyTable (
         |  a TIMESTAMP(3),
         |  b TIMESTAMP(9),
         |  c TIMESTAMP_LTZ(3),
         |  d TIMESTAMP_LTZ(9),
         |  e DATE,
         |  f TIME(3)
         |) WITH (
         |  'connector' = 'values',
         |  'data-id' = '$dataId',
         |  'bounded' = 'true'
         |)
         |""".stripMargin
    tEnv.executeSql(ddl)

    checkResult(
      """
        |select
        |  timestampadd(day, 1, a),
        |  timestampadd(hour, 1, a),
        |  timestampadd(minute, 1, a),
        |  timestampadd(second, 1, a),
        |  timestampadd(day, 1, b),
        |  timestampadd(hour, 1, b),
        |  timestampadd(minute, 1, b),
        |  timestampadd(second, 1, b),
        |  timestampadd(day, 1, c),
        |  timestampadd(hour, 1, c),
        |  timestampadd(minute, 1, c),
        |  timestampadd(second, 1, c),
        |  timestampadd(day, 1, d),
        |  timestampadd(hour, 1, d),
        |  timestampadd(minute, 1, d),
        |  timestampadd(second, 1, d),
        |  timestampadd(day, 1, e),
        |  timestampadd(hour, 1, e),
        |  timestampadd(minute, 1, e),
        |  timestampadd(second, 1, e),
        |  timestampadd(day, 1, f),
        |  timestampadd(hour, 1, f),
        |  timestampadd(minute, 1, f),
        |  timestampadd(second, 1, f)
        |from MyTable
        |""".stripMargin,
      Seq(
        row(
          LocalDateTime.of(2021, 7, 16, 16, 50, 0, 123000000),
          LocalDateTime.of(2021, 7, 15, 17, 50, 0, 123000000),
          LocalDateTime.of(2021, 7, 15, 16, 51, 0, 123000000),
          LocalDateTime.of(2021, 7, 15, 16, 50, 1, 123000000),
          LocalDateTime.of(2021, 7, 16, 16, 50, 0, 123456789),
          LocalDateTime.of(2021, 7, 15, 17, 50, 0, 123456789),
          LocalDateTime.of(2021, 7, 15, 16, 51, 0, 123456789),
          LocalDateTime.of(2021, 7, 15, 16, 50, 1, 123456789),
          Instant.ofEpochMilli(1626339000123L + 24 * 3600 * 1000L),
          Instant.ofEpochMilli(1626339000123L + 3600 * 1000L),
          Instant.ofEpochMilli(1626339000123L + 60 * 1000L),
          Instant.ofEpochMilli(1626339000123L + 1000L),
          Instant.ofEpochSecond(1626339000 + 24 * 3600, 123456789),
          Instant.ofEpochSecond(1626339000 + 3600, 123456789),
          Instant.ofEpochSecond(1626339000 + 60, 123456789),
          Instant.ofEpochSecond(1626339000 + 1, 123456789),
          LocalDate.of(2021, 7, 16),
          LocalDateTime.of(2021, 7, 15, 1, 0, 0),
          LocalDateTime.of(2021, 7, 15, 0, 1, 0),
          LocalDateTime.of(2021, 7, 15, 0, 0, 1),
          LocalTime.of(16, 50, 0, 123000000),
          LocalTime.of(17, 50, 0, 123000000),
          LocalTime.of(16, 51, 0, 123000000),
          LocalTime.of(16, 50, 1, 123000000)
        ))
    )

    // Tests for tinyint
    checkResult(
      """
        |select
        |  timestampadd(day, cast(1 as tinyint), a),
        |  timestampadd(hour, cast(1 as tinyint), a),
        |  timestampadd(minute, cast(1 as tinyint), a),
        |  timestampadd(second, cast(1 as tinyint), a),
        |  timestampadd(day, cast(1 as tinyint), b),
        |  timestampadd(hour, cast(1 as tinyint), b),
        |  timestampadd(minute, cast(1 as tinyint), b),
        |  timestampadd(second, cast(1 as tinyint), b),
        |  timestampadd(day, cast(1 as tinyint), c),
        |  timestampadd(hour, cast(1 as tinyint), c),
        |  timestampadd(minute, cast(1 as tinyint), c),
        |  timestampadd(second, cast(1 as tinyint), c),
        |  timestampadd(day, cast(1 as tinyint), d),
        |  timestampadd(hour, cast(1 as tinyint), d),
        |  timestampadd(minute, cast(1 as tinyint), d),
        |  timestampadd(second, cast(1 as tinyint), d),
        |  timestampadd(day, cast(1 as tinyint), e),
        |  timestampadd(hour, cast(1 as tinyint), e),
        |  timestampadd(minute, cast(1 as tinyint), e),
        |  timestampadd(second, cast(1 as tinyint), e),
        |  timestampadd(day, cast(1 as tinyint), f),
        |  timestampadd(hour, cast(1 as tinyint), f),
        |  timestampadd(minute, cast(1 as tinyint), f),
        |  timestampadd(second, cast(1 as tinyint), f)
        |from MyTable
        |""".stripMargin,
      Seq(
        row(
          LocalDateTime.of(2021, 7, 16, 16, 50, 0, 123000000),
          LocalDateTime.of(2021, 7, 15, 17, 50, 0, 123000000),
          LocalDateTime.of(2021, 7, 15, 16, 51, 0, 123000000),
          LocalDateTime.of(2021, 7, 15, 16, 50, 1, 123000000),
          LocalDateTime.of(2021, 7, 16, 16, 50, 0, 123456789),
          LocalDateTime.of(2021, 7, 15, 17, 50, 0, 123456789),
          LocalDateTime.of(2021, 7, 15, 16, 51, 0, 123456789),
          LocalDateTime.of(2021, 7, 15, 16, 50, 1, 123456789),
          Instant.ofEpochMilli(1626339000123L + 24 * 3600 * 1000L),
          Instant.ofEpochMilli(1626339000123L + 3600 * 1000L),
          Instant.ofEpochMilli(1626339000123L + 60 * 1000L),
          Instant.ofEpochMilli(1626339000123L + 1000L),
          Instant.ofEpochSecond(1626339000 + 24 * 3600, 123456789),
          Instant.ofEpochSecond(1626339000 + 3600, 123456789),
          Instant.ofEpochSecond(1626339000 + 60, 123456789),
          Instant.ofEpochSecond(1626339000 + 1, 123456789),
          LocalDate.of(2021, 7, 16),
          LocalDateTime.of(2021, 7, 15, 1, 0, 0),
          LocalDateTime.of(2021, 7, 15, 0, 1, 0),
          LocalDateTime.of(2021, 7, 15, 0, 0, 1),
          LocalTime.of(16, 50, 0, 123000000),
          LocalTime.of(17, 50, 0, 123000000),
          LocalTime.of(16, 51, 0, 123000000),
          LocalTime.of(16, 50, 1, 123000000)
        ))
    )

    // Tests for smallint
    checkResult(
      """
        |select
        |  timestampadd(day, cast(1 as smallint), a),
        |  timestampadd(hour, cast(1 as smallint), a),
        |  timestampadd(minute, cast(1 as smallint), a),
        |  timestampadd(second, cast(1 as smallint), a),
        |  timestampadd(day, cast(1 as smallint), b),
        |  timestampadd(hour, cast(1 as smallint), b),
        |  timestampadd(minute, cast(1 as smallint), b),
        |  timestampadd(second, cast(1 as smallint), b),
        |  timestampadd(day, cast(1 as smallint), c),
        |  timestampadd(hour, cast(1 as smallint), c),
        |  timestampadd(minute, cast(1 as smallint), c),
        |  timestampadd(second, cast(1 as smallint), c),
        |  timestampadd(day, cast(1 as smallint), d),
        |  timestampadd(hour, cast(1 as smallint), d),
        |  timestampadd(minute, cast(1 as smallint), d),
        |  timestampadd(second, cast(1 as smallint), d),
        |  timestampadd(day, cast(1 as smallint), e),
        |  timestampadd(hour, cast(1 as smallint), e),
        |  timestampadd(minute, cast(1 as smallint), e),
        |  timestampadd(second, cast(1 as smallint), e),
        |  timestampadd(day, cast(1 as smallint), f),
        |  timestampadd(hour, cast(1 as smallint), f),
        |  timestampadd(minute, cast(1 as smallint), f),
        |  timestampadd(second, cast(1 as smallint), f)
        |from MyTable
        |""".stripMargin,
      Seq(
        row(
          LocalDateTime.of(2021, 7, 16, 16, 50, 0, 123000000),
          LocalDateTime.of(2021, 7, 15, 17, 50, 0, 123000000),
          LocalDateTime.of(2021, 7, 15, 16, 51, 0, 123000000),
          LocalDateTime.of(2021, 7, 15, 16, 50, 1, 123000000),
          LocalDateTime.of(2021, 7, 16, 16, 50, 0, 123456789),
          LocalDateTime.of(2021, 7, 15, 17, 50, 0, 123456789),
          LocalDateTime.of(2021, 7, 15, 16, 51, 0, 123456789),
          LocalDateTime.of(2021, 7, 15, 16, 50, 1, 123456789),
          Instant.ofEpochMilli(1626339000123L + 24 * 3600 * 1000L),
          Instant.ofEpochMilli(1626339000123L + 3600 * 1000L),
          Instant.ofEpochMilli(1626339000123L + 60 * 1000L),
          Instant.ofEpochMilli(1626339000123L + 1000L),
          Instant.ofEpochSecond(1626339000 + 24 * 3600, 123456789),
          Instant.ofEpochSecond(1626339000 + 3600, 123456789),
          Instant.ofEpochSecond(1626339000 + 60, 123456789),
          Instant.ofEpochSecond(1626339000 + 1, 123456789),
          LocalDate.of(2021, 7, 16),
          LocalDateTime.of(2021, 7, 15, 1, 0, 0),
          LocalDateTime.of(2021, 7, 15, 0, 1, 0),
          LocalDateTime.of(2021, 7, 15, 0, 0, 1),
          LocalTime.of(16, 50, 0, 123000000),
          LocalTime.of(17, 50, 0, 123000000),
          LocalTime.of(16, 51, 0, 123000000),
          LocalTime.of(16, 50, 1, 123000000)
        ))
    )
  }

  @Test
  def testTryCast(): Unit = {
    checkResult("SELECT TRY_CAST('invalid' AS INT)", Seq(row(null)))
    checkResult("SELECT TRY_CAST(g AS DOUBLE) FROM testTable", Seq(row(null), row(null), row(null)))
  }

  @Test
  def testMultipleCoalesces(): Unit = {
    checkResult(
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
        "COALESCE(cast(NULL as double), cast(NULL as double))",
      Seq(row(1, 1, 2, 1, 3, 4, 1, 1, 2, 1, 3, 4, 1.0, 1.0, 2.0, 2.0, 2.0, null))
    )
  }

  @Test
  def testCurrentDatabase(): Unit = {
    checkResult("SELECT CURRENT_DATABASE()", Seq(row(tEnv.getCurrentDatabase)))
    // switch to another database
    tEnv
      .getCatalog(tEnv.getCurrentCatalog)
      .get()
      .createDatabase(
        "db1",
        new CatalogDatabaseImpl(new util.HashMap[String, String](), "db1"),
        false)
    tEnv.useDatabase("db1")
    checkResult("SELECT CURRENT_DATABASE()", Seq(row(tEnv.getCurrentDatabase)))
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
         |""".stripMargin
    tEnv.executeSql(ddl)

    checkResult(
      """
        | SELECT * FROM MyTable WHERE b LIKE '%"%'
        |""".stripMargin,
      Seq(row(2, "cbc\"ddd")))
  }

  @Test
  def testNonMergeableRandCall(): Unit = {
    // reported in FLINK-20887
    checkResult(
      s"""
         |SELECT b - a FROM (
         |  SELECT r + 5 AS a, r + 7 AS b FROM (
         |    SELECT RAND() AS r FROM SmallTable3
         |  ) t1
         |) t2
         |""".stripMargin,
      Seq(row(2.0), row(2.0), row(2.0))
    )
  }
}
