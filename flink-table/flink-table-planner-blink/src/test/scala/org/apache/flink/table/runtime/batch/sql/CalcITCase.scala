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

import org.apache.flink.api.common.typeinfo.BasicTypeInfo.{INT_TYPE_INFO, LONG_TYPE_INFO, STRING_TYPE_INFO}
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO
import org.apache.flink.api.common.typeinfo.SqlTimeTypeInfo.{DATE, TIME, TIMESTAMP}
import org.apache.flink.api.java.typeutils._
import org.apache.flink.api.scala._
import org.apache.flink.table.api.{TableConfigOptions, ValidationException}
import org.apache.flink.table.dataformat.DataFormatConverters.{DateConverter, TimestampConverter}
import org.apache.flink.table.dataformat.Decimal
import org.apache.flink.table.expressions.utils.{RichFunc1, RichFunc2, RichFunc3, SplitUDF}
import org.apache.flink.table.runtime.utils.BatchTestBase.row
import org.apache.flink.table.runtime.utils.TestData._
import org.apache.flink.table.runtime.utils.UserDefinedFunctionTestUtils._
import org.apache.flink.table.runtime.utils.{BatchScalaTableEnvUtil, BatchTestBase, UserDefinedFunctionTestUtils}
import org.apache.flink.table.util.DateTimeTestUtil._
import org.apache.flink.types.Row
import org.junit.Assert.assertEquals
import org.junit._

import java.sql.{Date, Timestamp}
import java.util

import scala.collection.Seq

class CalcITCase extends BatchTestBase {

  @Before
  def before(): Unit = {
    registerCollection("Table3", data3, type3, nullablesOfData3, "a, b, c")
    registerCollection("NullTable3", nullData3, type3, nullablesOfData3, "a, b, c")
    registerCollection("SmallTable3", smallData3, type3, nullablesOfData3, "a, b, c")
    registerCollection("testTable", buildInData, buildInType, "a,b,c,d,e,f,g,h,i,j")
  }

  @Test
  def testSelectStar(): Unit = {
    checkResult(
      "SELECT * FROM Table3",
      data3)
  }

  @Test
  def testSimpleSelectAll(): Unit = {
    checkResult(
      "SELECT a, b, c FROM Table3",
      data3)
  }

  @Test
  def testManySelectWithFilter(): Unit = {
    val data = Seq(
      (true, 1, 2, 3, 4, 5, 6, 7),
      (false, 1, 2, 3, 4, 5, 6, 7)
    )
    BatchScalaTableEnvUtil.registerCollection(tEnv, "MyT", data, "a, b, c, d, e, f, g, h")
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
      Seq(row(
        true, 1, 2, 3, 4, 5, 6, 7, true, 1, 2, 6, 3, 4, 5, 7, 7, 6, 5, 4, 3, 2, 1,
        true, 7, 5, 4, 3, 6, 2, 1, true, 2, true, 1, 6, 5, 4, 7, 3, true, 1, 2, 3,
        4, 5, 6, 7, true, 1, 2, 6, 3, 4, 5, 7, 7, 6, 5, 4, 3, 2, 1, true, 7, 5, 4,
        3, 6, 2, 1, true, 2, true, 1, 6, 5, 4, 7, 3
      )))
  }

  @Test
  def testManySelect(): Unit = {
    registerCollection(
      "ProjectionTestTable",
      projectionTestData, projectionTestDataType, nullablesOfProjectionTestData,
      "a, b, c, d, e, f, g, h")
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
          1, 10, 100, "1", "10", "100", 1000, "1000",
          1, 10, 100, 1000, "1", "10", "100", "1000",
          "1000", 1000, "100", "10", "1", 100, 10, 1,
          "1000", "100", "10", "1", 1000, 100, 10, 1,
          100, 1, 10, 1000, "100", "10", "1000", "1",
          1, 10, 100, "1", "10", "100", 1000, "1000",
          1, 10, 100, 1000, "1", "10", "100", "1000",
          "1000", 1000, "100", "10", "1", 100, 10, 1,
          "1000", "100", "10", "1", 1000, 100, 10, 1,
          100, 1, 10, 1000, "100", "10", "1000", "1"),
        row(
          2, 20, 200, "2", "20", "200", 2000, "2000",
          2, 20, 200, 2000, "2", "20", "200", "2000",
          "2000", 2000, "200", "20", "2", 200, 20, 2,
          "2000", "200", "20", "2", 2000, 200, 20, 2,
          200, 2, 20, 2000, "200", "20", "2000", "2",
          2, 20, 200, "2", "20", "200", 2000, "2000",
          2, 20, 200, 2000, "2", "20", "200", "2000",
          "2000", 2000, "200", "20", "2", 200, 20, 2,
          "2000", "200", "20", "2", 2000, 200, 20, 2,
          200, 2, 20, 2000, "200", "20", "2000", "2"),
        row(
          3, 30, 300, "3", "30", "300", 3000, "3000",
          3, 30, 300, 3000, "3", "30", "300", "3000",
          "3000", 3000, "300", "30", "3", 300, 30, 3,
          "3000", "300", "30", "3", 3000, 300, 30, 3,
          300, 3, 30, 3000, "300", "30", "3000", "3",
          3, 30, 300, "3", "30", "300", 3000, "3000",
          3, 30, 300, 3000, "3", "30", "300", "3000",
          "3000", 3000, "300", "30", "3", 300, 30, 3,
          "3000", "300", "30", "3", 3000, 300, 30, 3,
          300, 3, 30, 3000, "300", "30", "3000", "3")
      ))
  }

  @Test
  def testSelectWithNaming(): Unit = {
    checkResult(
      "SELECT `1-_./Ü`, b, c FROM (SELECT a as `1-_./Ü`, b, c FROM Table3)",
      data3)
  }

  @Test(expected = classOf[ValidationException])
  def testInvalidFields(): Unit = {
    checkResult(
      "SELECT a, foo FROM Table3",
      data3)
  }

  @Test
  def testAllRejectingFilter(): Unit = {
    checkResult(
      "SELECT * FROM Table3 WHERE false",
      Seq())
  }

  @Test
  def testAllPassingFilter(): Unit = {
    checkResult(
      "SELECT * FROM Table3 WHERE true",
      data3)
  }

  @Test
  def testFilterOnString(): Unit = {
    checkResult(
      "SELECT * FROM Table3 WHERE c LIKE '%world%'",
      Seq(
        row(3, 2L, "Hello world"),
        row(4, 3L, "Hello world, how are you?")
      ))
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
      ))
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
      ))
  }

  @Test
  def testAdvancedDataTypes(): Unit = {
    val data = Seq(
      row(
        UTCDate("1984-07-12"),
        UTCTime("14:34:24"),
        UTCTimestamp("1984-07-12 14:34:24")))
    registerCollection(
      "MyTable", data, new RowTypeInfo(DATE, TIME, TIMESTAMP), "a, b, c")

    checkResult(
      "SELECT a, b, c, DATE '1984-07-12', TIME '14:34:24', " +
          "TIMESTAMP '1984-07-12 14:34:24' FROM MyTable",
      Seq(
        row(
          UTCDate("1984-07-12"),
          UTCTime("14:34:24"),
          UTCTimestamp("1984-07-12 14:34:24"),
          UTCDate("1984-07-12"),
          UTCTime("14:34:24"),
          UTCTimestamp("1984-07-12 14:34:24"))))

    checkResult(
      "SELECT a, b, c, DATE '1984-07-12', TIME '14:34:24', " +
          "TIMESTAMP '1984-07-12 14:34:24' FROM MyTable " +
          "WHERE a = '1984-07-12' and b = '14:34:24' and c = '1984-07-12 14:34:24'",
      Seq(
        row(
          UTCDate("1984-07-12"),
          UTCTime("14:34:24"),
          UTCTimestamp("1984-07-12 14:34:24"),
          UTCDate("1984-07-12"),
          UTCTime("14:34:24"),
          UTCTimestamp("1984-07-12 14:34:24"))))

    checkResult(
      "SELECT a, b, c, DATE '1984-07-12', TIME '14:34:24', " +
          "TIMESTAMP '1984-07-12 14:34:24' FROM MyTable " +
          "WHERE '1984-07-12' = a and '14:34:24' = b and '1984-07-12 14:34:24' = c",
      Seq(
        row(
          UTCDate("1984-07-12"),
          UTCTime("14:34:24"),
          UTCTimestamp("1984-07-12 14:34:24"),
          UTCDate("1984-07-12"),
          UTCTime("14:34:24"),
          UTCTimestamp("1984-07-12 14:34:24"))))
  }

  @Test
  def testUserDefinedScalarFunction(): Unit = {
    tEnv.registerFunction("hashCode", MyHashCode)
    val data = Seq(row("a"), row("b"), row("c"))
    registerCollection("MyTable", data, new RowTypeInfo(STRING_TYPE_INFO), "text")

    checkResult(
      "SELECT hashCode(text) FROM MyTable",
      Seq(row(97), row(98), row(99)
      ))
  }

  @Test
  def testUDFWithInternalClass(): Unit = {
    tEnv.registerFunction("func", BinaryStringFunction)
    val data = Seq(row("a"), row("b"), row("c"))
    registerCollection("MyTable", data, new RowTypeInfo(STRING_TYPE_INFO), "text")

    checkResult(
      "SELECT func(text) FROM MyTable",
      Seq(row("a"), row("b"), row("c")
      ))
  }

  @Test
  def testTimeUDF(): Unit = {
    tEnv.registerFunction("func", DateFunction)
    val data = Seq(row(UTCDate("1984-07-12")))
    registerCollection("MyTable", data, new RowTypeInfo(DATE), "a")
    checkResult("SELECT func(a) FROM MyTable", Seq(row(UTCDate("1984-07-12"))))
  }

  @Test
  def testBinary(): Unit = {
    val data = Seq(row(1, 2, "hehe".getBytes))
    registerCollection(
      "MyTable",
      data,
      new RowTypeInfo(INT_TYPE_INFO, INT_TYPE_INFO, BYTE_PRIMITIVE_ARRAY_TYPE_INFO),
      "a, b, c")

    checkResult(
      "SELECT a, b, c FROM MyTable",
      data)
  }

  @Test
  def testUserDefinedScalarFunctionWithParameter(): Unit = {
    tEnv.registerFunction("RichFunc2", new RichFunc2)
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
    tEnv.registerFunction("RichFunc3", new RichFunc3)

    checkResult(
      "SELECT c FROM SmallTable3 where RichFunc3(c)=true",
      Seq(row("Hello"))
    )
  }

  @Test
  def testMultipleUserDefinedScalarFunctions(): Unit = {
    tEnv.registerFunction("RichFunc1", new RichFunc1)
    tEnv.registerFunction("RichFunc2", new RichFunc2)
    UserDefinedFunctionTestUtils.setJobParameters(env, Map("string.value" -> "Abc"))

    checkResult(
      "SELECT c FROM SmallTable3 where RichFunc2(c)='Abc#Hello' or RichFunc1(a)=3 and b=2",
      Seq(row("Hello"), row("Hello world"))
    )
  }

  @Test
  def testExternalTypeFunc1(): Unit = {
    tEnv.registerFunction("func1", RowFunc)
    tEnv.registerFunction("rowToStr", RowToStrFunc)
    tEnv.registerFunction("func2", ListFunc)
    tEnv.registerFunction("func3", StringFunc)
    val data = Seq(row("a"), row("b"), row("c"))
    registerCollection("MyTable", data, new RowTypeInfo(STRING_TYPE_INFO), "text")

    checkResult(
      "SELECT rowToStr(func1(text)), func2(text), func3(text) FROM MyTable",
      Seq(
        row("a", util.Arrays.asList("a"), "a"),
        row("b", util.Arrays.asList("b"), "b"),
        row("c", util.Arrays.asList("c"), "c")
      ))
  }

  @Ignore // TODO support agg
  @Test
  def testExternalTypeFunc2(): Unit = {
    tEnv.registerFunction("func1", RowFunc)
    tEnv.registerFunction("rowToStr", RowToStrFunc)
    tEnv.registerFunction("func2", ListFunc)
    tEnv.registerFunction("func3", StringFunc)
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
      ))
  }

  @Test
  def testPojoField(): Unit = {
    val data = Seq(
      row(new MyPojo(5, 105)),
      row(new MyPojo(6, 11)),
      row(new MyPojo(7, 12)))
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
    val data = Seq(
      row(new MyPojo(5, 105)),
      row(new MyPojo(6, 11)),
      row(new MyPojo(7, 12)))
    registerCollection(
      "MyTable",
      data,
      new RowTypeInfo(TypeExtractor.createTypeInfo(classOf[MyPojo])),
      "a")

    //1. external type for udf parameter
    tEnv.registerFunction("pojoFunc", MyPojoFunc)
    tEnv.registerFunction("toPojoFunc", MyToPojoFunc)
    checkResult(
      "SELECT pojoFunc(a) FROM MyTable",
      Seq(row(105), row(11), row(12)))

    //2. external type return in udf
    checkResult(
      "SELECT toPojoFunc(pojoFunc(a)) FROM MyTable",
      Seq(
        row(row(11, 11)),
        row(row(12, 12)),
        row(row(105, 105))))
  }

  // TODO
//  @Test
//  def testUDFWithGetResultTypeFromLiteral(): Unit = {
//    tEnv.registerFunction("hashCode0", LiteralHashCode)
//    tEnv.registerFunction("hashCode1", LiteralHashCode)
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
  def testInSmallValues(): Unit = {
    checkResult(
      "SELECT a FROM Table3 WHERE a in (1, 2)",
      Seq(row(1), row(2)))

    checkResult(
      "SELECT a FROM Table3 WHERE a in (1, 2) and b = 2",
      Seq(row(2)))
  }

  @Test
  def testInLargeValues(): Unit = {
    checkResult(
      "SELECT a FROM Table3 WHERE a in (1, 2, 3, 4, 5)",
      Seq(row(1), row(2), row(3), row(4), row(5)))

    checkResult(
      "SELECT a FROM Table3 WHERE a in (1, 2, 3, 4, 5) and b = 2",
      Seq(row(2), row(3)))

    checkResult(
      "SELECT c FROM Table3 WHERE c in ('Hi', 'H2', 'H3', 'H4', 'H5')",
      Seq(row("Hi")))
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
      Seq(row("Hi")))
  }

  @Test
  def testNotInLargeValues(): Unit = {
    checkResult(
      "SELECT a FROM SmallTable3 WHERE a not in (2, 3, 4, 5)",
      Seq(row(1)))

    checkResult(
      "SELECT a FROM SmallTable3 WHERE a not in (2, 3, 4, 5) or b = 2",
      Seq(row(1), row(2), row(3)))

    checkResult(
      "SELECT c FROM SmallTable3 WHERE c not in ('Hi', 'H2', 'H3', 'H4')",
      Seq(row("Hello"), row("Hello world")))
  }

  @Ignore // TODO support substring
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
      Seq(row("Hi")))
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
    val d = Decimal.castFrom(2.0002, 5, 4).toBigDecimal
    checkResult(
      "SELECT ROW(CAST(2.0002 AS DECIMAL(5, 4)), a, c) FROM SmallTable3",
      Seq(
        row(d, 1, "Hi"),
        row(d, 2, "Hello"),
        row(d, 3, "Hello world")
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
  def testValueConstructor(): Unit = {
    val data = Seq(row("foo", 12, UTCTimestamp("1984-07-12 14:34:24")))
    val tpe = new RowTypeInfo(STRING_TYPE_INFO, INT_TYPE_INFO, TIMESTAMP)
    registerCollection("MyTable", data, tpe, Array(false, false, false), "a, b, c")

    val table = parseQuery("SELECT ROW(a, b, c), ARRAY[12, b], MAP[a, c] FROM MyTable " +
        "WHERE (a, b, c) = ('foo', 12, TIMESTAMP '1984-07-12 14:34:24')")
    val result = executeQuery(table)

    val baseRow = result.head.getField(0).asInstanceOf[Row]
    assertEquals(data.head.getField(0), baseRow.getField(0))
    assertEquals(data.head.getField(1), baseRow.getField(1))
    assertEquals(data.head.getField(2), baseRow.getField(2))

    val arr = result.head.getField(1).asInstanceOf[Array[Integer]]
    assertEquals(12, arr(0))
    assertEquals(data.head.getField(1), arr(1))

    val hashMap = result.head.getField(2).asInstanceOf[util.HashMap[String, Timestamp]]
    assertEquals(data.head.getField(2),
      hashMap.get(data.head.getField(0).asInstanceOf[String]))
  }

  @Test
  def testSelectStarFromNestedTable(): Unit = {

    val table = BatchScalaTableEnvUtil.fromCollection(tEnv, Seq(
      ((0, 0), "0"),
      ((1, 1), "1"),
      ((2, 2), "2")
    ))
    tEnv.registerTable("MyTable", table)

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
    val table = BatchScalaTableEnvUtil.fromCollection(tEnv, Seq(
      (0L, "0"),
      (1L, "1"),
      (2L, "2")
    ), "a, b")
    tEnv.registerTable("MyTable", table)

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

  @Ignore //TODO support cast string to bigint.
  @Test
  def testSelectStarFromNestedValues2(): Unit = {
    val table = BatchScalaTableEnvUtil.fromCollection(tEnv, Seq(
      (0L, "0"),
      (1L, "1"),
      (2L, "2")
    ), "a, b")
    tEnv.registerTable("MyTable", table)
    checkResult(
      "select * from (select ARRAY[a,cast(b as BIGINT)], a from MyTable)",
      Seq(
        row("[0, 0]", 0),
        row("[1, 1]", 1),
        row("[2, 2]", 2)
      )
    )
  }

  @Ignore // TODO support Unicode
  @Test
  def testFunctionWithUnicodeParameters(): Unit = {
    val data = List(
      ("a\u0001b", "c\"d", "e\"\u0004f"), // uses Java/Scala escaping
      ("x\u0001y", "y\"z", "z\"\u0004z")
    )

    val splitUDF0 = new SplitUDF(deterministic = true)
    val splitUDF1 = new SplitUDF(deterministic = false)

    tEnv.registerFunction("splitUDF0", splitUDF0)
    tEnv.registerFunction("splitUDF1", splitUDF1)

    val t1 = BatchScalaTableEnvUtil.fromCollection(tEnv, data, "a, b, c")
    tEnv.registerTable("T1", t1)
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
      Seq(
        row("a", "a", "d", "d", "e", "e"),
        row("x", "x", "z", "z", "z", "z"))
    )
  }

  @Test
  def testCast(): Unit = {
    checkResult(
      "SELECT CAST(a AS VARCHAR(10)) FROM Table3 WHERE CAST(a AS VARCHAR(10)) = '1'",
      Seq(row(1)))
  }

  @Test
  def testLike(): Unit = {
    checkResult(
      "SELECT a FROM NullTable3 WHERE c LIKE '%llo%'",
      Seq(row(2), row(3), row(4)))

    checkResult(
      "SELECT a FROM NullTable3 WHERE CAST(a as VARCHAR(10)) LIKE CAST(b as VARCHAR(10))",
      Seq(row(1), row(2)))

    checkResult(
      "SELECT a FROM NullTable3 WHERE c NOT LIKE '%Comment%' AND c NOT LIKE '%Hello%'",
      Seq(row(1), row(5), row(6), row(null), row(null)))

    checkResult(
      "SELECT a FROM NullTable3 WHERE c LIKE 'Comment#%' and c LIKE '%2'",
      Seq(row(8), row(18)))

    checkResult(
      "SELECT a FROM NullTable3 WHERE c LIKE 'Comment#12'",
      Seq(row(18)))

    checkResult(
      "SELECT a FROM NullTable3 WHERE c LIKE '%omm%nt#12'",
      Seq(row(18)))
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

    BatchScalaTableEnvUtil.registerCollection(tEnv, "MyT", rows, "a, b")

    checkResult(
      "SELECT a FROM MyT WHERE b LIKE '%ha?_ha%' ESCAPE '?'",
      Seq(row(1), row(2), row(3)))

    checkResult(
      "SELECT a FROM MyT WHERE b LIKE '%ha?_ha' ESCAPE '?'",
      Seq(row(1)))

    checkResult(
      "SELECT a FROM MyT WHERE b LIKE 'ha?_ha%' ESCAPE '?'",
      Seq(row(1)))

    checkResult(
      "SELECT a FROM MyT WHERE b LIKE 'ha?_ha' ESCAPE '?'",
      Seq(row(1)))

    checkResult(
      "SELECT a FROM MyT WHERE b LIKE '%affh%ha?_ha%' ESCAPE '?'",
      Seq(row(3)))

    checkResult(
      "SELECT a FROM MyT WHERE b LIKE 'a?%?_ha' ESCAPE '?'",
      Seq(row(5)))

    checkResult(
      "SELECT a FROM MyT WHERE b LIKE 'h_?_ha' ESCAPE '?'",
      Seq(row(1)))
  }

  @Test
  def testChainLike(): Unit = {
    // special case to test CHAIN_PATTERN.
    checkResult(
      "SELECT a FROM NullTable3 WHERE c LIKE '% /sys/kvengine/KVServerRole/kvengine/kv_server%'",
      Seq())

    // special case to test CHAIN_PATTERN.
    checkResult(
      "SELECT a FROM NullTable3 WHERE c LIKE '%Tuple%%'",
      Seq(row(null), row(null)))

    // special case to test CHAIN_PATTERN.
    checkResult(
      "SELECT a FROM NullTable3 WHERE c LIKE '%/order/inter/touch/backwayprice.do%%'",
      Seq())
  }

  @Test
  def testEqual(): Unit = {
    checkResult(
      "SELECT a FROM Table3 WHERE c = 'Hi'",
      Seq(row(1)))

    checkResult(
      "SELECT c FROM Table3 WHERE c <> 'Hello' AND b = 2",
      Seq(row("Hello world")))
  }

  @Test
  def testSubString(): Unit = {
    checkResult(
      "SELECT SUBSTRING(c, 6, 13) FROM Table3 WHERE a = 6",
      Seq(row("Skywalker")))
  }

  @Test
  def testConcat(): Unit = {
    checkResult(
      "SELECT CONCAT(c, '-haha') FROM Table3 WHERE a = 1",
      Seq(row("Hi-haha")))

    checkResult(
      "SELECT CONCAT_WS('-x-', c, 'haha') FROM Table3 WHERE a = 1",
      Seq(row("Hi-x-haha")))
  }

  @Test
  def testStringAgg(): Unit = {
    checkResult(
      "SELECT MIN(c) FROM NullTable3",
      Seq(row("Comment#1")))

    checkResult(
      "SELECT SUM(b) FROM NullTable3 WHERE c = 'NullTuple' OR c LIKE '%Hello world%' GROUP BY c",
      Seq(row(1998), row(2), row(3)))
  }

  @Test
  def testStringUdf(): Unit = {
    tEnv.registerFunction("myFunc", MyStringFunc)
    checkResult(
      "SELECT myFunc(c) FROM Table3 WHERE a = 1",
      Seq(row("Hihaha")))
  }

  @Test
  def testNestUdf(): Unit = {
    tEnv.registerFunction("func", MyStringFunc)
    checkResult(
      "SELECT func(func(func(c))) FROM SmallTable3",
      Seq(row("Hello worldhahahahahaha"), row("Hellohahahahahaha"), row("Hihahahahahaha")))
  }


  @Test
  def testCurrentDate(): Unit = {
    // Execution in on Query should return the same value
    checkResult("SELECT CURRENT_DATE = CURRENT_DATE FROM testTable WHERE a = TRUE",
      Seq(row(true)))

    val d0 = DateConverter.INSTANCE.toInternal(new Date(System.currentTimeMillis()))

    val table = parseQuery("SELECT CURRENT_DATE FROM testTable WHERE a = TRUE")
    val result = executeQuery(table)
    val d1 = DateConverter.INSTANCE.toInternal(
      result.toList.head.getField(0).asInstanceOf[java.sql.Date])

    Assert.assertTrue(d0 <= d1 && d1 - d0 <= 1)
  }

  @Test
  def testCurrentTimestamp(): Unit = {
    // Execution in on Query should return the same value
    checkResult("SELECT CURRENT_TIMESTAMP = CURRENT_TIMESTAMP FROM testTable WHERE a = TRUE",
      Seq(row(true)))

    // CURRENT_TIMESTAMP should return the current timestamp
    val ts0 = System.currentTimeMillis()

    val table = parseQuery("SELECT CURRENT_TIMESTAMP FROM testTable WHERE a = TRUE")
    val result = executeQuery(table)
    val ts1 = TimestampConverter.INSTANCE.toInternal(
      result.toList.head.getField(0).asInstanceOf[java.sql.Timestamp])

    val ts2 = System.currentTimeMillis()

    Assert.assertTrue(ts0 <= ts1 && ts1 <= ts2)
  }

  @Test
  def testCurrentTime(): Unit = {
    // Execution in on Query should return the same value
    checkResult("SELECT CURRENT_TIME = CURRENT_TIME FROM testTable WHERE a = TRUE",
      Seq(row(true)))
  }

  def testTimestampCompareWithDate(): Unit = {
    checkResult("SELECT j FROM testTable WHERE j < DATE '2017-11-11'",
      Seq(row(true)))
  }

  /**
    * TODO Support below string timestamp format to cast to timestamp:
    * yyyy
    * yyyy-[m]m
    * yyyy-[m]m-[d]d
    * yyyy-[m]m-[d]d
    * yyyy-[m]m-[d]d [h]h:[m]m:[s]s.[ms][ms][ms][us][us][us]
    * yyyy-[m]m-[d]d [h]h:[m]m:[s]s.[ms][ms][ms][us][us][us]Z
    * yyyy-[m]m-[d]d [h]h:[m]m:[s]s.[ms][ms][ms][us][us][us]-[h]h:[m]m
    * yyyy-[m]m-[d]d [h]h:[m]m:[s]s.[ms][ms][ms][us][us][us]+[h]h:[m]m
    * yyyy-[m]m-[d]dT[h]h:[m]m:[s]s.[ms][ms][ms][us][us][us]
    * yyyy-[m]m-[d]dT[h]h:[m]m:[s]s.[ms][ms][ms][us][us][us]Z
    * yyyy-[m]m-[d]dT[h]h:[m]m:[s]s.[ms][ms][ms][us][us][us]-[h]h:[m]m
    * yyyy-[m]m-[d]dT[h]h:[m]m:[s]s.[ms][ms][ms][us][us][us]+[h]h:[m]m
    * [h]h:[m]m:[s]s.[ms][ms][ms][us][us][us]
    * [h]h:[m]m:[s]s.[ms][ms][ms][us][us][us]Z
    * [h]h:[m]m:[s]s.[ms][ms][ms][us][us][us]-[h]h:[m]m
    * [h]h:[m]m:[s]s.[ms][ms][ms][us][us][us]+[h]h:[m]m
    * T[h]h:[m]m:[s]s.[ms][ms][ms][us][us][us]
    * T[h]h:[m]m:[s]s.[ms][ms][ms][us][us][us]Z
    * T[h]h:[m]m:[s]s.[ms][ms][ms][us][us][us]-[h]h:[m]m
    * T[h]h:[m]m:[s]s.[ms][ms][ms][us][us][us]+[h]h:[m]m
    */
  @Ignore
  @Test
  def testTimestampCompareWithDateString(): Unit = {
    //j 2015-05-20 10:00:00.887
    checkResult("SELECT j FROM testTable WHERE j < '2017-11-11'",
      Seq(row(UTCTimestamp("2015-05-20 10:00:00.887"))))
  }

  @Test
  def testDateCompareWithDateString(): Unit = {
    checkResult("SELECT h FROM testTable WHERE h <= '2017-12-12'",
      Seq(
        row(UTCDate("2017-12-12")),
        row(UTCDate("2017-12-12"))
      ))
  }

  @Test
  def testDateEqualsWithDateString(): Unit = {
    checkResult("SELECT h FROM testTable WHERE h = '2017-12-12'",
      Seq(
        row(UTCDate("2017-12-12")),
        row(UTCDate("2017-12-12"))
      ))
  }

  @Test
  def testDateFormat(): Unit = {
    //j 2015-05-20 10:00:00.887
    checkResult("SELECT j, " +
        " DATE_FORMAT(j, 'yyyy/MM/dd HH:mm:ss')," +
        " DATE_FORMAT('2015-05-20 10:00:00.887', 'yyyy/MM/dd HH:mm:ss')," +
        " DATE_FORMAT('2015-05-20 10:00:00.887', 'yyyy-MM-dd HH:mm:ss', 'yyyy/MM/dd HH:mm:ss')" +
        " FROM testTable WHERE a = TRUE",
      Seq(
        row(UTCTimestamp("2015-05-20 10:00:00.887"),
          "2015/05/20 10:00:00",
          "2015/05/20 10:00:00",
          "2015/05/20 10:00:00")
      ))
  }

  @Test
  def testYear(): Unit = {
    checkResult("SELECT j, YEAR(j) FROM testTable WHERE a = TRUE",
      Seq(row(UTCTimestamp("2015-05-20 10:00:00.887"), "2015")))
  }

  @Test
  def testQuarter(): Unit = {
    checkResult("SELECT j, QUARTER(j) FROM testTable WHERE a = TRUE",
      Seq(row(UTCTimestamp("2015-05-20 10:00:00.887"), "2")))
  }

  @Test
  def testMonth(): Unit = {
    checkResult("SELECT j, MONTH(j) FROM testTable WHERE a = TRUE",
      Seq(row(UTCTimestamp("2015-05-20 10:00:00.887"), "5")))
  }

  @Test
  def testWeek(): Unit = {
    checkResult("SELECT j, WEEK(j) FROM testTable WHERE a = TRUE",
      Seq(row(UTCTimestamp("2015-05-20 10:00:00.887"), "21")))
  }

  @Test
  def testDayOfYear(): Unit = {
    checkResult("SELECT j, DAYOFYEAR(j) FROM testTable WHERE a = TRUE",
      Seq(row(UTCTimestamp("2015-05-20 10:00:00.887"), "140")))
  }

  @Test
  def testDayOfMonth(): Unit = {
    checkResult("SELECT j, DAYOFMONTH(j) FROM testTable WHERE a = TRUE",
      Seq(row(UTCTimestamp("2015-05-20 10:00:00.887"), "20")))
  }

  @Test
  def testDayOfWeek(): Unit = {
    checkResult("SELECT j, DAYOFWEEK(j) FROM testTable WHERE a = TRUE",
      Seq(row(UTCTimestamp("2015-05-20 10:00:00.887"), "4")))
  }

  @Test
  def testHour(): Unit = {
    checkResult("SELECT j, HOUR(j) FROM testTable WHERE a = TRUE",
      Seq(row(UTCTimestamp("2015-05-20 10:00:00.887"), "10")))
  }

  @Test
  def testMinute(): Unit = {
    checkResult("SELECT j, MINUTE(j) FROM testTable WHERE a = TRUE",
      Seq(row(UTCTimestamp("2015-05-20 10:00:00.887"), "0")))
  }

  @Test
  def testSecond(): Unit = {
    checkResult("SELECT j, SECOND(j) FROM testTable WHERE a = TRUE",
      Seq(row(UTCTimestamp("2015-05-20 10:00:00.887"), "0")))
  }

  @Test
  def testUnixTimestamp(): Unit = {
    checkResult("SELECT" +
        " UNIX_TIMESTAMP('2017-12-13 19:25:30')," +
        " UNIX_TIMESTAMP('2017-12-13 19:25:30', 'yyyy-MM-dd HH:mm:ss')" +
        " FROM testTable WHERE a = TRUE",
      Seq(row(1513193130, 1513193130)))
  }

  @Test
  def testFromUnixTime(): Unit = {
    checkResult("SELECT" +
        " FROM_UNIXTIME(1513193130), FROM_UNIXTIME(1513193130, 'MM/dd/yyyy HH:mm:ss')" +
        " FROM testTable WHERE a = TRUE",
      Seq(row("2017-12-13 19:25:30", "12/13/2017 19:25:30")))
  }

  @Test
  def testDateDiff(): Unit = {
    checkResult("SELECT" +
        " DATEDIFF('2017-12-14 01:00:34', '2016-12-14 12:00:00')," +
        " DATEDIFF(TIMESTAMP '2017-12-14 01:00:23', '2016-08-14 12:00:00')," +
        " DATEDIFF('2017-12-14 09:00:23', TIMESTAMP '2013-08-19 11:00:00')," +
        " DATEDIFF(TIMESTAMP '2017-12-14 09:00:23', TIMESTAMP '2018-08-19 11:00:00')" +
        " FROM testTable WHERE a = TRUE",
      Seq(row(365, 487, 1578, -248)))
  }

  @Test
  def testDateSub(): Unit = {
    checkResult("SELECT" +
        " DATE_SUB(TIMESTAMP '2017-12-14 09:00:23', 3)," +
        " DATE_SUB('2017-12-14 09:00:23', -3)" +
        " FROM testTable WHERE a = TRUE",
      Seq(row("2017-12-11", "2017-12-17")))
  }

  @Test
  def testDateAdd(): Unit = {
    checkResult("SELECT" +
        " DATE_ADD('2017-12-14', 4)," +
        " DATE_ADD(TIMESTAMP '2017-12-14 09:10:20',-4)" +
        " FROM testTable WHERE a = TRUE",
      Seq(row("2017-12-18", "2017-12-10")))
  }

  @Test
  def testToDate(): Unit = {
    checkResult("SELECT" +
        " TO_DATE(CAST(null AS VARCHAR))," +
        " TO_DATE('2016-12-31')," +
        " TO_DATE('2016-12-31', 'yyyy-MM-dd')",
      Seq(row(null, UTCDate("2016-12-31"), UTCDate("2016-12-31"))))
  }

  @Test
  def testToTimestamp(): Unit = {
    checkResult("SELECT" +
        " TO_TIMESTAMP(CAST(null AS VARCHAR))," +
        " TO_TIMESTAMP('2016-12-31 00:12:00')," +
        " TO_TIMESTAMP('2016-12-31', 'yyyy-MM-dd')",
      Seq(row(null, UTCTimestamp("2016-12-31 00:12:00"), UTCTimestamp("2016-12-31 00:00:00"))))
  }

  @Test
  def testCalcBinary(): Unit = {
    registerCollection(
      "BinaryT",
      nullData3.map((r) => row(r.getField(0), r.getField(1), r.getField(2).toString.getBytes)),
      new RowTypeInfo(INT_TYPE_INFO, LONG_TYPE_INFO, BYTE_PRIMITIVE_ARRAY_TYPE_INFO),
      "a, b, c",
      nullablesOfNullData3)
    checkResult(
      "select a, b, c from BinaryT where b < 1000",
      nullData3.map((r) => row(r.getField(0), r.getField(1), r.getField(2).toString.getBytes))
    )
  }

  @Test(expected = classOf[UnsupportedOperationException])
  def testOrderByBinary(): Unit = {
    registerCollection(
      "BinaryT",
      nullData3.map((r) => row(r.getField(0), r.getField(1), r.getField(2).toString.getBytes)),
      new RowTypeInfo(INT_TYPE_INFO, LONG_TYPE_INFO, BYTE_PRIMITIVE_ARRAY_TYPE_INFO),
      "a, b, c",
      nullablesOfNullData3)
    conf.getConf.setInteger(TableConfigOptions.SQL_RESOURCE_DEFAULT_PARALLELISM, 1)
    conf.getConf.setBoolean(TableConfigOptions.SQL_EXEC_SORT_RANGE_ENABLED, true)
    checkResult(
      "select * from BinaryT order by c",
      nullData3.sortBy((x : Row) =>
        x.getField(2).asInstanceOf[String]).map((r) =>
        row(r.getField(0), r.getField(1), r.getField(2).toString.getBytes)),
      isSorted = true
    )
  }

  @Test
  def testGroupByBinary(): Unit = {
    registerCollection(
      "BinaryT2",
      nullData3.map((r) => row(r.getField(0), r.getField(1).toString.getBytes, r.getField(2))),
      new RowTypeInfo(INT_TYPE_INFO, BYTE_PRIMITIVE_ARRAY_TYPE_INFO, STRING_TYPE_INFO),
      "a, b, c",
      nullablesOfNullData3)
    checkResult(
      "select sum(sumA) from (select sum(a) as sumA, b, c from BinaryT2 group by c, b) group by b",
      Seq(row(1), row(111), row(15), row(34), row(5), row(65), row(null))
    )
  }
}
