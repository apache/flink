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

import org.apache.flink.api.common.typeinfo.BasicTypeInfo.{INT_TYPE_INFO, STRING_TYPE_INFO}
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO
import org.apache.flink.api.common.typeinfo.SqlTimeTypeInfo.{DATE, TIME, TIMESTAMP}
import org.apache.flink.api.common.typeinfo.{SqlTimeTypeInfo, TypeInformation}
import org.apache.flink.api.java.typeutils.{ListTypeInfo, PojoField, PojoTypeInfo, RowTypeInfo, TypeExtractor}
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.table.api.ValidationException
import org.apache.flink.table.dataformat.{BaseRow, BinaryString, Decimal}
import org.apache.flink.table.expressions.utils.{RichFunc1, RichFunc2, RichFunc3, SplitUDF}
import org.apache.flink.table.functions.ScalarFunction
import org.apache.flink.table.runtime.utils.TestData._
import org.apache.flink.table.runtime.utils.{BatchScalaTableEnvUtil, BatchTestBase, UserDefinedFunctionTestUtils}
import org.apache.flink.table.runtime.utils.BatchTestBase.row
import org.apache.flink.table.util.DateTimeTestUtil._
import org.apache.flink.types.Row

import org.junit.Assert.assertEquals
import org.junit._

import java.sql.Timestamp
import java.util

import scala.collection.JavaConversions._
import scala.collection.Seq

class CalcITCase extends BatchTestBase {

  @Before
  def before(): Unit = {
    registerCollection("Table3", data3, type3, nullablesOfData3, "a, b, c")
    registerCollection("SmallTable3", smallData3, type3, nullablesOfData3, "a, b, c")
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

  @Ignore // TODO support like
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

  @Ignore // TODO support substring
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

  @Ignore // TODO support decimal with precision and scale
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
}

object MyHashCode extends ScalarFunction {
  def eval(s: String): Int = s.hashCode()
}

object OldHashCode extends ScalarFunction {
  def eval(s: String): Int = -1
}

object StringFunction extends ScalarFunction {
  def eval(s: String): String = s
}

object BinaryStringFunction extends ScalarFunction {
  def eval(s: BinaryString): BinaryString = s
}

object DateFunction extends ScalarFunction {
  def eval(d: Integer): Integer = d

  override def getResultType(signature: Array[Class[_]]): TypeInformation[_] =
    SqlTimeTypeInfo.DATE
}

// Understand type: Row wrapped as TypeInfoWrappedDataType.
object RowFunc extends ScalarFunction {
  def eval(s: String): Row = Row.of(s)

  override def getResultType(signature: Array[Class[_]]) =
    new RowTypeInfo(Types.STRING)
}

object RowToStrFunc extends ScalarFunction {
  def eval(s: BaseRow): String = s.getString(0).toString
}

// generic.
object ListFunc extends ScalarFunction {
  def eval(s: String): java.util.List[String] = util.Arrays.asList(s)

  override def getResultType(signature: Array[Class[_]]) =
    new ListTypeInfo(Types.STRING)
}

// internal but wrapped as TypeInfoWrappedDataType.
object StringFunc extends ScalarFunction {
  def eval(s: String): String = s

  override def getResultType(signature: Array[Class[_]]): TypeInformation[String] =
    Types.STRING
}

class MyPojo() {
  var f1: Int = 0
  var f2: Int = 0

  def this(f1: Int, f2: Int) {
    this()
    this.f1 = f1
    this.f2 = f2
  }

  override def equals(other: Any): Boolean = other match {
    case that: MyPojo =>
      (that canEqual this) &&
          f1 == that.f1 &&
          f2 == that.f2
    case _ => false
  }

  def canEqual(other: Any): Boolean = other.isInstanceOf[MyPojo]

  override def toString = s"MyPojo($f1, $f2)"
}

object MyPojoFunc extends ScalarFunction {
  def eval(s: MyPojo): Int = s.f2

  override def getParameterTypes(signature: Array[Class[_]]): Array[TypeInformation[_]] =
    Array(MyToPojoFunc.getResultType(signature))
}

object MyToPojoFunc extends ScalarFunction {
  def eval(s: Int): MyPojo = new MyPojo(s, s)

  override def getResultType(signature: Array[Class[_]]): PojoTypeInfo[MyPojo] = {
    val cls = classOf[MyPojo]
    new PojoTypeInfo[MyPojo](classOf[MyPojo], Seq(
      new PojoField(cls.getDeclaredField("f1"), Types.INT),
      new PojoField(cls.getDeclaredField("f2"), Types.INT)))
  }
}

// TODO support getResultType from literal args.
//object LiteralHashCode extends ScalarFunction {
//  def eval(s: String, t: String): Any = {
//    val code = s.hashCode()
//    if (t == "string") "str" + code else if (t == "int") code else throw new RuntimeException
//  }
//
//  override def getResultType(arguments: Array[AnyRef], signature: Array[Class[_]]) = {
//    if (arguments(1) == "string") {
//      Types.STRING
//    } else if (arguments(1) == "int") {
//      Types.INT
//    } else {
//      throw new RuntimeException
//    }
//  }
//}
