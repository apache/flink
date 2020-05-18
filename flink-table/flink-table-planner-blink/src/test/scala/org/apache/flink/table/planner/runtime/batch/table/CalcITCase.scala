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

package org.apache.flink.table.planner.runtime.batch.table

import org.apache.flink.api.scala._
import org.apache.flink.table.api.DataTypes._
import org.apache.flink.table.api._
import org.apache.flink.table.data.DecimalDataUtils
import org.apache.flink.table.functions.ScalarFunction
import org.apache.flink.table.planner.expressions.utils._
import org.apache.flink.table.planner.runtime.utils.TestData._
import org.apache.flink.table.planner.runtime.utils.{BatchTableEnvUtil, BatchTestBase, CollectionBatchExecTable, UserDefinedFunctionTestUtils}
import org.apache.flink.table.planner.utils.DateTimeTestUtil.localDateTime
import org.apache.flink.test.util.TestBaseUtils
import org.apache.flink.test.util.TestBaseUtils.compareResultAsText
import org.apache.flink.types.Row

import org.junit.Assert.assertEquals
import org.junit.{Before, Test}

import java.sql.{Date, Time, Timestamp}
import java.time.LocalDateTime
import java.util

import scala.collection.JavaConverters._
import scala.collection.{Seq, mutable}

class CalcITCase extends BatchTestBase {

  @Before
  override def before(): Unit = {
    super.before()
    registerCollection("Table3", data3, type3, "a, b, c", nullablesOfData3)
  }

  @Test
  def testSimpleSelectAll(): Unit = {
    val t = CollectionBatchExecTable.get3TupleDataSet(tEnv).select('_1, '_2, '_3)

    val expected = "1,1,Hi\n" + "2,2,Hello\n" + "3,2,Hello world\n" +
      "4,3,Hello world, how are you?\n" + "5,3,I am fine.\n" + "6,3,Luke Skywalker\n" +
      "7,4,Comment#1\n" + "8,4,Comment#2\n" + "9,4,Comment#3\n" + "10,4,Comment#4\n" +
      "11,5,Comment#5\n" + "12,5,Comment#6\n" + "13,5,Comment#7\n" + "14,5,Comment#8\n" +
      "15,5,Comment#9\n" + "16,6,Comment#10\n" + "17,6,Comment#11\n" + "18,6,Comment#12\n" +
      "19,6,Comment#13\n" + "20,6,Comment#14\n" + "21,6,Comment#15\n"
    val results = executeQuery(t)
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testSimpleSelectAllWithAs(): Unit = {
    val t = CollectionBatchExecTable.get3TupleDataSet(tEnv, "a, b, c").select('a, 'b, 'c)

    val expected = "1,1,Hi\n" + "2,2,Hello\n" + "3,2,Hello world\n" +
      "4,3,Hello world, how are you?\n" + "5,3,I am fine.\n" + "6,3,Luke Skywalker\n" +
      "7,4,Comment#1\n" + "8,4,Comment#2\n" + "9,4,Comment#3\n" + "10,4,Comment#4\n" +
      "11,5,Comment#5\n" + "12,5,Comment#6\n" + "13,5,Comment#7\n" + "14,5,Comment#8\n" +
      "15,5,Comment#9\n" + "16,6,Comment#10\n" + "17,6,Comment#11\n" + "18,6,Comment#12\n" +
      "19,6,Comment#13\n" + "20,6,Comment#14\n" + "21,6,Comment#15\n"
    val results = executeQuery(t)
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testSimpleSelectWithNaming(): Unit = {
    val t = CollectionBatchExecTable.get3TupleDataSet(tEnv)
      .select('_1 as 'a, '_2 as 'b, '_1 as 'c)
      .select('a, 'b)

    val expected = "1,1\n" + "2,2\n" + "3,2\n" + "4,3\n" + "5,3\n" + "6,3\n" + "7,4\n" +
      "8,4\n" + "9,4\n" + "10,4\n" + "11,5\n" + "12,5\n" + "13,5\n" + "14,5\n" + "15,5\n" +
      "16,6\n" + "17,6\n" + "18,6\n" + "19,6\n" + "20,6\n" + "21,6\n"
    val results = executeQuery(t)
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testSimpleSelectRenameAll(): Unit = {
    val t = CollectionBatchExecTable.get3TupleDataSet(tEnv)
      .select('_1 as 'a, '_2 as 'b, '_3 as 'c)
      .select('a, 'b)

    val expected = "1,1\n" + "2,2\n" + "3,2\n" + "4,3\n" + "5,3\n" + "6,3\n" + "7,4\n" +
      "8,4\n" + "9,4\n" + "10,4\n" + "11,5\n" + "12,5\n" + "13,5\n" + "14,5\n" + "15,5\n" +
      "16,6\n" + "17,6\n" + "18,6\n" + "19,6\n" + "20,6\n" + "21,6\n"
    val results = executeQuery(t)
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testSelectStar(): Unit = {
    val t = CollectionBatchExecTable.get3TupleDataSet(tEnv, "a, b, c").select('*)

    val expected = "1,1,Hi\n" + "2,2,Hello\n" + "3,2,Hello world\n" +
      "4,3,Hello world, how are you?\n" + "5,3,I am fine.\n" + "6,3,Luke Skywalker\n" +
      "7,4,Comment#1\n" + "8,4,Comment#2\n" + "9,4,Comment#3\n" + "10,4,Comment#4\n" +
      "11,5,Comment#5\n" + "12,5,Comment#6\n" + "13,5,Comment#7\n" + "14,5,Comment#8\n" +
      "15,5,Comment#9\n" + "16,6,Comment#10\n" + "17,6,Comment#11\n" + "18,6,Comment#12\n" +
      "19,6,Comment#13\n" + "20,6,Comment#14\n" + "21,6,Comment#15\n"
    val results = executeQuery(t)
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testAllRejectingFilter(): Unit = {
    val ds = CollectionBatchExecTable.get3TupleDataSet(tEnv, "a, b, c")

    val filterDs = ds.filter(false)

    val expected = "\n"
    val results = executeQuery(filterDs)
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testAllPassingFilter(): Unit = {
    val ds = CollectionBatchExecTable.get3TupleDataSet(tEnv, "a, b, c")

    val filterDs = ds.filter(true)
    val expected = "1,1,Hi\n" + "2,2,Hello\n" + "3,2,Hello world\n" + "4,3,Hello world, " +
      "how are you?\n" + "5,3,I am fine.\n" + "6,3,Luke Skywalker\n" + "7,4," +
      "Comment#1\n" + "8,4,Comment#2\n" + "9,4,Comment#3\n" + "10,4,Comment#4\n" + "11,5," +
      "Comment#5\n" + "12,5,Comment#6\n" + "13,5,Comment#7\n" + "14,5,Comment#8\n" + "15,5," +
      "Comment#9\n" + "16,6,Comment#10\n" + "17,6,Comment#11\n" + "18,6,Comment#12\n" + "19," +
      "6,Comment#13\n" + "20,6,Comment#14\n" + "21,6,Comment#15\n"
    val results = executeQuery(filterDs)
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testFilterOnStringTupleField(): Unit = {
    val ds = CollectionBatchExecTable.get3TupleDataSet(tEnv, "a, b, c")
    val filterDs = ds.filter('c.like("%world%"))

    val expected = "3,2,Hello world\n" + "4,3,Hello world, how are you?\n"
    val results = executeQuery(filterDs)
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testFilterOnIntegerTupleField(): Unit = {
    val ds = CollectionBatchExecTable.get3TupleDataSet(tEnv, "a, b, c")

    val filterDs = ds.filter('a % 2 === 0)

    val expected = "2,2,Hello\n" + "4,3,Hello world, how are you?\n" +
      "6,3,Luke Skywalker\n" + "8,4," + "Comment#2\n" + "10,4,Comment#4\n" +
      "12,5,Comment#6\n" + "14,5,Comment#8\n" + "16,6," +
      "Comment#10\n" + "18,6,Comment#12\n" + "20,6,Comment#14\n"
    val results = executeQuery(filterDs)
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testNotEquals(): Unit = {
    val ds = CollectionBatchExecTable.get3TupleDataSet(tEnv, "a, b, c")

    val filterDs = ds.filter('a % 2 !== 0)
    val expected = "1,1,Hi\n" + "3,2,Hello world\n" +
      "5,3,I am fine.\n" + "7,4,Comment#1\n" + "9,4,Comment#3\n" +
      "11,5,Comment#5\n" + "13,5,Comment#7\n" + "15,5,Comment#9\n" +
      "17,6,Comment#11\n" + "19,6,Comment#13\n" + "21,6,Comment#15\n"
    val results = executeQuery(filterDs)
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }


  @Test
  def testDisjunctivePredicate(): Unit = {
    val ds = CollectionBatchExecTable.get3TupleDataSet(tEnv, "a, b, c")

    val filterDs = ds.filter('a < 2 || 'a > 20)
    val expected = "1,1,Hi\n" + "21,6,Comment#15\n"
    val results = executeQuery(filterDs)
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testConsecutiveFilters(): Unit = {
    val ds = CollectionBatchExecTable.get3TupleDataSet(tEnv, "a, b, c")

    val filterDs = ds.filter('a % 2 !== 0).filter('b % 2 === 0)
    val expected = "3,2,Hello world\n" + "7,4,Comment#1\n" +
      "9,4,Comment#3\n" + "17,6,Comment#11\n" +
      "19,6,Comment#13\n" + "21,6,Comment#15\n"
    val results = executeQuery(filterDs)
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testFilterBasicType(): Unit = {
    val ds = CollectionBatchExecTable.getStringDataSet(tEnv)

    val filterDs = ds.filter('f0.like("H%"))

    val expected = "Hi\n" + "Hello\n" + "Hello world\n" + "Hello world, how are you?\n"
    val results = executeQuery(filterDs)
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testFilterOnCustomType(): Unit = {
    val filterDs = CollectionBatchExecTable.getCustomTypeDataSet(tEnv)
      .filter('myString.like("%a%"))
    val expected = "3,3,Hello world, how are you?\n" + "3,4,I am fine.\n" + "3,5,Luke Skywalker\n"
    val results = executeQuery(filterDs)
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testSimpleCalc(): Unit = {
    val t = CollectionBatchExecTable.get3TupleDataSet(tEnv)
      .select('_1, '_2, '_3)
      .where('_1 < 7)
      .select('_1, '_3)

    val expected = "1,Hi\n" + "2,Hello\n" + "3,Hello world\n" +
      "4,Hello world, how are you?\n" + "5,I am fine.\n" + "6,Luke Skywalker\n"
    val results = executeQuery(t)
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testCalcWithTwoFilters(): Unit = {
    val t = CollectionBatchExecTable.get3TupleDataSet(tEnv)
      .select('_1, '_2, '_3)
      .where('_1 < 7 && '_2 === 3)
      .select('_1, '_3)
      .where('_1 === 4)
      .select('_1)

    val expected = "4\n"
    val results = executeQuery(t)
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testCalcWithAggregation(): Unit = {
    val t = CollectionBatchExecTable.get3TupleDataSet(tEnv)
      .select('_1, '_2, '_3)
      .where('_1 < 15)
      .groupBy('_2)
      .select('_1.min, '_2.count as 'cnt)
      .where('cnt > 3)

    val expected = "7,4\n" + "11,4\n"
    val results = executeQuery(t)
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testCalcJoin(): Unit = {
    val ds1 = CollectionBatchExecTable.getSmall3TupleDataSet(tEnv, "a, b, c")
    val ds2 = CollectionBatchExecTable.get5TupleDataSet(tEnv, "d, e, f, g, h")

    val joinT = ds1.select('a, 'b).join(ds2).where('b === 'e).select('a, 'b, 'd, 'e, 'f)
      .where('b > 1).select('a, 'd).where('d === 2)

    val expected = "2,2\n" + "3,2\n"
    val results = executeQuery(joinT)
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testAdvancedDataTypes(): Unit = {

    val bd1 = BigDecimal("78.454654654654654").bigDecimal
    val bd2 = BigDecimal("4E+16").bigDecimal

    val t = BatchTableEnvUtil.fromCollection(tEnv,
      Seq((bd1, bd2, Date.valueOf("1984-07-12"),
          Time.valueOf("14:34:24"),
          Timestamp.valueOf("1984-07-12 14:34:24"))), "_1, _2, _3, _4, _5")
        .select('_1, '_2, '_3, '_4, '_5, BigDecimal("11.2"), BigDecimal("11.2").bigDecimal,
          Date.valueOf("1984-07-12"), Time.valueOf("14:34:24"),
          Timestamp.valueOf("1984-07-12 14:34:24"))

    // inferred Decimal(p,s) from BigDecimal.class
    val bd1x = bd1.setScale(DecimalDataUtils.DECIMAL_SYSTEM_DEFAULT.getScale)
    val bd2x = bd2.setScale(DecimalDataUtils.DECIMAL_SYSTEM_DEFAULT.getScale)

    val expected = s"$bd1x,$bd2x,1984-07-12,14:34:24,1984-07-12T14:34:24," +
      "11.2,11.2,1984-07-12,14:34:24,1984-07-12T14:34:24"
    val results = executeQuery(t)
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testUserDefinedScalarFunction() {
    registerFunction("hashCode", HashCode)
    val table = BatchTableEnvUtil.fromElements(tEnv, "a", "b", "c")
    val result = table.select("f0.hashCode()")
    val results = executeQuery(result)
    val expected = "97\n98\n99"
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testNumericAutocastInArithmetic() {
    val table = BatchTableEnvUtil.fromElements(tEnv,
      (1.toByte, 1.toShort, 1, 1L, 1.0f, 1.0d, 1L, 1001.1))
      .select('_1 + 1, '_2 + 1, '_3 + 1L, '_4 + 1.0f,
        '_5 + 1.0d, '_6 + 1, '_7 + 1.0d, '_8 + '_1)

    val results = executeQuery(table)
    val expected = "2,2,2,2.0,2.0,2.0,2.0,1002.1"
    compareResultAsText(results.asJava, expected)
  }

  @Test
  def testNumericAutocastInComparison() {
    val table = BatchTableEnvUtil.fromCollection(tEnv,
      Seq(
        (1.toByte, 1.toShort, 1, 1L, 1.0f, 1.0d),
        (2.toByte, 2.toShort, 2, 2L, 2.0f, 2.0d)),
      "a, b, c, d, e, f"
    ).filter('a > 1 && 'b > 1 && 'c > 1L && 'd > 1.0f && 'e > 1.0d && 'f > 1)

    val results = executeQuery(table)
    val expected: String = "2,2,2,2,2.0,2.0"
    compareResultAsText(results.asJava, expected)
  }

  @Test
  def testCasting() {
    val table = BatchTableEnvUtil.fromElements(tEnv, (1, 0.0, 1L, true))
      .select(
        // * -> String
        '_1.cast(STRING), '_2.cast(STRING), '_3.cast(STRING), '_4.cast(STRING),
        // NUMERIC TYPE -> Boolean
        '_1.cast(BOOLEAN), '_2.cast(BOOLEAN), '_3.cast(BOOLEAN),
        // NUMERIC TYPE -> NUMERIC TYPE
        '_1.cast(DOUBLE), '_2.cast(INT), '_3.cast(SMALLINT),
        // Boolean -> NUMERIC TYPE
        '_4.cast(DOUBLE),
        // identity casting
        '_1.cast(INT), '_2.cast(DOUBLE), '_3.cast(BIGINT), '_4.cast(BOOLEAN))

    val results = executeQuery(table)
    val expected = "1,0.0,1,true," + "true,false,true," +
      "1.0,0,1," + "1.0," + "1,0.0,1,true\n"
    compareResultAsText(results.asJava, expected)
  }

  @Test
  def testCastFromString() {
    val table = BatchTableEnvUtil.fromElements(tEnv, ("1", "true", "2.0"))
      .select('_1.cast(TINYINT), '_1.cast(SMALLINT), '_1.cast(INT), '_1.cast(BIGINT),
        '_3.cast(DOUBLE), '_3.cast(FLOAT), '_2.cast(BOOLEAN))

    val results = executeQuery(table)
    val expected = "1,1,1,1,2.0,2.0,true\n"
    compareResultAsText(results.asJava, expected)
  }

  @Test
  def testUserDefinedScalarFunctionWithParameter(): Unit = {
    registerFunction("RichFunc2", new RichFunc2)
    UserDefinedFunctionTestUtils.setJobParameters(env, Map("string.value" -> "ABC"))

    val ds = CollectionBatchExecTable.getSmall3TupleDataSet(tEnv, "a, b, c")
    tEnv.registerTable("t1", ds)

    val sqlQuery = "SELECT c FROM t1 where RichFunc2(c)='ABC#Hello'"

    val result = tEnv.sqlQuery(sqlQuery)

    val expected = "Hello"
    val results = executeQuery(result)
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testUserDefinedScalarFunctionWithDistributedCache(): Unit = {
    val words = "Hello\nWord"
    val filePath = UserDefinedFunctionTestUtils.writeCacheFile("test_words", words)
    env.registerCachedFile(filePath, "words")
    registerFunction("RichFunc3", new RichFunc3)

    val ds = CollectionBatchExecTable.getSmall3TupleDataSet(tEnv, "a, b, c")
    tEnv.registerTable("t1", ds)

    val sqlQuery = "SELECT c FROM t1 where RichFunc3(c)=true"

    val result = tEnv.sqlQuery(sqlQuery)

    val expected = "Hello"
    val results = executeQuery(result)
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testMultipleUserDefinedScalarFunctions(): Unit = {
    registerFunction("RichFunc1", new RichFunc1)
    registerFunction("RichFunc2", new RichFunc2)
    UserDefinedFunctionTestUtils.setJobParameters(env, Map("string.value" -> "Abc"))

    val ds = CollectionBatchExecTable.getSmall3TupleDataSet(tEnv, "a, b, c")
    tEnv.registerTable("t1", ds)

    val sqlQuery = "SELECT c FROM t1 where " +
      "RichFunc2(c)='Abc#Hello' or RichFunc1(a)=3 and b=2"

    val result = tEnv.sqlQuery(sqlQuery)

    val expected = "Hello\nHello world"
    val results = executeQuery(result)
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testScalarFunctionConstructorWithParams(): Unit = {
    val data = List(
      (1, 1L, "Jack#22"),
      (2, 2L, "John#19"),
      (3, 2L, "Anna#44"),
      (4, 3L, "nosharp"))
    val in = BatchTableEnvUtil.fromCollection(tEnv, data, "a, b, c")

    val func0 = new Func13("default")
    val func1 = new Func13("Sunny")
    val func2 = new Func13("kevin2")

    val result = in.select(func0('c), func1('c), func2('c))

    val results = executeQuery(result)

    val expected = "default-Anna#44,Sunny-Anna#44,kevin2-Anna#44\n" +
      "default-Jack#22,Sunny-Jack#22,kevin2-Jack#22\n" +
      "default-John#19,Sunny-John#19,kevin2-John#19\n" +
      "default-nosharp,Sunny-nosharp,kevin2-nosharp"
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testRowType(): Unit = {
    val data = new mutable.MutableList[(Int, Long, String)]
    data.+=((1, 1L, "Jack#22"))
    data.+=((2, 2L, "John#19"))
    data.+=((3, 2L, "Anna#44"))
    data.+=((4, 3L, "nosharp"))
    val in = BatchTableEnvUtil.fromCollection(tEnv, data, "a, b, c")

    // literals
    val result1 = in.select(row(1, "Hi", true))
    executeQuery(result1).foreach { record =>
      val row = record.getField(0).asInstanceOf[Row]
      assertEquals(1, row.getField(0))
      assertEquals("Hi", row.getField(1))
      assertEquals(true, row.getField(2))
    }

    // primitive type
    val result2 = in.select(row(1, 'a, 'b))
    executeQuery(result2).zipWithIndex.foreach { case (record, idx) =>
      val row = record.getField(0).asInstanceOf[Row]
      assertEquals(1, row.getField(0))
      assertEquals(data(idx)._1, row.getField(1))
      assertEquals(data(idx)._2, row.getField(2))
    }

    // non-primitive type
    val d = DecimalDataUtils.castFrom(2.0002, 5, 4)
    val result3 = in.select(row(BigDecimal(2.0002), 'a, 'c))
    executeQuery(result3).zipWithIndex.foreach { case (record, idx) =>
      val row = record.getField(0).asInstanceOf[Row]
      assertEquals(d.toBigDecimal, row.getField(0))
      assertEquals(data(idx)._1, row.getField(1))
      assertEquals(data(idx)._3, row.getField(2))
    }
  }

  @Test
  def testArrayType(): Unit = {
    val in = CollectionBatchExecTable.getSmall3TupleDataSet(tEnv)

    // literals
    val t1 = in.select(array("Hi", "Hello", "How are you"))
    val result1 = executeQuery(t1)
    val expected1 = "[Hi, Hello, How are you]\n" +
      "[Hi, Hello, How are you]\n" +
      "[Hi, Hello, How are you]\n"
    TestBaseUtils.compareResultAsText(result1.asJava, expected1)

    // primitive type
    val t2 = in.select(array(30, '_1, 10))
    val result2 = executeQuery(t2)
    val expected2 = "[30, 1, 10]\n" +
      "[30, 2, 10]\n" +
      "[30, 3, 10]\n"
    TestBaseUtils.compareResultAsText(result2.asJava, expected2)

    // non-primitive type
    val t3 = in.select(array("Test", '_3))
    val result3 = executeQuery(t3)
    val expected3 = "[Test, Hi]\n" +
      "[Test, Hello]\n" +
      "[Test, Hello world]\n"
    TestBaseUtils.compareResultAsText(result3.asJava, expected3)
  }

  @Test
  def testMapType(): Unit = {
    val in = CollectionBatchExecTable.getSmall3TupleDataSet(tEnv)

    // literals
    val t1 = in.select(map(1, "Hello", 2, "Hi"))
    val result1 = executeQuery(t1)
    val expected1 = "{1=Hello, 2=Hi}\n" +
      "{1=Hello, 2=Hi}\n" +
      "{1=Hello, 2=Hi}\n"
    TestBaseUtils.compareResultAsText(result1.asJava, expected1)

    // primitive type
    val t2 = in.select(map('_2, 30, 10L, '_1))
    val result2 = executeQuery(t2)
    val expected2 = "{1=30, 10=1}\n" +
      "{2=30, 10=2}\n" +
      "{2=30, 10=3}\n"
    TestBaseUtils.compareResultAsText(result2.asJava, expected2)

    // non-primitive type
    val t3 = in.select(map('_1, '_3))
    val result3 = executeQuery(t3)
    val expected3 = "{1=Hi}\n" +
      "{2=Hello}\n" +
      "{3=Hello world}\n"
    TestBaseUtils.compareResultAsText(result3.asJava, expected3)

    val data = new mutable.MutableList[(String, BigDecimal, String, BigDecimal)]
    data.+=(("AAA", BigDecimal.valueOf(123.45), "BBB", BigDecimal.valueOf(234.56)))
    data.+=(("CCC", BigDecimal.valueOf(345.67), "DDD", BigDecimal.valueOf(456.78)))
    data.+=(("EEE", BigDecimal.valueOf(567.89), "FFF", BigDecimal.valueOf(678.99)))
    val t4 = BatchTableEnvUtil.fromCollection(tEnv, data, "a, b, c, d")
      .select(map('a, 'b, 'c, 'd))
    val result4 = executeQuery(t4)
    val expected4 = "{AAA=123.45, BBB=234.56}\n" +
      "{DDD=456.78, CCC=345.67}\n" +
      "{EEE=567.89, FFF=678.99}\n"
    TestBaseUtils.compareResultAsText(result4.asJava, expected4)
  }

  @Test
  def testValueConstructor(): Unit = {
    val data = new mutable.MutableList[(String, Int, LocalDateTime)]
    data.+=(("foo", 12, localDateTime("1984-07-12 14:34:24")))
    val t = BatchTableEnvUtil.fromCollection(tEnv, data, "a, b, c").select(
      row('a, 'b, 'c),
      array(12, 'b),
      map('a, 'c))

    val result = executeQuery(t)

    val nestedRow = result.head.getField(0).asInstanceOf[Row]
    assertEquals(data.head._1, nestedRow.getField(0))
    assertEquals(data.head._2, nestedRow.getField(1))
    assertEquals(data.head._3, nestedRow.getField(2))

    val arr = result.head.getField(1).asInstanceOf[Array[Integer]]
    assertEquals(12, arr(0))
    assertEquals(data.head._2, arr(1))

    val hashMap = result.head.getField(2).asInstanceOf[util.HashMap[String, Timestamp]]
    assertEquals(data.head._3, hashMap.get(data.head._1.asInstanceOf[String]))
  }

  @Test
  def testSelectStarFromNestedTable(): Unit = {

    val sqlQuery = "SELECT * FROM MyTable"

    val table = BatchTableEnvUtil.fromCollection(tEnv, Seq(
      ((0, 0), "0"),
      ((1, 1), "1"),
      ((2, 2), "2")
    )).select('*)

    val results = executeQuery(table)
    results.zipWithIndex.foreach {
      case (row, i) =>
        val nestedRow = row.getField(0).asInstanceOf[Row]
        assertEquals(i, nestedRow.getField(0))
        assertEquals(i, nestedRow.getField(1))
        assertEquals(i.toString, row.getField(1))
    }
  }

  @Test
  def testFunctionWithUnicodeParameters(): Unit = {
    val data = List(
      ("a\u0001b", "c\"d", "e\\\"\u0004f"), // uses Java/Scala escaping
      ("x\u0001y", "y\"z", "z\\\"\u0004z")
    )

    val splitUDF0 = new SplitUDF(deterministic = true)
    val splitUDF1 = new SplitUDF(deterministic = false)

    // uses Java/Scala escaping
    val ds = BatchTableEnvUtil.fromCollection(tEnv, data, "a, b, c")
      .select(
        splitUDF0('a, "\u0001", 0) as 'a0,
        splitUDF1('a, "\u0001", 0) as 'a1,
        splitUDF0('b, "\"", 1) as 'b0,
        splitUDF1('b, "\"", 1) as 'b1,
        splitUDF0('c, "\\\"\u0004", 0) as 'c0,
        splitUDF1('c, "\\\"\u0004", 0) as 'c1)

    val results = executeQuery(ds)

    val expected = List("a,a,d,d,e,e", "x,x,z,z,z,z").mkString("\n")
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testSplitFieldsOnCustomType(): Unit = {
    tEnv.getConfig.setMaxGeneratedCodeLength(1) // splits fields

    val ds = CollectionBatchExecTable.getCustomTypeDataSet(tEnv, "myInt, myLong, myString")
      .filter('myString.like("%a%") && 'myString.charLength() > 12)
      .select('myInt, 'myLong, 'myString.charLength())

    val expected = "3,3,25\n" + "3,5,14\n"
    val results = executeQuery(ds)
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

}

@SerialVersionUID(1L)
object HashCode extends ScalarFunction {
  def eval(s: String): Int = s.hashCode
}

@SerialVersionUID(1L)
object OldHashCode extends ScalarFunction {
  def eval(s: String): Int = -1
}
