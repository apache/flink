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

package org.apache.flink.table.runtime.batch.table

import java.math.MathContext
import java.sql.{Date, Time, Timestamp}
import java.util

import org.apache.flink.api.scala._
import org.apache.flink.api.scala.util.CollectionDataSets
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.Types._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.expressions.Literal
import org.apache.flink.table.expressions.utils._
import org.apache.flink.table.functions.ScalarFunction
import org.apache.flink.table.runtime.utils.TableProgramsTestBase.TableConfigMode
import org.apache.flink.table.runtime.utils.{TableProgramsCollectionTestBase, TableProgramsTestBase, UserDefinedFunctionTestUtils}
import org.apache.flink.test.util.TestBaseUtils
import org.apache.flink.test.util.TestBaseUtils.compareResultAsText
import org.apache.flink.types.Row
import org.junit.Assert.assertEquals
import org.junit._
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

import scala.collection.JavaConverters._
import scala.collection.mutable

@RunWith(classOf[Parameterized])
class CalcITCase(
    configMode: TableConfigMode)
  extends TableProgramsCollectionTestBase(configMode) {

  @Test
  def testSimpleSelectAll(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val t = CollectionDataSets.get3TupleDataSet(env).toTable(tEnv).select('_1, '_2, '_3)

    val expected = "1,1,Hi\n" + "2,2,Hello\n" + "3,2,Hello world\n" +
      "4,3,Hello world, how are you?\n" + "5,3,I am fine.\n" + "6,3,Luke Skywalker\n" +
      "7,4,Comment#1\n" + "8,4,Comment#2\n" + "9,4,Comment#3\n" + "10,4,Comment#4\n" +
      "11,5,Comment#5\n" + "12,5,Comment#6\n" + "13,5,Comment#7\n" + "14,5,Comment#8\n" +
      "15,5,Comment#9\n" + "16,6,Comment#10\n" + "17,6,Comment#11\n" + "18,6,Comment#12\n" +
      "19,6,Comment#13\n" + "20,6,Comment#14\n" + "21,6,Comment#15\n"
    val results = t.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testSimpleSelectAllWithAs(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val t = CollectionDataSets.get3TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c).select('a, 'b, 'c)

    val expected = "1,1,Hi\n" + "2,2,Hello\n" + "3,2,Hello world\n" +
      "4,3,Hello world, how are you?\n" + "5,3,I am fine.\n" + "6,3,Luke Skywalker\n" +
      "7,4,Comment#1\n" + "8,4,Comment#2\n" + "9,4,Comment#3\n" + "10,4,Comment#4\n" +
      "11,5,Comment#5\n" + "12,5,Comment#6\n" + "13,5,Comment#7\n" + "14,5,Comment#8\n" +
      "15,5,Comment#9\n" + "16,6,Comment#10\n" + "17,6,Comment#11\n" + "18,6,Comment#12\n" +
      "19,6,Comment#13\n" + "20,6,Comment#14\n" + "21,6,Comment#15\n"
    val results = t.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testSimpleSelectWithNaming(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val t = CollectionDataSets.get3TupleDataSet(env).toTable(tEnv)
      .select('_1 as 'a, '_2 as 'b, '_1 as 'c)
      .select('a, 'b)

    val expected = "1,1\n" + "2,2\n" + "3,2\n" + "4,3\n" + "5,3\n" + "6,3\n" + "7,4\n" +
      "8,4\n" + "9,4\n" + "10,4\n" + "11,5\n" + "12,5\n" + "13,5\n" + "14,5\n" + "15,5\n" +
      "16,6\n" + "17,6\n" + "18,6\n" + "19,6\n" + "20,6\n" + "21,6\n"
    val results = t.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testSimpleSelectRenameAll(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val t = CollectionDataSets.get3TupleDataSet(env).toTable(tEnv)
      .select('_1 as 'a, '_2 as 'b, '_3 as 'c)
      .select('a, 'b)

    val expected = "1,1\n" + "2,2\n" + "3,2\n" + "4,3\n" + "5,3\n" + "6,3\n" + "7,4\n" +
      "8,4\n" + "9,4\n" + "10,4\n" + "11,5\n" + "12,5\n" + "13,5\n" + "14,5\n" + "15,5\n" +
      "16,6\n" + "17,6\n" + "18,6\n" + "19,6\n" + "20,6\n" + "21,6\n"
    val results = t.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testSelectStar(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val t = CollectionDataSets.getSmallNestedTupleDataSet(env).toTable(tEnv, 'a, 'b).select('*)

    val expected =
      "(1,1),one\n" + "(2,2),two\n" + "(3,3),three\n"
    val results = t.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testAllRejectingFilter(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val ds = CollectionDataSets.get3TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c)

    val filterDs = ds.filter( Literal(false) )

    val expected = "\n"
    val results = filterDs.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testAllPassingFilter(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val ds = CollectionDataSets.get3TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c)

    val filterDs = ds.filter( Literal(true) )
    val expected = "1,1,Hi\n" + "2,2,Hello\n" + "3,2,Hello world\n" + "4,3,Hello world, " +
      "how are you?\n" + "5,3,I am fine.\n" + "6,3,Luke Skywalker\n" + "7,4," +
      "Comment#1\n" + "8,4,Comment#2\n" + "9,4,Comment#3\n" + "10,4,Comment#4\n" + "11,5," +
      "Comment#5\n" + "12,5,Comment#6\n" + "13,5,Comment#7\n" + "14,5,Comment#8\n" + "15,5," +
      "Comment#9\n" + "16,6,Comment#10\n" + "17,6,Comment#11\n" + "18,6,Comment#12\n" + "19," +
      "6,Comment#13\n" + "20,6,Comment#14\n" + "21,6,Comment#15\n"
    val results = filterDs.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testFilterOnStringTupleField(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val ds = CollectionDataSets.get3TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c)
    val filterDs = ds.filter( 'c.like("%world%") )

    val expected = "3,2,Hello world\n" + "4,3,Hello world, how are you?\n"
    val results = filterDs.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testFilterOnIntegerTupleField(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val ds = CollectionDataSets.get3TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c)

    val filterDs = ds.filter( 'a % 2 === 0 )

    val expected = "2,2,Hello\n" + "4,3,Hello world, how are you?\n" +
      "6,3,Luke Skywalker\n" + "8,4," + "Comment#2\n" + "10,4,Comment#4\n" +
      "12,5,Comment#6\n" + "14,5,Comment#8\n" + "16,6," +
      "Comment#10\n" + "18,6,Comment#12\n" + "20,6,Comment#14\n"
    val results = filterDs.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testNotEquals(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val ds = CollectionDataSets.get3TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c)

    val filterDs = ds.filter( 'a % 2 !== 0)
    val expected = "1,1,Hi\n" + "3,2,Hello world\n" +
      "5,3,I am fine.\n" + "7,4,Comment#1\n" + "9,4,Comment#3\n" +
      "11,5,Comment#5\n" + "13,5,Comment#7\n" + "15,5,Comment#9\n" +
      "17,6,Comment#11\n" + "19,6,Comment#13\n" + "21,6,Comment#15\n"
    val results = filterDs.collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testDisjunctivePredicate(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val ds = CollectionDataSets.get3TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c)

    val filterDs = ds.filter( 'a < 2 || 'a > 20)
    val expected = "1,1,Hi\n" + "21,6,Comment#15\n"
    val results = filterDs.collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testConsecutiveFilters(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val ds = CollectionDataSets.get3TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c)

    val filterDs = ds.filter('a % 2 !== 0).filter('b % 2 === 0)
    val expected = "3,2,Hello world\n" + "7,4,Comment#1\n" +
      "9,4,Comment#3\n" + "17,6,Comment#11\n" +
      "19,6,Comment#13\n" + "21,6,Comment#15\n"
    val results = filterDs.collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testFilterBasicType(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val ds = CollectionDataSets.getStringDataSet(env)

    val filterDs = ds.toTable(tEnv, 'a).filter( 'a.like("H%") )

    val expected = "Hi\n" + "Hello\n" + "Hello world\n" + "Hello world, how are you?\n"
    val results = filterDs.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testFilterOnCustomType(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val ds = CollectionDataSets.getCustomTypeDataSet(env)
    val filterDs = ds.toTable(tEnv, 'myInt as 'i, 'myLong as 'l, 'myString as 's)
      .filter( 's.like("%a%") )

    val expected = "3,3,Hello world, how are you?\n" + "3,4,I am fine.\n" + "3,5,Luke Skywalker\n"
    val results = filterDs.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testSimpleCalc(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val t = CollectionDataSets.get3TupleDataSet(env).toTable(tEnv)
        .select('_1, '_2, '_3)
        .where('_1 < 7)
        .select('_1, '_3)

    val expected = "1,Hi\n" + "2,Hello\n" + "3,Hello world\n" +
      "4,Hello world, how are you?\n" + "5,I am fine.\n" + "6,Luke Skywalker\n"
      val results = t.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testCalcWithTwoFilters(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val t = CollectionDataSets.get3TupleDataSet(env).toTable(tEnv)
        .select('_1, '_2, '_3)
        .where('_1 < 7 && '_2 === 3)
        .select('_1, '_3)
        .where('_1 === 4)
        .select('_1)

    val expected = "4\n"
    val results = t.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testCalcWithAggregation(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val t = CollectionDataSets.get3TupleDataSet(env).toTable(tEnv)
        .select('_1, '_2, '_3)
        .where('_1 < 15)
        .groupBy('_2)
        .select('_1.min, '_2.count as 'cnt)
        .where('cnt > 3)

    val expected = "7,4\n" + "11,4\n"
    val results = t.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testCalcJoin(): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val ds1 = CollectionDataSets.getSmall3TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = CollectionDataSets.get5TupleDataSet(env).toTable(tEnv, 'd, 'e, 'f, 'g, 'h)

    val joinT = ds1.select('a, 'b).join(ds2).where('b === 'e).select('a, 'b, 'd, 'e, 'f)
      .where('b > 1).select('a, 'd).where('d === 2)

    val expected = "2,2\n" + "3,2\n"
    val results = joinT.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testAdvancedDataTypes(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)
    tEnv.getConfig.setDecimalContext(new MathContext(30))

    val t = env
      .fromElements((
        BigDecimal("78.454654654654654").bigDecimal,
        BigDecimal("4E+9999").bigDecimal,
        Date.valueOf("1984-07-12"),
        Time.valueOf("14:34:24"),
        Timestamp.valueOf("1984-07-12 14:34:24")))
      .toTable(tEnv, 'a, 'b, 'c, 'd, 'e)
      .select('a, 'b, 'c, 'd, 'e, BigDecimal("11.2"), BigDecimal("11.2").bigDecimal,
        Date.valueOf("1984-07-12"), Time.valueOf("14:34:24"),
        Timestamp.valueOf("1984-07-12 14:34:24"),
        BigDecimal("1").toExpr / BigDecimal("3"))

    val expected = "78.454654654654654,4E+9999,1984-07-12,14:34:24,1984-07-12 14:34:24.0," +
      "11.2,11.2,1984-07-12,14:34:24,1984-07-12 14:34:24.0,0.333333333333333333333333333333"
    val results = t.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testUserDefinedScalarFunction() {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv = TableEnvironment.getTableEnvironment(env, config)
    tableEnv.registerFunction("hashCode", OldHashCode)
    tableEnv.registerFunction("hashCode", HashCode)
    val table = env.fromElements("a", "b", "c").toTable(tableEnv, 'text)
    val result = table.select("text.hashCode()")
    val results = result.toDataSet[Row].collect()
    val expected = "97\n98\n99"
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testNumericAutocastInArithmetic() {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv = TableEnvironment.getTableEnvironment(env)

    val table = env.fromElements(
      (1.toByte, 1.toShort, 1, 1L, 1.0f, 1.0d, 1L, 1001.1)).toTable(tableEnv)
      .select('_1 + 1, '_2 + 1, '_3 + 1L, '_4 + 1.0f,
        '_5 + 1.0d, '_6 + 1, '_7 + 1.0d, '_8 + '_1)

    val results = table.toDataSet[Row].collect()
    val expected = "2,2,2,2.0,2.0,2.0,2.0,1002.1"
    compareResultAsText(results.asJava, expected)
  }

  @Test
  def testNumericAutocastInComparison() {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv = TableEnvironment.getTableEnvironment(env)

    val table = env.fromElements(
      (1.toByte, 1.toShort, 1, 1L, 1.0f, 1.0d),
      (2.toByte, 2.toShort, 2, 2L, 2.0f, 2.0d))
      .toTable(tableEnv, 'a, 'b, 'c, 'd, 'e, 'f)
      .filter('a > 1 && 'b > 1 && 'c > 1L && 'd > 1.0f && 'e > 1.0d && 'f > 1)

    val results = table.toDataSet[Row].collect()
    val expected: String = "2,2,2,2,2.0,2.0"
    compareResultAsText(results.asJava, expected)
  }

  @Test
  def testCasting() {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv = TableEnvironment.getTableEnvironment(env)
    val table = env.fromElements((1, 0.0, 1L, true)).toTable(tableEnv)
      .select(
        // * -> String
      '_1.cast(STRING), '_2.cast(STRING), '_3.cast(STRING), '_4.cast(STRING),
        // NUMERIC TYPE -> Boolean
      '_1.cast(BOOLEAN), '_2.cast(BOOLEAN), '_3.cast(BOOLEAN),
        // NUMERIC TYPE -> NUMERIC TYPE
      '_1.cast(DOUBLE), '_2.cast(INT), '_3.cast(SHORT),
        // Boolean -> NUMERIC TYPE
      '_4.cast(DOUBLE),
        // identity casting
      '_1.cast(INT), '_2.cast(DOUBLE), '_3.cast(LONG), '_4.cast(BOOLEAN))

    val results = table.toDataSet[Row].collect()
    val expected = "1,0.0,1,true," + "true,false,true," +
      "1.0,0,1," + "1.0," + "1,0.0,1,true\n"
    compareResultAsText(results.asJava, expected)
  }

  @Test
  def testCastFromString() {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv = TableEnvironment.getTableEnvironment(env)
    val table = env.fromElements(("1", "true", "2.0")).toTable(tableEnv)
      .select('_1.cast(BYTE), '_1.cast(SHORT), '_1.cast(INT), '_1.cast(LONG),
        '_3.cast(DOUBLE), '_3.cast(FLOAT), '_2.cast(BOOLEAN))

    val results = table.toDataSet[Row].collect()
    val expected = "1,1,1,1,2.0,2.0,true\n"
    compareResultAsText(results.asJava, expected)
  }

  @Test
  def testUserDefinedScalarFunctionWithParameter(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    tEnv.registerFunction("RichFunc2", new RichFunc2)
    UserDefinedFunctionTestUtils.setJobParameters(env, Map("string.value" -> "ABC"))

    val ds = CollectionDataSets.getSmall3TupleDataSet(env)
    tEnv.registerDataSet("t1", ds, 'a, 'b, 'c)

    val sqlQuery = "SELECT c FROM t1 where RichFunc2(c)='ABC#Hello'"

    val result = tEnv.sqlQuery(sqlQuery)

    val expected = "Hello"
    val results = result.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testUserDefinedScalarFunctionWithDistributedCache(): Unit = {
    val words = "Hello\nWord"
    val filePath = UserDefinedFunctionTestUtils.writeCacheFile("test_words", words)
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.registerCachedFile(filePath, "words")
    val tEnv = TableEnvironment.getTableEnvironment(env)
    tEnv.registerFunction("RichFunc3", new RichFunc3)

    val ds = CollectionDataSets.getSmall3TupleDataSet(env)
    tEnv.registerDataSet("t1", ds, 'a, 'b, 'c)

    val sqlQuery = "SELECT c FROM t1 where RichFunc3(c)=true"

    val result = tEnv.sqlQuery(sqlQuery)

    val expected = "Hello"
    val results = result.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testValueConstructor(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val rowValue = ("foo", 12, Timestamp.valueOf("1984-07-12 14:34:24"))

    val table = env.fromElements(rowValue).toTable(tEnv, 'a, 'b, 'c)

    val result = table.select(
      row('a, 'b, 'c),
      array(12, 'b),
      map('a, 'c),
      map('a, 'c).at('a) === 'c
    )

    val expected = "foo,12,1984-07-12 14:34:24.0,[12, 12],{foo=1984-07-12 14:34:24.0},true"
    val results = result.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)

    // Compare actual object to avoid undetected Calcite flattening
    val resultRow = results.asJava.get(0)
    assertEquals(rowValue._1, resultRow.getField(0).asInstanceOf[Row].getField(0))
    assertEquals(rowValue._2, resultRow.getField(1).asInstanceOf[Array[Integer]](1))
    assertEquals(rowValue._3,
      resultRow.getField(2).asInstanceOf[util.Map[String, Timestamp]].get(rowValue._1))
  }

  @Test
  def testMultipleUserDefinedScalarFunctions(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    tEnv.registerFunction("RichFunc1", new RichFunc1)
    tEnv.registerFunction("RichFunc2", new RichFunc2)
    UserDefinedFunctionTestUtils.setJobParameters(env, Map("string.value" -> "Abc"))

    val ds = CollectionDataSets.getSmall3TupleDataSet(env)
    tEnv.registerDataSet("t1", ds, 'a, 'b, 'c)

    val sqlQuery = "SELECT c FROM t1 where " +
      "RichFunc2(c)='Abc#Hello' or RichFunc1(a)=3 and b=2"

    val result = tEnv.sqlQuery(sqlQuery)

    val expected = "Hello\nHello world"
    val results = result.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testScalarFunctionConstructorWithParams(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv = TableEnvironment.getTableEnvironment(env, config)

    val data = new mutable.MutableList[(Int, Long, String)]
    data.+=((1, 1L, "Jack#22"))
    data.+=((2, 2L, "John#19"))
    data.+=((3, 2L, "Anna#44"))
    data.+=((4, 3L, "nosharp"))
    val in = env.fromCollection(data).toTable(tableEnv).as('a, 'b, 'c)

    val func0 = new Func13("default")
    val func1 = new Func13("Sunny")
    val func2 = new Func13("kevin2")

    val result = in.select(func0('c), func1('c),func2('c))

    val results = result.collect()

    val expected = "default-Anna#44,Sunny-Anna#44,kevin2-Anna#44\n" +
      "default-Jack#22,Sunny-Jack#22,kevin2-Jack#22\n" +
      "default-John#19,Sunny-John#19,kevin2-John#19\n" +
      "default-nosharp,Sunny-nosharp,kevin2-nosharp"
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testFunctionWithUnicodeParameters(): Unit = {
    val data = List(
      ("a\u0001b", "c\"d", "e\\\"\u0004f"), // uses Java/Scala escaping
      ("x\u0001y", "y\"z", "z\\\"\u0004z")
    )

    val env = ExecutionEnvironment.getExecutionEnvironment

    val tEnv = TableEnvironment.getTableEnvironment(env)

    val splitUDF0 = new SplitUDF(deterministic = true)
    val splitUDF1 = new SplitUDF(deterministic = false)

     // uses Java/Scala escaping
    val ds = env.fromCollection(data).toTable(tEnv, 'a, 'b, 'c)
      .select(
        splitUDF0('a, "\u0001", 0) as 'a0,
        splitUDF1('a, "\u0001", 0) as 'a1,
        splitUDF0('b, "\"", 1) as 'b0,
        splitUDF1('b, "\"", 1) as 'b1,
        splitUDF0('c, "\\\"\u0004", 0) as 'c0,
        splitUDF1('c, "\\\"\u0004", 0) as 'c1)

    val results = ds.collect()

    val expected = List("a,a,d,d,e,e", "x,x,z,z,z,z").mkString("\n")
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testSplitFieldsOnCustomType(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)
    tEnv.getConfig.setMaxGeneratedCodeLength(1)  // splits fields

    val ds = CollectionDataSets.getCustomTypeDataSet(env)
    val filterDs = ds.toTable(tEnv, 'myInt as 'i, 'myLong as 'l, 'myString as 's)
      .filter('s.like("%a%") && 's.charLength() > 12)
      .select('i, 'l, 's.charLength())

    val expected = "3,3,25\n" + "3,5,14\n"
    val results = filterDs.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }
}

object CalcITCase {

  @Parameterized.Parameters(name = "Table config = {0}")
  def parameters(): util.Collection[Array[java.lang.Object]] = {
    Seq[Array[AnyRef]](
      Array(TableProgramsTestBase.DEFAULT),
      Array(TableProgramsTestBase.NO_NULL)).asJava
  }
}

object HashCode extends ScalarFunction {
  def eval(s: String): Int = s.hashCode
}

object OldHashCode extends ScalarFunction {
  def eval(s: String): Int = -1
}
