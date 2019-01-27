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
package org.apache.flink.table.runtime.stream.sql

import java.io.{ByteArrayOutputStream, PrintStream}
import java.math.{BigDecimal => JBigDecimal}
import java.security.MessageDigest
import java.sql.{Date => SqlDate, Time => SqlTime, Timestamp => SqlTimestamp}
import java.text.SimpleDateFormat
import java.util
import java.util.{TimeZone, UUID}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.functions.ScalarFunction
import org.apache.flink.table.api.types.{DataType, DataTypes}
import org.apache.flink.table.api.{TableConfig, TableConfigOptions, TableEnvironment, Types}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.plan.util.FlinkRelOptUtil
import org.apache.flink.table.runtime.utils.{StreamingTestBase, TestingAppendSink}
import org.apache.flink.table.util.MemoryTableSourceSinkUtil
import org.apache.flink.types.Row

import org.junit.Assert._
import org.junit._

import scala.collection.mutable

/**
  * Just for blink
  */
class BuiltinScalarFunctionITCase extends StreamingTestBase {

  val tab: String = org.apache.commons.lang3.StringEscapeUtils.unescapeJava("\t")

  var origTimeZone = TimeZone.getDefault

  @Before
  override def before(): Unit = {
    super.before()
    origTimeZone = TimeZone.getDefault
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"))
  }

  @After
  def after(): Unit = {
    TimeZone.setDefault(origTimeZone)

    // restore the default zone for Calcite
    org.apache.calcite.avatica.util.DateTimeUtils
      .setUserZone(TimeZone.getTimeZone("UTC"))
  }

  @Test
  def testJsonValue(): Unit = {
    val data = new mutable.MutableList[(Int, String, String)]
    data.+=((1, "[10, 20, [30, 40]]", "$[2][*]"))
    data.+=(
      (2, "{\"aaa\":\"bbb\",\"ccc\":{\"ddd\":\"eee\",\"fff\":\"ggg\"," +
        "\"hhh\":[\"h0\",\"h1\",\"h2\"]},\"iii\":\"jjj\"}", "$.ccc.hhh[*]"))
    data.+=(
      (3, "{\"aaa\":\"bbb\",\"ccc\":{\"ddd\":\"eee\",\"fff\":\"ggg\"," +
        "\"hhh\":[\"h0\",\"h1\",\"h2\"]},\"iii\":\"jjj\"}", "$.ccc.hhh[1]"))
    data.+=((4, "[10, 20, [30, 40]]", null))
    data.+=((5, null, "$[2][*]"))
    data.+=((6, "{xx]", "$[2][*]"))

    val sqlQuery = "SELECT id, json_value(json, path) FROM T1"

    val t1 = env.fromCollection(data).toTable(tEnv, 'id, 'json, 'path, 'proctime.proctime)

    tEnv.registerTable("T1", t1)

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = List(
      "1,[30,40]",
      "2,[\"h0\",\"h1\",\"h2\"]",
      "3,h1",
      "4,null",
      "5,null",
      "6,null")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testLog(): Unit = {
    val data = new mutable.MutableList[(Int, Int, Int)]
    data.+=((1, 10, 100))
    data.+=((2, 2, 8))

    val sqlQuery = "SELECT id, log(base, x), log(2), log(10.0, 100.0) FROM T1"

    val t1 = env.fromCollection(data).toTable(tEnv, 'id, 'base, 'x, 'proctime.proctime)

    tEnv.registerTable("T1", t1)

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = List("1,2.0,0.6931471805599453,2.0", "2,3.0,0.6931471805599453,2.0")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testLn(): Unit = {
    val data = new mutable.MutableList[(Int, Int, Int)]
    data.+=((1, 10, 100))
    data.+=((2, 2, 8))

    val sqlQuery = "SELECT id, ln(x), ln(e()) FROM T1"

    val t1 = env.fromCollection(data).toTable(tEnv, 'id, 'base, 'x, 'proctime.proctime)

    tEnv.registerTable("T1", t1)

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = List("1,4.605170185988092,1.0", "2,2.0794415416798357,1.0")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testLog10(): Unit = {
    val data = new mutable.MutableList[(Int, Int)]
    data.+=((1, 100))
    data.+=((2, 10))

    val sqlQuery = "SELECT id, log10(x) FROM T1"

    val t1 = env.fromCollection(data).toTable(tEnv, 'id, 'x, 'proctime.proctime)

    tEnv.registerTable("T1", t1)

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = List("1,2.0", "2,1.0")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }


  @Test
  def testLog2(): Unit = {
    val data = new mutable.MutableList[(Int, Int)]
    data.+=((1, 8))
    data.+=((2, 2))

    val sqlQuery = "SELECT id, log2(x) FROM T1"

    val t1 = env.fromCollection(data).toTable(tEnv, 'id, 'x, 'proctime.proctime)

    tEnv.registerTable("T1", t1)

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = List("1,3.0", "2,1.0")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testE(): Unit = {
    val data = new mutable.MutableList[(Int, Int)]
    data.+=((1, 8))

    val sqlQuery = "SELECT id, e(), E() FROM T1"

    val t1 = env.fromCollection(data).toTable(tEnv, 'id, 'x, 'proctime.proctime)

    tEnv.registerTable("T1", t1)

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = List("1,2.718281828459045,2.718281828459045")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testPi(): Unit = {
    val data = new mutable.MutableList[(Int, Int)]
    data.+=((1, 8))

    val sqlQuery = "SELECT id, PI() FROM T1"

    val t1 = env.fromCollection(data).toTable(tEnv, 'id, 'x, 'proctime.proctime)

    tEnv.registerTable("T1", t1)

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = List("1,3.141592653589793")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testRand(): Unit = {
    val data = new mutable.MutableList[(Int, Int)]
    data.+=((1, 8))

    val random1 = new java.util.Random(1)
    val random3 = new java.util.Random(3)

    val sqlQuery = "SELECT id, rand(1), rand(3) FROM T1"

    val t1 = env.fromCollection(data).toTable(tEnv, 'id, 'x, 'proctime.proctime)

    tEnv.registerTable("T1", t1)

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = List("1," + random1.nextDouble().toString + "," + random3.nextDouble().toString)
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testRound(): Unit = {
    val data = new mutable.MutableList[(Double, Int)]
    data.+=((0.5, 1))
    val a: String = ""
    val sqlQuery = "SELECT " +
      "round(125.315), " +
      "round(-125.315, 2), " +
      "round(125.315, 0), " +
      "round(x, d), " +
      "round(1.4, 1) FROM T1"

    val t1 = env.fromCollection(data).toTable(tEnv, 'x, 'd, 'proctime.proctime)

    tEnv.registerTable("T1", t1)

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = List("125,-125.32,125,0.5,1.4")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testIsDecimal(): Unit = {
    val data = new mutable.MutableList[(String, String, String,
      String, String, String, String)]
    data.+=(("1", "123", "2", "11.4445", "3", "asd", null))

    val sqlQuery = "SELECT IS_DECIMAL(a),IS_DECIMAL(b),IS_DECIMAL(c)," +
      "IS_DECIMAL(d),IS_DECIMAL(e),IS_DECIMAL(f),IS_DECIMAL(g) from T1"

    val t1 = env.fromCollection(data).toTable(tEnv, 'a, 'b, 'c, 'd, 'e, 'f, 'g, 'proctime.proctime)

    tEnv.registerTable("T1", t1)

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = List("true,true,true,true,true,false,false")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testIsDigit(): Unit = {
    val data = new mutable.MutableList[(String, String, String,
      String, String, String, String)]
    data.+=(("1", "123", "2", "11.4445", "3", "asd", null))

    val sqlQuery = "SELECT IS_DIGIT(a),IS_DIGIT(b),IS_DIGIT(c)," +
      "IS_DIGIT(d),IS_DIGIT(e),IS_DIGIT(f),IS_DIGIT(g) from T1"

    val t1 = env.fromCollection(data).toTable(tEnv, 'a, 'b, 'c, 'd, 'e, 'f, 'g, 'proctime.proctime)

    tEnv.registerTable("T1", t1)

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = List("true,true,true,false,true,false,false")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testIsAlpha(): Unit = {
    val data = new mutable.MutableList[(String, String, String,
      String, String, String, String)]
    data.+=(("1", "123", "2", "11.4445", "3", "asd", null))

    val sqlQuery = "SELECT IS_ALPHA(a),IS_ALPHA(b),IS_ALPHA(c)," +
      "IS_ALPHA(d),IS_ALPHA(e),IS_ALPHA(f),IS_ALPHA(g) from T1"

    val t1 = env.fromCollection(data).toTable(tEnv, 'a, 'b, 'c, 'd, 'e, 'f, 'g, 'proctime.proctime)

    tEnv.registerTable("T1", t1)

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = List("false,false,false,false,false,true,false")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testIf(): Unit = {
    val data = new mutable.MutableList[(Int, Int, String, String)]
    data.+=((1, 2, "Jack", "Harry")) //"Jack"
    data.+=((1, 2, "Jack", null)) //"Jack"
    data.+=((1, 2, null, "Harry")) //null

    val sqlQuery = "SELECT IF(int1 < int2, str1, str2) from T1"

    val t1 = env.fromCollection(data).toTable(tEnv, 'int1, 'int2, 'str1, 'str2, 'proctime.proctime)

    tEnv.registerTable("T1", t1)

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = List("Jack", "Jack", "null")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testPower(): Unit = {
    val data = new mutable.MutableList[(Double, Int, Long)]
    data.+=((3.51, 2, 3))

    val sqlQuery = "SELECT " +
      "power(double1, double1), " +
      "power(long1, long1), " +
      "power(int1, int1), " +
      "power(1.99, 2.99), " +
      "power(0.985, cast(2 as double)) from T1"

    val t1 = env.fromCollection(data).toTable(tEnv, 'double1, 'int1, 'long1, 'proctime.proctime)

    tEnv.registerTable("T1", t1)

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = List("82.04043959962172,27.0,4.0,7.826556026010586,0.970225")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testConcat(): Unit = {
    val data = new mutable.MutableList[(String, String, String)]
    data.+=((null, null, null)) //""
    data.+=(("Hello", "My", "World")) //"HelloMyWorld"
    data.+=(("Hello", null, "World")) //"HelloWorld"
    data.+=((null, null, "World")) //"World"

    val sqlQuery = "SELECT CONCAT(str1, str2, str3) from T1"

    val t1 = env.fromCollection(data).toTable(tEnv, 'str1, 'str2, 'str3, 'proctime.proctime)

    tEnv.registerTable("T1", t1)

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = List("", "HelloMyWorld", "HelloWorld", "World")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testConcatWithImplicitCast(): Unit = {
    val data = new mutable.MutableList[(String, Boolean, Int, Double, SqlDate)]
    data.+=(("Hello", true, 123, 123.45, SqlDate.valueOf("2018-08-08")))

    val sqlQuery = "SELECT CONCAT(f1, f2, f3, f4, f5) from T1"

    val t1 = env.fromCollection(data).toTable(tEnv, 'f1, 'f2, 'f3, 'f4, 'f5, 'proctime.proctime)

    tEnv.registerTable("T1", t1)

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = List("Hellotrue123123.452018-08-08")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testUserDefinedConcat(): Unit = {
    val data = new mutable.MutableList[(String, String, String)]
    data.+=((null, null, null))
    data.+=(("Hello", "My", "World"))
    data.+=(("Hello", null, "World"))
    data.+=((null, null, "World"))

    val sqlQuery = "SELECT CONCAT(str1, str2, str3) from T1"

    tEnv.registerFunction("CONCAT", Concat)

    val t1 = env.fromCollection(data).toTable(tEnv, 'str1, 'str2, 'str3, 'proctime.proctime)

    tEnv.registerTable("T1", t1)

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = List("hello", "hello", "hello", "hello")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testConcatWs(): Unit = {
    val data = new mutable.MutableList[(String, String, String,
      String)]
    data.+=(("\t", null, "Harry", "John"))//"Harry|John"
    data.+=(("|", "Jack", null, null))//"Jack"
    data.+=((null, "Jack", "Harry", "John"))//"JackHarryJohn"
    data.+=(("|", "Jack", "Harry", "John"))//"Jack|Harry|John"

    val sqlQuery = "SELECT CONCAT_WS(sep , str1, str2, str3) from T1"

    val t1 = env.fromCollection(data).toTable(tEnv, 'sep, 'str1, 'str2, 'str3, 'proctime.proctime)

    tEnv.registerTable("T1", t1)

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = List("Harry" + tab + "John", "Jack", "JackHarryJohn", "Jack|Harry|John")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testConcatWsWithImplicitCast(): Unit = {
    val data = new mutable.MutableList[(String, String, Boolean, Int, Double, SqlDate)]
    data.+=((null, "Hello", true, 123, 123.45, SqlDate.valueOf("2018-08-08")))
    data.+=(("|", "Hello", true, 123, 123.45, SqlDate.valueOf("2018-08-08")))
    data.+=(("\t", "Hello", true, 123, 123.45, SqlDate.valueOf("2018-08-08")))

    val sqlQuery = "SELECT CONCAT_WS(sep, f1, f2, f3, f4, f5) from T1"

    val t1 = env.fromCollection(data)
      .toTable(tEnv,'sep, 'f1, 'f2, 'f3, 'f4, 'f5, 'proctime.proctime)

    tEnv.registerTable("T1", t1)

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = List(
      "Hellotrue123123.452018-08-08",
      "Hello|true|123|123.45|2018-08-08",
      "Hello" + tab + "true" + tab + "123" + tab + "123.45" + tab + "2018-08-08"
    )
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testChr(): Unit = {
    val data = new mutable.MutableList[(Int, Int, Byte, Short, Long)]
    data.+= ((333, 255, 97.toByte, 65.toShort, 66L))//"ÿ","a","A", "B"

    val sqlQuery = "SELECT CHR(int1), CHR(int2),CHR(byte1),CHR(short1),CHR(long1) from T1"

    val t1 = env.fromCollection(data).toTable(
      tEnv, 'int1, 'int2, 'byte1, 'short1, 'long1, 'proctime.proctime)

    tEnv.registerTable("T1", t1)

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = List("M,ÿ,a,A,B")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testLpad(): Unit = {
    val data = new mutable.MutableList[(String, Int, String)]
    data.+=(("", -2, ""))
    data.+=(("", 0, ""))
    data.+=(("", 2, "s"))
    data.+=(("C", 4, "HelloWorld"))
    data.+=(("HelloWorld", 15, "你好"))
    data.+=(("John", 2, "C"))
    data.+=(("asd", 2, ""))
    data.+=(("asd", 4, ""))
    data.+=(("c", 2, null))
    data.+=((null, 2, "C"))

    val sqlQuery = "SELECT str,len,pad,LPAD(str, len, pad) as `result` from T1"

    val t1 = env.fromCollection(data)
      .toTable(tEnv, 'str, 'len, 'pad, 'proctime.proctime)

    tEnv.registerTable("T1", t1)

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = List(
      ",-2,,null",
      ",0,,",
      ",2,s,ss",
      "C,4,HelloWorld,HelC",
      "HelloWorld,15,你好,你好你好你HelloWorld",
      "John,2,C,Jo",
      "asd,2,,as",
      "asd,4,,null",
      "c,2,null,null",
      "null,2,C,null"
    )
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testRpad(): Unit = {
    val data = new mutable.MutableList[(String, Int, String)]
    data.+=(("", -2, ""))
    data.+=(("", 0, ""))
    data.+=(("", 2, "s"))
    data.+=(("C", 4, "HelloWorld"))
    data.+=(("HelloWorld", 15, "John"))
    data.+=(("John", 2, "C"))
    data.+=(("asd", 2, ""))
    data.+=(("asd", 4, ""))
    data.+=(("c", 2, null))
    data.+=((null, 2, "C"))

    val sqlQuery = "SELECT str,len,pad,RPAD(str, len, pad) as `result` from T1"

    val t1 = env.fromCollection(data)
      .toTable(tEnv, 'str, 'len, 'pad, 'proctime.proctime)

    tEnv.registerTable("T1", t1)

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = List(
      ",-2,,null",
      ",0,,",
      ",2,s,ss",
      "C,4,HelloWorld,CHel",
      "HelloWorld,15,John,HelloWorldJohnJ",
      "John,2,C,Jo",
      "asd,2,,as",
      "asd,4,,null",
      "c,2,null,null",
      "null,2,C,null"
    )
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testRepeat(): Unit = {
    val data = new mutable.MutableList[(String, Int)]
    data.+=(("Hello", -9))//""
    data.+=(("Hello", 2))//"HelloHello"
    data.+=(("J", 9))//"JJJJJJJJJ"
    data.+=((null, 9))//null

    val sqlQuery = "SELECT REPEAT(str,n) from T1"

    val t1 = env.fromCollection(data).toTable(tEnv, 'str, 'n, 'proctime.proctime)

    tEnv.registerTable("T1", t1)

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = List("", "HelloHello", "JJJJJJJJJ", "null")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testReverse(): Unit = {
    val data = new mutable.MutableList[(String, String, String, String)]
    data.+=(("iPhoneX", "Alibaba", "World", null))

    val sqlQuery = "SELECT REVERSE(str1),REVERSE(str2),REVERSE(str3),REVERSE(str4) from T1"

    val t1 = env.fromCollection(data).toTable(tEnv, 'str1, 'str2, 'str3, 'str4, 'proctime.proctime)

    tEnv.registerTable("T1", t1)

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = List("XenohPi,ababilA,dlroW,null")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testReplace(): Unit = {
    val data = new mutable.MutableList[(String, String, String, String)]
    data.+=(("hello", "hello world", ".World", null))

    val sqlQuery = "SELECT REPLACE(str1, 'e', 'o'), " +
      "REPLACE(str2, ' ', 'X')," +
      "REPLACE(str3, '.', 'See ')," +
      "REPLACE(str4, 'x', 'y') from T1"

    val t1 = env.fromCollection(data).toTable(tEnv, 'str1, 'str2, 'str3, 'str4)

    tEnv.registerTable("T1", t1)

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = List("hollo,helloXworld,See World,null")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testSplitIndex(): Unit = {
    val data = new mutable.MutableList[(String, String, Int)]
    data.+=(("Jack" + tab + "John" + tab + "Mary", "\t", 2)) //"Mary"
    data.+=(("Jack,John,Mary", ",", 3))//null
    data.+=(("Jack,John,Mary", null, 0))//null
    data.+=((null, ",", 0))//null

    val sqlQuery = "SELECT SPLIT_INDEX(str, sep, index) from T1"

    val t1 = env.fromCollection(data).toTable(tEnv, 'str, 'sep, 'index, 'proctime.proctime)

    tEnv.registerTable("T1", t1)

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = List("Mary", "null", "null", "null")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testRegExpReplace(): Unit = {
    val data = new mutable.MutableList[(String, String, String)]
    data.+=(("100-200","(\\d+)", "num"))
    data.+=(("2014-03-13","(", "s"))
    data.+=(("2014-03-13","", "s"))
    data.+=(("2014-03-13","-", ""))
    data.+=(("2014-03-13","-", null))
    data.+=(("2014-03-13",null, ""))
    data.+=((null,"-", ""))

    val sqlQuery = "SELECT str1, pattern1, replace1, " +
      "REGEXP_REPLACE(str1, pattern1, replace1) as `result` from T1"

    val t1 = env.fromCollection(data)
      .toTable(tEnv,
        'str1, 'pattern1, 'replace1, 'proctime.proctime)

    tEnv.registerTable("T1", t1)

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = List(
      "100-200,(\\d+),num,num-num",
      "2014-03-13,(,s,null",
      "2014-03-13,,s,2014-03-13",
      "2014-03-13,-,,20140313",
      "2014-03-13,-,null,null",
      "2014-03-13,null,,null",
      "null,-,,null"
    )
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testRegExpExtract(): Unit = {
    val data = new mutable.MutableList[(String, String, Int)]
    data.+=(("100-200", "(\\d+)-(\\d+)", 1))
    data.+=(("foothebar", "(", 2))
    data.+=(("foothebar", "", 2))
    data.+=(("foothebar", "foo(.*?)(bar)", 2))
    data.+=(("foothebar", null, 2))
    data.+=((null, "foo(.*?)(bar)", 2))

    val sqlQuery = "SELECT " +
      "str1, pattern1, index1, " +
      "REGEXP_EXTRACT(str1, pattern1, index1) as `result` from T1"

    val t1 = env.fromCollection(data)
      .toTable(tEnv,
        'str1, 'pattern1, 'index1, 'proctime.proctime)

    tEnv.registerTable("T1", t1)

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = List(
      "100-200,(\\d+)-(\\d+),1,100",
      "foothebar,(,2,null",
      "foothebar,,2,null",
      "foothebar,foo(.*?)(bar),2,bar",
      "foothebar,null,2,null",
      "null,foo(.*?)(bar),2,null"
    )
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testKeyValue(): Unit = {
    val data = new mutable.MutableList[(String, String, String, String)]
    data.+=(("k1:v1|k2:v2", null, ":", "k3"))
    data.+=(("k1:v1" + tab + "k2:v2", "\t", ":", "k3"))
    data.+=(("k1:v1|k2:v2", "|", ":", null))
    data.+=(("k1:v1|k2:v2", "|", null, "k3"))
    data.+=(("k1=v1;k2=v2", ";", "=", "k2"))
    data.+=(("k1=v1;k2=v2;", ";", "=", "k2"))
    data.+=(("k1=v1;k2", ";", "=", "k2"))
    data.+=(("k1;k2=v2", ";", "=", "k1"))
    data.+=(("k=1=v1;k2=v2", ";", "=", "k=1"))
    data.+=(("k1==v1;k2=v2", ";", "=", "k1"))
    data.+=(("k1==v1;k2=v2", ";", "=", "k1="))
    data.+=(("k1k1=v1;k2=v2", ";", "=", "k1"))
    data.+=((null, "|", ":", "k3"))
    data.+=(("k1=v1abababk2=v2", "ababc", "=", "abk2"))
    data.+=(("k1=v1abababk2=v2", "ababc", "=", "k2"))
    data.+=(("k1:v1 k2:v2", null, ":", "k2"))
    data.+=(("k1 v1;k2 v2", ";", null, "k2"))

    val sqlQuery = "SELECT str,split1,split2,key1," +
      "KEYVALUE(str, split1, split2, key1) as `result` from T1"

    val t1 = env.fromCollection(data)
      .toTable(tEnv, 'str, 'split1, 'split2, 'key1, 'proctime.proctime)

    tEnv.registerTable("T1", t1)

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = List(
      "k1:v1|k2:v2,null,:,k3,null",
      "k1:v1" + tab + "k2:v2," + tab + ",:,k3,null",
      "k1:v1|k2:v2,|,:,null,null",
      "k1:v1|k2:v2,|,null,k3,null",
      "k1=v1;k2=v2,;,=,k2,v2",
      "k1=v1;k2=v2;,;,=,k2,v2",
      "k1=v1;k2,;,=,k2,null",
      "k1;k2=v2,;,=,k1,null",
      "k=1=v1;k2=v2,;,=,k=1,null",
      "k1==v1;k2=v2,;,=,k1,=v1",
      "k1==v1;k2=v2,;,=,k1=,null",
      "k1k1=v1;k2=v2,;,=,k1,null",
      "null,|,:,k3,null",
      "k1=v1abababk2=v2,ababc,=,abk2,null",
      "k1=v1abababk2=v2,ababc,=,k2,v2",
      "k1:v1 k2:v2,null,:,k2,v2",
      "k1 v1;k2 v2,;,null,k2,v2"
    )
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testNullEqual(): Unit = {
    val data = new mutable.MutableList[(String, String, String, String)]
    data.+=(("k1:v1|k2:v2", null, ":", "k3"))

    val sqlQuery = "SELECT str,split1,split2,key1," +
      "case when keyvalue('' , ',' , '=' , 'spm-cnt') = '0.0.0.0' then 'xx' end " +
      " from T1"

    val t1 = env.fromCollection(data)
             .toTable(tEnv, 'str, 'split1, 'split2, 'key1, 'proctime.proctime)

    tEnv.registerTable("T1", t1)

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = List(
      "k1:v1|k2:v2,null,:,k3,null"
    )
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testHashCode(): Unit = {
    val data = new mutable.MutableList[(String, String, String, Boolean, Short, Int, Long,
      Float, Double, JBigDecimal, SqlDate, SqlTime, SqlTimestamp, Array[Byte])]
    data.+=(("k1=v1;k2=v2", "k1:v1,k2:v2", null, true, 1, 2, 3L, 4.0f, 5.0,
      BigDecimal(1).bigDecimal, SqlDate.valueOf("2017-11-10"), SqlTime.valueOf("22:23:24"),
      SqlTimestamp.valueOf("2017-10-15 00:00:00"), "hello world".getBytes))

    val sqlQuery =
      s"""
         |SELECT
         |  HASH_CODE(str1), HASH_CODE(str2), HASH_CODE(nullstr), HASH_CODE(bool),
         |  HASH_CODE(short1), HASH_CODE(int1), HASH_CODE(long1), HASH_CODE(float1),
         |  HASH_CODE(double1), HASH_CODE(decimal1), HASH_CODE(date1), HASH_CODE(time1),
         |  HASH_CODE(timestamp1), HASH_CODE(bytearray)
         |FROM T1
       """.stripMargin

    val t1 = env.fromCollection(data).toTable(tEnv,
      'str1, 'str2, 'nullstr, 'bool, 'short1, 'int1, 'long1, 'float1, 'double1,
      'decimal1, 'date1, 'time1, 'timestamp1, 'bytearray, 'proctime.proctime)

    tEnv.registerTable("T1", t1)

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = List(
      Math.abs(data(0)._1.hashCode) + "," +
        Math.abs(data(0)._2.hashCode) + "," +
        "null,1231,1,2,3,1082130432,1075052544,1571411461," +
        "17480,80604000,492079455,-1528836094")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testMd5(): Unit = {
    val data = new mutable.MutableList[(String, String, String)]
    data.+=(("k1=v1;k2=v2", "k1:v1,k2:v2", null))

    val sqlQuery = "SELECT MD5(str1), MD5(str2, 'utf8')," +
      "MD5(nullstr, 'utf8'),MD5(str2, nullstr) from T1"

    val t1 = env.fromCollection(data).toTable(tEnv, 'str1, 'str2, 'nullstr, 'proctime.proctime)

    tEnv.registerTable("T1", t1)

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected =
      List("19c17f42b4d6a90f7f9ffc2ea9bdd775,e646cfbcd66318124b8886fa13ff8d08,null,null")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testRegExp(): Unit = {
    val data = new mutable.MutableList[(String, String)]
    data.+=(("k1:v1|k2:v2", "("))
    data.+=(("k1:v1|k2:v2", "k3"))
    data.+=(("k1:v1|k2:v2", null))
    data.+=(("k1=v1;k2=v2", "k2*"))
    data.+=((null, "k3"))

    val sqlQuery = "SELECT str1,pattern1," +
      "REGEXP(str1, pattern1) as `result` from T1"

    val t1 = env.fromCollection(data).toTable(tEnv, 'str1, 'pattern1, 'proctime.proctime)

    tEnv.registerTable("T1", t1)

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = List(
      "k1:v1|k2:v2,(,false",
      "k1:v1|k2:v2,k3,false",
      "k1:v1|k2:v2,null,null",
      "k1=v1;k2=v2,k2*,true",
      "null,k3,null"
    )
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testParseUrl(): Unit = {
    val data = new mutable.MutableList[(String, String)]
    data.+=(("http://facebook.com/path/p1.php?query=1", null))

    val sqlQuery = "SELECT " +
      "PARSE_URL(url1, 'QUERY', 'query'), " +
      "PARSE_URL(url1, 'QUERY')," +
      "PARSE_URL(url1, 'HOST')," +
      "PARSE_URL(url1, 'PATH')," +
      "PARSE_URL(url1, 'QUERY')," +
      "PARSE_URL(url1, 'REF')," +
      "PARSE_URL(url1, 'PROTOCOL')," +
      "PARSE_URL(url1, 'FILE')," +
      "PARSE_URL(url1, 'AUTHORITY')," +
      "PARSE_URL(nullstr, 'QUERY')," +
      "PARSE_URL(url1, 'USERINFO')," +
      "PARSE_URL(nullstr, 'QUERY', 'query')" +
      " from T1"

    val t1 = env.fromCollection(data).toTable(
      tEnv, 'url1, 'nullstr, 'proctime.proctime)

    tEnv.registerTable("T1", t1)

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = List(
      "1,query=1,facebook.com,/path/p1.php,query=1," +
        "null,http,/path/p1.php?query=1,facebook.com,null,null,null")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Ignore
  @Test
  def testPrint(): Unit = {
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"))

    val data = new mutable.MutableList[(
      String, Byte, Short,
        Int, Long, Array[Byte],
        Double, SqlDate, SqlTimestamp)]
    data.+=(("Jack", 97.toByte, 65.toShort,
      11, 23.toLong, "Hello world".getBytes,
      56.3, SqlDate.valueOf("2017-11-10"), SqlTimestamp.valueOf("2017-10-15 00:00:00")))
    data.+=((null, 0.toByte, 0.toShort,
      2, 0.toLong, "你好".getBytes,
      0, SqlDate.valueOf("2017-11-10"), SqlTimestamp.valueOf("2017-10-15 00:00:00")))

    env.setParallelism(1)

    val t1 = env.fromCollection(data).toTable(tEnv, 'str1, 'byte1, 'short1, 'int1,
      'long1, 'byteArray1, 'double1, 'date1, 'timestamp1, 'proctime.proctime)
    tEnv.registerTable("T1", t1)

    val fieldNames = Array(
      "str1", "str2", "byte1", "short1",
      "int1", "long1", "byteArray1",
      "double1", "date1", "timestamp1")
    val fieldTypes: Array[DataType] = Array(
      DataTypes.STRING, DataTypes.STRING, DataTypes.BYTE, DataTypes.SHORT,
      DataTypes.INT, DataTypes.LONG, DataTypes.BYTE_ARRAY,
      DataTypes.DOUBLE, DataTypes.DATE, DataTypes.TIMESTAMP)
    val sink = new MemoryTableSourceSinkUtil.UnsafeMemoryAppendTableSink
    tEnv.registerTableSink("targetTable", fieldNames, fieldTypes, sink)

    val sqlQuery = "INSERT INTO targetTable SELECT " +
      "PRINT('str1: ', str1), " +
      "PRINT(str1, str1), " +
      "PRINT('byte1 ', byte1), " +
      "PRINT('short1 ', short1), " +
      "PRINT('int1 ', int1), " +
      "PRINT('long1 ', long1), " +
      "PRINT('byteArray1: ', byteArray1), " +
      "PRINT('double1: ', double1), " +
      "PRINT('date1: ', date1), " +
      "PRINT('timestamp1: ', timestamp1)" +
      " from T1"
    tEnv.sqlUpdate(sqlQuery)

    // output to special stream
    val baos = new ByteArrayOutputStream
    val ps = new PrintStream(baos)
    val old = System.out // save output stream
    System.setOut(ps)

    // execute
    env.execute()

    // flush
    System.out.flush()
    System.setOut(old) // set output stream back

    val expected = "str1: Jack\n" +
      "JackJack\n" +
      "byte1 97\n" +
      "short1 65\n" +
      "int1 11\n" +
      "long1 23\n" +
      "byteArray1: Hello world\n" +
      "double1: 56.3\n" +
      "date1: 17480\n" +
      "timestamp1: 1508025600000\n" +
      "str1: null\n" +
      "nullnull\n" +
      "byte1 0\n" +
      "short1 0\n" +
      "int1 2\n" +
      "long1 0\n" +
      "byteArray1: 你好\n" +
      "double1: 0.0\n" +
      "date1: 17480\n" +
      "timestamp1: 1508025600000\n"
    assertEquals(expected, baos.toString)
  }

  @Test
  def testNow(): Unit = {
    val data = new mutable.MutableList[(Int, String)]
    data.+=((2000, null))

    val sqlQuery = "SELECT NOW(), NOW(a) from T1"

    val t1 = env.fromCollection(data).toTable(tEnv, 'a, 'nullstr, 'proctime.proctime)

    tEnv.registerTable("T1", t1)

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    println(sink.getAppendResults.sorted)
    //val expected = List("true,false")
    //assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testWeek(): Unit = {
    val data = new mutable.MutableList[(String, SqlDate, SqlTimestamp)]
    data.+=(("2017-09-15",
              SqlDate.valueOf("2017-11-10"),
              SqlTimestamp.valueOf("2017-10-15 00:00:00")))

    val sqlQuery = "SELECT " +
      "WEEK(TIMESTAMP '2017-09-15 00:00:00'), " +
      "WEEK(date1), " +
      "WEEK(ts1), " +
      "WEEK(CAST(dateStr AS DATE)) from T1"

    val t1 = env.fromCollection(data).toTable(
      tEnv, 'dateStr,'date1,'ts1, 'proctime.proctime)

    tEnv.registerTable("T1", t1)

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = List("37,45,41,37")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testDateFormat(): Unit = {
    val data = new mutable.MutableList[(String, String, String)]
    data.+=(("0915-2017", "2017-09-15 1:00:00", null))

    val sqlQuery = "SELECT " +
      "DATE_FORMAT(TIMESTAMP '2017-09-15 23:00:00', 'yyMMdd HH:mm:ss'), " +
      "DATE_FORMAT(datetime1, 'yyMMdd'), " +
      "DATE_FORMAT(nullstr, 'yyMMdd'), " +
      "DATE_FORMAT(datetime1, nullstr), " +
      "DATE_FORMAT(date1, 'MMdd-yyyy', nullstr), " +
      "DATE_FORMAT(date1, 'MMdd-yyyy', 'yyyyMMdd')," +
      "DATE_FORMAT(date1, 'MMdd-yyyygg', 'yyyyMMddgg')" +
      " from T1"

    val t1 = env.fromCollection(data)
      .toTable(tEnv, 'date1, 'datetime1, 'nullstr, 'proctime.proctime)

    tEnv.registerTable("T1", t1)

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = List("170915 23:00:00,170915,null,null,null,20170915,null")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testUnixTimestamp(): Unit = {
    val data = new mutable.MutableList[(String, String, String)]
    data.+=(("0915-2017", "2017-09-15 00:00:00", null))

    val sqlQuery = "SELECT " +
      "UNIX_TIMESTAMP(datetime1)," +
      "UNIX_TIMESTAMP(nullstr)," +
      "UNIX_TIMESTAMP(date1,nullstr)," +
      "UNIX_TIMESTAMP(date1,'MMdd-yyyy')," +
      "UNIX_TIMESTAMP(date1,'MMdd-yyyygg')," +
      "UNIX_TIMESTAMP(TIMESTAMP '2017-10-15 23:11:12')" +
      " from T1"

    val t1 = env.fromCollection(data)
      .toTable(tEnv, 'date1, 'datetime1, 'nullstr, 'proctime.proctime)

    tEnv.registerTable("T1", t1)

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = List("1505433600,null,null,1505433600,-9223372036854775808,1508109072")
    assertEquals(expected, sink.getAppendResults.sorted)
    println(sink.getAppendResults.sorted)
  }

  @Test
  def testUnixTimestampDeterministic(): Unit = {
    val data = new mutable.MutableList[(String, String, String)]
    data.+=(("0915-2017", "2017-09-15 00:00:00", null))

    val sqlQuery = "SELECT " +
      "UNIX_TIMESTAMP()," +
      "date1" +
      " from T1 WHERE UNIX_TIMESTAMP() > 0"

    val t1 = env.fromCollection(data)
      .toTable(tEnv, 'date1, 'datetime1, 'nullstr, 'proctime.proctime)

    tEnv.registerTable("T1", t1)

    val relNode = tEnv.sqlQuery(sqlQuery).getRelNode
    val optimized = tEnv.optimize(relNode, updatesAsRetraction = false)
    val result = FlinkRelOptUtil.toString(optimized)
    assertEquals(
    """StreamExecCalc(select=[UNIX_TIMESTAMP() AS EXPR$0, date1])
      |+- StreamExecCalc(select=[date1], where=[>(UNIX_TIMESTAMP(), 0)])
      |   +- StreamExecCalc(select=[date1])
      |      +- StreamExecDataStreamScan(table=[[builtin, default, _DataStreamTable_0]])
      |""".stripMargin,
      result)
  }

  @Test
  def testFromUnixTime(): Unit = {
    val data = new mutable.MutableList[(Int, String)]
    data.+=((1505404000, null))

    val sqlQuery = "SELECT " +
      "FROM_UNIXTIME(unixtime1), " +
      "FROM_UNIXTIME(unixtime1,'MMdd-yyyy'), " +
      "FROM_UNIXTIME(unixtime1,'MMdd-yyyyggg'), " +
      "FROM_UNIXTIME(unixtime1,nullstr) " +
      "from T1"

    val t1 = env.fromCollection(data).toTable(tEnv, 'unixtime1, 'nullstr, 'proctime.proctime)

    tEnv.registerTable("T1", t1)

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    val formatter2 = new SimpleDateFormat("MMdd-yyyy")

    val expected = List(
      formatter.format(new SqlTimestamp(1505404000L * 1000))
      + "," + formatter2.format(new SqlTimestamp(1505404000L * 1000))
        + ",null,null")
    assertEquals(expected, sink.getAppendResults.sorted)
  }

  @Test
  def testDateDiff(): Unit = {
    val data = new mutable.MutableList[(String, String, String)]
    data.+=(
      ("2017-10-15 00:00:00", "2017-09-15 00:00:00", null))

    val sqlQuery = "SELECT " +
      "DATEDIFF(datetime1, datetime2), " +
      "DATEDIFF(TIMESTAMP '2017-10-15 23:00:00',datetime2), " +
      "DATEDIFF(datetime2,TIMESTAMP '2017-10-15 23:00:00'), " +
      "DATEDIFF(datetime2,nullstr), " +
      "DATEDIFF(nullstr,TIMESTAMP '2017-10-15 23:00:00'), " +
      "DATEDIFF(nullstr,datetime2), " +
      "DATEDIFF(TIMESTAMP '2017-10-15 23:00:00',TIMESTAMP '2017-9-15 00:00:00'), " +
      "DATEDIFF('2016-02-31','2016-2-28'), " +
      "DATEDIFF('2017-10-13 0:0:0','2017-10-11 17:41:00'), " +
      "DATEDIFF('2017-10-13 0:0:0',TIMESTAMP '2017-10-11 17:41:00') from T1"

    val t1 = env.fromCollection(data)
      .toTable(tEnv, 'datetime1, 'datetime2, 'nullstr, 'proctime.proctime)

    tEnv.registerTable("T1", t1)

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    // JDK's bug? SimpleDateFormat will parse '2016-02-31' as date ''2016-03-03'
    // Here, keep consistant with JDK
    val expected = List("30,30,-30,null,null,null,30,3,2,2")
    assertEquals(expected, sink.getAppendResults.sorted)
  }

  @Test
  def testDateSub(): Unit = {
    val data = new mutable.MutableList[(String, String)]
    data.+=(("2017-10-15", null))

    val sqlQuery = "SELECT " +
      "DATE_SUB(date1, 30), " +
      "DATE_SUB(TIMESTAMP '2017-10-15 23:00:00',30), " +
      "DATE_SUB(nullstr,30), " +
      "DATE_SUB('2017-10--11',1), " +
      "DATE_SUB('2017--10-11',1)" +
      " from T1"

    val t1 = env.fromCollection(data).toTable(tEnv, 'date1, 'nullstr, 'proctime.proctime)

    tEnv.registerTable("T1", t1)

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = List("2017-09-15,2017-09-15,null,null,null")
    assertEquals(expected, sink.getAppendResults.sorted)
  }

  @Test
  def testDateAdd(): Unit = {
    val data = new mutable.MutableList[(String, String)]
    data.+=(("2017-09-15 00:00:00", null))

    val sqlQuery = "SELECT " +
      "DATE_ADD(datetime1, 30), " +
      "DATE_ADD(TIMESTAMP '2017-09-15 23:00:00',30)," +
      "DATE_ADD(nullstr,30) " +
      "from T1"

    val t1 = env.fromCollection(data).toTable(tEnv, 'datetime1, 'nullstr, 'proctime.proctime)

    tEnv.registerTable("T1", t1)

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = List("2017-10-15,2017-10-15,null")
    assertEquals(expected, sink.getAppendResults.sorted)
  }

  @Test
  def testYear(): Unit = {
    val data = new mutable.MutableList[(String, String, SqlDate, SqlTimestamp)]
    data.+=(
      ("2017-10-15 00:00:00",
        "2015-09-15",
        SqlDate.valueOf("2017-11-10"),
        SqlTimestamp.valueOf("2017-10-15 00:00:00"))
    )

    val sqlQuery = "SELECT " +
      "YEAR(TIMESTAMP '2016-09-15 00:00:00'), " +
      "YEAR(DATE '2017-09-22'), " +
      "YEAR(tdate), " +
      "YEAR(ts), " +
      "YEAR(CAST(dateStr AS DATE)), " +
      "YEAR(CAST(tsStr AS TIMESTAMP)) " +
      "from T1"

    val t1 = env.fromCollection(data)
      .toTable(tEnv, 'tsStr, 'dateStr, 'tdate, 'ts, 'proctime.proctime)

    tEnv.registerTable("T1", t1)

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = List("2016,2017,2017,2017,2015,2017")
    assertEquals(expected, sink.getAppendResults.sorted)
  }

  @Test
  def testMonth(): Unit = {
    val data = new mutable.MutableList[(String, String, SqlDate, SqlTimestamp)]
    data.+=(
      ("2017-10-15 00:00:00",
        "2015-09-15",
        SqlDate.valueOf("2017-11-10"),
        SqlTimestamp.valueOf("2017-10-15 00:00:00"))
    )

    val sqlQuery = "SELECT " +
      "MONTH(TIMESTAMP '2016-09-15 00:00:00'), " +
      "MONTH(DATE '2017-09-22'), " +
      "MONTH(tdate), " +
      "MONTH(ts), " +
      "MONTH(CAST(dateStr AS DATE)), " +
      "MONTH(CAST(tsStr AS TIMESTAMP)) " +
      "from T1"

    val t1 = env.fromCollection(data)
             .toTable(tEnv, 'tsStr, 'dateStr, 'tdate, 'ts, 'proctime.proctime)

    tEnv.registerTable("T1", t1)

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = List("9,9,11,10,9,10")
    assertEquals(expected, sink.getAppendResults.sorted)
  }

  @Test
  def testDay(): Unit = {
    val data = new mutable.MutableList[(String, String, SqlDate, SqlTimestamp)]
    data.+=(
      ("2017-10-15 00:00:00",
        "2015-09-15",
        SqlDate.valueOf("2017-11-10"),
        SqlTimestamp.valueOf("2017-10-15 00:00:00"))
    )

    val sqlQuery = "SELECT " +
      "DAYOFMONTH(TIMESTAMP '2016-09-15 00:00:00'), " +
      "DAYOFMONTH(DATE '2017-09-22'), " +
      "DAYOFMONTH(tdate), " +
      "DAYOFMONTH(ts), " +
      "DAYOFMONTH(CAST(dateStr AS DATE)), " +
      "DAYOFMONTH(CAST(tsStr AS TIMESTAMP)) " +
      "from T1"

    val t1 = env.fromCollection(data)
             .toTable(tEnv, 'tsStr, 'dateStr, 'tdate, 'ts, 'proctime.proctime)

    tEnv.registerTable("T1", t1)

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = List("15,22,10,15,15,15")
    assertEquals(expected, sink.getAppendResults.sorted)
  }

  @Test
  def testHour(): Unit = {
    val data =
      new mutable.MutableList[(String, String, String, SqlTime, SqlTimestamp)]
    data.+=(("2017-10-15 11:12:13", "22:23:24", null,
      SqlTime.valueOf("22:23:24"),
      SqlTimestamp.valueOf("2017-10-15 11:12:13")))

    val sqlQuery = "SELECT " +
      "HOUR(TIMESTAMP '2016-09-20 23:33:33')," +
      "HOUR(TIME '23:30:33'), " +
      "HOUR(time2), " +
      "HOUR(timestamp1), " +
      "HOUR(CAST(time1 AS TIME)), " +
      "HOUR(CAST(datetime1 AS TIMESTAMP)) " +
      "from T1"

    val t1 = env.fromCollection(data)
      .toTable(tEnv, 'datetime1, 'time1, 'nullstr, 'time2, 'timestamp1, 'proctime.proctime)

    tEnv.registerTable("T1", t1)

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = List("23,23,22,11,22,11")
    assertEquals(expected, sink.getAppendResults.sorted)
  }

  @Test
  def testMinute(): Unit = {
    val data =
      new mutable.MutableList[(String, String, String, SqlTime, SqlTimestamp)]
    data.+=(("2017-10-15 11:12:13", "22:23:24", null,
      SqlTime.valueOf("22:23:24"),
      SqlTimestamp.valueOf("2017-10-15 11:12:13")))

    val sqlQuery = "SELECT " +
      "MINUTE(TIMESTAMP '2016-09-20 23:33:33')," +
      "MINUTE(TIME '23:30:33'), " +
      "MINUTE(time2), " +
      "MINUTE(timestamp1), " +
      "MINUTE(CAST(time1 AS TIME)), " +
      "MINUTE(CAST(datetime1 AS TIMESTAMP)) " +
      "from T1"

    val t1 = env.fromCollection(data)
      .toTable(tEnv, 'datetime1, 'time1, 'nullstr, 'time2, 'timestamp1, 'proctime.proctime)

    tEnv.registerTable("T1", t1)

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = List("33,30,23,12,23,12")
    assertEquals(expected, sink.getAppendResults.sorted)
  }

  @Test
  def testSecond(): Unit = {
    val data =
      new mutable.MutableList[(String, String, String, SqlTime, SqlTimestamp)]
    data.+=(("2017-10-15 11:12:13", "22:23:24", null,
      SqlTime.valueOf("22:23:24"),
      SqlTimestamp.valueOf("2017-10-15 11:12:13")))

    val sqlQuery = "SELECT " +
      "SECOND(TIMESTAMP '2016-09-20 23:33:33')," +
      "SECOND(TIME '23:30:33'), " +
      "SECOND(time2), " +
      "SECOND(timestamp1), " +
      "SECOND(CAST(time1 AS TIME)), " +
      "SECOND(CAST(datetime1 AS TIMESTAMP)) " +
      "from T1"

    val t1 = env.fromCollection(data)
      .toTable(tEnv, 'datetime1, 'time1, 'nullstr, 'time2, 'timestamp1, 'proctime.proctime)

    tEnv.registerTable("T1", t1)

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = List("33,33,24,13,24,13")
    assertEquals(expected, sink.getAppendResults.sorted)
  }

  @Test
  def testBin(): Unit = {
    val data = new mutable.MutableList[(Int, Long)]
    data.+=((1, 12L))
    data.+=((2, 10L))
    data.+=((3, 0L))
    data.+=((4, 10000000000L))

    val sqlQuery = "SELECT id, bin(x) FROM T1"

    val t1 = env.fromCollection(data).toTable(tEnv, 'id, 'x, 'proctime.proctime)

    tEnv.registerTable("T1", t1)

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = List("1,1100", "2,1010", "3,0", "4,1001010100000010111110010000000000")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testBitOperator(): Unit = {
    val data = new mutable.MutableList[(Int, Int, Long, Long)]
    data.+=((1, 1, 2L, 2L))

    val sqlQuery = "SELECT " +
      "bitand(a, b), bitand(c, d), " +
      "bitor(a, b), bitor(c, d), " +
      "bitxor(a, b), bitxor(c, d), " +
      "bitnot(a), bitnot(c) " +
      "FROM T1"

    val t1 = env.fromCollection(data).toTable(tEnv, 'a, 'b, 'c, 'd, 'proctime.proctime)

    tEnv.registerTable("T1", t1)

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = List(
      (1 & 1) + "," + (2L & 2L) + "," + (1 | 1) + "," + (2L | 2L) + "," +
        (1 ^ 1) + "," + (2L ^ 2L) + "," + (~1) + "," + (~2L))
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  /**
    * please using operator `/`
    */
  @Test
  @Deprecated
  def testDIV(): Unit = {
    val data = new mutable.MutableList[(Int, Int, Int, Int)]
    data.+=((1, 2, 2, 1))

    val sqlQuery = "SELECT " +
      "a / b, DIV(a, b), " +
      "c / d, DIV(c, d)" +
      "FROM T1"

    val t1 = env.fromCollection(data).toTable(tEnv, 'a, 'b, 'c, 'd, 'proctime.proctime)

    tEnv.registerTable("T1", t1)

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = List(
      (1.0 / 2) + "," + (1 / 2) + "," + (2.0 / 1) + "," + (2 / 1))
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testToBase64(): Unit = {
    val data = new mutable.MutableList[(Int, Long, Array[Byte])]
    data.+=((3, 2L, "Hello world".getBytes))
    data.+=((1, 1L, "Hi".getBytes))
    data.+=((2, 2L, "Hello".getBytes))

    val ds = env.fromCollection(data)

    val t = ds.toTable(tEnv, 'a, 'b, 'c, 'proctime.proctime)
    tEnv.registerTable("T1", t)

    val sqlQuery = "SELECT to_base64(from_base64(to_base64(c))) FROM T1"

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = List("SGVsbG8gd29ybGQ=", "SGk=", "SGVsbG8=")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testFromBase64Null(): Unit = {
    val data = new mutable.MutableList[(Int, Long, String)]
    data.+=((1, 1L, null))

    val ds = env.fromCollection(data)

    val t = ds.toTable(tEnv, 'a, 'b, 'c, 'proctime.proctime)
    tEnv.registerTable("T1", t)

    val sqlQuery = "SELECT from_base64(c) FROM T1"

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = List("null")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testIfBinary(): Unit = {
    implicit val tpe: TypeInformation[Row] = new RowTypeInfo(
      Types.INT,
      Types.PRIMITIVE_ARRAY(Types.BYTE),
      Types.PRIMITIVE_ARRAY(Types.BYTE)) // tpe is automatically

    val data = new mutable.MutableList[Row]
    data += Row.of(Integer.valueOf(1), "Hello".getBytes, "Hi".getBytes)
    data += Row.of(Integer.valueOf(2), "Hello world".getBytes, "Hello".getBytes)
    data += Row.of(Integer.valueOf(3), "Hi".getBytes, "Hello world".getBytes)

    val ds = env.fromCollection(data)

    val t = ds.toTable(tEnv, 'a, 'b, 'c, 'proctime.proctime)
    tEnv.registerTable("T1", t)

    val sqlQuery = "SELECT if(a <= 2, b, c) FROM T1"

    val returnType = new RowTypeInfo(Types.PRIMITIVE_ARRAY(Types.BYTE))
    val result = tEnv.sqlQuery(sqlQuery).toAppendStream(returnType)
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = List(
      "[72, 101, 108, 108, 111, 32, 119, 111, 114, 108, 100]", //Hello world".getBytes
      "[72, 101, 108, 108, 111, 32, 119, 111, 114, 108, 100]", //Hello world".getBytes
      "[72, 101, 108, 108, 111]") //"Hello".getBytes.toString
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testUUID(): Unit = {
    val data = new mutable.MutableList[(Int, Array[Byte], Array[Byte])]
    data.+=((1, "Hello".getBytes, "Hi".getBytes))

    val ds = env.fromCollection(data)

    val t = ds.toTable(tEnv, 'a, 'b, 'c, 'proctime.proctime)
    tEnv.registerTable("T1", t)

    val sqlQuery = "SELECT uuid() FROM T1"

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    //assertEquals(expected.sorted, sink.getAppendResults.sorted)
    println(sink.getAppendResults.sorted)
  }

  @Test
  def testUUIDWithBytes(): Unit = {
    val b = "Hi".getBytes
    val md5: MessageDigest = MessageDigest.getInstance("MD5")
    md5.update(b, 0, b.length)


    val data = new mutable.MutableList[(Int, String, Array[Byte])]
    data.+=((1, "Hi", "Hi".getBytes))

    val ds = env.fromCollection(data)


    val t = ds.toTable(tEnv, 'a, 'b, 'c, 'proctime.proctime)
    tEnv.registerTable("T1", t)

    val sqlQuery = "SELECT uuid(c) FROM T1"

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = List(UUID.nameUUIDFromBytes("Hi".getBytes).toString)
    //uuid:c1a5298f-939e-37e8-b962-a5edfc206918
    //md5("Hi"):c1a5298f939e87e8f962a5edfc206918
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testCardinality(): Unit = {
    val data = new mutable.MutableList[(Int, Array[Int])]
    data.+=((1, Array(1, 2, 3)))
    val ds = env.fromCollection(data)

    val t = ds.toTable(tEnv, 'a, 'b, 'proctime.proctime)
    tEnv.registerTable("T1", t)

    val sqlQuery = "SELECT cardinality(b) FROM T1"

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = List(Array(1, 2, 3).length.toString)
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testSubString(): Unit = {
    val data = new mutable.MutableList[(String, String)]
    data.+=(("k1=v1;k2=v2", null))

    val sqlQuery = "SELECT " +
      "SUBSTRING('', 222222222)," +
      "SUBSTRING(str, 2)," +
      "SUBSTRING(str, -2)," +
      "SUBSTRING(str, -2, 1), " +
      "SUBSTRING(str, 2, 1)," +
      "SUBSTRING(str, 22)," +
      "SUBSTRING(str, -22)," +
      "SUBSTRING(str, 1)," +
      "SUBSTRING(str, 0)," +
      "SUBSTRING(nullstr, 0) from T1"

    val t1 = env.fromCollection(data).toTable(tEnv, 'str, 'nullstr, 'proctime.proctime)

    tEnv.registerTable("T1", t1)

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = List(",1=v1;k2=v2,v2,v,1,,,k1=v1;k2=v2,k1=v1;k2=v2,null")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testToDate(): Unit = {
    val data = new mutable.MutableList[(Int, String, String)]
    data.+=((100, "2017-09-15", "2017/09/15"))

    val sqlQuery = "SELECT " +
      "TO_DATE(date1), " +
      "TO_DATE(date2), " +
      "TO_DATE(date3, 'yyyy/MM/dd') " +
      " from T1"

    val t1 = env.fromCollection(data)
      .toTable(tEnv, 'date1, 'date2, 'date3, 'proctime.proctime)

    tEnv.registerTable("T1", t1)

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = List("1970-04-11,2017-09-15,2017-09-15")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testToTimestamp(): Unit = {
    val data = new mutable.MutableList[(Long, String, String)]
    data.+=((1513135677000L, "2017-09-15 00:00:00", "20170915000000"))

    val sqlQuery = "SELECT " +
      "TO_TIMESTAMP(timestamp1), " +
      "TO_TIMESTAMP(timestamp2), " +
      "TO_TIMESTAMP(timestamp3, 'yyyyMMddHHmmss')" +
      " from T1"

    val t1 = env.fromCollection(data)
      .toTable(tEnv, 'timestamp1, 'timestamp2, 'timestamp3, 'proctime.proctime)

    tEnv.registerTable("T1", t1)

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = List("2017-12-13 03:27:57.0,2017-09-15 00:00:00.0,2017-09-15 00:00:00.0")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testFromTimestamp(): Unit = {
    val data = new mutable.MutableList[(Long, String)]
    data.+=((1513135677000L, "2017-09-15 00:00:00"))

    val sqlQuery = "SELECT " +
      "FROM_TIMESTAMP(TO_TIMESTAMP(timestamp1)), " +
      "FROM_TIMESTAMP(TO_TIMESTAMP(timestamp2)) " +
      " from T1"

    val t1 = env.fromCollection(data)
      .toTable(tEnv, 'timestamp1, 'timestamp2, 'proctime.proctime)

    tEnv.registerTable("T1", t1)

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = List("1513135677000,1505433600000")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testLiteral(): Unit = {
    val data = new mutable.MutableList[(String, String, String)]
    data.+=((null, null, null))

    val udf0 = new LiteralUDF("\"\\")
    val udf1 = new LiteralUDF("\u0001xyz")
    val udf2 = new LiteralUDF("\u0001\u0012")

    tEnv.registerFunction("udf0", udf0)
    tEnv.registerFunction("udf1", udf1)
    tEnv.registerFunction("udf2", udf2)

    val sqlQuery = "SELECT udf0('\"\\') as str1, " +
      "udf1('\u0001xyz') as str2, " +
      "udf2('\u0001\u0012') as str3 from T1"

    val t1 = env.fromCollection(data).toTable(tEnv, 'str1, 'str2, 'str3)

    tEnv.registerTable("T1", t1)

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = List("\"\\,\u0001xyz,\u0001\u0012")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testStringToMap(): Unit = {
    val data = new mutable.MutableList[(String, String)]
    data.+=(("k1=v1,k2=v2,k3=v3", "test1:1;test2:2;test3:3"))

    val t1 = env.fromCollection(data).toTable(tEnv, 'a, 'b)
    tEnv.registerTable("T1", t1)

    val sqlQuery =
      """
        |SELECT
        |  map1['k1'], map1['k3'], map1['non-exist'], map2
        |FROM (
        | SELECT str_to_map(a) as map1, str_to_map(b, ';', ':') as map2 from T1
        |)
      """.stripMargin


    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    result.addSink(new SinkFunction[Row] {
      override def invoke(in: Row): Unit = {
        assertEquals(in.getField(0), "v1")
        assertEquals(in.getField(1), "v3")
        assertEquals(in.getField(2), null)
        val expectedMap = new util.HashMap[String, String]()
        expectedMap.put("test1", "1")
        expectedMap.put("test2", "2")
        expectedMap.put("test3", "3")
        assertEquals(in.getField(3), expectedMap)
      }
    })
    env.execute()
  }

  @Test
  def testStrToMapWithNull: Unit = {
    val data = new mutable.MutableList[(String, String)]
    data.+=((null, ""))

    val t1 = env.fromCollection(data).toTable(tEnv, 'a, 'b)
    tEnv.registerTable("T1", t1)

    val sqlQuery =
      """
        |SELECT STR_TO_MAP(SUBSTRING('', 1, -1))
        |       ,STR_TO_MAP(a)
        |       ,STR_TO_MAP(b)
        |FROM T1
      """.stripMargin

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    result.addSink((in: Row) => {
      assertEquals(in.getField(0), null)
      assertEquals(in.getField(1), null)
      val expectedMap = new util.HashMap[String, String]()
      assertEquals(in.getField(2), expectedMap)
    })
    env.execute()
  }

  @Test
  def testTimeZoneCurrentCast(): Unit = {
    val data = new mutable.MutableList[(String, String)]
    data.+=(("2018-03-12 14:00:00", "2018-03-12 14:00:00"))

    //--CURRENT_DATE,
    //--CURRENT_TIME
    val cases = Seq(
      "LOCALTIMESTAMP",
      "CURRENT_TIMESTAMP",
      "CURRENT_TIMESTAMP",
      "FROM_UNIXTIME(NOW())",
      "proctime"
    )
    val stmt = cases.map( "CAST(" + _ + " AS VARCHAR(25))")
      .foldLeft("") { (s1, s2) =>
        if (s1.length == 0)  s2
        else s1 + ",\n" + s2
      }
    val sqlQuery =
      s"""
         |SELECT
         | ${stmt}
         |FROM T1
       """.stripMargin

    // UTC            + 0
    // Asia/Tokyo     + 9
    // Asia/Shanghai  + 8
    // Asia/Bangkok   + 7
    // Asia/Rangoon   + 6.5
    // Asia/Kolkata   + 5.5
    // Asia/Kathmandu + 5.75 //in Java, it is +5.5
    // TODO: add Daylight Saving Time cases
    val zones = Seq ("UTC", "Asia/Kolkata",  "Asia/Rangoon", "Asia/Shanghai")

    zones.foreach( zoneId => {
      val tableConfig = new TableConfig();
      val testZone = TimeZone.getTimeZone(zoneId);
      tableConfig.setTimeZone(testZone)

      tableConfig.getConf.setDouble(TableConfigOptions.SQL_RESOURCE_DEFAULT_CPU, 0.001)
      val env = StreamExecutionEnvironment.getExecutionEnvironment
      env.setParallelism(4)
      val tEnv = TableEnvironment.getTableEnvironment(env, tableConfig)

      val t1 = env.fromCollection(data).toTable(tEnv, 'a, 'b, 'proctime.proctime)
      tEnv.registerTable("T1", t1)
      val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
      val sink = new TestingAppendSink
      result.addSink(sink)
      env.execute()

      val  sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      val currMillis = System.currentTimeMillis()
      sdf.setTimeZone(testZone)
      val results = sink.getAppendResults
      assertTrue(results.size == 1)
      results(0).split(",").foreach(x => {
        // assume that the test cases can be done in one minutes
        assertTrue(currMillis - sdf.parse(x).getTime() < 1000 * 60)
      })
    })
  }
}

object Concat extends ScalarFunction {
  def eval(field1: String, field2: String, field3: String): String = {
    "hello"
  }
}

class LiteralUDF(constant: String) extends ScalarFunction {
  def eval(x: String): String = {
    assert(x == constant)
    x
  }
  override def isDeterministic: Boolean = false
}
