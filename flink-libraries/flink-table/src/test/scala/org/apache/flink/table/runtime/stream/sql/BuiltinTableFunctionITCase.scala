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

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.runtime.utils.{StreamingTestBase, TestingAppendSink}
import org.apache.flink.types.Row
import org.junit.Assert._
import org.junit._

/**
  * Just for blink
  */
class BuiltinTableFunctionITCase extends StreamingTestBase {

  @Test
  def testStringSplit(): Unit = {
    val tab = org.apache.commons.lang3.StringEscapeUtils.unescapeJava("\002")
    val data = List(("abc-bcd","-"), ("hhh","-"), ("xxx" + tab + "yyy", tab))

    val sqlQuery = "SELECT d, v FROM T1, lateral table(STRING_SPLIT(d, s)) as T(v)"

    val t1 = env.fromCollection(data).toTable(tEnv, 'd, 's, 'proctime.proctime)

    tEnv.registerTable("T1", t1)

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = List("abc-bcd,abc", "abc-bcd,bcd", "hhh,hhh", "xxx" + tab + "yyy," +
      "xxx","xxx" + tab + "yyy,yyy")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testJsonTuple(): Unit = {
    val data = List(("{\"qwe\":\"asd\",\"qwe2\":\"asd2\",\"qwe3\":\"asd3\"}","qwe3"),
      ("{\"qwe\":\"asd4\",\"qwe2\":\"asd5\",\"qwe3\":\"asd3\"}","qwe2"))

    val sqlQuery = "SELECT d, v FROM T1, lateral table(JSON_TUPLE(d, 'qwe', s)) as T(v)"

    val t1 = env.fromCollection(data).toTable(tEnv, 'd, 's, 'proctime.proctime)

    tEnv.registerTable("T1", t1)

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = List("{\"qwe\":\"asd\",\"qwe2\":\"asd2\",\"qwe3\":\"asd3\"},asd",
      "{\"qwe\":\"asd\",\"qwe2\":\"asd2\",\"qwe3\":\"asd3\"},asd3",
      "{\"qwe\":\"asd4\",\"qwe2\":\"asd5\",\"qwe3\":\"asd3\"},asd4",
      "{\"qwe\":\"asd4\",\"qwe2\":\"asd5\",\"qwe3\":\"asd3\"},asd5")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testGenerateSeries(): Unit = {
    val data = List((1,3), (-2,1))

    val sqlQuery = "SELECT s, e, v FROM T1, lateral table(generate_series(s, e)) as T(v)"

    val t1 = env.fromCollection(data).toTable(tEnv, 's, 'e, 'proctime.proctime)

    tEnv.registerTable("T1", t1)

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = List("1,3,1","1,3,2","-2,1,-2","-2,1,-1","-2,1,0")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testDynamicMultiKeyValue(): Unit = {
    val data = List(
      ("a=1,b=2,c=3", ",", "=", "a", "b"),
      (null, ",", "=", "a", "b"),
      ("a=2,b=3,c=4", "-", "=", "a", "c"),
      ("a=11", ",", "=", "a", "b"),
      ("a=12", null, "=", "a", "b"),
      ("a=13", ",", null, "a", "b"),
      ("a=14", ",", "=", "", "b"),
      ("a=15", ",", "=", "a", ""),
      ("a=16,,b=2,c=3", ",,", "=", "a", "b"),
      ("a==17==2,b=2,==3", ",", "==", "a", "b"),
      ("aa=18,b=2,c=3", ",", "=", "a", "b"),
      (",,aa=19,,a=20,b=2,c=3", ",", "=", ",a", "b"),
      ("a=1 b=2,c=3", null, "=", "a", "b"),
      ("a=1,b 2,c=3", ",", null, ",a", "b"),
      ("a=1,b=2|c=3", "|,","=", "a", "c"),
      ("a=1,b:2,c:3", ",", ":=", "a", "c"))

    val sqlQuery = "SELECT d, c1, c2 FROM T1, " +
      "lateral table(multi_keyvalue(d, sep1, sep2, f1, f2)) as T(c1, c2)"

    val t1 = env.fromCollection(data).toTable(tEnv, 'd, 'sep1, 'sep2, 'f1, 'f2, 'proctime.proctime)

    tEnv.registerTable("T1", t1)

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = List(
      "a=1,b=2,c=3,1,2",
      "null,null,null",
      "a=2,b=3,c=4,null,null",
      "a=11,11,null",
      "a=12,12,null",
      "a=13,null,null",
      "a=14,null,null",
      "a=15,15,null",
      "a=16,,b=2,c=3,16,2",
      "a==17==2,b=2,==3,null,2",
      "aa=18,b=2,c=3,null,2",
      ",,aa=19,,a=20,b=2,c=3,null,2",
      "a=1 b=2,c=3,1,null",
      "a=1,b 2,c=3,null,2",
      "a=1,b=2|c=3,1,3",
      "a=1,b:2,c:3,1,3")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testConstantMultiKeyValue1(): Unit = {
    val data = List(("a=1,b=2,c=3"), (""), ("a=2,b=3,c=4"), ("a=1"))

    val sqlQuery = "SELECT d, c1, c2 FROM T1, " +
      "lateral table(multi_keyvalue(d, ',', '=', 'a', 'b')) as T(c1, c2)"

    val t1 = env.fromCollection(data).toTable(tEnv, 'd, 'proctime.proctime)

    tEnv.registerTable("T1", t1)

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = List("a=1,b=2,c=3,1,2", ",null,null", "a=2,b=3,c=4,2,3", "a=1,1,null")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testConstantMultiKeyValue2(): Unit = {
    val data = List(("a=1,b=2,c=3"), (""), ("a:2|b=3,c=4"), ("a:1"))

    val sqlQuery = "SELECT d, c1, c2 FROM T1, " +
      "lateral table(multi_keyvalue(d, ',|', '=:', 'a', 'b')) as T(c1, c2)"

    val t1 = env.fromCollection(data).toTable(tEnv, 'd, 'proctime.proctime)

    tEnv.registerTable("T1", t1)

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = List("a=1,b=2,c=3,1,2", ",null,null", "a:2|b=3,c=4,2,3", "a:1,1,null")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testConstantMultiKeyValue3(): Unit = {
    val data = List(("a=1 b=2 c=3"), (""), ("a=2\tb=3 c=4"), ("a=1"))

    val sqlQuery = "SELECT d, c1, c2 FROM T1, " +
      "lateral table(multi_keyvalue(d, null, '=', 'a', 'b')) as T(c1, c2)"

    val t1 = env.fromCollection(data).toTable(tEnv, 'd, 'proctime.proctime)

    tEnv.registerTable("T1", t1)

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = List("a=1 b=2 c=3,1,2", ",null,null", "a=2\tb=3 c=4,2,3", "a=1,1,null")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testConstantMultiKeyValue4(): Unit = {
    val data = List(("a\t1,b 2,c=3"), (""), ("a 2,b 3,c=4"), ("a=1"))

    val sqlQuery = "SELECT d, c1, c2 FROM T1, " +
      "lateral table(multi_keyvalue(d, ',', null, 'a', 'b')) as T(c1, c2)"

    val t1 = env.fromCollection(data).toTable(tEnv, 'd, 'proctime.proctime)

    tEnv.registerTable("T1", t1)

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = List("a\t1,b 2,c=3,1,2", ",null,null", "a 2,b 3,c=4,2,3", "a=1,null,null")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

}
