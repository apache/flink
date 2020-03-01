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

import org.apache.flink.api.scala._
import org.apache.flink.table.api.Types
import org.apache.flink.table.api.scala._
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedScalarFunctions.UdfWithOpen
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedTableFunctions.StringSplit
import org.apache.flink.table.planner.runtime.utils.{StreamingTestBase, TestSinkUtil, TestingAppendSink, TestingAppendTableSink}
import org.apache.flink.table.planner.utils.{RF, TableFunc7}
import org.apache.flink.types.Row

import org.junit.Assert.assertEquals
import org.junit.{Before, Test}

import java.lang.{Boolean => JBoolean}

import scala.collection.mutable

class CorrelateITCase extends StreamingTestBase {

  @Before
  override def before(): Unit = {
    super.before()
    tEnv.registerFunction("STRING_SPLIT", new StringSplit())
  }

  @Test
  // BLINK-13614111: Fix IndexOutOfBoundsException when UDTF is used on the
  // same name field of different tables
  def testUdtfForSameFieldofDifferentSource(): Unit = {
    val data = List(
      (1, 2, "abc-bcd"),
      (1, 2, "hhh"),
      (1, 2, "xxx"))

    val data2 = List(
      (1, "abc-bcd"),
      (1, "hhh"),
      (1, "xxx"))

    val t1 = env.fromCollection(data).toTable(tEnv, 'a, 'b, 'c)
    tEnv.registerTable("T1", t1)

    val t2 = env.fromCollection(data2).toTable(tEnv, 'a, 'c)
    tEnv.registerTable("T2", t2)

    val query1 = "SELECT a, v FROM T1, lateral table(STRING_SPLIT(c, '-')) as T(v)"
    tEnv.registerTable("TMP1", tEnv.sqlQuery(query1))

    val query2 = "SELECT a, v FROM T2, lateral table(STRING_SPLIT(c, '-')) as T(v)"
    tEnv.registerTable("TMP2", tEnv.sqlQuery(query2))

    val sql =
      """
        |SELECT * FROM TMP1
        |UNION ALL
        |SELECT * FROM TMP2
      """.stripMargin


    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = List(
      "1,abc", "1,abc", "1,bcd", "1,bcd", "1,hhh", "1,hhh", "1,xxx", "1,xxx")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testConstantTableFunc(): Unit = {
    tEnv.registerFunction("str_split", new StringSplit())
    val query = "SELECT * FROM LATERAL TABLE(str_split()) as T0(d)"
    val sink = new TestingAppendSink
    tEnv.sqlQuery(query).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = List("a", "b", "c")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testConstantTableFunc2(): Unit = {
    tEnv.registerFunction("str_split", new StringSplit())
    val query = "SELECT * FROM LATERAL TABLE(str_split('Jack,John', ',')) as T0(d)"
    val sink = new TestingAppendSink
    tEnv.sqlQuery(query).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = List("Jack", "John")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testUdfIsOpenedAfterUdtf(): Unit = {
    val data = List(
      (1, 2, "abc-bcd"),
      (1, 2, "hhh"),
      (1, 2, "xxx"))

    val t1 = env.fromCollection(data).toTable(tEnv, 'a, 'b, 'c)
    tEnv.registerTable("T1", t1)

    // UdfWithOpen checks open method is opened, and add a '$' prefix to the given string
    tEnv.registerFunction("func", new UdfWithOpen)

    val query1 =
      """
        |SELECT a, v
        |FROM T1, lateral table(STRING_SPLIT(c, '-')) as T(v)
        |WHERE func(v) LIKE '$%'
      """.stripMargin

    val sink = new TestingAppendSink
    tEnv.sqlQuery(query1).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = List("1,abc", "1,bcd","1,hhh", "1,xxx")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testMultipleCorrelate(): Unit = {
    val data = new mutable.MutableList[(String, String, String)]
    data.+=(("1", "1,L", "A,B"))
    data.+=(("2", "2,L", "B,C"))

    val t = env.fromCollection(data).toTable(tEnv, 'a, 'b, 'c)
    tEnv.registerTable("T1", t)
    tEnv.registerFunction("str_split", new StringSplit())
    val sink1 = new TestingAppendSink
    val sink2 = new TestingAppendSink

    // correlate 1
    val t1 = tEnv.sqlQuery(
      s"""
         |SELECT a, b, s
         |FROM T1, LATERAL TABLE(str_split(c, ',')) as T2(s)
       """.stripMargin)
    t1.toAppendStream[Row].addSink(sink1)

    // correlate 2
    val t2 = tEnv.sqlQuery(
      s"""
        |SELECT a, c, s
        |FROM T1, LATERAL TABLE(str_split(b, ',')) as T3(s)
      """.stripMargin)
    t2.toAppendStream[Row].addSink(sink2)

    env.execute()

    val expected = List("1,1,L,A", "1,1,L,B", "1,A,B,1", "1,A,B,L",
                        "2,2,L,B", "2,2,L,C", "2,B,C,2", "2,B,C,L")
    assertEquals(
      expected.sorted,
      (sink1.getAppendResults ++ sink2.getAppendResults).sorted)
  }

  @Test
  def testMultipleEvals(): Unit = {
    val row = Row.of(
      12.asInstanceOf[Integer],
      true.asInstanceOf[JBoolean],
      Row.of(1.asInstanceOf[Integer], 2.asInstanceOf[Integer], 3.asInstanceOf[Integer])
    )

    val rowType = Types.ROW(Types.INT, Types.BOOLEAN, Types.ROW(Types.INT, Types.INT, Types.INT))
    val in = env.fromElements(row, row)(rowType).toTable(tEnv, 'a, 'b, 'c)

    val sink = new TestingAppendSink

    tEnv.registerTable("MyTable", in)
    tEnv.registerFunction("rfFunc", new RF)
    tEnv.registerFunction("tfFunc", new TableFunc7)
    tEnv.sqlQuery(
      "SELECT rfFunc(a) as d, e FROM MyTable, LATERAL TABLE(tfFunc(rfFunc(a))) as T(e)")
        .toAppendStream[Row].addSink(sink)

    env.execute()

    assertEquals(List(), sink.getAppendResults.sorted)
  }

  @Test
  def testReUsePerRecord(): Unit = {
    val data = List(
      (1, 2, "3018-06-10|2018-06-03"),
      (1, 2, "2018-06-01"),
      (1, 2, "2018-06-02"))

    val t1 = env.fromCollection(data).toTable(tEnv, 'a, 'b, 'c)
    tEnv.registerTable("T1", t1)

    val query1 = "SELECT a, v FROM T1, lateral table(STRING_SPLIT(c, '|')) as T(v)"
    tEnv.registerTable("TMP1", tEnv.sqlQuery(query1))

    val sql =
      """
        |SELECT * FROM TMP1
        |where TIMESTAMPADD(day, 3, cast(v as date)) > DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd')
      """.stripMargin

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = List("1,3018-06-10")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testLeftJoinWithEmptyOutput(): Unit = {
    val data = List(
      (1, 2, ""),
      (1, 3, ""))

    val t1 = env.fromCollection(data).toTable(tEnv, 'a, 'b, 'c)
    tEnv.registerTable("T1", t1)

    val sql = "SELECT * FROM T1 left join lateral table(STRING_SPLIT(c, '|')) as T(v) on true"

    val result = tEnv.sqlQuery(sql)
    val sink = TestSinkUtil.configureSink(result, new TestingAppendTableSink)
    tEnv.registerTableSink("MySink", sink)
    tEnv.insertInto("MySink", result)
    tEnv.execute("test")

    val expected = List("1,2,,null", "1,3,,null")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testProjectCorrelateInput(): Unit = {
    val data = List(
      (1, 2, "3018-06-10|2018-06-03"),
      (1, 2, "2018-06-01"),
      (1, 2, "2018-06-02"))

    val t1 = env.fromCollection(data).toTable(tEnv, 'a, 'b, 'c)
    tEnv.registerTable("T1", t1)

    val sql = "SELECT v FROM T1, lateral table(STRING_SPLIT(c, '|')) as T(v)"

    val result = tEnv.sqlQuery(sql)
    val sink = TestSinkUtil.configureSink(result, new TestingAppendTableSink)
    tEnv.registerTableSink("MySink", sink)
    tEnv.insertInto("MySink", result)
    tEnv.execute("test")

    val expected = List("3018-06-10", "2018-06-03", "2018-06-01", "2018-06-02")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testPartialProjectCorrelate(): Unit = {
    val data = List(
      (1, 2, "3018-06-10|2018-06-03"),
      (1, 2, "2018-06-01"),
      (1, 2, "2018-06-02"))

    val t1 = env.fromCollection(data).toTable(tEnv, 'a, 'b, 'c)
    tEnv.registerTable("T1", t1)

    val sql = "SELECT a, v FROM T1, lateral table(STRING_SPLIT(c, '|')) as T(v)"

    val result = tEnv.sqlQuery(sql)
    val sink = TestSinkUtil.configureSink(result, new TestingAppendTableSink)
    tEnv.registerTableSink("MySink", sink)
    tEnv.insertInto("MySink", result)
    tEnv.execute("test")

    val expected = List("1,3018-06-10", "1,2018-06-03", "1,2018-06-01", "1,2018-06-02")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testProjectCorrelateInputWithEmptyOutput(): Unit = {
    val data = List(
      (1, 2, "a"),
      (1, 3, ""))

    val t1 = env.fromCollection(data).toTable(tEnv, 'a, 'b, 'c)
    tEnv.registerTable("T1", t1)

    val sql = "SELECT v FROM T1, lateral table(STRING_SPLIT(c, '|')) as T(v)"

    val result = tEnv.sqlQuery(sql)
    val sink = TestSinkUtil.configureSink(result, new TestingAppendTableSink)
    tEnv.registerTableSink("MySink", sink)
    tEnv.insertInto("MySink", result)
    tEnv.execute("test")

    val expected = List("a")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testLeftJoinProjectCorrelateInputWithEmptyOutput(): Unit = {
    val data = List(
      (1, 2, ""),
      (1, 3, ""))

    val t1 = env.fromCollection(data).toTable(tEnv, 'a, 'b, 'c)
    tEnv.registerTable("T1", t1)

    val sql = "SELECT v FROM T1 left join lateral table(STRING_SPLIT(c, '|')) as T(v) on true"

    val result = tEnv.sqlQuery(sql)
    val sink = TestSinkUtil.configureSink(result, new TestingAppendTableSink)
    tEnv.registerTableSink("MySink", sink)
    tEnv.insertInto("MySink", result)
    tEnv.execute("test")

    // output two null
    val expected = List("null", "null")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testPartialProjectWithEmptyOutput(): Unit = {
    val data = List(
      (1, 2, "a"),
      (1, 3, ""))

    val t1 = env.fromCollection(data).toTable(tEnv, 'a, 'b, 'c)
    tEnv.registerTable("T1", t1)

    val sql = "SELECT a, v FROM T1, lateral table(STRING_SPLIT(c, '|')) as T(v)"

    val result = tEnv.sqlQuery(sql)
    val sink = TestSinkUtil.configureSink(result, new TestingAppendTableSink)
    tEnv.registerTableSink("MySink", sink)
    tEnv.insertInto("MySink", result)
    tEnv.execute("test")

    val expected = List("1,a")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testLeftJoinPartialProjectWithEmptyOutput(): Unit = {
    val data = List(
      (1, 2, ""),
      (1, 3, ""))

    val t1 = env.fromCollection(data).toTable(tEnv, 'a, 'b, 'c)
    tEnv.registerTable("T1", t1)

    val sql = "SELECT b, v FROM T1 left join lateral table(STRING_SPLIT(c, '|')) as T(v) on true"

    val result = tEnv.sqlQuery(sql)
    val sink = TestSinkUtil.configureSink(result, new TestingAppendTableSink)
    tEnv.registerTableSink("MySink", sink)
    tEnv.insertInto("MySink", result)
    tEnv.execute("test")

    val expected = List("2,null", "3,null")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  // TODO support agg
//  @Test
//  def testCountStarOnCorrelate(): Unit = {
//    val data = List(
//      (1, 2, "3018-06-10|2018-06-03"),
//      (1, 2, "2018-06-01"),
//      (1, 2, "2018-06-02"))
//
//    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    val tEnv = TableEnvironment.getTableEnvironment(env)
//
//    val t1 = env.fromCollection(data).toTable(tEnv, 'a, 'b, 'c)
//    tEnv.registerTable("T1", t1)
//
//    val sql = "SELECT count(*) FROM T1, lateral table(STRING_SPLIT(c, '|')) as T(v)"
//
//    val sink = new TestingUpsertTableSink(Array(0))
//    tEnv.sqlQuery(sql).writeToSink(sink)
//    tEnv.execute()
//
//    val expected = List("1", "2", "3", "4")
//    assertEquals(expected.sorted, sink.getUpsertResults.sorted)
//  }
//
//  @Test
//  def testCountStarOnLeftCorrelate(): Unit = {
//    val data = List(
//      (1, 2, ""),
//      (1, 3, ""))
//
//    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    val tEnv = TableEnvironment.getTableEnvironment(env)
//
//    val t1 = env.fromCollection(data).toTable(tEnv, 'a, 'b, 'c)
//    tEnv.registerTable("T1", t1)
//
//    val sql =
//      "SELECT count(*) FROM T1 left join lateral table(STRING_SPLIT(c, '|')) as T(v) on " +
//      "true"
//
//    val sink = new TestingUpsertTableSink(Array(0))
//    tEnv.sqlQuery(sql).writeToSink(sink)
//    tEnv.execute()
//
//    val expected = List("1", "2")
//    assertEquals(expected.sorted, sink.getUpsertResults.sorted)
//  }
}
