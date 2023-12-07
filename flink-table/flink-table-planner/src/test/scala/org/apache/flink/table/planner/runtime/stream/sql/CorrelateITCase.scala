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
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.api.internal.TableEnvironmentInternal
import org.apache.flink.table.planner.runtime.utils.{StreamingTestBase, TestingAppendSink, TestingAppendTableSink, TestSinkUtil}
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedScalarFunctions.UdfWithOpen
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedTableFunctions.{NonDeterministicTableFunc, StringSplit}
import org.apache.flink.table.planner.utils.{RF, TableFunc7}
import org.apache.flink.types.Row

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.{BeforeEach, Test}

import java.lang.{Boolean => JBoolean}

import scala.collection.mutable

class CorrelateITCase extends StreamingTestBase {

  @BeforeEach
  override def before(): Unit = {
    super.before()
    tEnv.createTemporarySystemFunction("STRING_SPLIT", new StringSplit())
  }

  @Test
  // Fix IndexOutOfBoundsException when UDTF is used on the
  // same name field of different tables
  def testUdtfForSameFieldofDifferentSource(): Unit = {
    val data = List((1, 2, "abc-bcd"), (1, 2, "hhh"), (1, 2, "xxx"))

    val data2 = List((1, "abc-bcd"), (1, "hhh"), (1, "xxx"))

    val t1 = env.fromCollection(data).toTable(tEnv, 'a, 'b, 'c)
    tEnv.createTemporaryView("T1", t1)

    val t2 = env.fromCollection(data2).toTable(tEnv, 'a, 'c)
    tEnv.createTemporaryView("T2", t2)

    val query1 = "SELECT a, v FROM T1, lateral table(STRING_SPLIT(c, '-')) as T(v)"
    tEnv.createTemporaryView("TMP1", tEnv.sqlQuery(query1))

    val query2 = "SELECT a, v FROM T2, lateral table(STRING_SPLIT(c, '-')) as T(v)"
    tEnv.createTemporaryView("TMP2", tEnv.sqlQuery(query2))

    val sql =
      """
        |SELECT * FROM TMP1
        |UNION ALL
        |SELECT * FROM TMP2
      """.stripMargin

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toDataStream.addSink(sink)
    env.execute()

    val expected = List("1,abc", "1,abc", "1,bcd", "1,bcd", "1,hhh", "1,hhh", "1,xxx", "1,xxx")
    assertThat(sink.getAppendResults.sorted).isEqualTo(expected.sorted)
  }

  @Test
  def testConstantTableFunc(): Unit = {
    tEnv.createTemporarySystemFunction("str_split", new StringSplit())
    val query = "SELECT * FROM LATERAL TABLE(str_split()) as T0(d)"
    val sink = new TestingAppendSink
    tEnv.sqlQuery(query).toDataStream.addSink(sink)
    env.execute()

    val expected = List("a", "b", "c")
    assertThat(sink.getAppendResults.sorted).isEqualTo(expected.sorted)
  }

  @Test
  def testConstantTableFunc2(): Unit = {
    tEnv.createTemporarySystemFunction("str_split", new StringSplit())
    val query = "SELECT * FROM LATERAL TABLE(str_split('Jack,John', ',')) as T0(d)"
    val sink = new TestingAppendSink
    tEnv.sqlQuery(query).toDataStream.addSink(sink)
    env.execute()

    val expected = List("Jack", "John")
    assertThat(sink.getAppendResults.sorted).isEqualTo(expected.sorted)
  }

  @Test
  def testConstantTableFunc3(): Unit = {
    val data = List((1, 2, "abc-bcd"), (1, 2, "hhh"), (1, 2, "xxx"))

    val t1 = env.fromCollection(data).toTable(tEnv, 'a, 'b, 'c)
    tEnv.createTemporaryView("T1", t1)

    tEnv.createTemporarySystemFunction("str_split", new StringSplit())
    val query = "SELECT * FROM T1, LATERAL TABLE(str_split('Jack,John', ',')) as T0(d)"
    val sink = new TestingAppendSink
    tEnv.sqlQuery(query).toDataStream.addSink(sink)
    env.execute()

    val expected = List(
      "1,2,abc-bcd,Jack",
      "1,2,abc-bcd,John",
      "1,2,hhh,Jack",
      "1,2,hhh,John",
      "1,2,xxx,Jack",
      "1,2,xxx,John")
    assertThat(sink.getAppendResults.sorted).isEqualTo(expected.sorted)
  }

  @Test
  def testConstantNonDeterministicTableFunc(): Unit = {
    tEnv.createTemporarySystemFunction("str_split", new NonDeterministicTableFunc())
    val query = "SELECT * FROM LATERAL TABLE(str_split('Jack#John')) as T0(d)"
    val sink = new TestingAppendSink
    tEnv.sqlQuery(query).toDataStream.addSink(sink)
    env.execute()

    val res = sink.getAppendResults
    assertThat(res.size).isOne
    assertThat(res(0)).isIn("Jack", "John")
  }

  @Test
  def testConstantNonDeterministicTableFunc2(): Unit = {
    val data = List((1, 2, "abc-bcd"), (1, 2, "hhh"), (1, 2, "xxx"))

    val t1 = env.fromCollection(data).toTable(tEnv, 'a, 'b, 'c)
    tEnv.createTemporaryView("T1", t1)

    tEnv.createTemporarySystemFunction("str_split", new NonDeterministicTableFunc())
    val query = "SELECT * FROM T1, LATERAL TABLE(str_split('Jack#John')) as T0(d)"
    val sink = new TestingAppendSink
    tEnv.sqlQuery(query).toDataStream.addSink(sink)
    env.execute()

    val res = sink.getAppendResults
    assertThat(res.size).isEqualTo(3)
  }

  @Test
  def testUdfIsOpenedAfterUdtf(): Unit = {
    val data = List((1, 2, "abc-bcd"), (1, 2, "hhh"), (1, 2, "xxx"))

    val t1 = env.fromCollection(data).toTable(tEnv, 'a, 'b, 'c)
    tEnv.createTemporaryView("T1", t1)

    // UdfWithOpen checks open method is opened, and add a '$' prefix to the given string
    tEnv.createTemporarySystemFunction("func", new UdfWithOpen)

    val query1 =
      """
        |SELECT a, v
        |FROM T1, lateral table(STRING_SPLIT(c, '-')) as T(v)
        |WHERE func(v) LIKE '$%'
      """.stripMargin

    val sink = new TestingAppendSink
    tEnv.sqlQuery(query1).toDataStream.addSink(sink)
    env.execute()

    val expected = List("1,abc", "1,bcd", "1,hhh", "1,xxx")
    assertThat(sink.getAppendResults.sorted).isEqualTo(expected.sorted)
  }

  @Test
  def testMultipleCorrelate(): Unit = {
    val data = new mutable.MutableList[(String, String, String)]
    data.+=(("1", "1,L", "A,B"))
    data.+=(("2", "2,L", "B,C"))

    val t = env.fromCollection(data).toTable(tEnv, 'a, 'b, 'c)
    tEnv.createTemporaryView("T1", t)
    tEnv.createTemporarySystemFunction("str_split", new StringSplit())
    val sink1 = new TestingAppendSink
    val sink2 = new TestingAppendSink

    // correlate 1
    val t1 = tEnv.sqlQuery(s"""
                              |SELECT a, b, s
                              |FROM T1, LATERAL TABLE(str_split(c, ',')) as T2(s)
       """.stripMargin)
    t1.toDataStream.addSink(sink1)

    // correlate 2
    val t2 = tEnv.sqlQuery(s"""
                              |SELECT a, c, s
                              |FROM T1, LATERAL TABLE(str_split(b, ',')) as T3(s)
      """.stripMargin)
    t2.toDataStream.addSink(sink2)

    env.execute()

    val expected =
      List("1,1,L,A", "1,1,L,B", "1,A,B,1", "1,A,B,L", "2,2,L,B", "2,2,L,C", "2,B,C,2", "2,B,C,L")
    assertThat((sink1.getAppendResults ++ sink2.getAppendResults).sorted).isEqualTo(expected.sorted)
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

    tEnv.createTemporaryView("MyTable", in)
    tEnv.createTemporarySystemFunction("rfFunc", new RF)
    tEnv.createTemporarySystemFunction("tfFunc", new TableFunc7)
    tEnv
      .sqlQuery("SELECT rfFunc(a) as d, e FROM MyTable, LATERAL TABLE(tfFunc(rfFunc(a))) as T(e)")
      .toDataStream
      .addSink(sink)

    env.execute()

    assertThat(sink.getAppendResults.sorted).isEqualTo(List())
  }

  @Test
  def testReUsePerRecord(): Unit = {
    val data = List((1, 2, "3018-06-10|2018-06-03"), (1, 2, "2018-06-01"), (1, 2, "2018-06-02"))

    val t1 = env.fromCollection(data).toTable(tEnv, 'a, 'b, 'c)
    tEnv.createTemporaryView("T1", t1)

    val query1 = "SELECT a, v FROM T1, lateral table(STRING_SPLIT(c, '|')) as T(v)"
    tEnv.createTemporaryView("TMP1", tEnv.sqlQuery(query1))

    val sql =
      """
        |SELECT * FROM TMP1
        |where TIMESTAMPADD(day, 3, cast(v as date)) > DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd')
      """.stripMargin

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toDataStream.addSink(sink)
    env.execute()

    val expected = List("1,3018-06-10")
    assertThat(sink.getAppendResults.sorted).isEqualTo(expected.sorted)
  }

  @Test
  def testLeftJoinWithEmptyOutput(): Unit = {
    val data = List((1, 2, ""), (1, 3, ""))

    val t1 = env.fromCollection(data).toTable(tEnv, 'a, 'b, 'c)
    tEnv.createTemporaryView("T1", t1)

    val sql = "SELECT * FROM T1 left join lateral table(STRING_SPLIT(c, '|')) as T(v) on true"

    val result = tEnv.sqlQuery(sql)
    val sink = TestSinkUtil.configureSink(result, new TestingAppendTableSink)
    tEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal("MySink", sink)
    result.executeInsert("MySink").await()

    val expected = List("1,2,,null", "1,3,,null")
    assertThat(sink.getAppendResults.sorted).isEqualTo(expected.sorted)
  }

  @Test
  def testProjectCorrelateInput(): Unit = {
    val data = List((1, 2, "3018-06-10|2018-06-03"), (1, 2, "2018-06-01"), (1, 2, "2018-06-02"))

    val t1 = env.fromCollection(data).toTable(tEnv, 'a, 'b, 'c)
    tEnv.createTemporaryView("T1", t1)

    val sql = "SELECT v FROM T1, lateral table(STRING_SPLIT(c, '|')) as T(v)"

    val result = tEnv.sqlQuery(sql)
    val sink = TestSinkUtil.configureSink(result, new TestingAppendTableSink)
    tEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal("MySink", sink)
    result.executeInsert("MySink").await()

    val expected = List("3018-06-10", "2018-06-03", "2018-06-01", "2018-06-02")
    assertThat(sink.getAppendResults.sorted).isEqualTo(expected.sorted)
  }

  @Test
  def testPartialProjectCorrelate(): Unit = {
    val data = List((1, 2, "3018-06-10|2018-06-03"), (1, 2, "2018-06-01"), (1, 2, "2018-06-02"))

    val t1 = env.fromCollection(data).toTable(tEnv, 'a, 'b, 'c)
    tEnv.createTemporaryView("T1", t1)

    val sql = "SELECT a, v FROM T1, lateral table(STRING_SPLIT(c, '|')) as T(v)"

    val result = tEnv.sqlQuery(sql)
    val sink = TestSinkUtil.configureSink(result, new TestingAppendTableSink)
    tEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal("MySink", sink)
    result.executeInsert("MySink").await()

    val expected = List("1,3018-06-10", "1,2018-06-03", "1,2018-06-01", "1,2018-06-02")
    assertThat(sink.getAppendResults.sorted).isEqualTo(expected.sorted)
  }

  @Test
  def testProjectCorrelateInputWithEmptyOutput(): Unit = {
    val data = List((1, 2, "a"), (1, 3, ""))

    val t1 = env.fromCollection(data).toTable(tEnv, 'a, 'b, 'c)
    tEnv.createTemporaryView("T1", t1)

    val sql = "SELECT v FROM T1, lateral table(STRING_SPLIT(c, '|')) as T(v)"

    val result = tEnv.sqlQuery(sql)
    val sink = TestSinkUtil.configureSink(result, new TestingAppendTableSink)
    tEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal("MySink", sink)
    result.executeInsert("MySink").await()

    val expected = List("a")
    assertThat(sink.getAppendResults.sorted).isEqualTo(expected.sorted)
  }

  @Test
  def testLeftJoinProjectCorrelateInputWithEmptyOutput(): Unit = {
    val data = List((1, 2, ""), (1, 3, ""))

    val t1 = env.fromCollection(data).toTable(tEnv, 'a, 'b, 'c)
    tEnv.createTemporaryView("T1", t1)

    val sql = "SELECT v FROM T1 left join lateral table(STRING_SPLIT(c, '|')) as T(v) on true"

    val result = tEnv.sqlQuery(sql)
    val sink = TestSinkUtil.configureSink(result, new TestingAppendTableSink)
    tEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal("MySink", sink)
    result.executeInsert("MySink").await()

    // output two null
    val expected = List("null", "null")
    assertThat(sink.getAppendResults.sorted).isEqualTo(expected.sorted)
  }

  @Test
  def testPartialProjectWithEmptyOutput(): Unit = {
    val data = List((1, 2, "a"), (1, 3, ""))

    val t1 = env.fromCollection(data).toTable(tEnv, 'a, 'b, 'c)
    tEnv.createTemporaryView("T1", t1)

    val sql = "SELECT a, v FROM T1, lateral table(STRING_SPLIT(c, '|')) as T(v)"

    val result = tEnv.sqlQuery(sql)
    val sink = TestSinkUtil.configureSink(result, new TestingAppendTableSink)
    tEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal("MySink", sink)
    result.executeInsert("MySink").await()

    val expected = List("1,a")
    assertThat(sink.getAppendResults.sorted).isEqualTo(expected.sorted)
  }

  @Test
  def testLeftJoinPartialProjectWithEmptyOutput(): Unit = {
    val data = List((1, 2, ""), (1, 3, ""))

    val t1 = env.fromCollection(data).toTable(tEnv, 'a, 'b, 'c)
    tEnv.createTemporaryView("T1", t1)

    val sql = "SELECT b, v FROM T1 left join lateral table(STRING_SPLIT(c, '|')) as T(v) on true"

    val result = tEnv.sqlQuery(sql)
    val sink = TestSinkUtil.configureSink(result, new TestingAppendTableSink)
    tEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal("MySink", sink)
    result.executeInsert("MySink").await()

    val expected = List("2,null", "3,null")
    assertThat(sink.getAppendResults.sorted).isEqualTo(expected.sorted)
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
//    tEnv.createTemporaryView("T1", t1)
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
//    tEnv.createTemporaryView("T1", t1)
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
