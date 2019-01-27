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

import java.lang.{Integer => JInt}

import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.table.api.functions.ScalarFunction
import org.apache.flink.table.api.scala._
import org.apache.flink.table.runtime.utils.TemporalTableUtils._
import org.apache.flink.table.runtime.utils.{StreamingTestBase, TestingAppendSink, TestingRetractSink}
import org.apache.flink.types.Row
import org.junit.Assert.assertEquals
import org.junit.Test

class TemporalTableJoinITCase extends StreamingTestBase {

  val data = List(
    (1, 12, "Julian"),
    (2, 15, "Hello"),
    (3, 15, "Fabian"),
    (8, 11, "Hello world"),
    (9, 12, "Hello world!"))

  val dataWithNull = List(
    Row.of(null, new JInt(15), "Hello"),
    Row.of(new JInt(3), new JInt(15), "Fabian"),
    Row.of(null, new JInt(11), "Hello world"),
    Row.of(new JInt(9), new JInt(12), "Hello world!"))

  @Test
  def testJoinTemporalTable(): Unit = {
    val stream: DataStream[(Int, Int, String)] = env.fromCollection(data)
    val streamTable = stream.toTable(tEnv, 'id, 'len, 'content, 'proc.proctime)
    tEnv.registerTable("T", streamTable)

    val temporalTable = new TestingTemporalTableSource()
    tEnv.registerTableSource("csvTemporal", temporalTable)

    val sql = "SELECT T.id, T.len, T.content, D.name FROM T JOIN csvTemporal " +
      "for system_time as of PROCTIME() AS D ON T.id = D.id"

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq(
      "1,12,Julian,Julian",
      "2,15,Hello,Jark",
      "3,15,Fabian,Fabian")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
    assertEquals(0, temporalTable.getFetcherResourceCount)
  }

  @Test
  def testJoinTemporalTableOnNullableKey(): Unit = {

    implicit val tpe: TypeInformation[Row] = new RowTypeInfo(
      BasicTypeInfo.INT_TYPE_INFO,
      BasicTypeInfo.INT_TYPE_INFO,
      BasicTypeInfo.STRING_TYPE_INFO)
    val stream = env.fromCollection(dataWithNull)

    val streamTable = stream.toTable(tEnv, 'id, 'len, 'content, 'proc.proctime)
    tEnv.registerTable("T", streamTable)

    val temporalTable = new TestingTemporalTableSource
    tEnv.registerTableSource("csvTemporal", temporalTable)

    val sql = "SELECT T.id, T.len, D.name FROM T JOIN csvTemporal " +
      "for system_time as of PROCTIME() AS D ON T.id = D.id"

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq("3,15,Fabian")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
    assertEquals(0, temporalTable.getFetcherResourceCount)
  }

  @Test
  def testJoinTemporalTableWithPushDown(): Unit = {
    val stream: DataStream[(Int, Int, String)] = env.fromCollection(data)
    val streamTable = stream.toTable(tEnv, 'id, 'len, 'content, 'proc.proctime)
    tEnv.registerTable("T", streamTable)

    val temporalTable = new TestingTemporalTableSource
    tEnv.registerTableSource("csvTemporal", temporalTable)

    val sql = "SELECT T.id, T.len, T.content, D.name FROM T JOIN csvTemporal " +
      "for system_time as of PROCTIME() AS D ON T.id = D.id AND D.age > 20"

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq(
      "2,15,Hello,Jark",
      "3,15,Fabian,Fabian")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
    assertEquals(0, temporalTable.getFetcherResourceCount)
  }

  @Test
  def testJoinTemporalTableWithNonEqualFilter(): Unit = {
    val stream: DataStream[(Int, Int, String)] = env.fromCollection(data)
    val streamTable = stream.toTable(tEnv, 'id, 'len, 'content, 'proc.proctime)
    tEnv.registerTable("T", streamTable)

    val temporalTable = new TestingTemporalTableSource
    tEnv.registerTableSource("csvTemporal", temporalTable)

    val sql = "SELECT T.id, T.len, T.content, D.name, D.age FROM T JOIN csvTemporal " +
      "for system_time as of PROCTIME() AS D ON T.id = D.id WHERE T.len <= D.age"

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq(
      "2,15,Hello,Jark,22",
      "3,15,Fabian,Fabian,33")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
    assertEquals(0, temporalTable.getFetcherResourceCount)
  }

  @Test
  def testJoinTemporalTableOnMultiFields(): Unit = {
    val stream: DataStream[(Int, Int, String)] = env.fromCollection(data)
    val streamTable = stream.toTable(tEnv, 'id, 'len, 'content, 'proc.proctime)
    tEnv.registerTable("T", streamTable)

    val temporalTable = new TestingTemporalTableSource
    tEnv.registerTableSource("csvTemporal", temporalTable)

    val sql = "SELECT T.id, T.len, D.name FROM T JOIN csvTemporal " +
      "for system_time as of PROCTIME() AS D ON T.id = D.id AND T.content = D.name"

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq(
      "1,12,Julian",
      "3,15,Fabian")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
    assertEquals(0, temporalTable.getFetcherResourceCount)
  }

  @Test
  def testJoinTemporalTableOnMultiKeyFields(): Unit = {
    val stream: DataStream[(Int, Int, String)] = env.fromCollection(data)
    val streamTable = stream.toTable(tEnv, 'id, 'len, 'content, 'proc.proctime)
    tEnv.registerTable("T", streamTable)

    val temporalTable = new TestingTemporalTableSourceWithDoubleKey
    tEnv.registerTableSource("csvTemporal", temporalTable)

    val sql = "SELECT T.id, T.len, D.name FROM T JOIN csvTemporal " +
      "for system_time as of PROCTIME() AS D ON T.content = D.name AND T.id = D.id"

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq(
      "1,12,Julian",
      "3,15,Fabian")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
    assertEquals(0, temporalTable.getFetcherResourceCount)
  }

  @Test
  def testJoinTemporalTableOnMultiKeyFields2(): Unit = {
    val stream: DataStream[(Int, Int, String)] = env.fromCollection(data)
    val streamTable = stream.toTable(tEnv, 'id, 'len, 'content, 'proc.proctime)
    tEnv.registerTable("T", streamTable)

    // pk is (id: Int, name: String)
    val temporalTable = new TestingTemporalTableSourceWithDoubleKey
    tEnv.registerTableSource("csvTemporal", temporalTable)

    // test left table's join key define order diffs from right's
    val sql = "SELECT t1.id, t1.len, D.name FROM (select content, id, len FROM T) t1 " +
      "JOIN csvTemporal for system_time as of PROCTIME() AS D " +
      "ON t1.content = D.name AND t1.id = D.id"

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq(
      "1,12,Julian",
      "3,15,Fabian")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
    assertEquals(0, temporalTable.getFetcherResourceCount)
  }

  @Test
  def testJoinTemporalTableOnMultiKeyFieldsWithConstantKey(): Unit = {
    val stream: DataStream[(Int, Int, String)] = env.fromCollection(data)
    val streamTable = stream.toTable(tEnv, 'id, 'len, 'content, 'proc.proctime)
    tEnv.registerTable("T", streamTable)

    val temporalTable = new TestingTemporalTableSourceWithDoubleKey
    tEnv.registerTableSource("csvTemporal", temporalTable)

    val sql = "SELECT T.id, T.len, D.name FROM T JOIN csvTemporal " +
      "for system_time as of PROCTIME() AS D ON T.content = D.name AND 3 = D.id"

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq("3,15,Fabian")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
    assertEquals(0, temporalTable.getFetcherResourceCount)
  }

  @Test
  def testJoinTemporalTableOnMultiKeyFieldsWithStringConstantKey(): Unit = {
    val stream: DataStream[(Int, Int, String)] = env.fromCollection(data)
    val streamTable = stream.toTable(tEnv, 'id, 'len, 'content, 'proc.proctime)
    tEnv.registerTable("T", streamTable)

    val temporalTable = new TestingTemporalTableSourceWithDoubleKey
    tEnv.registerTableSource("csvTemporal", temporalTable)

    val sql = "SELECT T.id, T.len, D.name FROM T JOIN csvTemporal " +
      "for system_time as of PROCTIME() AS D ON D.name = 'Fabian' AND T.id = D.id"

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq("3,15,Fabian")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
    assertEquals(0, temporalTable.getFetcherResourceCount)
  }

  @Test
  def testJoinTemporalTableOnMultiConstantKey(): Unit = {
    val stream: DataStream[(Int, Int, String)] = env.fromCollection(data)
    val streamTable = stream.toTable(tEnv, 'id, 'len, 'content, 'proc.proctime)
    tEnv.registerTable("T", streamTable)

    val temporalTable = new TestingTemporalTableSourceWithDoubleKey
    tEnv.registerTableSource("csvTemporal", temporalTable)

    val sql = "SELECT T.id, T.len, D.name FROM T JOIN csvTemporal " +
      "for system_time as of PROCTIME() AS D ON D.name = 'Fabian' AND 3 = D.id"

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq(
      "1,12,Fabian",
      "2,15,Fabian",
      "3,15,Fabian",
      "8,11,Fabian",
      "9,12,Fabian"
    )
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
    assertEquals(0, temporalTable.getFetcherResourceCount)
  }

  @Test
  def testLeftJoinTemporalTable(): Unit = {
    val stream: DataStream[(Int, Int, String)] = env.fromCollection(data)
    val streamTable = stream.toTable(tEnv, 'id, 'len, 'content, 'proc.proctime)
    tEnv.registerTable("T", streamTable)

    val temporalTable = new TestingTemporalTableSource
    tEnv.registerTableSource("csvTemporal", temporalTable)

    val sql = "SELECT T.id, T.len, D.name, D.age FROM T LEFT JOIN csvTemporal " +
      "for system_time as of PROCTIME() AS D ON T.id = D.id"

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq(
      "1,12,Julian,11",
      "2,15,Jark,22",
      "3,15,Fabian,33",
      "8,11,null,null",
      "9,12,null,null")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
    assertEquals(0, temporalTable.getFetcherResourceCount)
  }

  @Test
  def testLeftJoinTemporalTableOnNullableKey(): Unit = {

    implicit val tpe: TypeInformation[Row] = new RowTypeInfo(
      BasicTypeInfo.INT_TYPE_INFO,
      BasicTypeInfo.INT_TYPE_INFO,
      BasicTypeInfo.STRING_TYPE_INFO)
    val stream = env.fromCollection(dataWithNull)
    val streamTable = stream.toTable(tEnv, 'id, 'len, 'content, 'proc.proctime)
    tEnv.registerTable("T", streamTable)

    val temporalTable = new TestingTemporalTableSource
    tEnv.registerTableSource("csvTemporal", temporalTable)

    val sql = "SELECT T.id, T.len, D.name FROM T LEFT OUTER JOIN csvTemporal " +
      "for system_time as of PROCTIME() AS D ON T.id = D.id"

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq(
      "null,15,null",
      "3,15,Fabian",
      "null,11,null",
      "9,12,null")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
    assertEquals(0, temporalTable.getFetcherResourceCount)
  }
  @Test
  def testLeftJoinTemporalTableOnMultKeyFields(): Unit = {
    val stream: DataStream[(Int, Int, String)] = env.fromCollection(data)
    val streamTable = stream.toTable(tEnv, 'id, 'len, 'content, 'proc.proctime)
    tEnv.registerTable("T", streamTable)

    val temporalTable = new TestingTemporalTableSource
    tEnv.registerTableSource("csvTemporal", temporalTable)

    val sql = "SELECT T.id, T.len, D.name, D.age FROM T LEFT JOIN csvTemporal " +
      "for system_time as of PROCTIME() AS D ON T.id = D.id and T.content = D.name"

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq(
      "1,12,Julian,11",
      "2,15,null,null",
      "3,15,Fabian,33",
      "8,11,null,null",
      "9,12,null,null")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
    assertEquals(0, temporalTable.getFetcherResourceCount)
  }


  @Test
  def testCollectOnNestedJoinTemporalTable(): Unit = {
    val data = List(
      (1, 1, (12, "45.6")),
      (2, 2, (12, "45.612")),
      (3, 2, (13, "41.6")),
      (4, 3, (14, "45.2136")),
      (5, 3, (18, "42.6"))
    )
    tEnv.registerTable("src", env.fromCollection(data).toTable(tEnv).as('a, 'b, 'c))

    // register temporalTable table
    val temporalTable = new TestingTemporalTableSource
    tEnv.registerTableSource("temporalTable", temporalTable)

    val sql = "SELECT a, b, COLLECT(c) as `set` FROM src GROUP BY a, b"
    val view1 = tEnv.sqlQuery(sql)
    tEnv.registerTable("v1", view1)

    val toCompositeObj = ToCompositeObj
    tEnv.registerFunction("toCompObj", toCompositeObj)

    val sql1 =
      s"""
         |SELECT
         |  a, b, COLLECT(toCompObj(t.b, D.name, D.age, t.point)) as info
         |from (
         | select
         |  a, b, V.sid, V.point
         | from
         |  v1, unnest(v1.`set`) as V(sid, point)
         |) t
         |JOIN temporalTable FOR SYSTEM_TIME AS OF PROCTIME() AS D
         |ON t.b = D.id
         |group by t.a, t.b
       """.stripMargin

    val sink = new TestingRetractSink
    tEnv.sqlQuery(sql1).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = List(
      "1,1,{CompositeObj(1,Julian,11,45.6)=1}",
      "2,2,{CompositeObj(2,Jark,22,45.612)=1}",
      "3,2,{CompositeObj(2,Jark,22,41.6)=1}",
      "4,3,{CompositeObj(3,Fabian,33,45.2136)=1}",
      "5,3,{CompositeObj(3,Fabian,33,42.6)=1}")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

}

// ------------------------------- Utils ----------------------------------------------


object TestWrapperUdf extends ScalarFunction {
  def eval(id: Int): Int = {
    id
  }

  def eval(id: String): String = {
    id
  }
}

object TestMod extends ScalarFunction {
  def eval(src: Int, mod: Int): Int = {
    src % mod
  }
}

object TestExceptionThrown extends ScalarFunction {
  def eval(src: String): Int = {
    throw new NumberFormatException("Cannot parse this input.")
  }
}

object ToCompositeObj extends ScalarFunction {
  def eval(id: Int, name: String, age: Int): CompositeObj = {
    CompositeObj(id, name, age, "0.0")
  }

  def eval(id: Int, name: String, age: Int, point: String): CompositeObj = {
    CompositeObj(id, name, age, point)
  }
}

case class CompositeObj(id: Int, name: String, age: Int, point: String)
