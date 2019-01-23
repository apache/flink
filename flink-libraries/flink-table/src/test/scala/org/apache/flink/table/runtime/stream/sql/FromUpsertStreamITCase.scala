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

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{TableEnvironment, Types}
import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.runtime.utils.{StreamITCase, StreamTestData, StreamingWithStateTestBase}
import org.apache.flink.types.Row
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.table.runtime.stream.sql.FromUpsertStreamITCase.{TestPojo, TimestampWithEqualWatermark}
import org.apache.flink.table.runtime.stream.table.{RowCollector, TestUpsertSink}
import org.junit.Assert._
import org.junit._

class FromUpsertStreamITCase extends StreamingWithStateTestBase {

  /** test upsert stream registered table **/
  @Test
  def testRegisterUpsertStream(): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    env.setStateBackend(getStateBackend)
    // set parallelism to 1 to ensure data input order, since it is processing time
    env.setParallelism(1)
    StreamITCase.clear

    val ds = StreamTestData.get3TupleUpsertStream(env)
    val t = tEnv.fromUpsertStream(ds, 'a, 'b.key, 'c)
    tEnv.registerTable("MyTableRow", t)
    val sqlQuery = "SELECT a, b, c FROM MyTableRow"
    val result = tEnv.sqlQuery(sqlQuery).toRetractStream[Row]
    result.addSink(new StreamITCase.RetractingSink)
    env.execute()

    val expected = List("6,3,Luke Skywalker", "15,5,Comment#9", "21,6,Comment#15")
    assertEquals(expected.sorted, StreamITCase.retractedResults.sorted)
  }

  @Test
  def testUpsertSinkFromUpsertSource(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    env.setStateBackend(getStateBackend)
    // set parallelism to 1 to ensure data input order, since it is processing time
    env.setParallelism(1)
    StreamITCase.clear

    tEnv.registerTableSink(
      "upsertSink",
      new TestUpsertSink(Array("b"), false).configure(
        Array[String]("a", "b", "c"),
        Array[TypeInformation[_]](Types.INT, Types.LONG, Types.STRING)))

    val ds = StreamTestData.get3TupleUpsertStream(env)
    val t = ds.toTableFromUpsertStream(tEnv, 'a, 'b.key, 'c)
    tEnv.registerTable("MyTableRow", t)
    val sqlQuery = "SELECT a, b, c FROM MyTableRow"
    tEnv.sqlQuery(sqlQuery).insertInto("upsertSink")
    env.execute()

    val results = RowCollector.getAndClearValues
    val upserted = RowCollector.upsertResults(results, Array(1))
    val expected = List("6,3,Luke Skywalker", "15,5,Comment#9", "21,6,Comment#15")
    assertEquals(expected.sorted, upserted.sorted)
  }

  /** test upsert stream registered single row table **/
  @Test
  def testRegisterSingleRowTableFromUpsertStream(): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    env.setStateBackend(getStateBackend)
    env.setParallelism(1)
    StreamITCase.clear

    val sqlQuery = "SELECT a, b, c FROM MyTableRow WHERE c < 3"

    val data = List(
      (true, ("Hello", "Worlds", 1)),
      (true, ("Hello", "Hiden", 5)),
      (true, ("Hello again", "Worlds", 2)))

    val ds = env.fromCollection(data)
      .assignTimestampsAndWatermarks(new TimestampWithEqualWatermark())

    val t = tEnv.fromUpsertStream(ds, 'a, 'b, 'c, 'proctime.proctime)
    tEnv.registerTable("MyTableRow", t)

    val result = tEnv.sqlQuery(sqlQuery).toRetractStream[Row]
    result.addSink(new StreamITCase.RetractingSink)
    env.execute()

    val expected = List("Hello again,Worlds,2")
    assertEquals(expected.sorted, StreamITCase.retractedResults.sorted)
  }

  /** test upsert stream registered table without schema **/
  @Test
  def testRegisterUpsertStreamWithoutSchema(): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    env.setStateBackend(getStateBackend)
    env.setParallelism(1)
    StreamITCase.clear

    val sqlQuery = "SELECT _1, _2, _3 FROM MyTableRow WHERE _3 < 3"

    val data = List(
      (true, ("Hello", "Worlds", 1)),
      (true, ("Hello", "Hiden", 5)),
      (true, ("Hello again", "Worlds", 2)))

    val ds = env.fromCollection(data)
    val t = tEnv.fromUpsertStream(ds)
    tEnv.registerTable("MyTableRow", t)

    val result = tEnv.sqlQuery(sqlQuery).toRetractStream[Row]
    result.addSink(new StreamITCase.RetractingSink)
    env.execute()

    val expected = List("Hello again,Worlds,2")
    assertEquals(expected.sorted, StreamITCase.retractedResults.sorted)
  }

  @Test
  def testCalcTransposeUpsertToRetract(): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    env.setStateBackend(getStateBackend)
    env.setParallelism(1)
    StreamITCase.clear

    val ds = StreamTestData.get3TupleUpsertStream(env)
    val t = tEnv.fromUpsertStream(ds, 'a, 'b.key, 'c)
    tEnv.registerTable("MyTableRow", t)
    val sqlQuery = "SELECT a, count(b) FROM (SELECT a, b, c FROM MyTableRow) GROUP BY a"
    val result = tEnv.sqlQuery(sqlQuery).toRetractStream[Row]
    result.addSink(new StreamITCase.RetractingSink)
    env.execute()

    val expected = List("6,1", "15,1", "21,1")
    assertEquals(expected.sorted, StreamITCase.retractedResults.sorted)
  }

  @Test
  def testPojoAndUpsertSink(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    env.setStateBackend(getStateBackend)
    env.setParallelism(1)

    val p1 = new TestPojo
    p1.a = 12
    p1.b = 42L
    p1.c = "Test 1."

    val p2 = new TestPojo
    p2.a = 12
    p2.b = 43L
    p2.c = "Test 2."

    val p3 = new TestPojo
    p3.a = 13
    p3.b = 44L
    p3.c = "Test 3."

    val data = List(
      (true, p1),
      (true, p2),
      (true, p3)
    )

    val ds = env.fromCollection(data)
    // use aliases, swap all attributes, and skip b2
    val t = tEnv.fromUpsertStream(ds, 'b, 'c as 'c, 'a.key as 'a)
    tEnv.registerTable("MyTableRow", t)

    tEnv.registerTableSink(
      "upsertSink",
      new TestUpsertSink(Array("a"), false).configure(
        Array[String]("a", "b", "c"),
        Array[TypeInformation[_]](Types.INT, Types.LONG, Types.STRING)))

    val sqlQuery = "SELECT a, b, c FROM MyTableRow"
    tEnv.sqlQuery(sqlQuery).insertInto("upsertSink")
    env.execute()

    val results = RowCollector.getAndClearValues
    val upserted = RowCollector.upsertResults(results, Array(0))
    val expected = List("12,43,Test 2.", "13,44,Test 3.")
    assertEquals(expected.sorted, upserted.sorted)
  }
}

object FromUpsertStreamITCase {

  class TimestampWithEqualWatermark
    extends AssignerWithPunctuatedWatermarks[(Boolean, (String, String, Int))] {

    override def checkAndGetNextWatermark(
        lastElement: (Boolean, (String, String, Int)),
        extractedTimestamp: Long)
    : Watermark = {
      new Watermark(extractedTimestamp)
    }

    override def extractTimestamp(
        element: (Boolean, (String, String, Int)),
        previousElementTimestamp: Long): Long = {
      element._2._3
    }
  }

  class TestPojo() {
    var a: Int = _
    var b: Long = _
    var b2: String = "skip me"
    var c: String = _
  }
}
