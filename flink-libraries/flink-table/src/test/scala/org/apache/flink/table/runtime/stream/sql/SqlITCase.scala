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

import java.util

import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{TableEnvironment, Types}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.runtime.utils.TimeTestUtil.EventTimeSourceFunction
import org.apache.flink.table.runtime.utils.{StreamITCase, StreamTestData, StreamingWithStateTestBase}
import org.apache.flink.types.Row
import org.apache.flink.table.utils.MemoryTableSinkUtil

import scala.collection.JavaConverters._
import org.junit.Assert._
import org.junit._

class SqlITCase extends StreamingWithStateTestBase {

   /** test row stream registered table **/
  @Test
  def testRowRegister(): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.clear

    val sqlQuery = "SELECT * FROM MyTableRow WHERE c < 3"

    val data = List(
      Row.of("Hello", "Worlds", Int.box(1)),
      Row.of("Hello", "Hiden", Int.box(5)),
      Row.of("Hello again", "Worlds", Int.box(2)))
        
    implicit val tpe: TypeInformation[Row] = new RowTypeInfo(
      BasicTypeInfo.STRING_TYPE_INFO,
      BasicTypeInfo.STRING_TYPE_INFO,
      BasicTypeInfo.INT_TYPE_INFO) // tpe is automatically 
    
    val ds = env.fromCollection(data)
    
    val t = ds.toTable(tEnv).as('a, 'b, 'c)
    tEnv.registerTable("MyTableRow", t)

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    result.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = List("Hello,Worlds,1","Hello again,Worlds,2")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }
    
  /** test unbounded groupBy (without window) **/
  @Test
  def testUnboundedGroupBy(): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.clear

    val sqlQuery = "SELECT b, COUNT(a) FROM MyTable GROUP BY b"

    val t = StreamTestData.get3TupleDataStream(env).toTable(tEnv).as('a, 'b, 'c)
    tEnv.registerTable("MyTable", t)

    val result = tEnv.sqlQuery(sqlQuery).toRetractStream[Row]
    result.addSink(new StreamITCase.RetractingSink).setParallelism(1)
    env.execute()

    val expected = List("1,1", "2,2", "3,3", "4,4", "5,5", "6,6")
    assertEquals(expected.sorted, StreamITCase.retractedResults.sorted)
  }

  @Test
  def testUnboundedGroupByCollect(): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    env.setStateBackend(getStateBackend)
    StreamITCase.clear

    val sqlQuery = "SELECT b, COLLECT(a) FROM MyTable GROUP BY b"

    val t = StreamTestData.get3TupleDataStream(env).toTable(tEnv).as('a, 'b, 'c)
    tEnv.registerTable("MyTable", t)

    val result = tEnv.sql(sqlQuery).toRetractStream[Row]
    result.addSink(new StreamITCase.RetractingSink).setParallelism(1)
    env.execute()

    val expected = List(
      "1,{1=1}",
      "2,{2=1, 3=1}",
      "3,{4=1, 5=1, 6=1}",
      "4,{7=1, 8=1, 9=1, 10=1}",
      "5,{11=1, 12=1, 13=1, 14=1, 15=1}",
      "6,{16=1, 17=1, 18=1, 19=1, 20=1, 21=1}")
    assertEquals(expected.sorted, StreamITCase.retractedResults.sorted)
  }

  @Test
  def testUnboundedGroupByCollectWithObject(): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    env.setStateBackend(getStateBackend)
    StreamITCase.clear

    val sqlQuery = "SELECT b, COLLECT(c) FROM MyTable GROUP BY b"

    val data = List(
      (1, 1, (12, "45.6")),
      (2, 2, (12, "45.612")),
      (3, 2, (13, "41.6")),
      (4, 3, (14, "45.2136")),
      (5, 3, (18, "42.6"))
    )

    tEnv.registerTable("MyTable",
      env.fromCollection(data).toTable(tEnv).as('a, 'b, 'c))

    val result = tEnv.sql(sqlQuery).toRetractStream[Row]
    result.addSink(new StreamITCase.RetractingSink).setParallelism(1)
    env.execute()

    val expected = List(
      "1,{(12,45.6)=1}",
      "2,{(13,41.6)=1, (12,45.612)=1}",
      "3,{(18,42.6)=1, (14,45.2136)=1}")
    assertEquals(expected.sorted, StreamITCase.retractedResults.sorted)
  }

  /** test selection **/
  @Test
  def testSelectExpressionFromTable(): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.clear

    val sqlQuery = "SELECT a * 2, b - 1 FROM MyTable"

    val t = StreamTestData.getSmall3TupleDataStream(env).toTable(tEnv).as('a, 'b, 'c)
    tEnv.registerTable("MyTable", t)

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    result.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = List("2,0", "4,1", "6,1")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  /** test filtering with registered table **/
  @Test
  def testSimpleFilter(): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.clear

    val sqlQuery = "SELECT * FROM MyTable WHERE a = 3"

    val t = StreamTestData.getSmall3TupleDataStream(env).toTable(tEnv).as('a, 'b, 'c)
    tEnv.registerTable("MyTable", t)

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    result.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = List("3,2,Hello world")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  /** test filtering with registered datastream **/
  @Test
  def testDatastreamFilter(): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.clear

    val sqlQuery = "SELECT * FROM MyTable WHERE _1 = 3"

    val t = StreamTestData.getSmall3TupleDataStream(env)
    tEnv.registerDataStream("MyTable", t)

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    result.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = List("3,2,Hello world")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  /** test union with registered tables **/
  @Test
  def testUnion(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.clear

    val sqlQuery = "SELECT * FROM T1 " +
      "UNION ALL " +
      "SELECT * FROM T2"

    val t1 = StreamTestData.getSmall3TupleDataStream(env).toTable(tEnv).as('a, 'b, 'c)
    tEnv.registerTable("T1", t1)
    val t2 = StreamTestData.getSmall3TupleDataStream(env).toTable(tEnv).as('a, 'b, 'c)
    tEnv.registerTable("T2", t2)

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    result.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = List(
      "1,1,Hi", "1,1,Hi",
      "2,2,Hello", "2,2,Hello",
      "3,2,Hello world", "3,2,Hello world")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  /** test union with filter **/
  @Test
  def testUnionWithFilter(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.clear

    val sqlQuery = "SELECT * FROM T1 WHERE a = 3 " +
      "UNION ALL " +
      "SELECT * FROM T2 WHERE a = 2"

    val t1 = StreamTestData.getSmall3TupleDataStream(env).toTable(tEnv).as('a, 'b, 'c)
    tEnv.registerTable("T1", t1)
    val t2 = StreamTestData.getSmall3TupleDataStream(env).toTable(tEnv).as('a, 'b, 'c)
    tEnv.registerTable("T2", t2)

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    result.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = List(
      "2,2,Hello",
      "3,2,Hello world")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  /** test union of a table and a datastream **/
  @Test
  def testUnionTableWithDataSet(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.clear

    val sqlQuery = "SELECT c FROM T1 WHERE a = 3 " +
      "UNION ALL " +
      "SELECT c FROM T2 WHERE a = 2"

    val t1 = StreamTestData.getSmall3TupleDataStream(env).toTable(tEnv).as('a, 'b, 'c)
    tEnv.registerTable("T1", t1)
    val t2 = StreamTestData.get3TupleDataStream(env)
    tEnv.registerDataStream("T2", t2, 'a, 'b, 'c)

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    result.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = List("Hello", "Hello world")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testUnnestPrimitiveArrayFromTable(): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.clear

    val data = List(
      (1, Array(12, 45), Array(Array(12, 45))),
      (2, Array(41, 5), Array(Array(18), Array(87))),
      (3, Array(18, 42), Array(Array(1), Array(45)))
    )
    val stream = env.fromCollection(data)
    tEnv.registerDataStream("T", stream, 'a, 'b, 'c)

    val sqlQuery = "SELECT a, b, s FROM T, UNNEST(T.b) AS A (s)"

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    result.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = List(
      "1,[12, 45],12",
      "1,[12, 45],45",
      "2,[41, 5],41",
      "2,[41, 5],5",
      "3,[18, 42],18",
      "3,[18, 42],42"
    )
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testUnnestArrayOfArrayFromTable(): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.clear

    val data = List(
      (1, Array(12, 45), Array(Array(12, 45))),
      (2, Array(41, 5), Array(Array(18), Array(87))),
      (3, Array(18, 42), Array(Array(1), Array(45)))
    )
    val stream = env.fromCollection(data)
    tEnv.registerDataStream("T", stream, 'a, 'b, 'c)

    val sqlQuery = "SELECT a, s FROM T, UNNEST(T.c) AS A (s)"

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    result.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = List(
      "1,[12, 45]",
      "2,[18]",
      "2,[87]",
      "3,[1]",
      "3,[45]")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testUnnestObjectArrayFromTableWithFilter(): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.clear

    val data = List(
      (1, Array((12, "45.6"), (12, "45.612"))),
      (2, Array((13, "41.6"), (14, "45.2136"))),
      (3, Array((18, "42.6")))
    )
    val stream = env.fromCollection(data)
    tEnv.registerDataStream("T", stream, 'a, 'b)

    val sqlQuery = "SELECT a, b, s, t FROM T, UNNEST(T.b) AS A (s, t) WHERE s > 13"

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    result.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = List(
      "2,[(13,41.6), (14,45.2136)],14,45.2136",
      "3,[(18,42.6)],18,42.6")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testHopStartEndWithHaving(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setStateBackend(getStateBackend)
    StreamITCase.clear
    env.setParallelism(1)

    val sqlQueryHopStartEndWithHaving =
      """
        |SELECT
        |  c AS k,
        |  COUNT(a) AS v,
        |  HOP_START(rowtime, INTERVAL '1' MINUTE, INTERVAL '1' MINUTE) AS windowStart,
        |  HOP_END(rowtime, INTERVAL '1' MINUTE, INTERVAL '1' MINUTE) AS windowEnd
        |FROM T1
        |GROUP BY HOP(rowtime, INTERVAL '1' MINUTE, INTERVAL '1' MINUTE), c
        |HAVING
        |  SUM(b) > 1 AND
        |    QUARTER(HOP_START(rowtime, INTERVAL '1' MINUTE, INTERVAL '1' MINUTE)) = 1
      """.stripMargin

    val data = Seq(
      Left(14000005L, (1, 1L, "Hi")),
      Left(14000000L, (2, 1L, "Hello")),
      Left(14000002L, (3, 1L, "Hello")),
      Right(14000010L),
      Left(8640000000L, (4, 1L, "Hello")), // data for the quarter to validate having filter
      Left(8640000001L, (4, 1L, "Hello")),
      Right(8640000010L)
    )

    val t1 = env.addSource(new EventTimeSourceFunction[(Int, Long, String)](data))
      .toTable(tEnv, 'a, 'b, 'c, 'rowtime.rowtime)

    tEnv.registerTable("T1", t1)

    val resultHopStartEndWithHaving = tEnv.sql(sqlQueryHopStartEndWithHaving).toAppendStream[Row]
    resultHopStartEndWithHaving.addSink(new StreamITCase.StringSink[Row])

    env.execute()

    val expected = List(
      "Hello,2,1970-01-01 03:53:00.0,1970-01-01 03:54:00.0"
    )
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testInsertIntoMemoryTable(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    MemoryTableSinkUtil.clear

    val t = StreamTestData.getSmall3TupleDataStream(env)
        .assignAscendingTimestamps(x => x._2)
      .toTable(tEnv, 'a, 'b, 'c, 'rowtime.rowtime)
    tEnv.registerTable("sourceTable", t)

    val fieldNames = Array("d", "e", "f", "t")
    val fieldTypes = Array(Types.INT, Types.LONG, Types.STRING, Types.SQL_TIMESTAMP)
      .asInstanceOf[Array[TypeInformation[_]]]
    val sink = new MemoryTableSinkUtil.UnsafeMemoryAppendTableSink
    tEnv.registerTableSink("targetTable", fieldNames, fieldTypes, sink)

    val sql = "INSERT INTO targetTable SELECT a, b, c, rowtime FROM sourceTable"
    tEnv.sqlUpdate(sql)
    env.execute()

    val expected = List(
      "1,1,Hi,1970-01-01 00:00:00.001",
      "2,2,Hello,1970-01-01 00:00:00.002",
      "3,2,Hello world,1970-01-01 00:00:00.002")
    assertEquals(expected.sorted, MemoryTableSinkUtil.results.sorted)
  }

}
