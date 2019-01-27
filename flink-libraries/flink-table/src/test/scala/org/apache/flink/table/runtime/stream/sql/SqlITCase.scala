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

import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.scala._
import org.apache.flink.table.api.TableConfigOptions
import org.apache.flink.table.api.scala._
import org.apache.flink.table.expressions.utils.{Func15, SplitUDF}
import org.apache.flink.table.runtime.utils._
import org.apache.flink.types.Row
import org.junit.Assert._
import org.junit._

import scala.collection.mutable

class SqlITCase extends StreamingTestBase {

   /** test row stream registered table **/
  @Test
  def testRowRegister(): Unit = {
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

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sqlQuery).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = List("Hello,Worlds,1","Hello again,Worlds,2")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  /** test selection **/
  @Test
  def testSelectExpressionFromTable(): Unit = {
    val sqlQuery = "SELECT a * 2, b - 1 FROM MyTable"

    val t = StreamTestData.getSmall3TupleDataStream(env).toTable(tEnv).as('a, 'b, 'c)
    tEnv.registerTable("MyTable", t)

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sqlQuery).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = List("2,0", "4,1", "6,1")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  /** test filtering with registered table **/
  @Test
  def testSimpleFilter(): Unit = {
    val sqlQuery = "SELECT * FROM MyTable WHERE a = 3"

    val t = StreamTestData.getSmall3TupleDataStream(env).toTable(tEnv).as('a, 'b, 'c)
    tEnv.registerTable("MyTable", t)

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sqlQuery).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = List("3,2,Hello world")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  /** test filtering with registered datastream **/
  @Test
  def testDatastreamFilter(): Unit = {
    val sqlQuery = "SELECT * FROM MyTable WHERE _1 = 3"

    val t = StreamTestData.getSmall3TupleDataStream(env)
    tEnv.registerDataStream("MyTable", t)

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sqlQuery).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = List("3,2,Hello world")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  /** test union with registered tables **/
  @Test
  def testUnion(): Unit = {
    val sqlQuery = "SELECT * FROM T1 " +
      "UNION ALL " +
      "SELECT * FROM T2"

    val t1 = StreamTestData.getSmall3TupleDataStream(env).toTable(tEnv).as('a, 'b, 'c)
    tEnv.registerTable("T1", t1)
    val t2 = StreamTestData.getSmall3TupleDataStream(env).toTable(tEnv).as('a, 'b, 'c)
    tEnv.registerTable("T2", t2)

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sqlQuery).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = List(
      "1,1,Hi", "1,1,Hi",
      "2,2,Hello", "2,2,Hello",
      "3,2,Hello world", "3,2,Hello world")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  /** test union with filter **/
  @Test
  def testUnionWithFilter(): Unit = {
    val sqlQuery = "SELECT * FROM T1 WHERE a = 3 " +
      "UNION ALL " +
      "SELECT * FROM T2 WHERE a = 2"

    val t1 = StreamTestData.getSmall3TupleDataStream(env).toTable(tEnv).as('a, 'b, 'c)
    tEnv.registerTable("T1", t1)
    val t2 = StreamTestData.getSmall3TupleDataStream(env).toTable(tEnv).as('a, 'b, 'c)
    tEnv.registerTable("T2", t2)

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sqlQuery).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = List(
      "2,2,Hello",
      "3,2,Hello world")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  /** test union of a table and a datastream **/
  @Test
  def testUnionTableWithDataSet(): Unit = {
    val sqlQuery = "SELECT c FROM T1 WHERE a = 3 " +
      "UNION ALL " +
      "SELECT c FROM T2 WHERE a = 2"

    val t1 = StreamTestData.getSmall3TupleDataStream(env).toTable(tEnv).as('a, 'b, 'c)
    tEnv.registerTable("T1", t1)
    val t2 = StreamTestData.get3TupleDataStream(env)
    tEnv.registerDataStream("T2", t2, 'a, 'b, 'c)

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sqlQuery).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = List("Hello", "Hello world")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  /** test union of multiple inputs **/
  @Test
  def testUnionOfMultiInputs(): Unit = {
    tEnv.getConfig.getConf.setBoolean(TableConfigOptions.SQL_EXEC_SOURCE_VALUES_INPUT_ENABLED, true)

    val sqlQuery = "select max(v) as x, min(v) as n from \n" +
      "(values cast(-100 as double), cast(2 as double), cast(-86.4 as double)) as t(v)"

    val sink = new TestingRetractSink
    tEnv.sqlQuery(sqlQuery).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = List("2.0,-100.0")
    assertEquals(expected, sink.getRetractResults)
  }

  @Test
  def testUnionWithDifferentType(): Unit = {
    val data = List((1.toByte, "2"))
    val stream = env.fromCollection(data)
    tEnv.registerDataStream("T", stream, 'a, 'b)

    val sqlQuery = "SELECT a FROM T UNION SELECT b FROM T"

    val sink = new TestingRetractSink
    tEnv.sqlQuery(sqlQuery).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = List(
      "1","2"
    )
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testUnionWithDifferentTypeNullRight(): Unit = {
    val data = new mutable.MutableList[(Int, String)]
    data.+=((1, null))

    val stream = env.fromCollection(data)
    tEnv.registerDataStream("T", stream, 'a, 'b)

    val sqlQuery = "SELECT a FROM T UNION SELECT b FROM T"

    val sink = new TestingRetractSink
    tEnv.sqlQuery(sqlQuery).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = List("1","null")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testUnionWithDifferentTypeNullLeft(): Unit = {
    val data = new mutable.MutableList[(Int, String)]
    data.+=((1, null))
    val stream = env.fromCollection(data)
    tEnv.registerDataStream("T", stream, 'a, 'b)

    val sqlQuery = "SELECT b FROM T UNION SELECT a FROM T"

    val sink = new TestingRetractSink
    tEnv.sqlQuery(sqlQuery).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = List("1","null")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testIntersectWithDifferentType(): Unit = {
    val data = List((1.toByte, "1"))
    val stream = env.fromCollection(data)
    tEnv.registerDataStream("T", stream, 'a, 'b)

    val sqlQuery = "SELECT a FROM T INTERSECT SELECT b FROM T"

    val sink = new TestingRetractSink
    tEnv.sqlQuery(sqlQuery).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = List(
      "1"
    )
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testIntersectWithDifferentTypeNullRight(): Unit = {
    val data = new mutable.MutableList[(Int, String)]
    data.+=((1, null))
    val stream = env.fromCollection(data)
    tEnv.registerDataStream("T", stream, 'a, 'b)

    val sqlQuery = "SELECT a FROM T INTERSECT SELECT b FROM T"

    val sink = new TestingRetractSink
    tEnv.sqlQuery(sqlQuery).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = List()
    assertEquals(expected, sink.getRetractResults)
  }

  @Test
  def testIntersectWithDifferentTypeNullLeft(): Unit = {
    val data = new mutable.MutableList[(Int, String)]
    data.+=((1, null))
    val stream = env.fromCollection(data)
    tEnv.registerDataStream("T", stream, 'a, 'b)

    val sqlQuery = "SELECT b FROM T INTERSECT SELECT a FROM T"

    val sink = new TestingRetractSink
    tEnv.sqlQuery(sqlQuery).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = List()
    assertEquals(expected, sink.getRetractResults)
  }

  @Test
  def testExceptWithDifferentType(): Unit = {
    val data = List((1.toByte, "2"))
    val stream = env.fromCollection(data)
    tEnv.registerDataStream("T", stream, 'a, 'b)

    val sqlQuery = "SELECT a FROM T EXCEPT SELECT b FROM T"

    val sink = new TestingRetractSink
    tEnv.sqlQuery(sqlQuery).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = List(
      "1"
    )
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testExceptWithDifferentTypeNullRight(): Unit = {
    val data = new mutable.MutableList[(Int, String)]
    data.+=((1, null))
    val stream = env.fromCollection(data)
    tEnv.registerDataStream("T", stream, 'a, 'b)

    val sqlQuery = "SELECT a FROM T EXCEPT SELECT b FROM T"

    val sink = new TestingRetractSink
    tEnv.sqlQuery(sqlQuery).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = List(
      "1"
    )
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testExceptWithDifferentTypeNullLeft(): Unit = {
    val data = new mutable.MutableList[(Int, String)]
    data.+=((1, null))
    val stream = env.fromCollection(data)
    tEnv.registerDataStream("T", stream, 'a, 'b)

    val sqlQuery = "SELECT b FROM T EXCEPT SELECT a FROM T"

    val sink = new TestingRetractSink
    tEnv.sqlQuery(sqlQuery).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = List(
      "null"
    )
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testUnnestPrimitiveArrayFromTable(): Unit = {
    val data = List(
      (1, Array(12, 45), Array(Array(12, 45))),
      (2, Array(41, 5), Array(Array(18), Array(87))),
      (3, Array(18, 42), Array(Array(1), Array(45)))
    )
    val stream = env.fromCollection(data)
    tEnv.registerDataStream("T", stream, 'a, 'b, 'c)

    val sqlQuery = "SELECT a, b, s FROM T, UNNEST(T.b) AS A (s)"

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sqlQuery).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = List(
      "1,[12, 45],12",
      "1,[12, 45],45",
      "2,[41, 5],41",
      "2,[41, 5],5",
      "3,[18, 42],18",
      "3,[18, 42],42"
    )
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testUnnestArrayOfArrayFromTable(): Unit = {
    val data = List(
      (1, Array(12, 45), Array(Array(12, 45))),
      (2, Array(41, 5), Array(Array(18), Array(87))),
      (3, Array(18, 42), Array(Array(1), Array(45)))
    )
    val stream = env.fromCollection(data)
    tEnv.registerDataStream("T", stream, 'a, 'b, 'c)

    val sqlQuery = "SELECT a, s FROM T, UNNEST(T.c) AS A (s)"

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sqlQuery).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = List(
      "1,[12, 45]",
      "2,[18]",
      "2,[87]",
      "3,[1]",
      "3,[45]")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testUnnestObjectArrayFromTableWithFilter(): Unit = {
    val data = List(
      (1, Array((12, "45.6"), (12, "45.612"))),
      (2, Array((13, "41.6"), (14, "45.2136"))),
      (3, Array((18, "42.6")))
    )
    val stream = env.fromCollection(data)
    tEnv.registerDataStream("T", stream, 'a, 'b)

    val sqlQuery = "SELECT a, s, t FROM T, UNNEST(T.b) AS A (s, t) WHERE s > 13"

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sqlQuery).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = List(
      "2,14,45.2136",
      "3,18,42.6")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testUnnestMultiSetFromCollectResult(): Unit = {
    val sqlQuery = "SELECT b, COLLECT(a) as `set` FROM MyTable GROUP BY b"

    val t = StreamTestData.get3TupleDataStream(env).toTable(tEnv).as('a, 'b, 'c)
    tEnv.registerTable("MyTable", t)

    val view1 = tEnv.sqlQuery(sqlQuery)
    tEnv.registerTable("view1", view1)

    val sqlQuery1 = "SELECT b, s FROM view1 t1, UNNEST(t1.`set`) AS A (s) where t1.b < 3"

    val sink = new TestingRetractSink
    tEnv.sqlQuery(sqlQuery1).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = List(
      "1,1",
      "2,2",
      "2,3"
    )
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testUnnestObjectMultiSetFromUnboundedGroupByCollectResult(): Unit = {
    val sqlQuery = "SELECT b, COLLECT(c) as `set` FROM MyTable GROUP BY b"
    val data = List(
      (1, 1, (12, "45.6")),
      (2, 2, (12, "45.612")),
      (3, 2, (13, "41.6")),
      (4, 3, (14, "45.2136")),
      (5, 3, (18, "42.6")))
    tEnv.registerTable("MyTable", env.fromCollection(data).toTable(tEnv).as('a, 'b, 'c))

    val view1 = tEnv.sqlQuery(sqlQuery)
    tEnv.registerTable("v1", view1)

    val sqlQuery1 = "SELECT b, id, point FROM v1 t1, unnest(t1.`set`) AS A(id, point) where b < 3"

    val sink = new TestingRetractSink
    tEnv.sqlQuery(sqlQuery1).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = List(
      "1,12,45.6",
      "2,12,45.612",
      "2,13,41.6")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testLeftUnnestMultiSetFromCollectResult(): Unit = {
    val data = List(
      (1, "1", "Hello"),
      (1, "2", "Hello2"),
      (2, "2", "Hello"),
      (3, null.asInstanceOf[String], "Hello"),
      (4, "4", "Hello"),
      (5, "5", "Hello"),
      (5, null.asInstanceOf[String], "Hello"),
      (6, "6", "Hello"),
      (7, "7", "Hello World"),
      (7, "8", "Hello World"))

    val sqlQuery = "SELECT a, COLLECT(b) as `set` FROM MyTable GROUP BY a"

    val t = env.fromCollection(data).toTable(tEnv).as('a, 'b, 'c)
    tEnv.registerTable("MyTable", t)

    val view1 = tEnv.sqlQuery(sqlQuery)
    tEnv.registerTable("v1", view1)

    val sqlQuery1 = "SELECT a, s FROM v1 t1 left join unnest(t1.`set`) AS A(s) on true where a < 5"

    val sink = new TestingRetractSink
    tEnv.sqlQuery(sqlQuery1).toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = List(
      "1,1",
      "1,2",
      "2,2",
      "3,null",
      "4,4"
    )
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Ignore
  @Test
  def testPrintSink(): Unit = {
    env.setParallelism(1)

    val t = StreamTestData.getSmall3TupleDataStream(env)
      .assignAscendingTimestamps(x => x._2)
      .toTable(tEnv, 'a, 'b, 'c)
    tEnv.registerTable("sourceTable", t)

    val sql = "INSERT INTO console SELECT a, b, c FROM sourceTable"
    tEnv.sqlUpdate(sql)

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

    val expected = List(
      "task-1> (+)1,1,Hi",
      "task-1> (+)2,2,Hello",
      "task-1> (+)3,2,Hello world")
    assertEquals(expected.sorted, baos.toString.split('\n').toList.sorted)
  }

  @Test
  def testNotIn(): Unit = {
    val sqlQuery =
      s"""
         |SELECT * FROM MyTable WHERE a not in (
         |3, 4, 5, 6, 7, 8, 9, 10,
         |15, 16, 17, 18, 19, 20,
         |25, 26, 27, 28, 29, 30,
         |35, 36, 37, 38, 39, 40,
         |45, 46, 47, 48, 49, 50,
         |55, 56, 57, 58, 59, 60,
         |65, 66, 67, 68, 69, 70,
         |75, 76, 77, 78, 79, 80,
         |85, 86, 87, 88, 89, 90,
         |95, 96, 97, 98, 99, 100
         |)""".stripMargin

    val t = StreamTestData.getSmall3TupleDataStream(env).toTable(tEnv).as('a, 'b, 'c)
    tEnv.registerTable("MyTable", t)

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sqlQuery).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = List("1,1,Hi", "2,2,Hello")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testUDFWithLongVarargs(): Unit = {
    tEnv.registerFunction("func15", Func15)

    val parameters = "c," + (0 until 255).map(_ => "a").mkString(",")
    val sqlQuery = s"SELECT func15($parameters) FROM T1"

    val t1 = StreamTestData.getSmall3TupleDataStream(env).toTable(tEnv).as('a, 'b, 'c)
    tEnv.registerTable("T1", t1)

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sqlQuery).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = List(
      "Hi255",
      "Hello255",
      "Hello world255")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testUDTFWithLongVarargs(): Unit = {
    tEnv.registerFunction("udtf", new JavaUserDefinedTableFunctions.JavaTableFunc1)

    val parameters = (0 until 300).map(_ => "c").mkString(",")
    val sqlQuery = s"SELECT T1.a, T2.x FROM T1 " +
      s"JOIN LATERAL TABLE(udtf($parameters)) as T2(x) ON TRUE"

    val t1 = StreamTestData.getSmall3TupleDataStream(env).toTable(tEnv).as('a, 'b, 'c)
    tEnv.registerTable("T1", t1)

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sqlQuery).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = List(
      "1,600",
      "2,1500",
      "3,3300")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

}
