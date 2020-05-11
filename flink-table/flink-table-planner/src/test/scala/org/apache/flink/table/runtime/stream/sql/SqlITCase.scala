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

import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{EnvironmentSettings, Types}
import org.apache.flink.table.descriptors.{Rowtime, Schema}
import org.apache.flink.table.expressions.utils.Func15
import org.apache.flink.table.runtime.stream.sql.SqlITCase.TimestampAndWatermarkWithOffset
import org.apache.flink.table.runtime.utils.JavaUserDefinedAggFunctions.MultiArgCount
import org.apache.flink.table.runtime.utils.TimeTestUtil.EventTimeSourceFunction
import org.apache.flink.table.runtime.utils.{JavaUserDefinedTableFunctions, StreamITCase, StreamTestData, StreamingWithStateTestBase}
import org.apache.flink.table.utils.{InMemoryTableFactory, MemoryTableSourceSinkUtil}
import org.apache.flink.types.Row

import org.junit.Assert._
import org.junit._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class SqlITCase extends StreamingWithStateTestBase {

  val data = List(
    (1000L, "1", "Hello"),
    (2000L, "2", "Hello"),
    (3000L, null.asInstanceOf[String], "Hello"),
    (4000L, "4", "Hello"),
    (5000L, null.asInstanceOf[String], "Hello"),
    (6000L, "6", "Hello"),
    (7000L, "7", "Hello World"),
    (8000L, "8", "Hello World"),
    (20000L, "20", "Hello World"))

  @Before
  def clear(): Unit = {
    StreamITCase.clear
  }

  @Test
  def testDistinctAggWithMergeOnEventTimeSessionGroupWindow(): Unit = {
    // create a watermark with 10ms offset to delay the window emission by 10ms to verify merge
    val sessionWindowTestData = List(
      (1L, 2, "Hello"),       // (1, Hello)       - window
      (2L, 2, "Hello"),       // (1, Hello)       - window, deduped
      (8L, 2, "Hello"),       // (2, Hello)       - window, deduped during merge
      (10L, 3, "Hello"),      // (2, Hello)       - window, forwarded during merge
      (9L, 9, "Hello World"), // (1, Hello World) - window
      (4L, 1, "Hello"),       // (1, Hello)       - window, triggering merge
      (16L, 16, "Hello"))     // (3, Hello)       - window (not merged)

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val stream = env
      .fromCollection(sessionWindowTestData)
      .assignTimestampsAndWatermarks(
        new TimestampAndWatermarkWithOffset[(Long, Int, String)](10L))

    val settings = EnvironmentSettings.newInstance().useOldPlanner().build()
    val tEnv = StreamTableEnvironment.create(env, settings)
    val table = stream.toTable(tEnv, 'a, 'b, 'c, 'rowtime.rowtime)
    tEnv.registerTable("MyTable", table)
    tEnv.registerFunction("myCount", new MultiArgCount)

    val sqlQuery = "SELECT c, " +
      "  COUNT(DISTINCT b)," +
      "  SUM(DISTINCT b)," +
      "  myCount(DISTINCT b, 1)," +
      "  myCount(DISTINCT 1, b)," +
      "  SESSION_END(rowtime, INTERVAL '0.005' SECOND) " +
      "FROM MyTable " +
      "GROUP BY SESSION(rowtime, INTERVAL '0.005' SECOND), c "

    val results = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    results.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = Seq(
      "Hello World,1,9,1,1,1970-01-01 00:00:00.014", // window starts at [9L] till {14L}
      "Hello,1,16,1,1,1970-01-01 00:00:00.021",    // window starts at [16L] till {21L}, not merged
      "Hello,3,6,3,3,1970-01-01 00:00:00.015"      // window starts at [1L,2L],
                                                   // merged with [8L,10L], by [4L], till {15L}
    )
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testDistinctAggOnRowTimeTumbleWindow(): Unit = {
    val settings = EnvironmentSettings.newInstance().useOldPlanner().build()
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = StreamTableEnvironment.create(env, settings)
    env.setParallelism(1)

    val t = StreamTestData.get5TupleDataStream(env).assignAscendingTimestamps(x => x._2)
      .toTable(tEnv, 'a, 'b, 'c, 'd, 'e, 'rowtime.rowtime)
    tEnv.registerTable("MyTable", t)

    val sqlQuery = "SELECT a, " +
      "  SUM(DISTINCT e), " +
      "  MIN(DISTINCT e), " +
      "  COUNT(DISTINCT e)" +
      "FROM MyTable " +
      "GROUP BY a, " +
      "  TUMBLE(rowtime, INTERVAL '5' SECOND) "

    val results = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    results.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = List(
      "1,1,1,1",
      "2,3,1,2",
      "3,5,2,2",
      "4,3,1,2",
      "5,6,1,3")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testRowTimeTumbleWindow(): Unit = {
    val settings = EnvironmentSettings.newInstance().useOldPlanner().build()
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = StreamTableEnvironment.create(env, settings)
    env.setParallelism(1)

    val stream = env
                 .fromCollection(data)
                 .assignTimestampsAndWatermarks(
                   new TimestampAndWatermarkWithOffset[(Long, String, String)](0L))
    val table = stream.toTable(tEnv, 'a, 'b, 'c, 'rowtime.rowtime)

    tEnv.registerTable("T1", table)

    val sqlQuery = "SELECT c, COUNT(*), COUNT(1), COUNT(b) FROM T1 " +
      "GROUP BY TUMBLE(rowtime, interval '5' SECOND), c"

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    result.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = List("Hello World,2,2,2", "Hello World,1,1,1", "Hello,4,4,3", "Hello,2,2,1")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testNonWindowedCount(): Unit = {
    val settings = EnvironmentSettings.newInstance().useOldPlanner().build()
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env, settings)
    env.setParallelism(1)

    val stream = env.fromCollection(data)
    val table = stream.toTable(tEnv, 'a, 'b, 'c)

    tEnv.registerTable("T1", table)

    val sqlQuery = "SELECT c, COUNT(*), COUNT(1), COUNT(b) FROM T1 GROUP BY c"

    val result = tEnv.sqlQuery(sqlQuery).toRetractStream[Row]
    result.addSink(new StreamITCase.RetractingSink)
    env.execute()

    val expected = List("Hello World,3,3,3", "Hello,6,6,4")
    assertEquals(expected.sorted, StreamITCase.retractedResults.sorted)
  }

   /** test row stream registered table **/
  @Test
  def testRowRegister(): Unit = {
    val settings = EnvironmentSettings.newInstance().useOldPlanner().build()
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env, settings)

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
    
    val t = ds.toTable(tEnv).as("a", "b", "c")
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
    val settings = EnvironmentSettings.newInstance().useOldPlanner().build()
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env, settings)

    val sqlQuery = "SELECT b, COUNT(a) FROM MyTable GROUP BY b"

    val t = StreamTestData.get3TupleDataStream(env).toTable(tEnv).as("a", "b", "c")
    tEnv.registerTable("MyTable", t)

    val result = tEnv.sqlQuery(sqlQuery).toRetractStream[Row]
    result.addSink(new StreamITCase.RetractingSink).setParallelism(1)
    env.execute()

    val expected = List("1,1", "2,2", "3,3", "4,4", "5,5", "6,6")
    assertEquals(expected.sorted, StreamITCase.retractedResults.sorted)
  }

  @Test
  def testDistinctGroupBy(): Unit = {
    val settings = EnvironmentSettings.newInstance().useOldPlanner().build()
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env, settings)

    val sqlQuery =
      "SELECT b, " +
      "  SUM(DISTINCT (a / 3)), " +
      "  COUNT(DISTINCT SUBSTRING(c FROM 1 FOR 2))," +
      "  COUNT(DISTINCT c) " +
      "FROM MyTable " +
      "GROUP BY b"

    val t = StreamTestData.get3TupleDataStream(env).toTable(tEnv).as("a", "b", "c")
    tEnv.registerTable("MyTable", t)

    val result = tEnv.sqlQuery(sqlQuery).toRetractStream[Row]
    result.addSink(new StreamITCase.RetractingSink).setParallelism(1)
    env.execute()

    val expected = List("1,0,1,1", "2,1,1,2", "3,3,3,3", "4,5,1,4", "5,12,1,5", "6,18,1,6")
    assertEquals(expected.sorted, StreamITCase.retractedResults.sorted)
  }

  @Test
  def testDistinctWithRetraction(): Unit = {
    val settings = EnvironmentSettings.newInstance().useOldPlanner().build()
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env, settings)

    val data = new mutable.MutableList[(Int, Long, String)]
    data.+=((1, 1L, "Hi"))
    data.+=((1, 1L, "Hi World"))
    data.+=((1, 1L, "Test"))
    data.+=((2, 1L, "Hi World"))
    data.+=((2, 1L, "Test"))
    data.+=((3, 1L, "Hi World"))
    data.+=((3, 1L, "Hi World"))
    data.+=((3, 1L, "Hi World"))
    data.+=((4, 1L, "Hi World"))
    data.+=((4, 1L, "Test"))

    val t = env.fromCollection(data).toTable(tEnv).as("a", "b", "c")
    tEnv.registerTable("MyTable", t)

    // "1,1,3", "2,1,2", "3,1,1", "4,1,2"
    val distinct = "SELECT a, COUNT(DISTINCT b) AS distinct_b, COUNT(DISTINCT c) AS distinct_c " +
      "FROM MyTable GROUP BY a"
    val nestedDistinct = s"SELECT distinct_b, COUNT(DISTINCT distinct_c) " +
      s"FROM ($distinct) GROUP BY distinct_b"

    val result = tEnv.sqlQuery(nestedDistinct).toRetractStream[Row]
    result.addSink(new StreamITCase.RetractingSink).setParallelism(1)

    env.execute()

    val expected = List("1,3")
    assertEquals(expected.sorted, StreamITCase.retractedResults.sorted)
  }

  @Test
  def testUnboundedGroupByCollect(): Unit = {
    val settings = EnvironmentSettings.newInstance().useOldPlanner().build()
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env, settings)
    env.setStateBackend(getStateBackend)
    StreamITCase.clear

    val sqlQuery = "SELECT b, COLLECT(a) FROM MyTable GROUP BY b"

    val t = StreamTestData.get3TupleDataStream(env).toTable(tEnv).as("a", "b", "c")
    tEnv.registerTable("MyTable", t)

    val result = tEnv.sqlQuery(sqlQuery).toRetractStream[Row]
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
    val settings = EnvironmentSettings.newInstance().useOldPlanner().build()
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env, settings)
    env.setStateBackend(getStateBackend)

    val sqlQuery = "SELECT b, COLLECT(c) FROM MyTable GROUP BY b"

    val data = List(
      (1, 1, (12, "45.6")),
      (2, 2, (12, "45.612")),
      (3, 2, (13, "41.6")),
      (4, 3, (14, "45.2136")),
      (5, 3, (18, "42.6"))
    )

    tEnv.registerTable("MyTable",
      env.fromCollection(data).toTable(tEnv).as("a", "b", "c"))

    val result = tEnv.sqlQuery(sqlQuery).toRetractStream[Row]
    result.addSink(new StreamITCase.RetractingSink).setParallelism(1)
    env.execute()

    val expected = List(
      "1,{(12,45.6)=1}",
      "2,{(13,41.6)=1, (12,45.612)=1}",
      "3,{(18,42.6)=1, (14,45.2136)=1}")
    assertEquals(expected.sorted, StreamITCase.retractedResults.sorted)
  }

  /** test select star **/
  @Test
  def testSelectStar(): Unit = {
    val settings = EnvironmentSettings.newInstance().useOldPlanner().build()
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env, settings)

    val sqlQuery = "SELECT * FROM MyTable"

    val t = StreamTestData.getSmallNestedTupleDataStream(env).toTable(tEnv).as("a", "b")
    tEnv.registerTable("MyTable", t)

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    result.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = List("(1,1),one", "(2,2),two", "(3,3),three")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  /** test selection **/
  @Test
  def testSelectExpressionFromTable(): Unit = {
    val settings = EnvironmentSettings.newInstance().useOldPlanner().build()
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env, settings)

    val sqlQuery = "SELECT a * 2, b - 1 FROM MyTable"

    val t = StreamTestData.getSmall3TupleDataStream(env).toTable(tEnv).as("a", "b", "c")
    tEnv.registerTable("MyTable", t)

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    result.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = List("2,0", "4,1", "6,1")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testSelectExpressionWithSplitFromTable(): Unit = {
    val settings = EnvironmentSettings.newInstance().useOldPlanner().build()
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env, settings)
    tEnv.getConfig.setMaxGeneratedCodeLength(1) // split every field

    val sqlQuery = "SELECT a * 2, b - 1 FROM MyTable"

    val t = StreamTestData.getSmall3TupleDataStream(env).toTable(tEnv).as("a", "b", "c")
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
    val settings = EnvironmentSettings.newInstance().useOldPlanner().build()
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env, settings)

    val sqlQuery = "SELECT * FROM MyTable WHERE a = 3"

    val t = StreamTestData.getSmall3TupleDataStream(env).toTable(tEnv).as("a", "b", "c")
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
    val settings = EnvironmentSettings.newInstance().useOldPlanner().build()
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env, settings)

    val sqlQuery = "SELECT * FROM MyTable WHERE _1 = 3"

    val t = StreamTestData.getSmall3TupleDataStream(env)
    tEnv.createTemporaryView("MyTable", t)

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    result.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = List("3,2,Hello world")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  /** test union with registered tables **/
  @Test
  def testUnion(): Unit = {
    val settings = EnvironmentSettings.newInstance().useOldPlanner().build()
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env, settings)

    val sqlQuery = "SELECT * FROM T1 " +
      "UNION ALL " +
      "SELECT * FROM T2"

    val t1 = StreamTestData.getSmall3TupleDataStream(env).toTable(tEnv).as("a", "b", "c")
    tEnv.registerTable("T1", t1)
    val t2 = StreamTestData.getSmall3TupleDataStream(env).toTable(tEnv).as("a", "b", "c")
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
    val settings = EnvironmentSettings.newInstance().useOldPlanner().build()
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env, settings)

    val sqlQuery = "SELECT * FROM T1 WHERE a = 3 " +
      "UNION ALL " +
      "SELECT * FROM T2 WHERE a = 2"

    val t1 = StreamTestData.getSmall3TupleDataStream(env).toTable(tEnv).as("a", "b", "c")
    tEnv.registerTable("T1", t1)
    val t2 = StreamTestData.getSmall3TupleDataStream(env).toTable(tEnv).as("a", "b", "c")
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
    val settings = EnvironmentSettings.newInstance().useOldPlanner().build()
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env, settings)

    val sqlQuery = "SELECT c FROM T1 WHERE a = 3 " +
      "UNION ALL " +
      "SELECT c FROM T2 WHERE a = 2"

    val t1 = StreamTestData.getSmall3TupleDataStream(env).toTable(tEnv).as("a", "b", "c")
    tEnv.registerTable("T1", t1)
    val t2 = StreamTestData.get3TupleDataStream(env)
    tEnv.createTemporaryView("T2", t2, 'a, 'b, 'c)

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    result.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = List("Hello", "Hello world")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testUnnestPrimitiveArrayFromTable(): Unit = {
    val settings = EnvironmentSettings.newInstance().useOldPlanner().build()
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env, settings)

    val data = List(
      (1, Array(12, 45), Array(Array(12, 45))),
      (2, Array(41, 5), Array(Array(18), Array(87))),
      (3, Array(18, 42), Array(Array(1), Array(45)))
    )
    val stream = env.fromCollection(data)
    tEnv.createTemporaryView("T", stream, 'a, 'b, 'c)

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
    val settings = EnvironmentSettings.newInstance().useOldPlanner().build()
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env, settings)

    val data = List(
      (1, Array(12, 45), Array(Array(12, 45))),
      (2, Array(41, 5), Array(Array(18), Array(87))),
      (3, Array(18, 42), Array(Array(1), Array(45)))
    )
    val stream = env.fromCollection(data)
    tEnv.createTemporaryView("T", stream, 'a, 'b, 'c)

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
    val settings = EnvironmentSettings.newInstance().useOldPlanner().build()
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env, settings)

    val data = List(
      (1, Array((12, "45.6"), (12, "45.612"))),
      (2, Array((13, "41.6"), (14, "45.2136"))),
      (3, Array((18, "42.6")))
    )
    val stream = env.fromCollection(data)
    tEnv.createTemporaryView("T", stream, 'a, 'b)

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
  def testUnnestMultiSetFromCollectResult(): Unit = {
    val settings = EnvironmentSettings.newInstance().useOldPlanner().build()
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env, settings)
    env.setStateBackend(getStateBackend)

    val data = List(
      (1, 1, (12, "45.6")),
      (2, 2, (12, "45.612")),
      (3, 2, (13, "41.6")),
      (4, 3, (14, "45.2136")),
      (5, 3, (18, "42.6")))
    tEnv.registerTable("t1", env.fromCollection(data).toTable(tEnv).as("a", "b", "c"))

    val t2 = tEnv.sqlQuery("SELECT b, COLLECT(c) as `set` FROM t1 GROUP BY b")
    tEnv.registerTable("t2", t2)

    val result = tEnv
      .sqlQuery("SELECT b, id, point FROM t2, UNNEST(t2.`set`) AS A(id, point) WHERE b < 3")
      .toRetractStream[Row]
    result.addSink(new StreamITCase.RetractingSink).setParallelism(1)
    env.execute()

    val expected = List(
      "1,12,45.6",
      "2,12,45.612",
      "2,13,41.6")
    assertEquals(expected.sorted, StreamITCase.retractedResults.sorted)
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

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val settings = EnvironmentSettings.newInstance().useOldPlanner().build()
    val tEnv = StreamTableEnvironment.create(env, settings)

    val t1 = env.fromCollection(data).toTable(tEnv).as("a", "b", "c")
    tEnv.registerTable("t1", t1)

    val t2 = tEnv.sqlQuery("SELECT a, COLLECT(b) as `set` FROM t1 GROUP BY a")
    tEnv.registerTable("t2", t2)

    val result = tEnv
      .sqlQuery("SELECT a, s FROM t2 LEFT JOIN UNNEST(t2.`set`) AS A(s) ON TRUE WHERE a < 5")
      .toRetractStream[Row]
    result.addSink(new StreamITCase.RetractingSink).setParallelism(1)
    env.execute()

    val expected = List(
      "1,1",
      "1,2",
      "2,2",
      "3,null",
      "4,4"
    )
    assertEquals(expected.sorted, StreamITCase.retractedResults.sorted)
  }

  @Test
  def testHopStartEndWithHaving(): Unit = {
    val settings = EnvironmentSettings.newInstance().useOldPlanner().build()
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env, settings)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setStateBackend(getStateBackend)
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

    val resultHopStartEndWithHaving = tEnv
      .sqlQuery(sqlQueryHopStartEndWithHaving)
      .toAppendStream[Row]
    resultHopStartEndWithHaving.addSink(new StreamITCase.StringSink[Row])

    env.execute()

    val expected = List(
      "Hello,2,1970-01-01 03:53:00.0,1970-01-01 03:54:00.0"
    )
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testWriteReadTableSourceSink(): Unit = {
    val settings = EnvironmentSettings.newInstance().useOldPlanner().build()
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = StreamTableEnvironment.create(env, settings)
    MemoryTableSourceSinkUtil.clear()

    val desc = new Schema()
      .field("a", Types.INT)
      .field("e", Types.LONG)
      .field("f", Types.STRING)
      .field("t", Types.SQL_TIMESTAMP)
        .rowtime(new Rowtime().timestampsFromField("t").watermarksPeriodicAscending())
      .field("proctime", Types.SQL_TIMESTAMP).proctime()
    val properties = desc.toProperties

    val t = StreamTestData.getSmall3TupleDataStream(env)
      .assignAscendingTimestamps(x => x._2)
      .toTable(tEnv, 'a, 'b, 'c, 'rowtime.rowtime)
    tEnv.registerTable("sourceTable", t)

    tEnv.registerTableSource("targetTable",
      new InMemoryTableFactory(3).createStreamTableSource(properties))
    tEnv.registerTableSink("targetTable",
      new InMemoryTableFactory(3).createStreamTableSink(properties))

    tEnv.sqlUpdate("INSERT INTO targetTable SELECT a, b, c, rowtime FROM sourceTable")
    tEnv.execute("job name")
    tEnv.sqlQuery("SELECT a, e, f, t from targetTable")
      .addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = List(
      "1,1,Hi,1970-01-01 00:00:00.001",
      "2,2,Hello,1970-01-01 00:00:00.002",
      "3,2,Hello world,1970-01-01 00:00:00.002")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testUDFWithLongVarargs(): Unit = {
    val settings = EnvironmentSettings.newInstance().useOldPlanner().build()
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env, settings)

    tEnv.registerFunction("func15", Func15)

    val parameters = "c," + (0 until 300).map(_ => "a").mkString(",")
    val sqlQuery = s"SELECT func15($parameters) FROM T1"

    val t1 = StreamTestData.getSmall3TupleDataStream(env).toTable(tEnv).as("a", "b", "c")
    tEnv.registerTable("T1", t1)

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    result.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = List(
      "Hi300",
      "Hello300",
      "Hello world300")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testUDTFWithLongVarargs(): Unit = {
    val settings = EnvironmentSettings.newInstance().useOldPlanner().build()
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env, settings)

    tEnv.registerFunction("udtf", new JavaUserDefinedTableFunctions.JavaTableFunc1)

    val parameters = (0 until 300).map(_ => "c").mkString(",")
    val sqlQuery = s"SELECT T1.a, T2.x FROM T1 " +
      s"JOIN LATERAL TABLE(udtf($parameters)) as T2(x) ON TRUE"

    val t1 = StreamTestData.getSmall3TupleDataStream(env).toTable(tEnv).as("a", "b", "c")
    tEnv.registerTable("T1", t1)

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    result.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = List(
      "1,600",
      "2,1500",
      "3,3300")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testVeryBigQuery(): Unit = {
    val settings = EnvironmentSettings.newInstance().useOldPlanner().build()
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env, settings)

    val t = StreamTestData.getSingletonDataStream(env).toTable(tEnv).as("a", "b", "c")
    tEnv.registerTable("MyTable", t)

    val sqlQuery = new StringBuilder
    sqlQuery.append("SELECT ")
    val expected = new StringBuilder
    for (i <- 0 until 500) {
      sqlQuery.append(s"a + b + $i, ")
      expected.append((1 + 42L + i).toString + ",")
    }
    sqlQuery.append("c FROM MyTable")
    expected.append("Hi")

    val result = tEnv.sqlQuery(sqlQuery.toString()).toAppendStream[Row]
    result.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    assertEquals(List(expected.toString()), StreamITCase.testResults.sorted)
  }

  @Test
  def testProjectionWithManyColumns(): Unit = {
    val settings = EnvironmentSettings.newInstance().useOldPlanner().build()
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env, settings)

    // force code split
    tEnv.getConfig.setMaxGeneratedCodeLength(1)

    val length = 1000
    val rowData = List.range(0, length)
    val row: Row = new Row(length)
    val fieldTypes = new ArrayBuffer[TypeInformation[_]]()
    val fieldNames = new ArrayBuffer[String]()
    rowData.foreach { i =>
      row.setField(i, i)
      fieldTypes += Types.INT()
      fieldNames += s"f$i"
    }

    val data = new mutable.MutableList[Row]
    data.+=(row)
    val t = env.fromCollection(data)(new RowTypeInfo(fieldTypes.toArray: _*)).toTable(tEnv)
    tEnv.registerTable("MyTable", t)

    val expected = List(rowData.reverse.mkString(","))
    val sql =
      s"""
         |SELECT ${fieldNames.reverse.mkString(", ")} FROM MyTable
       """.stripMargin

    val result = tEnv.sqlQuery(sql).toAppendStream[Row]
    result.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    assertEquals(expected, StreamITCase.testResults)
  }
}

object SqlITCase {

  class TimestampAndWatermarkWithOffset[T <: Product](
      offset: Long) extends AssignerWithPunctuatedWatermarks[T] {

    override def checkAndGetNextWatermark(
        lastElement: T,
        extractedTimestamp: Long): Watermark = {
      new Watermark(extractedTimestamp - offset)
    }

    override def extractTimestamp(
        element: T,
        previousElementTimestamp: Long): Long = {
      element.productElement(0).asInstanceOf[Long]
    }
  }

}
