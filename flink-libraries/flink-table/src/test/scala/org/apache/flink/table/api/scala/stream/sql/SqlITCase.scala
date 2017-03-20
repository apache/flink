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

package org.apache.flink.table.api.scala.stream.sql

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{TableEnvironment, TableException}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.scala.stream.utils.{StreamingWithStateTestBase, StreamITCase,
StreamTestData}
import org.apache.flink.types.Row
import org.junit.Assert._
import org.junit._

import scala.collection.mutable

class SqlITCase extends StreamingWithStateTestBase {

  val data = List(
    (1L, 1, "Hello"),
    (2L, 2, "Hello"),
    (3L, 3, "Hello"),
    (4L, 4, "Hello"),
    (5L, 5, "Hello"),
    (6L, 6, "Hello"),
    (7L, 7, "Hello World"),
    (8L, 8, "Hello World"),
    (20L, 20, "Hello World"))

  /** test selection **/
  @Test
  def testSelectExpressionFromTable(): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()

    val sqlQuery = "SELECT a * 2, b - 1 FROM MyTable"

    val t = StreamTestData.getSmall3TupleDataStream(env).toTable(tEnv).as('a, 'b, 'c)
    tEnv.registerTable("MyTable", t)

    val result = tEnv.sql(sqlQuery).toDataStream[Row]
    result.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList("2,0", "4,1", "6,1")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  /** test filtering with registered table **/
  @Test
  def testSimpleFilter(): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()

    val sqlQuery = "SELECT * FROM MyTable WHERE a = 3"

    val t = StreamTestData.getSmall3TupleDataStream(env).toTable(tEnv).as('a, 'b, 'c)
    tEnv.registerTable("MyTable", t)

    val result = tEnv.sql(sqlQuery).toDataStream[Row]
    result.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList("3,2,Hello world")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  /** test filtering with registered datastream **/
  @Test
  def testDatastreamFilter(): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()

    val sqlQuery = "SELECT * FROM MyTable WHERE _1 = 3"

    val t = StreamTestData.getSmall3TupleDataStream(env)
    tEnv.registerDataStream("MyTable", t)

    val result = tEnv.sql(sqlQuery).toDataStream[Row]
    result.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList("3,2,Hello world")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  /** test union with registered tables **/
  @Test
  def testUnion(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()

    val sqlQuery = "SELECT * FROM T1 " +
      "UNION ALL " +
      "SELECT * FROM T2"

    val t1 = StreamTestData.getSmall3TupleDataStream(env).toTable(tEnv).as('a, 'b, 'c)
    tEnv.registerTable("T1", t1)
    val t2 = StreamTestData.getSmall3TupleDataStream(env).toTable(tEnv).as('a, 'b, 'c)
    tEnv.registerTable("T2", t2)

    val result = tEnv.sql(sqlQuery).toDataStream[Row]
    result.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList(
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
    StreamITCase.testResults = mutable.MutableList()

    val sqlQuery = "SELECT * FROM T1 WHERE a = 3 " +
      "UNION ALL " +
      "SELECT * FROM T2 WHERE a = 2"

    val t1 = StreamTestData.getSmall3TupleDataStream(env).toTable(tEnv).as('a, 'b, 'c)
    tEnv.registerTable("T1", t1)
    val t2 = StreamTestData.getSmall3TupleDataStream(env).toTable(tEnv).as('a, 'b, 'c)
    tEnv.registerTable("T2", t2)

    val result = tEnv.sql(sqlQuery).toDataStream[Row]
    result.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList(
      "2,2,Hello",
      "3,2,Hello world")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  /** test union of a table and a datastream **/
  @Test
  def testUnionTableWithDataSet(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()

    val sqlQuery = "SELECT c FROM T1 WHERE a = 3 " +
      "UNION ALL " +
      "SELECT c FROM T2 WHERE a = 2"

    val t1 = StreamTestData.getSmall3TupleDataStream(env).toTable(tEnv).as('a, 'b, 'c)
    tEnv.registerTable("T1", t1)
    val t2 = StreamTestData.get3TupleDataStream(env)
    tEnv.registerDataStream("T2", t2, 'a, 'b, 'c)

    val result = tEnv.sql(sqlQuery).toDataStream[Row]
    result.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList("Hello", "Hello world")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testUnboundPartitionedProcessingWindowWithRange(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStateBackend(getStateBackend)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()

    // for sum aggregation ensure that every time the order of each element is consistent
    env.setParallelism(1)

    val t1 = env.fromCollection(data).toTable(tEnv).as('a, 'b, 'c)

    tEnv.registerTable("T1", t1)

    val sqlQuery = "SELECT " +
      "c, " +
      "count(a) OVER (PARTITION BY c ORDER BY ProcTime()  RANGE UNBOUNDED preceding) as cnt1, " +
      "sum(a) OVER (PARTITION BY c ORDER BY ProcTime() RANGE UNBOUNDED preceding) as cnt2 " +
      "from T1"

    val result = tEnv.sql(sqlQuery).toDataStream[Row]
    result.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList(
      "Hello World,1,7", "Hello World,2,15", "Hello World,3,35",
      "Hello,1,1", "Hello,2,3", "Hello,3,6", "Hello,4,10", "Hello,5,15", "Hello,6,21")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testUnboundPartitionedProcessingWindowWithRow(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStateBackend(getStateBackend)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()

    val t1 = env.fromCollection(data).toTable(tEnv).as('a, 'b, 'c)

    tEnv.registerTable("T1", t1)

    val sqlQuery = "SELECT " +
      "c, " +
      "count(a) OVER (PARTITION BY c ORDER BY ProcTime() ROWS BETWEEN UNBOUNDED preceding AND " +
      "CURRENT ROW)" +
      "from T1"

    val result = tEnv.sql(sqlQuery).toDataStream[Row]
    result.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList(
      "Hello World,1", "Hello World,2", "Hello World,3",
      "Hello,1", "Hello,2", "Hello,3", "Hello,4", "Hello,5", "Hello,6")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testUnboundNonPartitionedProcessingWindowWithRange(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStateBackend(getStateBackend)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()

    // for sum aggregation ensure that every time the order of each element is consistent
    env.setParallelism(1)

    val t1 = env.fromCollection(data).toTable(tEnv).as('a, 'b, 'c)

    tEnv.registerTable("T1", t1)

    val sqlQuery = "SELECT " +
      "c, " +
      "count(a) OVER (ORDER BY ProcTime()  RANGE UNBOUNDED preceding) as cnt1, " +
      "sum(a) OVER (ORDER BY ProcTime() RANGE UNBOUNDED preceding) as cnt2 " +
      "from T1"

    val result = tEnv.sql(sqlQuery).toDataStream[Row]
    result.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList(
      "Hello World,7,28", "Hello World,8,36", "Hello World,9,56",
      "Hello,1,1", "Hello,2,3", "Hello,3,6", "Hello,4,10", "Hello,5,15", "Hello,6,21")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testUnboundNonPartitionedProcessingWindowWithRow(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStateBackend(getStateBackend)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()

    val t1 = env.fromCollection(data).toTable(tEnv).as('a, 'b, 'c)

    tEnv.registerTable("T1", t1)

    val sqlQuery = "SELECT " +
      "count(a) OVER (ORDER BY ProcTime() ROWS BETWEEN UNBOUNDED preceding AND CURRENT ROW)" +
      "from T1"

    val result = tEnv.sql(sqlQuery).toDataStream[Row]
    result.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList("1", "2", "3", "4", "5", "6", "7", "8", "9")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  /**
    *  All aggregates must be computed on the same window.
    */
  @Test(expected = classOf[TableException])
  def testMultiWindow(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStateBackend(getStateBackend)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()

    val t1 = env.fromCollection(data).toTable(tEnv).as('a, 'b, 'c)

    tEnv.registerTable("T1", t1)

    val sqlQuery = "SELECT " +
      "c, " +
      "count(a) OVER (PARTITION BY c ORDER BY ProcTime() RANGE UNBOUNDED preceding) as cnt1, " +
      "sum(a) OVER (PARTITION BY b ORDER BY ProcTime() RANGE UNBOUNDED preceding) as cnt2 " +
      "from T1"

    val result = tEnv.sql(sqlQuery).toDataStream[Row]
    result.addSink(new StreamITCase.StringSink)
    env.execute()
  }
  
  /**
   * 
   * //////////////////////////////////////////////////////
   * START TESTING BOUNDED PROC TIME ROW AGGREGATIO
   * //////////////////////////////////////////////////////
   * 
   */
  
  @Test
  def testUnpartitionedMaxAggregatation(): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    env.setParallelism(1)
    StreamITCase.testResults = mutable.MutableList()

    val t = StreamTestData.get5TupleDataStream(env).toTable(tEnv).as('a, 'b, 'c, 'd, 'e)
    tEnv.registerTable("MyTable", t)

    val sqlQuery = "SELECT a, MAX(c) OVER (" +
      "ORDER BY procTime() ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) " +
      "AS sumC " +
      "FROM MyTable"

    val result = tEnv.sql(sqlQuery).toDataStream[Row]
    result.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList(
      "1,0",
      "2,1",
      "2,2",
      "3,3",
      "3,4",
      "3,5",
      "4,6",
      "4,7",
      "4,8",
      "4,9",
      "5,10",
      "5,11",
      "5,12",
      "5,13",
      "5,14")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testMinAggregatation(): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    env.setParallelism(1)
    StreamITCase.testResults = mutable.MutableList()

    val sqlQuery = "SELECT a, MIN(c)" +
      "OVER (PARTITION BY a ORDER BY procTime() ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) " +
      "AS avgC " +
      "FROM MyTable"

    val t = StreamTestData.get5TupleDataStream(env).toTable(tEnv).as('a, 'b, 'c, 'd, 'e)
    tEnv.registerTable("MyTable", t)

    val result = tEnv.sql(sqlQuery).toDataStream[Row]
    result.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList(
      "1,0",
      "2,1",
      "2,1",
      "3,3",
      "3,3",
      "3,3",
      "4,6",
      "4,6",
      "4,6",
      "4,7",
      "5,10",
      "5,10",
      "5,10",
      "5,11",
      "5,12")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testSumAggregatation(): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    env.setParallelism(1)
    StreamITCase.testResults = mutable.MutableList()

    val sqlQuery = "SELECT a, SUM(c) OVER " +
      "(PARTITION BY a ORDER BY procTime() ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) " +
      "AS sumC " +
      "FROM MyTable"

    val t = StreamTestData.get5TupleDataStream(env).toTable(tEnv).as('a, 'b, 'c, 'd, 'e)
    tEnv.registerTable("MyTable", t)

    val result = tEnv.sql(sqlQuery).toDataStream[Row]
    result.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList(
      "1,0",
      "2,1",
      "2,3",
      "3,3",
      "3,7",
      "3,12",
      "4,6",
      "4,13",
      "4,21",
      "4,24",
      "5,10",
      "5,21",
      "5,33",
      "5,36",
      "5,39")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testSumMinAggregatation(): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    env.setParallelism(1)
    StreamITCase.testResults = mutable.MutableList()

    val t = StreamTestData.get5TupleDataStream(env).toTable(tEnv).as('a, 'b, 'c, 'd, 'e)
    tEnv.registerTable("MyTable", t)

    val sqlQuery = "SELECT a,  " +
      " SUM(c) OVER (" +
      " PARTITION BY a ORDER BY procTime() ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS sumC , " +
      " MIN(c) OVER (" +
      " PARTITION BY a ORDER BY procTime() ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS minC " +
      " FROM MyTable"

    val result = tEnv.sql(sqlQuery).toDataStream[Row]
    result.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList(
      "1,0,0",
      "2,1,1",
      "2,3,1",
      "3,3,3",
      "3,7,3",
      "3,12,3",
      "4,6,6",
      "4,13,6",
      "4,21,6",
      "4,24,7",
      "5,10,10",
      "5,21,10",
      "5,33,10",
      "5,36,11",
      "5,39,12")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testSumMinUnpartitionedAggregatation(): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    env.setParallelism(1)
    StreamITCase.testResults = mutable.MutableList()

    val t = StreamTestData.get5TupleDataStream(env).toTable(tEnv).as('a, 'b, 'c, 'd, 'e)
    tEnv.registerTable("MyTable", t)

    val sqlQuery = "SELECT a,  " +
      " SUM(c) OVER (" +
      " ORDER BY procTime() ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS sumC , " +
      " MIN(c) OVER (" +
      " ORDER BY procTime() ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS minC " +
      " FROM MyTable"

    val result = tEnv.sql(sqlQuery).toDataStream[Row]
    result.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList(
      "1,0,0",
      "2,1,0",
      "2,3,0",
      "3,6,1",
      "3,9,2",
      "3,12,3",
      "4,15,4",
      "4,18,5",
      "4,21,6",
      "4,24,7",
      "5,27,8",
      "5,30,9",
      "5,33,10",
      "5,36,11",
      "5,39,12")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testMaxAvgUnpartitionedAggregatation(): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    env.setParallelism(1)
    StreamITCase.testResults = mutable.MutableList()

    val t = StreamTestData.get5TupleDataStream(env).toTable(tEnv).as('a, 'b, 'c, 'd, 'e)
    tEnv.registerTable("MyTable", t)

    val sqlQuery = "SELECT a,  " +
      " SUM(c) OVER (" +
      " ORDER BY procTime() ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS sumC , " +
      " MIN(c) OVER (" +
      " ORDER BY procTime() ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS minC " +
      " FROM MyTable"

    val result = tEnv.sql(sqlQuery).toDataStream[Row]
    result.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList(
      "1,0,0",
      "2,1,0",
      "2,3,0",
      "3,6,1",
      "3,9,2",
      "3,12,3",
      "4,15,4",
      "4,18,5",
      "4,21,6",
      "4,24,7",
      "5,27,8",
      "5,30,9",
      "5,33,10",
      "5,36,11",
      "5,39,12")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testMaxAvgSumUnpartitionedAggregatation(): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    env.setParallelism(1)
    StreamITCase.testResults = mutable.MutableList()

    val t = StreamTestData.get5TupleDataStream(env).toTable(tEnv).as('a, 'b, 'c, 'd, 'e)
    tEnv.registerTable("MyTable", t)

    val sqlQuery = "SELECT a,  " +
      " MAX(c) OVER (" +
      " ORDER BY procTime() ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS maxC , " +
      " AVG(c) OVER (" +
      " ORDER BY procTime() ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS avgC ," +
      " SUM(c) OVER (" +
      " ORDER BY procTime() ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS sumC " +
      " FROM MyTable"

    val result = tEnv.sql(sqlQuery).toDataStream[Row]
    result.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList(
        "1,0,0,0",
        "2,1,0,1",
        "2,2,1,3", 
        "3,3,2,6", 
        "3,4,3,9",
        "3,5,4,12",
        "4,6,5,15",
        "4,7,6,18",
        "4,8,7,21", 
        "4,9,8,24", 
        "5,10,9,27",
        "5,11,10,30",
        "5,12,11,33",
        "5,13,12,36",
        "5,14,13,39"
      )
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testMaxAvgSumPartitionedAggregatation(): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    env.setParallelism(1)
    StreamITCase.testResults = mutable.MutableList()

    val t = StreamTestData.get5TupleDataStream(env).toTable(tEnv).as('a, 'b, 'c, 'd, 'e)
    tEnv.registerTable("MyTable", t)

    val sqlQuery = "SELECT a,  " +
      " MAX(c) OVER (" +
      " PARTITION BY a ORDER BY procTime() ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS maxC , " +
      " AVG(c) OVER (" +
      " PARTITION BY a ORDER BY procTime() ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS avgC ," +
      " SUM(c) OVER (" +
      " PARTITION BY a ORDER BY procTime() ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS sumC " +
      " FROM MyTable"

    val result = tEnv.sql(sqlQuery).toDataStream[Row]
    result.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList(
      "1,0,0,0",
      "2,1,1,1",
      "2,2,1,3",
      "3,3,3,3",
      "3,4,3,7",
      "3,5,4,12",
      "4,6,6,6",
      "4,7,6,13",
      "4,8,7,21",
      "4,9,8,24",
      "5,10,10,10",
      "5,11,10,21",
      "5,12,11,33",
      "5,13,12,36",
      "5,14,13,39")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testSumUnpartitionedAggregatation(): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    env.setParallelism(1)
    StreamITCase.testResults = mutable.MutableList()

    val t = StreamTestData.get5TupleDataStream(env).toTable(tEnv).as('a, 'b, 'c, 'd, 'e)
    tEnv.registerTable("MyTable", t)

    val sqlQuery = "SELECT SUM(c) OVER " +
      "(ORDER BY procTime() ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) " +
      "AS sumC " +
      "FROM MyTable"

    val result = tEnv.sql(sqlQuery).toDataStream[Row]
    result.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList(
      "0",
      "1",
      "3",
      "6",
      "9",
      "12",
      "15",
      "18",
      "21",
      "24",
      "27",
      "30",
      "33",
      "36",
      "39")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testAvgAggregatation(): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    env.setParallelism(1)
    StreamITCase.testResults = mutable.MutableList()

    val sqlQuery = "SELECT a, AVG(c) OVER " +
      "(PARTITION BY a ORDER BY procTime() ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) " +
      "AS avgC, e " +
      "FROM MyTable"

    val t = StreamTestData.get5TupleDataStream(env).toTable(tEnv).as('a, 'b, 'c, 'd, 'e)
    tEnv.registerTable("MyTable", t)

    val result = tEnv.sql(sqlQuery).toDataStream[Row]
    result.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList(
      "1,0,1",
      "2,1,1",
      "2,1,2",
      "3,3,2",
      "3,3,2",
      "3,4,3",
      "4,6,2",
      "4,6,1",
      "4,7,1",
      "4,8,2",
      "5,10,1",
      "5,10,3",
      "5,11,3",
      "5,12,2",
      "5,13,2")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testAvgAggregatation2(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    env.setParallelism(1)
    StreamITCase.testResults = mutable.MutableList()

    val t = StreamTestData.get5TupleDataStream(env).toTable(tEnv).as('a, 'b, 'c, 'd, 'e)
    tEnv.registerTable("MyTable", t)

    val sqlQuery = "SELECT a, AVG(c) OVER " +
      "(PARTITION BY a ORDER BY procTime() ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) " +
      "AS avgC, d " +
      "FROM MyTable"

    val result = tEnv.sql(sqlQuery).toDataStream[Row]
    result.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList(
      "1,0,Hallo",
      "2,1,Hallo Welt",
      "2,1,Hallo Welt wie",
      "3,3,Hallo Welt wie gehts?",
      "3,3,ABC",
      "3,4,BCD",
      "4,6,CDE",
      "4,6,DEF",
      "4,7,EFG",
      "4,8,FGH",
      "5,10,GHI",
      "5,10,HIJ",
      "5,11,IJK",
      "5,12,JKL",
      "5,13,KLM")

    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

   /**
   * 
   * //////////////////////////////////////////////////////
   * END TESTING BOUNDED PROC TIME ROW AGGREGATIO
   * //////////////////////////////////////////////////////
   * 
   */
  
}
