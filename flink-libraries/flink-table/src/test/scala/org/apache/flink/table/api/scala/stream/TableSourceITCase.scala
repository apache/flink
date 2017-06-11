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

package org.apache.flink.table.api.scala.stream

import java.lang.{Integer => JInt, Long => JLong}

import java.sql.Timestamp
import org.apache.flink.table.api.java.utils.Pojos._
import org.apache.flink.table.api.scala.stream.TableSources._
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.table.api.scala.stream.utils.StreamITCase
import org.apache.flink.table.api.scala._
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.util.StreamingMultipleProgramsTestBase
import org.apache.flink.table.api.{TableEnvironment, Types}
import org.apache.flink.table.utils.{CommonTestData, TestFilterableTableSource}
import org.apache.flink.types.Row
import org.junit.Assert._
import org.junit.Test

import scala.collection.mutable

class TableSourceITCase extends StreamingMultipleProgramsTestBase {

  @Test
  def testCsvTableSourceSQL(): Unit = {

    val csvTable = CommonTestData.getCsvTableSource

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()

    tEnv.registerTableSource("persons", csvTable)

    tEnv.sql(
      "SELECT id, `first`, `last`, score FROM persons WHERE id < 4 ")
      .toAppendStream[Row]
      .addSink(new StreamITCase.StringSink[Row])

    env.execute()

    val expected = mutable.MutableList(
      "1,Mike,Smith,12.3",
      "2,Bob,Taylor,45.6",
      "3,Sam,Miller,7.89")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testCsvTableSourceTableAPI(): Unit = {

    val csvTable = CommonTestData.getCsvTableSource
    StreamITCase.testResults = mutable.MutableList()

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)

    tEnv.registerTableSource("csvTable", csvTable)
    tEnv.scan("csvTable")
      .where('id > 4)
      .select('last, 'score * 2)
      .toAppendStream[Row]
      .addSink(new StreamITCase.StringSink[Row])

    env.execute()

    val expected = mutable.MutableList(
      "Williams,69.0",
      "Miller,13.56",
      "Smith,180.2",
      "Williams,4.68")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testCsvTableSourceWithFilterable(): Unit = {
    StreamITCase.testResults = mutable.MutableList()
    val tableName = "MyTable"
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    tEnv.registerTableSource(tableName, new TestFilterableTableSource)
    tEnv.scan(tableName)
      .where("amount > 4 && price < 9")
      .select("id, name")
      .addSink(new StreamITCase.StringSink[Row])

    env.execute()

    val expected = mutable.MutableList(
      "5,Record_5", "6,Record_6", "7,Record_7", "8,Record_8")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testRowTypeGroupWindowWithTimestampColumn(): Unit = {
    val datas: List[Row] = List(
      Row.of(JLong.valueOf(1L), JInt.valueOf(1), "Hi"),
      Row.of(JLong.valueOf(2L), JInt.valueOf(2), "Hello"),
      Row.of(JLong.valueOf(4L), JInt.valueOf(2), "Hello"),
      Row.of(JLong.valueOf(8L), JInt.valueOf(3), "Hello world"),
      Row.of(JLong.valueOf(16L), JInt.valueOf(3), "Hello world"))

   val typeInfo = new RowTypeInfo(
      Array(Types.LONG, Types.INT, Types.STRING)
      .asInstanceOf[Array[TypeInformation[_]]],
      Array("ts", "id", "name"))

    StreamITCase.testResults = mutable.MutableList()
    val tableName = "MyTable"
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = TableEnvironment.getTableEnvironment(env)

    tEnv.registerTableSource(tableName, new StreamTableSource0(datas, typeInfo,"ts", 0L))
    val querySql =
      "SELECT " +
        "TUMBLE_START(rowtime, INTERVAL '3' SECOND) as winStart," +
        "TUMBLE_END(rowtime, INTERVAL '3' SECOND) as winEnd," +
        "COUNT(1) as cnt, name " +
        "FROM MyTable GROUP BY name, " +
        "TUMBLE(rowtime, INTERVAL '3' SECOND)"

    val table = tEnv.sql(querySql)

    table.toAppendStream[Row].addSink(new StreamITCase.StringSink[Row])

    env.execute()

    val expected = mutable.MutableList(
      "1970-01-01 00:00:00.0,1970-01-01 00:00:03.0,1,Hi",
      "1970-01-01 00:00:00.0,1970-01-01 00:00:03.0,2,Hello",
      "1970-01-01 00:00:00.0,1970-01-01 00:00:03.0,2,Hello world")

    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testPojoTypeGroupWindowWithTimestampColumnSQL(): Unit = {
    val datas: List[Row] = List(
      Row.of(JLong.valueOf(1L), JInt.valueOf(1), "Hi"),
      Row.of(JLong.valueOf(2L), JInt.valueOf(2), "Hello"),
      Row.of(JLong.valueOf(4L), JInt.valueOf(2), "Hello"),
      Row.of(JLong.valueOf(8L), JInt.valueOf(3), "Hello world"),
      Row.of(JLong.valueOf(16L), JInt.valueOf(3), "Hello world"))

   val typeInfo = new RowTypeInfo(
      Array(Types.LONG, Types.INT, Types.STRING)
      .asInstanceOf[Array[TypeInformation[_]]],
      Array("ts", "id", "name"))

    StreamITCase.testResults = mutable.MutableList()
    val tableName = "MyTable"
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = TableEnvironment.getTableEnvironment(env)

    tEnv.registerTableSource(tableName, new StreamTableSource0(datas, typeInfo,"ts", 0L))
    val querySql =
      "SELECT " +
        "TUMBLE_START(rowtime, INTERVAL '3' SECOND) as winStart," +
        "TUMBLE_END(rowtime, INTERVAL '3' SECOND) as winEnd," +
        "COUNT(1) as cnt, name " +
        "FROM MyTable GROUP BY name, " +
        "TUMBLE(rowtime, INTERVAL '3' SECOND)"

    val table = tEnv.sql(querySql)

    table.toAppendStream[Pojo0].addSink(new StreamITCase.StringSink[Pojo0])

    env.execute()

    val expected = mutable.MutableList(
      "Pojo0{winStart=1970-01-01 00:00:00.0, winEnd=1970-01-01 00:00:03.0, name='Hello world', " +
      "cnt=2}",
      "Pojo0{winStart=1970-01-01 00:00:00.0, winEnd=1970-01-01 00:00:03.0, name='Hello', cnt=2}",
      "Pojo0{winStart=1970-01-01 00:00:00.0, winEnd=1970-01-01 00:00:03.0, name='Hi', cnt=1}")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testPojoTypeGroupWindowWithTimestampColumn(): Unit = {
    val datas: List[Row] = List(
      Row.of(JLong.valueOf(1L), JInt.valueOf(1), "Hi"),
      Row.of(JLong.valueOf(2L), JInt.valueOf(2), "Hello"),
      Row.of(JLong.valueOf(4L), JInt.valueOf(2), "Hello"),
      Row.of(JLong.valueOf(8L), JInt.valueOf(3), "Hello world"),
      Row.of(JLong.valueOf(16L), JInt.valueOf(3), "Hello world"))

   val typeInfo = new RowTypeInfo(
      Array(Types.LONG, Types.INT, Types.STRING)
      .asInstanceOf[Array[TypeInformation[_]]],
      Array("ts", "id", "name"))

    StreamITCase.testResults = mutable.MutableList()
    val tableName = "MyTable"
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = TableEnvironment.getTableEnvironment(env)

    tEnv.registerTableSource(tableName, new StreamTableSource0(datas, typeInfo,"ts", 0L))

    val table = tEnv.scan(tableName)
      .window(Tumble over 3.seconds on 'rowtime as 'w)
      .groupBy('w, 'name)
      .select('w.start as 'winStart, 'w.end as 'winEnd, 'ts.count as 'cnt, 'name)

    table.toAppendStream[Pojo0].addSink(new StreamITCase.StringSink[Pojo0])

    env.execute()

    val expected = mutable.MutableList(
      "Pojo0{winStart=1970-01-01 00:00:00.0, winEnd=1970-01-01 00:00:03.0, name='Hello world', " +
      "cnt=2}",
      "Pojo0{winStart=1970-01-01 00:00:00.0, winEnd=1970-01-01 00:00:03.0, name='Hello', cnt=2}",
      "Pojo0{winStart=1970-01-01 00:00:00.0, winEnd=1970-01-01 00:00:03.0, name='Hi', cnt=1}")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testRowTypeOverWindowWithTimestampColumn(): Unit = {
    val datas: List[Row] = List(
      Row.of(JLong.valueOf(1L), Timestamp.valueOf("2017-06-11 12:12:12.999"), "Hi"),
      Row.of(JLong.valueOf(2L), Timestamp.valueOf("2017-06-11 12:12:12.999"), "Hello"),
      Row.of(JLong.valueOf(4L), Timestamp.valueOf("2017-06-11 12:12:12.999"), "Hello"),
      Row.of(JLong.valueOf(8L), Timestamp.valueOf("2017-06-11 12:12:12.999"), "Hello world"),
      Row.of(JLong.valueOf(16L), Timestamp.valueOf("2017-06-11 12:12:12.999"), "Hello world"))

   val typeInfo = new RowTypeInfo(
      Array(Types.LONG, Types.SQL_TIMESTAMP, Types.STRING)
      .asInstanceOf[Array[TypeInformation[_]]],
      Array("ts", "id", "name"))

    StreamITCase.testResults = mutable.MutableList()
    val tableName = "MyTable"
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = TableEnvironment.getTableEnvironment(env)

    tEnv.registerTableSource(tableName, new StreamTableSource0(datas, typeInfo,"ts", 0L))
    val sqlQuery = "SELECT " +
      "id, " +
      "name, " +
      "count(ts) OVER (PARTITION BY name ORDER BY rowtime RANGE UNBOUNDED preceding) as myCnt, " +
      "sum(ts) OVER (PARTITION BY name ORDER BY rowtime RANGE UNBOUNDED preceding) as mySum " +
      "from MyTable"

    val table = tEnv.sql(sqlQuery)

    table.toAppendStream[Row].addSink(new StreamITCase.StringSink[Row])

    env.execute()

    val expected = mutable.MutableList(
      "2017-06-11 12:12:12.999,Hello world,1,8",
      "2017-06-11 12:12:12.999,Hello world,2,24",
      "2017-06-11 12:12:12.999,Hello,1,2",
      "2017-06-11 12:12:12.999,Hello,2,6",
      "2017-06-11 12:12:12.999,Hi,1,1")

    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testPojoTypeOverWindowWithTimestampColumnSQL(): Unit = {
    val datas: List[Row] = List(
      Row.of(JLong.valueOf(1L), Timestamp.valueOf("2017-06-11 12:12:12.999"), "Hi"),
      Row.of(JLong.valueOf(2L), Timestamp.valueOf("2017-06-11 12:12:12.999"), "Hello"),
      Row.of(JLong.valueOf(4L), Timestamp.valueOf("2017-06-11 12:12:12.999"), "Hello"),
      Row.of(JLong.valueOf(8L), Timestamp.valueOf("2017-06-11 12:12:12.999"), "Hello world"),
      Row.of(JLong.valueOf(16L), Timestamp.valueOf("2017-06-11 12:12:12.999"), "Hello world"))

   val typeInfo = new RowTypeInfo(
      Array(Types.LONG, Types.SQL_TIMESTAMP, Types.STRING)
      .asInstanceOf[Array[TypeInformation[_]]],
      Array("ts", "id", "name"))

    StreamITCase.testResults = mutable.MutableList()
    val tableName = "MyTable"
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = TableEnvironment.getTableEnvironment(env)

    tEnv.registerTableSource(tableName, new StreamTableSource0(datas, typeInfo,"ts", 0L))
    val sqlQuery = "SELECT " +
      "id, " +
      "name, " +
      "count(ts) OVER (PARTITION BY name ORDER BY rowtime RANGE UNBOUNDED preceding) as myCnt, " +
      "sum(ts) OVER (PARTITION BY name ORDER BY rowtime RANGE UNBOUNDED preceding) as mySum " +
      "from MyTable"

    val table = tEnv.sql(sqlQuery)

    table.toAppendStream[Pojo1].addSink(new StreamITCase.StringSink[Pojo1])

    env.execute()

    val expected = mutable.MutableList(
      "Pojo1{id=2017-06-11 12:12:12.999, name='Hello world', myCnt=1, mySum=8}",
      "Pojo1{id=2017-06-11 12:12:12.999, name='Hello world', myCnt=2, mySum=24}",
      "Pojo1{id=2017-06-11 12:12:12.999, name='Hello', myCnt=1, mySum=2}",
      "Pojo1{id=2017-06-11 12:12:12.999, name='Hello', myCnt=2, mySum=6}",
      "Pojo1{id=2017-06-11 12:12:12.999, name='Hi', myCnt=1, mySum=1}")

    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testPojoTypeOverWindowWithTimestampColumn(): Unit = {
    val datas: List[Row] = List(
      Row.of(JLong.valueOf(1L), Timestamp.valueOf("2017-06-11 12:12:12.999"), "Hi"),
      Row.of(JLong.valueOf(2L), Timestamp.valueOf("2017-06-11 12:12:12.999"), "Hello"),
      Row.of(JLong.valueOf(4L), Timestamp.valueOf("2017-06-11 12:12:12.999"), "Hello"),
      Row.of(JLong.valueOf(8L), Timestamp.valueOf("2017-06-11 12:12:12.999"), "Hello world"),
      Row.of(JLong.valueOf(16L), Timestamp.valueOf("2017-06-11 12:12:12.999"), "Hello world"))

   val typeInfo = new RowTypeInfo(
      Array(Types.LONG, Types.SQL_TIMESTAMP, Types.STRING)
      .asInstanceOf[Array[TypeInformation[_]]],
      Array("ts", "id", "name"))

    StreamITCase.testResults = mutable.MutableList()
    val tableName = "MyTable"
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = TableEnvironment.getTableEnvironment(env)

    tEnv.registerTableSource(tableName, new StreamTableSource0(datas, typeInfo,"ts", 0L))

    val windowedTable = tEnv.scan(tableName)
      .window(Over partitionBy 'name orderBy 'rowtime preceding UNBOUNDED_RANGE as 'w)
      .select('id, 'name, 'ts.count over 'w as 'myCnt, 'ts.sum over 'w as 'mySum)

    windowedTable.toAppendStream[Pojo1].addSink(new StreamITCase.StringSink[Pojo1])

    env.execute()

    val expected = mutable.MutableList(
      "Pojo1{id=2017-06-11 12:12:12.999, name='Hello world', myCnt=1, mySum=8}",
      "Pojo1{id=2017-06-11 12:12:12.999, name='Hello world', myCnt=2, mySum=24}",
      "Pojo1{id=2017-06-11 12:12:12.999, name='Hello', myCnt=1, mySum=2}",
      "Pojo1{id=2017-06-11 12:12:12.999, name='Hello', myCnt=2, mySum=6}",
      "Pojo1{id=2017-06-11 12:12:12.999, name='Hi', myCnt=1, mySum=1}")

    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }
}
