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
package org.apache.flink.table.planner.runtime.stream.table

import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.api.config.ExecutionConfigOptions
import org.apache.flink.table.api.config.ExecutionConfigOptions.RowtimeInserter
import org.apache.flink.table.planner.factories.TestValuesTableFactory
import org.apache.flink.table.planner.factories.TestValuesTableFactory.changelogRow
import org.apache.flink.table.planner.runtime.utils.BatchTestBase.row
import org.apache.flink.table.planner.runtime.utils.StreamingTestBase
import org.apache.flink.table.planner.runtime.utils.TestData._
import org.apache.flink.table.utils.LegacyRowResource
import org.apache.flink.util.ExceptionUtils

import org.junit.{Rule, Test}
import org.junit.Assert.{assertEquals, assertFalse, assertTrue, fail}
import org.junit.rules.ExpectedException

import java.lang.{Long => JLong}
import java.math.{BigDecimal => JBigDecimal}
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConversions._
import scala.util.{Failure, Success, Try}

class TableSinkITCase extends StreamingTestBase {

  @Rule
  def usesLegacyRows: LegacyRowResource = LegacyRowResource.INSTANCE

  var _expectedEx: ExpectedException = ExpectedException.none

  @Rule
  def expectedEx: ExpectedException = _expectedEx

  @Test
  def testAppendSinkOnAppendTable(): Unit = {
    val t = env
      .fromCollection(tupleData3)
      .assignAscendingTimestamps(_._1.toLong)
      .toTable(tEnv, 'id, 'num, 'text, 'rowtime.rowtime)

    tEnv.executeSql(s"""
                       |CREATE TABLE appendSink (
                       |  `t` TIMESTAMP(3),
                       |  `icnt` BIGINT,
                       |  `nsum` BIGINT
                       |) WITH (
                       |  'connector' = 'values',
                       |  'sink-insert-only' = 'true'
                       |)
                       |""".stripMargin)

    val table = t
      .window(Tumble.over(5.millis).on('rowtime).as('w))
      .groupBy('w)
      .select('w.end.as('t), 'id.count.as('icnt), 'num.sum.as('nsum))
    table.executeInsert("appendSink").await()

    val result = TestValuesTableFactory.getResultsAsStrings("appendSink")
    val expected = List(
      "1970-01-01T00:00:00.005,4,8",
      "1970-01-01T00:00:00.010,5,18",
      "1970-01-01T00:00:00.015,5,24",
      "1970-01-01T00:00:00.020,5,29",
      "1970-01-01T00:00:00.025,2,12"
    )
    assertEquals(expected.sorted, result.sorted)
  }

  @Test
  def testInsertWithTargetColumnsAndSqlHint(): Unit = {
    val t = env
      .fromCollection(smallTupleData3)
      .toTable(tEnv, 'id, 'num, 'text)
    tEnv.createTemporaryView("src", t)

    tEnv.executeSql(s"""
                       |CREATE TABLE appendSink (
                       |  `t` INT,
                       |  `num` BIGINT,
                       |  `text` STRING
                       |) WITH (
                       |  'connector' = 'values',
                       |  'sink-insert-only' = 'true'
                       |)
                       |""".stripMargin)
    tEnv
      .executeSql(
        "INSERT INTO appendSink /*+ OPTIONS('sink.parallelism' = '1') */(t, num, text) SELECT id, num, text FROM src")
      .await()

    val result = TestValuesTableFactory.getResultsAsStrings("appendSink")
    val expected = List("1,1,Hi", "2,2,Hello", "3,2,Hello world")
    assertEquals(expected.sorted, result.sorted)
  }

  @Test
  def testAppendSinkWithNestedRow(): Unit = {
    val t = env
      .fromCollection(smallTupleData3)
      .toTable(tEnv, 'id, 'num, 'text)
    tEnv.createTemporaryView("src", t)

    tEnv.executeSql(s"""
                       |CREATE TABLE appendSink (
                       |  `t` INT,
                       |  `item` ROW<`num` BIGINT, `text` STRING>
                       |) WITH (
                       |  'connector' = 'values',
                       |  'sink-insert-only' = 'true'
                       |)
                       |""".stripMargin)
    tEnv.executeSql("INSERT INTO appendSink SELECT id, ROW(num, text) FROM src").await()

    val result = TestValuesTableFactory.getResultsAsStrings("appendSink")
    val expected = List("1,1,Hi", "2,2,Hello", "3,2,Hello world")
    assertEquals(expected.sorted, result.sorted)
  }

  @Test
  def testAppendSinkOnAppendTableForInnerJoin(): Unit = {
    val ds1 = env.fromCollection(smallTupleData3).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = env.fromCollection(tupleData5).toTable(tEnv, 'd, 'e, 'f, 'g, 'h)

    tEnv.executeSql(s"""
                       |CREATE TABLE appendSink (
                       |  `c` STRING,
                       |  `g` STRING
                       |) WITH (
                       |  'connector' = 'values',
                       |  'sink-insert-only' = 'true'
                       |)
                       |""".stripMargin)

    val table = ds1
      .join(ds2)
      .where('b === 'e)
      .select('c, 'g)
    table.executeInsert("appendSink").await()

    val result = TestValuesTableFactory.getResultsAsStrings("appendSink")
    val expected = List("Hi,Hallo", "Hello,Hallo Welt", "Hello world,Hallo Welt")
    assertEquals(expected.sorted, result.sorted)
  }

  @Test
  def testRetractSinkOnUpdatingTable(): Unit = {
    val t = env
      .fromCollection(tupleData3)
      .assignAscendingTimestamps(_._1.toLong)
      .toTable(tEnv, 'id, 'num, 'text)

    tEnv.executeSql(s"""
                       |CREATE TABLE retractSink (
                       |  `len` INT,
                       |  `icnt` BIGINT,
                       |  `nsum` BIGINT
                       |) WITH (
                       |  'connector' = 'values',
                       |  'sink-insert-only' = 'false'
                       |)
                       |""".stripMargin)

    val table = t
      .select('id, 'num, 'text.charLength().as('len))
      .groupBy('len)
      .select('len, 'id.count.as('icnt), 'num.sum.as('nsum))
    table.executeInsert("retractSink").await()

    val result = TestValuesTableFactory.getResultsAsStrings("retractSink")
    val expected = List("2,1,1", "5,1,2", "11,1,2", "25,1,3", "10,7,39", "14,1,3", "9,9,41")
    assertEquals(expected.sorted, result.sorted)

  }

  @Test
  def testRetractSinkOnAppendTable(): Unit = {
    val t = env
      .fromCollection(tupleData3)
      .assignAscendingTimestamps(_._1.toLong)
      .toTable(tEnv, 'id, 'num, 'text, 'rowtime.rowtime)

    tEnv.executeSql(s"""
                       |CREATE TABLE retractSink (
                       |  `t` TIMESTAMP(3),
                       |  `icnt` BIGINT,
                       |  `nsum` BIGINT
                       |) WITH (
                       |  'connector' = 'values',
                       |  'sink-insert-only' = 'false'
                       |)
                       |""".stripMargin)

    val table = t
      .window(Tumble.over(5.millis).on('rowtime).as('w))
      .groupBy('w)
      .select('w.end.as('t), 'id.count.as('icnt), 'num.sum.as('nsum))
    table.executeInsert("retractSink").await()

    val rawResult = TestValuesTableFactory.getRawResultsAsStrings("retractSink")
    assertFalse(
      "Received retraction messages for append only table",
      rawResult.exists(_.startsWith("-"))
    ) // maybe -U or -D

    val result = TestValuesTableFactory.getResultsAsStrings("retractSink")
    val expected = List(
      "1970-01-01T00:00:00.005,4,8",
      "1970-01-01T00:00:00.010,5,18",
      "1970-01-01T00:00:00.015,5,24",
      "1970-01-01T00:00:00.020,5,29",
      "1970-01-01T00:00:00.025,2,12"
    )
    assertEquals(expected.sorted, result.sorted)
  }

  @Test
  def testUpsertSinkOnNestedAggregation(): Unit = {
    val t = env
      .fromCollection(tupleData3)
      .assignAscendingTimestamps(_._1.toLong)
      .toTable(tEnv, 'id, 'num, 'text)

    tEnv.executeSql(s"""
                       |CREATE TABLE upsertSink (
                       |  `cnt` BIGINT,
                       |  `lencnt` BIGINT,
                       |  `cTrue` BOOLEAN,
                       |  PRIMARY KEY (cnt, cTrue) NOT ENFORCED
                       |) WITH (
                       |  'connector' = 'values',
                       |  'sink-insert-only' = 'false'
                       |)
                       |""".stripMargin)

    val table = t
      .select('id, 'num, 'text.charLength().as('len), ('id > 0).as('cTrue))
      .groupBy('len, 'cTrue)
      // test query field name is different with registered sink field name
      .select('len, 'id.count.as('count), 'cTrue)
      .groupBy('count, 'cTrue)
      .select('count, 'len.count.as('lencnt), 'cTrue)
    table.executeInsert("upsertSink").await()

    val rawResult = TestValuesTableFactory.getRawResultsAsStrings("upsertSink")
    assertTrue("Results must include delete messages", rawResult.exists(_.startsWith("-D(")))

    val result = TestValuesTableFactory.getResultsAsStrings("upsertSink")
    val expected = List("1,5,true", "7,1,true", "9,1,true")
    assertEquals(expected.sorted, result.sorted)
  }

  @Test
  def testUpsertSinkOnAppendingTable(): Unit = {
    val t = env
      .fromCollection(tupleData3)
      .assignAscendingTimestamps(_._1.toLong)
      .toTable(tEnv, 'id, 'num, 'text, 'rowtime.rowtime)

    tEnv.executeSql(s"""
                       |CREATE TABLE upsertSink (
                       |  `num` BIGINT,
                       |  `wend` TIMESTAMP(3),
                       |  `icnt` BIGINT,
                       |  PRIMARY KEY (num, wend, icnt) NOT ENFORCED
                       |) WITH (
                       |  'connector' = 'values',
                       |  'sink-insert-only' = 'false'
                       |)
                       |""".stripMargin)

    val table = t
      .window(Tumble.over(5.millis).on('rowtime).as('w))
      .groupBy('w, 'num)
      // test query field name is different with registered sink field name
      .select('num, 'w.end.as('window_end), 'id.count.as('icnt))
    table.executeInsert("upsertSink").await()

    val rawResult = TestValuesTableFactory.getRawResultsAsStrings("upsertSink")
    assertFalse(
      "Received retraction messages for append only table",
      rawResult.exists(_.startsWith("-"))
    ) // maybe -D or -U

    val result = TestValuesTableFactory.getResultsAsStrings("upsertSink")
    val expected = List(
      "1,1970-01-01T00:00:00.005,1",
      "2,1970-01-01T00:00:00.005,2",
      "3,1970-01-01T00:00:00.005,1",
      "3,1970-01-01T00:00:00.010,2",
      "4,1970-01-01T00:00:00.010,3",
      "4,1970-01-01T00:00:00.015,1",
      "5,1970-01-01T00:00:00.015,4",
      "5,1970-01-01T00:00:00.020,1",
      "6,1970-01-01T00:00:00.020,4",
      "6,1970-01-01T00:00:00.025,2"
    )
    assertEquals(expected.sorted, result.sorted)
  }

  @Test
  def testUpsertSinkOnAppendingTableWithoutFullKey1(): Unit = {
    val t = env
      .fromCollection(tupleData3)
      .assignAscendingTimestamps(_._1.toLong)
      .toTable(tEnv, 'id, 'num, 'text, 'rowtime.rowtime)

    tEnv.executeSql(s"""
                       |CREATE TABLE upsertSink (
                       |  `wend` TIMESTAMP(3),
                       |  `icnt` BIGINT,
                       |  PRIMARY KEY (wend, icnt) NOT ENFORCED
                       |) WITH (
                       |  'connector' = 'values',
                       |  'sink-insert-only' = 'false'
                       |)
                       |""".stripMargin)

    val table = t
      .window(Tumble.over(5.millis).on('rowtime).as('w))
      .groupBy('w, 'num)
      .select('w.end.as('wend), 'id.count.as('cnt))
    table.executeInsert("upsertSink").await()

    val rawResult = TestValuesTableFactory.getRawResultsAsStrings("upsertSink")
    assertFalse(
      "Received retraction messages for append only table",
      rawResult.exists(_.startsWith("-"))
    ) // may -D or -U

    val rawExpected = List(
      "+I(1970-01-01T00:00:00.005,1)",
      "+I(1970-01-01T00:00:00.005,2)",
      "+I(1970-01-01T00:00:00.005,1)",
      "+I(1970-01-01T00:00:00.010,2)",
      "+I(1970-01-01T00:00:00.010,3)",
      "+I(1970-01-01T00:00:00.015,1)",
      "+I(1970-01-01T00:00:00.015,4)",
      "+I(1970-01-01T00:00:00.020,1)",
      "+I(1970-01-01T00:00:00.020,4)",
      "+I(1970-01-01T00:00:00.025,2)"
    )
    assertEquals(rawExpected.sorted, rawResult.sorted)
  }

  @Test
  def testUpsertSinkOnAppendingTableWithoutFullKey2(): Unit = {
    val t = env
      .fromCollection(tupleData3)
      .assignAscendingTimestamps(_._1.toLong)
      .toTable(tEnv, 'id, 'num, 'text, 'rowtime.rowtime)

    tEnv.executeSql(s"""
                       |CREATE TABLE upsertSink (
                       |  `num` BIGINT,
                       |  `cnt` BIGINT,
                       |  PRIMARY KEY (num) NOT ENFORCED
                       |) WITH (
                       |  'connector' = 'values',
                       |  'sink-insert-only' = 'false'
                       |)
                       |""".stripMargin)

    val table = t
      .window(Tumble.over(5.millis).on('rowtime).as('w))
      .groupBy('w, 'num)
      .select('num, 'id.count.as('cnt))
    table.executeInsert("upsertSink").await()

    val rawResult = TestValuesTableFactory.getRawResultsAsStrings("upsertSink")
    assertFalse(
      "Received retraction messages for append only table",
      rawResult.exists(_.startsWith("-"))
    ) // may -D or -U

    val expected = List(
      "+I(1,1)",
      "+I(2,2)",
      "+I(3,1)",
      "+I(3,2)",
      "+I(4,3)",
      "+I(4,1)",
      "+I(5,4)",
      "+I(5,1)",
      "+I(6,4)",
      "+I(6,2)")
    assertEquals(expected.sorted, rawResult.sorted)
  }

  @Test
  def testUpsertSinkWithFilter(): Unit = {
    val t = env
      .fromCollection(tupleData3)
      .assignAscendingTimestamps(_._1.toLong)
      .toTable(tEnv, 'id, 'num, 'text)

    tEnv.executeSql(s"""
                       |CREATE TABLE upsertSink (
                       |  `num` BIGINT,
                       |  `cnt` BIGINT,
                       |  PRIMARY KEY (num) NOT ENFORCED
                       |) WITH (
                       |  'connector' = 'values',
                       |  'sink-insert-only' = 'false'
                       |)
                       |""".stripMargin)

    // num, cnt
    //   1, 1
    //   2, 2
    //   3, 3
    //   4, 4
    //   5, 5
    //   6, 6

    val table = t
      .groupBy('num)
      .select('num, 'id.count.as('cnt))
      .where('cnt <= 3)
    table.executeInsert("upsertSink").await()

    val result = TestValuesTableFactory.getResultsAsStrings("upsertSink")
    val expected = List("1,1", "2,2", "3,3")
    assertEquals(expected.sorted, result.sorted)
  }

  @Test
  def testMultiRowtimeWithRowtimeInserter(): Unit = {
    val t = env
      .fromCollection(tupleData3)
      .assignAscendingTimestamps(_._1.toLong)
      .toTable(tEnv, 'id, 'num, 'text, 'rowtime.rowtime)

    tEnv.executeSql(s"""
                       |CREATE TABLE sink (
                       |  `num` BIGINT,
                       |  `ts1` TIMESTAMP(3),
                       |  `ts2` TIMESTAMP(3)
                       |) WITH (
                       |  'connector' = 'values',
                       |  'sink-insert-only' = 'true'
                       |)
                       |""".stripMargin)

    val table = t
      .window(Tumble.over(5.milli).on('rowtime).as('w))
      .groupBy('num, 'w)
      .select('num, 'w.rowtime.as('rowtime1), 'w.rowtime.as('rowtime2))

    thrown.expect(classOf[TableException])
    thrown.expectMessage(
      "The query contains more than one rowtime attribute column [rowtime1, rowtime2] for " +
        "writing into table 'default_catalog.default_database.sink'.")
    table.executeInsert("sink")
  }

  @Test
  def testMultiRowtimeWithoutTimestampInserter(): Unit = {
    val t = env
      .fromCollection(tupleData3)
      .assignAscendingTimestamps(_._1.toLong)
      .toTable(tEnv, 'id, 'num, 'text, 'rowtime.rowtime)

    // disable inserter
    tEnv.getConfig.set(
      ExecutionConfigOptions.TABLE_EXEC_SINK_ROWTIME_INSERTER,
      RowtimeInserter.DISABLED)

    tEnv.executeSql(s"""
                       |CREATE TABLE sink (
                       |  `num` BIGINT,
                       |  `ts1` TIMESTAMP(3),
                       |  `ts2` TIMESTAMP(3)
                       |) WITH (
                       |  'connector' = 'values',
                       |  'sink-insert-only' = 'true'
                       |)
                       |""".stripMargin)

    val table = t
      .window(Tumble.over(5.milli).on('rowtime).as('w))
      .groupBy('num, 'w)
      .select('num, 'w.rowtime.as('rowtime1), 'w.rowtime.as('rowtime2))
    table.executeInsert("sink").await()

    val result = TestValuesTableFactory.getResultsAsStrings("sink")
    assertEquals(result.size(), 10)

    // clean up
    tEnv.getConfig
      .set(
        ExecutionConfigOptions.TABLE_EXEC_SINK_ROWTIME_INSERTER,
        ExecutionConfigOptions.TABLE_EXEC_SINK_ROWTIME_INSERTER.defaultValue())
  }

  @Test
  def testDecimalOnSinkFunctionTableSink(): Unit = {
    tEnv.executeSql(s"""
                       |CREATE TABLE sink (
                       |  `c` VARCHAR(5),
                       |  `b` DECIMAL(10, 0),
                       |  `d` CHAR(5)
                       |) WITH (
                       |  'connector' = 'values',
                       |  'sink-insert-only' = 'true'
                       |)
                       |""".stripMargin)

    val table = env
      .fromCollection(tupleData3)
      .toTable(tEnv, 'a, 'b, 'c)
      .where('a > 20)
      .select("12345", 55.cast(DataTypes.DECIMAL(10, 0)), "12345".cast(DataTypes.CHAR(5)))
    table.executeInsert("sink").await()

    val result = TestValuesTableFactory.getResultsAsStrings("sink")
    val expected = Seq("12345,55,12345")
    assertEquals(expected.sorted, result.sorted)
  }

  @Test
  def testDecimalOnOutputFormatTableSink(): Unit = {
    tEnv.executeSql(s"""
                       |CREATE TABLE sink (
                       |  `c` VARCHAR(5),
                       |  `b` DECIMAL(10, 0),
                       |  `d` CHAR(5)
                       |) WITH (
                       |  'connector' = 'values',
                       |  'sink-insert-only' = 'true',
                       |  'runtime-sink' = 'OutputFormat'
                       |)
                       |""".stripMargin)

    val table = env
      .fromCollection(tupleData3)
      .toTable(tEnv, 'a, 'b, 'c)
      .where('a > 20)
      .select("12345", 55.cast(DataTypes.DECIMAL(10, 0)), "12345".cast(DataTypes.CHAR(5)))
    table.executeInsert("sink").await()

    val result = TestValuesTableFactory.getResultsAsStrings("sink")
    val expected = Seq("12345,55,12345")
    assertEquals(expected.sorted, result.sorted)
  }

  /**
   * Writing changelog of an aggregation into a memory sink, and read it again as a changelog
   * source, and apply another aggregation, then verify the result.
   */
  @Test
  def testChangelogSourceAndChangelogSink(): Unit = {
    val orderData = List(
      rowOf(1L, "user1", new JBigDecimal("10.02")),
      rowOf(1L, "user2", new JBigDecimal("71.2")),
      rowOf(1L, "user1", new JBigDecimal("8.1")),
      rowOf(2L, "user3", new JBigDecimal("11.3")),
      rowOf(2L, "user4", new JBigDecimal("9.99")),
      rowOf(2L, "user1", new JBigDecimal("10")),
      rowOf(2L, "user3", new JBigDecimal("21.03"))
    )
    val dataId = TestValuesTableFactory.registerData(orderData)
    tEnv.executeSql(s"""
                       |CREATE TABLE orders (
                       |  product_id BIGINT,
                       |  user_name STRING,
                       |  order_price DECIMAL(18, 2)
                       |) WITH (
                       |  'connector' = 'values',
                       |  'data-id' = '$dataId'
                       |)
                       |""".stripMargin)
    tEnv.executeSql("""
                      |CREATE TABLE changelog_sink (
                      |  product_id BIGINT,
                      |  user_name STRING,
                      |  order_price DECIMAL(18, 2)
                      |) WITH (
                      |  'connector' = 'values',
                      |  'sink-insert-only' = 'false'
                      |)
                      |""".stripMargin)
    tEnv
      .executeSql("""
                    |INSERT INTO changelog_sink
                    |SELECT product_id, user_name, SUM(order_price)
                    |FROM orders
                    |GROUP BY product_id, user_name
                    |""".stripMargin)
      .await()

    val rawResult = TestValuesTableFactory.getRawResultsAsStrings("changelog_sink")
    val expected = List(
      "+I(1,user2,71.20)",
      "+I(1,user1,10.02)",
      "-U(1,user1,10.02)",
      "+U(1,user1,18.12)",
      "+I(2,user4,9.99)",
      "+I(2,user1,10.00)",
      "+I(2,user3,11.30)",
      "-U(2,user3,11.30)",
      "+U(2,user3,32.33)"
    )
    assertEquals(expected.sorted, rawResult.sorted)

    // register the changelog sink as a changelog source again
    val changelogData = expected.map {
      s =>
        val kindString = s.substring(0, 2)
        val fields = s.substring(3, s.length - 1).split(",")
        changelogRow(kindString, JLong.valueOf(fields(0)), fields(1), new JBigDecimal(fields(2)))
    }
    val dataId2 = TestValuesTableFactory.registerData(changelogData)
    tEnv.executeSql(s"""
                       |CREATE TABLE changelog_source (
                       |  product_id BIGINT,
                       |  user_name STRING,
                       |  price DECIMAL(18, 2)
                       |) WITH (
                       |  'connector' = 'values',
                       |  'data-id' = '$dataId2',
                       |  'changelog-mode' = 'I,UB,UA,D'
                       |)
                       |""".stripMargin)
    tEnv.executeSql("""
                      |CREATE TABLE final_sink (
                      |  user_name STRING,
                      |  total_pay DECIMAL(18, 2),
                      |  PRIMARY KEY (user_name) NOT ENFORCED
                      |) WITH (
                      |  'connector' = 'values',
                      |  'sink-insert-only' = 'false'
                      |)
                      |""".stripMargin)
    tEnv
      .executeSql("""
                    |INSERT INTO final_sink
                    |SELECT user_name, SUM(price) as total_pay
                    |FROM changelog_source
                    |GROUP BY user_name
                    |""".stripMargin)
      .await()
    val finalResult = TestValuesTableFactory.getResultsAsStrings("final_sink")
    val finalExpected = List("user1,28.12", "user2,71.20", "user3,32.33", "user4,9.99")
    assertEquals(finalExpected.sorted, finalResult.sorted)
  }

  @Test
  def testMetadataSourceAndSink(): Unit = {
    val dataId = TestValuesTableFactory.registerData(nullData4)
    // tests metadata at different locations and casting in both sources and sinks
    tEnv.executeSql(s"""
                       |CREATE TABLE MetadataSource (
                       |  category STRING,
                       |  shopId INT,
                       |  num BIGINT METADATA FROM 'metadata_1'
                       |) WITH (
                       |  'connector' = 'values',
                       |  'data-id' = '$dataId',
                       |  'readable-metadata' = 'metadata_1:INT'
                       |)
                       |""".stripMargin)
    tEnv.executeSql(
      s"""
         |CREATE TABLE MetadataSink (
         |  category STRING METADATA FROM 'metadata_1',
         |  shopId INT,
         |  metadata_3 BIGINT METADATA VIRTUAL,
         |  num STRING METADATA FROM 'metadata_2'
         |) WITH (
         |  'connector' = 'values',
         |  'readable-metadata' = 'metadata_1:STRING, metadata_2:INT, metadata_3:BIGINT',
         |  'writable-metadata' = 'metadata_1:STRING, metadata_2:INT'
         |)
         |""".stripMargin)

    tEnv
      .executeSql(s"""
                     |INSERT INTO MetadataSink
                     |SELECT category, shopId, CAST(num AS STRING)
                     |FROM MetadataSource
                     |""".stripMargin)
      .await()

    val result = TestValuesTableFactory.getResultsAsStrings("MetadataSink")
    val expected =
      List("1,book,12", "2,book,null", "3,fruit,44", "4,book,11", "4,fruit,null", "5,fruit,null")
    assertEquals(expected.sorted, result.sorted)
  }

  @Test
  def testParallelismWithSinkFunction(): Unit = {
    val negativeParallelism = -1
    val validParallelism = 1
    val index = new AtomicInteger(1)

    Try(
      innerTestSetParallelism(
        "SinkFunction",
        negativeParallelism,
        index = index.getAndIncrement)) match {
      case Success(_) => fail("this should not happen")
      case Failure(t) =>
        val exception =
          ExceptionUtils.findThrowableWithMessage(t, s"Invalid configured parallelism")
        assertTrue(exception.isPresent)
    }

    assertTrue(
      Try(
        innerTestSetParallelism(
          "SinkFunction",
          validParallelism,
          index = index.getAndIncrement)).isSuccess)
  }

  @Test
  def testParallelismOnChangelogMode(): Unit = {
    val dataId = TestValuesTableFactory.registerData(data1)
    val sourceTableName = s"test_para_source"
    val sinkTableWithoutPkName = s"test_para_sink_without_pk"
    val sinkTableWithPkName = s"test_para_sink_with_pk"
    val sinkParallelism = 2

    tEnv.executeSql(s"""
                       |CREATE TABLE $sourceTableName (
                       |  the_month INT,
                       |  area STRING,
                       |  product INT
                       |) WITH (
                       |  'connector' = 'values',
                       |  'data-id' = '$dataId',
                       |  'bounded' = 'true'
                       |)
                       |""".stripMargin)
    tEnv.executeSql(s"""
                       |CREATE TABLE $sinkTableWithoutPkName (
                       |  the_month INT,
                       |  area STRING,
                       |  product INT
                       |) WITH (
                       |  'connector' = 'values',
                       |  'runtime-sink' = 'SinkFunction',
                       |  'sink.parallelism' = '$sinkParallelism',
                       |  'sink-changelog-mode-enforced' = 'I,D'
                       |)
                       |""".stripMargin)
    // source is insert only, it never produce a delete, so we do not require a pk for the sink
    Try(
      tEnv
        .executeSql(s"INSERT INTO $sinkTableWithoutPkName SELECT * FROM $sourceTableName")
        .await()).isSuccess

    tEnv.executeSql(s"""
                       |CREATE TABLE $sinkTableWithPkName (
                       |  the_month INT,
                       |  area STRING,
                       |  product INT,
                       |  PRIMARY KEY (area) NOT ENFORCED
                       |) WITH (
                       |  'connector' = 'values',
                       |  'runtime-sink' = 'SinkFunction',
                       |  'sink.parallelism' = '$sinkParallelism',
                       |  'sink-changelog-mode-enforced' = 'I,D'
                       |)
                       |""".stripMargin)

    assertTrue(
      Try(
        tEnv
          .executeSql(s"INSERT INTO $sinkTableWithPkName SELECT * FROM $sourceTableName")
          .await()).isSuccess)

  }

  @Test
  def testPartialInsert(): Unit = {
    tEnv.executeSql(s"""
                       |CREATE TABLE testSink (
                       |  `a` INT,
                       |  `b` DOUBLE
                       |)
                       |WITH (
                       |  'connector' = 'values',
                       |  'sink-insert-only' = 'false'
                       |)
                       |""".stripMargin)

    val t = env.fromCollection(tupleData2).toTable(tEnv, 'x, 'y)
    tEnv.createTemporaryView("MyTable", t)

    tEnv
      .executeSql(s"""
                     |INSERT INTO testSink (b)
                     |SELECT sum(y) FROM MyTable GROUP BY x
                     |""".stripMargin)
      .await()
    val expected = List("null,0.1", "null,0.4", "null,1.0", "null,2.2", "null,3.9")
    val result = TestValuesTableFactory.getResultsAsStrings("testSink")
    assertEquals(expected.sorted, result.sorted)
  }

  @Test
  def testPartialInsertWithPartitionAndComputedColumn(): Unit = {
    tEnv.executeSql(s"""
                       |CREATE TABLE testSink (
                       |  `a` INT,
                       |  `b` AS `a` + 1,
                       |  `c` STRING,
                       |  `d` INT,
                       |  `e` DOUBLE
                       |)
                       |PARTITIONED BY (`c`, `d`)
                       |WITH (
                       |  'connector' = 'values',
                       |  'sink-insert-only' = 'false'
                       |)
                       |""".stripMargin)

    val t = env.fromCollection(tupleData2).toTable(tEnv, 'x, 'y)
    tEnv.createTemporaryView("MyTable", t)

    tEnv
      .executeSql(s"""
                     |INSERT INTO testSink PARTITION(`c`='2021', `d`=1) (e)
                     |SELECT sum(y) FROM MyTable GROUP BY x
                     |""".stripMargin)
      .await()
    val expected = List(
      "null,2021,1,0.1",
      "null,2021,1,0.4",
      "null,2021,1,1.0",
      "null,2021,1,2.2",
      "null,2021,1,3.9")
    val result = TestValuesTableFactory.getResultsAsStrings("testSink")
    assertEquals(expected.sorted, result.sorted)
  }

  @Test
  def testFullInsertWithPartitionAndComputedColumn(): Unit = {
    tEnv.executeSql(s"""
                       |CREATE TABLE testSink (
                       |  `a` INT,
                       |  `b` AS `a` + 1,
                       |  `c` STRING,
                       |  `d` INT,
                       |  `e` DOUBLE
                       |)
                       |PARTITIONED BY (`c`, `d`)
                       |WITH (
                       |  'connector' = 'values',
                       |  'sink-insert-only' = 'false'
                       |)
                       |""".stripMargin)

    val t = env.fromCollection(tupleData2).toTable(tEnv, 'x, 'y)
    tEnv.createTemporaryView("MyTable", t)

    tEnv
      .executeSql(s"""
                     |INSERT INTO testSink PARTITION(`c`='2021', `d`=1) (a, e)
                     |SELECT x, sum(y) FROM MyTable GROUP BY x
                     |""".stripMargin)
      .await()
    val expected =
      List("1,2021,1,0.1", "2,2021,1,0.4", "3,2021,1,1.0", "4,2021,1,2.2", "5,2021,1,3.9")
    val result = TestValuesTableFactory.getResultsAsStrings("testSink")
    assertEquals(expected.sorted, result.sorted)
  }

  @Test
  def testPartialInsertWithDynamicPartitionAndComputedColumn1(): Unit = {
    tEnv.executeSql(s"""
                       |CREATE TABLE testSink (
                       |  `a` INT,
                       |  `b` AS `a` + 1,
                       |  `c` STRING,
                       |  `d` INT,
                       |  `e` DOUBLE
                       |)
                       |PARTITIONED BY (`c`, `d`)
                       |WITH (
                       |  'connector' = 'values',
                       |  'sink-insert-only' = 'false'
                       |)
                       |""".stripMargin)

    val t = env.fromCollection(tupleData2).toTable(tEnv, 'x, 'y)
    tEnv.createTemporaryView("MyTable", t)

    tEnv
      .executeSql(s"""
                     |INSERT INTO testSink (e)
                     |SELECT sum(y) FROM MyTable GROUP BY x
                     |""".stripMargin)
      .await()
    val expected = List(
      "null,null,null,0.1",
      "null,null,null,0.4",
      "null,null,null,1.0",
      "null,null,null,2.2",
      "null,null,null,3.9")
    val result = TestValuesTableFactory.getResultsAsStrings("testSink")
    assertEquals(expected.sorted, result.sorted)
  }

  @Test
  def testPartialInsertWithComplexReorderAndComputedColumn(): Unit = {
    tEnv.executeSql(s"""
                       |CREATE TABLE testSink (
                       |  `a` INT,
                       |  `b` AS `a` + 1,
                       |  `c` STRING,
                       |  `c1` STRING,
                       |  `c2` STRING,
                       |  `c3` BIGINT,
                       |  `d` INT,
                       |  `e` DOUBLE
                       |)
                       |PARTITIONED BY (`c`, `d`)
                       |WITH (
                       |  'connector' = 'values',
                       |  'sink-insert-only' = 'false'
                       |)
                       |""".stripMargin)

    val t = env.fromCollection(tupleData2).toTable(tEnv, 'x, 'y)
    tEnv.createTemporaryView("MyTable", t)

    tEnv
      .executeSql(s"""
                     |INSERT INTO testSink (a,c2,e,c,c1,c3,d)
                     |SELECT 1,'c2',sum(y),'c','c1',33333,12 FROM MyTable GROUP BY x
                     |""".stripMargin)
      .await()
    val expected = List(
      "1,c,c1,c2,33333,12,0.1",
      "1,c,c1,c2,33333,12,0.4",
      "1,c,c1,c2,33333,12,1.0",
      "1,c,c1,c2,33333,12,2.2",
      "1,c,c1,c2,33333,12,3.9")
    val result = TestValuesTableFactory.getResultsAsStrings("testSink")
    assertEquals(expected.sorted, result.sorted)
  }

  @Test
  def testPartialInsertWithComplexReorder(): Unit = {
    tEnv.executeSql(s"""
                       |CREATE TABLE testSink (
                       |  `a` INT,
                       |  `c` STRING,
                       |  `c1` STRING,
                       |  `c2` STRING,
                       |  `c3` BIGINT,
                       |  `d` INT,
                       |  `e` DOUBLE
                       |)
                       |WITH (
                       |  'connector' = 'values',
                       |  'sink-insert-only' = 'false'
                       |)
                       |""".stripMargin)

    val t = env.fromCollection(tupleData2).toTable(tEnv, 'x, 'y)
    tEnv.createTemporaryView("MyTable", t)

    tEnv
      .executeSql(s"""
                     |INSERT INTO testSink (a,c2,e,c,c1,c3,d)
                     |SELECT 1,'c2',sum(y),'c','c1',33333,12 FROM MyTable GROUP BY x
                     |""".stripMargin)
      .await()
    val expected = List(
      "1,c,c1,c2,33333,12,0.1",
      "1,c,c1,c2,33333,12,0.4",
      "1,c,c1,c2,33333,12,1.0",
      "1,c,c1,c2,33333,12,2.2",
      "1,c,c1,c2,33333,12,3.9")
    val result = TestValuesTableFactory.getResultsAsStrings("testSink")
    assertEquals(expected.sorted, result.sorted)
  }

  @Test
  def testPartialInsertWithDynamicPartitionAndComputedColumn2(): Unit = {
    tEnv.executeSql(s"""
                       |CREATE TABLE testSink (
                       |  `a` INT,
                       |  `b` AS `a` + 1,
                       |  `c` STRING,
                       |  `d` INT,
                       |  `e` DOUBLE
                       |)
                       |PARTITIONED BY (`c`, `d`)
                       |WITH (
                       |  'connector' = 'values',
                       |  'sink-insert-only' = 'false'
                       |)
                       |""".stripMargin)

    val t = env.fromCollection(tupleData2).toTable(tEnv, 'x, 'y)
    tEnv.createTemporaryView("MyTable", t)

    tEnv
      .executeSql(s"""
                     |INSERT INTO testSink (c, d, e)
                     |SELECT '2021', 1, sum(y) FROM MyTable GROUP BY x
                     |""".stripMargin)
      .await()
    val expected = List(
      "null,2021,1,0.1",
      "null,2021,1,0.4",
      "null,2021,1,1.0",
      "null,2021,1,2.2",
      "null,2021,1,3.9")
    val result = TestValuesTableFactory.getResultsAsStrings("testSink")
    assertEquals(expected.sorted, result.sorted)
  }

  @Test
  def testPartialInsertWithReorder(): Unit = {
    tEnv.executeSql(s"""
                       |CREATE TABLE testSink (
                       |  `a` INT,
                       |  `b` AS `a` + 1,
                       |  `c` STRING,
                       |  `d` INT,
                       |  `e` DOUBLE
                       |)
                       |PARTITIONED BY (`c`, `d`)
                       |WITH (
                       |  'connector' = 'values',
                       |  'sink-insert-only' = 'false'
                       |)
                       |""".stripMargin)

    val t = env.fromCollection(tupleData2).toTable(tEnv, 'x, 'y)
    tEnv.createTemporaryView("MyTable", t)

    tEnv
      .executeSql(s"""
                     |-- the target columns is reordered (compare with the columns of sink)
                     |INSERT INTO testSink (e, d, c)
                     |SELECT sum(y), 1, '2021' FROM MyTable GROUP BY x
                     |""".stripMargin)
      .await()
    val expected = List(
      "null,2021,1,0.1",
      "null,2021,1,0.4",
      "null,2021,1,1.0",
      "null,2021,1,2.2",
      "null,2021,1,3.9")
    val result = TestValuesTableFactory.getResultsAsStrings("testSink")
    assertEquals(expected.sorted, result.sorted)
  }

  @Test
  def testPartialInsertWithDynamicAndStaticPartition1(): Unit = {
    tEnv.executeSql(s"""
                       |CREATE TABLE testSink (
                       |  `a` INT,
                       |  `b` AS `a` + 1,
                       |  `c` STRING,
                       |  `d` INT,
                       |  `e` DOUBLE
                       |)
                       |PARTITIONED BY (`c`, `d`)
                       |WITH (
                       |  'connector' = 'values',
                       |  'sink-insert-only' = 'false'
                       |)
                       |""".stripMargin)

    val t = env.fromCollection(tupleData2).toTable(tEnv, 'x, 'y)
    tEnv.createTemporaryView("MyTable", t)

    tEnv
      .executeSql(s"""
                     |INSERT INTO testSink PARTITION(`c`='2021') (d, e)
                     |SELECT 1, sum(y) FROM MyTable GROUP BY x
                     |""".stripMargin)
      .await()
    val expected = List(
      "null,2021,1,0.1",
      "null,2021,1,0.4",
      "null,2021,1,1.0",
      "null,2021,1,2.2",
      "null,2021,1,3.9")
    val result = TestValuesTableFactory.getResultsAsStrings("testSink")
    assertEquals(expected.sorted, result.sorted)
  }

  @Test
  def testPartialInsertWithDynamicAndStaticPartition2(): Unit = {
    tEnv.executeSql(s"""
                       |CREATE TABLE testSink (
                       |  `a` INT,
                       |  `b` AS `a` + 1,
                       |  `c` STRING,
                       |  `d` INT,
                       |  `e` DOUBLE
                       |)
                       |PARTITIONED BY (`c`, `d`)
                       |WITH (
                       |  'connector' = 'values',
                       |  'sink-insert-only' = 'false'
                       |)
                       |""".stripMargin)

    val t = env.fromCollection(tupleData2).toTable(tEnv, 'x, 'y)
    tEnv.createTemporaryView("MyTable", t)

    tEnv
      .executeSql(s"""
                     |INSERT INTO testSink PARTITION(`c`='2021') (e)
                     |SELECT sum(y) FROM MyTable GROUP BY x
                     |""".stripMargin)
      .await()
    val expected = List(
      "null,2021,null,0.1",
      "null,2021,null,0.4",
      "null,2021,null,1.0",
      "null,2021,null,2.2",
      "null,2021,null,3.9")
    val result = TestValuesTableFactory.getResultsAsStrings("testSink")
    assertEquals(expected.sorted, result.sorted)
  }

  @Test
  def testPartialInsertWithDynamicAndStaticPartition3(): Unit = {
    tEnv.executeSql(s"""
                       |CREATE TABLE testSink (
                       |  `a` INT,
                       |  `b` AS `a` + 1,
                       |  `c` STRING,
                       |  `d` INT,
                       |  `e` DOUBLE
                       |)
                       |PARTITIONED BY (`c`, `d`)
                       |WITH (
                       |  'connector' = 'values',
                       |  'sink-insert-only' = 'false'
                       |)
                       |""".stripMargin)

    val t = env.fromCollection(tupleData2).toTable(tEnv, 'x, 'y)
    tEnv.createTemporaryView("MyTable", t)

    expectedEx.expect(classOf[ValidationException])
    expectedEx.expectMessage("Target column 'e' is assigned more than once")

    tEnv
      .executeSql(s"""
                     |INSERT INTO testSink PARTITION(`c`='2021') (e, e)
                     |SELECT 1, sum(y) FROM MyTable GROUP BY x
                     |""".stripMargin)
      .await()
  }

  private def innerTestSetParallelism(provider: String, parallelism: Int, index: Int): Unit = {
    val dataId = TestValuesTableFactory.registerData(data1)
    val sourceTableName = s"test_para_source_${provider.toLowerCase.trim}_$index"
    val sinkTableName = s"test_para_sink_${provider.toLowerCase.trim}_$index"
    tEnv.executeSql(s"""
                       |CREATE TABLE $sourceTableName (
                       |  the_month INT,
                       |  area STRING,
                       |  product INT
                       |) WITH (
                       |  'connector' = 'values',
                       |  'data-id' = '$dataId',
                       |  'bounded' = 'true'
                       |)
                       |""".stripMargin)
    tEnv.executeSql(s"""
                       |CREATE TABLE $sinkTableName (
                       |  the_month INT,
                       |  area STRING,
                       |  product INT
                       |) WITH (
                       |  'connector' = 'values',
                       |  'sink-insert-only' = 'true',
                       |  'runtime-sink' = '$provider',
                       |  'sink.parallelism' = '$parallelism'
                       |)
                       |""".stripMargin)
    tEnv.executeSql(s"INSERT INTO $sinkTableName SELECT * FROM $sourceTableName").await()
  }

  @Test
  def testExecuteInsertToTableDescriptor(): Unit = {
    val sourceDescriptor = TableDescriptor
      .forConnector("values")
      .schema(
        Schema
          .newBuilder()
          .column("f0", DataTypes.INT())
          .build())
      .option("bounded", "true")
      .build()

    // Explicit schema

    TestValuesTableFactory.clearAllData()
    val tableId1 = TestValuesTableFactory.registerData(Seq(row(42)))
    tEnv.createTemporaryTable("T1", sourceDescriptor.toBuilder.option("data-id", tableId1).build)

    tEnv
      .from("T1")
      .executeInsert(
        TableDescriptor
          .forConnector("values")
          .schema(
            Schema
              .newBuilder()
              .column("f0", DataTypes.INT())
              .build())
          .build())
      .await()
    assertEquals(Seq("+I(42)"), TestValuesTableFactory.getOnlyRawResultsAsStrings.toList)

    // Derived schema

    TestValuesTableFactory.clearAllData()
    val tableId2 = TestValuesTableFactory.registerData(Seq(row(42)))
    tEnv.createTemporaryTable("T2", sourceDescriptor.toBuilder.option("data-id", tableId2).build)

    tEnv
      .from("T2")
      .executeInsert(TableDescriptor.forConnector("values").build())
      .await()
    assertEquals(Seq("+I(42)"), TestValuesTableFactory.getOnlyRawResultsAsStrings.toList)

    // Enriched schema

    TestValuesTableFactory.clearAllData()
    val tableId3 = TestValuesTableFactory.registerData(Seq(row(42)))
    tEnv.createTemporaryTable("T3", sourceDescriptor.toBuilder.option("data-id", tableId3).build)

    tEnv
      .from("T3")
      .executeInsert(
        TableDescriptor
          .forConnector("values")
          .option("writable-metadata", "m1:INT")
          .schema(
            Schema
              .newBuilder()
              .columnByMetadata("m1", DataTypes.INT(), true)
              .primaryKey("f0")
              .build())
          .build())
      .await()
    assertEquals(Seq("+I(42)"), TestValuesTableFactory.getOnlyRawResultsAsStrings.toList)
    TestValuesTableFactory.clearAllData()
  }

  @Test
  def testStatementSetInsertUsingTableDescriptor(): Unit = {
    val schema = Schema
      .newBuilder()
      .column("f0", DataTypes.INT())
      .build()

    val sourceDescriptor = TableDescriptor
      .forConnector("values")
      .schema(schema)
      .option("bounded", "true")
      .build()

    // Explicit schema

    TestValuesTableFactory.clearAllData()
    val tableId1 = TestValuesTableFactory.registerData(Seq(row(42)))
    tEnv.createTemporaryTable("T1", sourceDescriptor.toBuilder.option("data-id", tableId1).build)

    tEnv
      .createStatementSet()
      .addInsert(
        TableDescriptor
          .forConnector("values")
          .schema(schema)
          .build(),
        tEnv.from("T1")
      )
      .execute()
      .await()
    assertEquals(Seq("+I(42)"), TestValuesTableFactory.getOnlyRawResultsAsStrings.toList)

    // Derived schema

    TestValuesTableFactory.clearAllData()
    val tableId2 = TestValuesTableFactory.registerData(Seq(row(42)))
    tEnv.createTemporaryTable("T2", sourceDescriptor.toBuilder.option("data-id", tableId2).build)

    tEnv
      .createStatementSet()
      .addInsert(
        TableDescriptor.forConnector("values").build(),
        tEnv.from("T2")
      )
      .execute()
      .await()
    assertEquals(Seq("+I(42)"), TestValuesTableFactory.getOnlyRawResultsAsStrings.toList)

    // Enriched schema

    TestValuesTableFactory.clearAllData()
    val tableId3 = TestValuesTableFactory.registerData(Seq(row(42)))
    tEnv.createTemporaryTable("T3", sourceDescriptor.toBuilder.option("data-id", tableId3).build)

    tEnv
      .createStatementSet()
      .addInsert(
        TableDescriptor
          .forConnector("values")
          .option("writable-metadata", "m1:INT")
          .schema(
            Schema
              .newBuilder()
              .columnByMetadata("m1", DataTypes.INT(), true)
              .primaryKey("f0")
              .build())
          .build(),
        tEnv.from("T3")
      )
      .execute()
      .await()
    assertEquals(Seq("+I(42)"), TestValuesTableFactory.getOnlyRawResultsAsStrings.toList)
  }

  @Test
  def testAppendStreamToSinkWithoutPkForceKeyBy(): Unit = {
    val t = env
      .fromCollection(tupleData3)
      .assignAscendingTimestamps(_._1.toLong)
      .toTable(tEnv, 'id, 'num, 'text, 'rowtime.rowtime)
    tEnv.getConfig.set(
      ExecutionConfigOptions.TABLE_EXEC_SINK_KEYED_SHUFFLE,
      ExecutionConfigOptions.SinkKeyedShuffle.FORCE)

    tEnv.executeSql(s"""
                       |CREATE TABLE sink (
                       |  `t` TIMESTAMP(3),
                       |  `icnt` BIGINT,
                       |  `nsum` BIGINT
                       |) WITH (
                       |  'connector' = 'values',
                       |  'sink-insert-only' = 'false',
                       |  'sink.parallelism' = '4'
                       |)
                       |""".stripMargin)

    val table = t
      .window(Tumble.over(5.millis).on('rowtime).as('w))
      .groupBy('w)
      .select('w.end.as('t), 'id.count.as('icnt), 'num.sum.as('nsum))
    table.executeInsert("sink").await()

    val result = TestValuesTableFactory.getResultsAsStrings("sink")
    val expected = List(
      "1970-01-01T00:00:00.005,4,8",
      "1970-01-01T00:00:00.010,5,18",
      "1970-01-01T00:00:00.015,5,24",
      "1970-01-01T00:00:00.020,5,29",
      "1970-01-01T00:00:00.025,2,12"
    )
    assertEquals(expected.sorted, result.sorted)
  }
}
