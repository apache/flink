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
import org.apache.flink.table.planner.factories.TestValuesTableFactory
import org.apache.flink.table.planner.factories.TestValuesTableFactory.changelogRow
import org.apache.flink.table.planner.runtime.utils.StreamingTestBase
import org.apache.flink.table.planner.runtime.utils.TestData.{data1, nullData4, smallTupleData3, tupleData3, tupleData5}
import org.apache.flink.table.utils.LegacyRowResource
import org.apache.flink.util.ExceptionUtils

import org.junit.Assert.{assertEquals, assertFalse, assertTrue, fail}
import org.junit.{Rule, Test}

import java.lang.{Long => JLong}
import java.math.{BigDecimal => JBigDecimal}
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConversions._
import scala.util.{Failure, Success, Try}

class TableSinkITCase extends StreamingTestBase {

  @Rule
  def usesLegacyRows: LegacyRowResource = LegacyRowResource.INSTANCE

  @Test
  def testAppendSinkOnAppendTable(): Unit = {
    val t = env.fromCollection(tupleData3)
      .assignAscendingTimestamps(_._1.toLong)
      .toTable(tEnv, 'id, 'num, 'text, 'rowtime.rowtime)

    tEnv.executeSql(
      s"""
         |CREATE TABLE appendSink (
         |  `t` TIMESTAMP(3),
         |  `icnt` BIGINT,
         |  `nsum` BIGINT
         |) WITH (
         |  'connector' = 'values',
         |  'sink-insert-only' = 'true'
         |)
         |""".stripMargin)

    val table = t.window(Tumble over 5.millis on 'rowtime as 'w)
      .groupBy('w)
      .select('w.end as 't, 'id.count as 'icnt, 'num.sum as 'nsum)
    table.executeInsert("appendSink").await()

    val result = TestValuesTableFactory.getResults("appendSink")
    val expected = List(
      "1970-01-01T00:00:00.005,4,8",
      "1970-01-01T00:00:00.010,5,18",
      "1970-01-01T00:00:00.015,5,24",
      "1970-01-01T00:00:00.020,5,29",
      "1970-01-01T00:00:00.025,2,12")
    assertEquals(expected.sorted, result.sorted)
  }

  @Test
  def testAppendSinkWithNestedRow(): Unit = {
    val t = env.fromCollection(smallTupleData3)
      .toTable(tEnv, 'id, 'num, 'text)
    tEnv.createTemporaryView("src", t)

    tEnv.executeSql(
      s"""
         |CREATE TABLE appendSink (
         |  `t` INT,
         |  `item` ROW<`num` BIGINT, `text` STRING>
         |) WITH (
         |  'connector' = 'values',
         |  'sink-insert-only' = 'true'
         |)
         |""".stripMargin)
    tEnv.executeSql("INSERT INTO appendSink SELECT id, ROW(num, text) FROM src").await()

    val result = TestValuesTableFactory.getResults("appendSink")
    val expected = List(
      "1,1,Hi",
      "2,2,Hello",
      "3,2,Hello world")
    assertEquals(expected.sorted, result.sorted)
  }

  @Test
  def testAppendSinkOnAppendTableForInnerJoin(): Unit = {
    val ds1 = env.fromCollection(smallTupleData3).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = env.fromCollection(tupleData5).toTable(tEnv, 'd, 'e, 'f, 'g, 'h)

    tEnv.executeSql(
      s"""
         |CREATE TABLE appendSink (
         |  `c` STRING,
         |  `g` STRING
         |) WITH (
         |  'connector' = 'values',
         |  'sink-insert-only' = 'true'
         |)
         |""".stripMargin)

    val table = ds1.join(ds2).where('b === 'e)
      .select('c, 'g)
   table.executeInsert("appendSink").await()

    val result = TestValuesTableFactory.getResults("appendSink")
    val expected = List("Hi,Hallo", "Hello,Hallo Welt", "Hello world,Hallo Welt")
    assertEquals(expected.sorted, result.sorted)
  }

  @Test
  def testRetractSinkOnUpdatingTable(): Unit = {
    val t = env.fromCollection(tupleData3)
      .assignAscendingTimestamps(_._1.toLong)
      .toTable(tEnv, 'id, 'num, 'text)

    tEnv.executeSql(
      s"""
         |CREATE TABLE retractSink (
         |  `len` INT,
         |  `icnt` BIGINT,
         |  `nsum` BIGINT
         |) WITH (
         |  'connector' = 'values',
         |  'sink-insert-only' = 'false'
         |)
         |""".stripMargin)

    val table = t.select('id, 'num, 'text.charLength() as 'len)
      .groupBy('len)
      .select('len, 'id.count as 'icnt, 'num.sum as 'nsum)
    table.executeInsert("retractSink").await()

    val result = TestValuesTableFactory.getResults("retractSink")
    val expected = List(
      "2,1,1", "5,1,2", "11,1,2",
      "25,1,3", "10,7,39", "14,1,3", "9,9,41")
    assertEquals(expected.sorted, result.sorted)

  }

  @Test
  def testRetractSinkOnAppendTable(): Unit = {
    val t = env.fromCollection(tupleData3)
      .assignAscendingTimestamps(_._1.toLong)
      .toTable(tEnv, 'id, 'num, 'text, 'rowtime.rowtime)

    tEnv.executeSql(
      s"""
         |CREATE TABLE retractSink (
         |  `t` TIMESTAMP(3),
         |  `icnt` BIGINT,
         |  `nsum` BIGINT
         |) WITH (
         |  'connector' = 'values',
         |  'sink-insert-only' = 'false'
         |)
         |""".stripMargin)

    val table = t.window(Tumble over 5.millis on 'rowtime as 'w)
      .groupBy('w)
      .select('w.end as 't, 'id.count as 'icnt, 'num.sum as 'nsum)
    table.executeInsert("retractSink").await()

    val rawResult = TestValuesTableFactory.getRawResults("retractSink")
    assertFalse(
      "Received retraction messages for append only table",
      rawResult.exists(_.startsWith("-"))) // maybe -U or -D

    val result = TestValuesTableFactory.getResults("retractSink")
    val expected = List(
      "1970-01-01T00:00:00.005,4,8",
      "1970-01-01T00:00:00.010,5,18",
      "1970-01-01T00:00:00.015,5,24",
      "1970-01-01T00:00:00.020,5,29",
      "1970-01-01T00:00:00.025,2,12")
    assertEquals(expected.sorted, result.sorted)
  }

  @Test
  def testUpsertSinkOnNestedAggregation(): Unit = {
    val t = env.fromCollection(tupleData3)
      .assignAscendingTimestamps(_._1.toLong)
      .toTable(tEnv, 'id, 'num, 'text)

    tEnv.executeSql(
      s"""
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

    val table = t.select('id, 'num, 'text.charLength() as 'len, ('id > 0) as 'cTrue)
      .groupBy('len, 'cTrue)
      // test query field name is different with registered sink field name
      .select('len, 'id.count as 'count, 'cTrue)
      .groupBy('count, 'cTrue)
      .select('count, 'len.count as 'lencnt, 'cTrue)
    table.executeInsert("upsertSink").await()

    val rawResult = TestValuesTableFactory.getRawResults("upsertSink")
    assertTrue(
      "Results must include delete messages",
      rawResult.exists(_.startsWith("-D(")))

    val result = TestValuesTableFactory.getResults("upsertSink")
    val expected = List("1,5,true", "7,1,true", "9,1,true")
    assertEquals(expected.sorted, result.sorted)
  }

  @Test
  def testUpsertSinkOnAppendingTable(): Unit = {
    val t = env.fromCollection(tupleData3)
      .assignAscendingTimestamps(_._1.toLong)
      .toTable(tEnv, 'id, 'num, 'text, 'rowtime.rowtime)

    tEnv.executeSql(
      s"""
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

    val table = t.window(Tumble over 5.millis on 'rowtime as 'w)
      .groupBy('w, 'num)
      // test query field name is different with registered sink field name
      .select('num, 'w.end as 'window_end, 'id.count as 'icnt)
    table.executeInsert("upsertSink").await()

    val rawResult = TestValuesTableFactory.getRawResults("upsertSink")
    assertFalse(
      "Received retraction messages for append only table",
      rawResult.exists(_.startsWith("-"))) // maybe -D or -U

    val result = TestValuesTableFactory.getResults("upsertSink")
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
      "6,1970-01-01T00:00:00.025,2")
    assertEquals(expected.sorted, result.sorted)
  }

  @Test
  def testUpsertSinkOnAppendingTableWithoutFullKey1(): Unit = {
    val t = env.fromCollection(tupleData3)
      .assignAscendingTimestamps(_._1.toLong)
      .toTable(tEnv, 'id, 'num, 'text, 'rowtime.rowtime)

    tEnv.executeSql(
      s"""
         |CREATE TABLE upsertSink (
         |  `wend` TIMESTAMP(3),
         |  `icnt` BIGINT,
         |  PRIMARY KEY (wend, icnt) NOT ENFORCED
         |) WITH (
         |  'connector' = 'values',
         |  'sink-insert-only' = 'false'
         |)
         |""".stripMargin)

    val table = t.window(Tumble over 5.millis on 'rowtime as 'w)
      .groupBy('w, 'num)
      .select('w.end as 'wend, 'id.count as 'cnt)
    table.executeInsert("upsertSink").await()

    val rawResult = TestValuesTableFactory.getRawResults("upsertSink")
    assertFalse(
      "Received retraction messages for append only table",
      rawResult.exists(_.startsWith("-"))) // may -D or -U

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
      "+I(1970-01-01T00:00:00.025,2)")
    assertEquals(rawExpected.sorted, rawResult.sorted)
  }

  @Test
  def testUpsertSinkOnAppendingTableWithoutFullKey2(): Unit = {
    val t = env.fromCollection(tupleData3)
      .assignAscendingTimestamps(_._1.toLong)
      .toTable(tEnv, 'id, 'num, 'text, 'rowtime.rowtime)

    tEnv.executeSql(
      s"""
         |CREATE TABLE upsertSink (
         |  `num` BIGINT,
         |  `cnt` BIGINT,
         |  PRIMARY KEY (num) NOT ENFORCED
         |) WITH (
         |  'connector' = 'values',
         |  'sink-insert-only' = 'false'
         |)
         |""".stripMargin)

    val table = t.window(Tumble over 5.millis on 'rowtime as 'w)
      .groupBy('w, 'num)
      .select('num, 'id.count as 'cnt)
    table.executeInsert("upsertSink").await()

    val rawResult = TestValuesTableFactory.getRawResults("upsertSink")
    assertFalse(
      "Received retraction messages for append only table",
      rawResult.exists(_.startsWith("-"))) // may -D or -U

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
    val t = env.fromCollection(tupleData3)
      .assignAscendingTimestamps(_._1.toLong)
      .toTable(tEnv, 'id, 'num, 'text)

    tEnv.executeSql(
      s"""
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

    val table = t.groupBy('num)
      .select('num, 'id.count as 'cnt)
      .where('cnt <= 3)
    table.executeInsert("upsertSink").await()

    val result = TestValuesTableFactory.getResults("upsertSink")
    val expected = List("1,1", "2,2", "3,3")
    assertEquals(expected.sorted, result.sorted)
  }

  @Test
  def testMultiRowtime(): Unit = {
    val t = env.fromCollection(tupleData3)
      .assignAscendingTimestamps(_._1.toLong)
      .toTable(tEnv, 'id, 'num, 'text, 'rowtime.rowtime)

    tEnv.executeSql(
      s"""
         |CREATE TABLE sink (
         |  `num` BIGINT,
         |  `ts1` TIMESTAMP(3),
         |  `ts2` TIMESTAMP(3)
         |) WITH (
         |  'connector' = 'values',
         |  'sink-insert-only' = 'true'
         |)
         |""".stripMargin)

    val table = t.window(Tumble over 5.milli on 'rowtime as 'w)
      .groupBy('num, 'w)
      .select('num, 'w.rowtime as 'rowtime1, 'w.rowtime as 'rowtime2)

    thrown.expect(classOf[TableException])
    thrown.expectMessage("Found more than one rowtime field: [rowtime1, rowtime2] " +
      "in the query when insert into 'default_catalog.default_database.sink'")
    table.executeInsert("sink")
  }

  @Test
  def testDecimalOnSinkFunctionTableSink(): Unit = {
    tEnv.executeSql(
      s"""
         |CREATE TABLE sink (
         |  `c` VARCHAR(5),
         |  `b` DECIMAL(10, 0),
         |  `d` CHAR(5)
         |) WITH (
         |  'connector' = 'values',
         |  'sink-insert-only' = 'true'
         |)
         |""".stripMargin)

    val table = env.fromCollection(tupleData3)
      .toTable(tEnv, 'a, 'b, 'c)
      .where('a > 20)
      .select("12345", 55.cast(DataTypes.DECIMAL(10, 0)), "12345".cast(DataTypes.CHAR(5)))
    table.executeInsert("sink").await()

    val result = TestValuesTableFactory.getResults("sink")
    val expected = Seq("12345,55,12345")
    assertEquals(expected.sorted, result.sorted)
  }

  @Test
  def testDecimalOnOutputFormatTableSink(): Unit = {
    tEnv.executeSql(
      s"""
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

    val table = env.fromCollection(tupleData3)
      .toTable(tEnv, 'a, 'b, 'c)
      .where('a > 20)
      .select("12345", 55.cast(DataTypes.DECIMAL(10, 0)), "12345".cast(DataTypes.CHAR(5)))
    table.executeInsert("sink").await()

    val result = TestValuesTableFactory.getResults("sink")
    val expected = Seq("12345,55,12345")
    assertEquals(expected.sorted, result.sorted)
  }

  /**
   * Writing changelog of an aggregation into a memory sink, and read it again as a
   * changelog source, and apply another aggregation, then verify the result.
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
      rowOf(2L, "user3", new JBigDecimal("21.03")))
    val dataId = TestValuesTableFactory.registerData(orderData)
    tEnv.executeSql(
      s"""
         |CREATE TABLE orders (
         |  product_id BIGINT,
         |  user_name STRING,
         |  order_price DECIMAL(18, 2)
         |) WITH (
         |  'connector' = 'values',
         |  'data-id' = '$dataId'
         |)
         |""".stripMargin)
    tEnv.executeSql(
      """
        |CREATE TABLE changelog_sink (
        |  product_id BIGINT,
        |  user_name STRING,
        |  order_price DECIMAL(18, 2)
        |) WITH (
        |  'connector' = 'values',
        |  'sink-insert-only' = 'false'
        |)
        |""".stripMargin)
    tEnv.executeSql(
      """
        |INSERT INTO changelog_sink
        |SELECT product_id, user_name, SUM(order_price)
        |FROM orders
        |GROUP BY product_id, user_name
        |""".stripMargin).await()

    val rawResult = TestValuesTableFactory.getRawResults("changelog_sink")
    val expected = List(
      "+I(1,user2,71.20)",
      "+I(1,user1,10.02)",
      "-U(1,user1,10.02)",
      "+U(1,user1,18.12)",
      "+I(2,user4,9.99)",
      "+I(2,user1,10.00)",
      "+I(2,user3,11.30)",
      "-U(2,user3,11.30)",
      "+U(2,user3,32.33)")
    assertEquals(expected.sorted, rawResult.sorted)

    // register the changelog sink as a changelog source again
    val changelogData = expected.map { s =>
      val kindString = s.substring(0, 2)
      val fields = s.substring(3, s.length - 1).split(",")
      changelogRow(kindString, JLong.valueOf(fields(0)), fields(1), new JBigDecimal(fields(2)))
    }
    val dataId2 = TestValuesTableFactory.registerData(changelogData)
    tEnv.executeSql(
      s"""
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
    tEnv.executeSql(
      """
        |CREATE TABLE final_sink (
        |  user_name STRING,
        |  total_pay DECIMAL(18, 2),
        |  PRIMARY KEY (user_name) NOT ENFORCED
        |) WITH (
        |  'connector' = 'values',
        |  'sink-insert-only' = 'false'
        |)
        |""".stripMargin)
    tEnv.executeSql(
      """
        |INSERT INTO final_sink
        |SELECT user_name, SUM(price) as total_pay
        |FROM changelog_source
        |GROUP BY user_name
        |""".stripMargin).await()
    val finalResult = TestValuesTableFactory.getResults("final_sink")
    val finalExpected = List(
      "user1,28.12", "user2,71.20", "user3,32.33", "user4,9.99")
    assertEquals(finalExpected.sorted, finalResult.sorted)
  }

  @Test
  def testNotNullEnforcer(): Unit = {
    val dataId = TestValuesTableFactory.registerData(nullData4)
    tEnv.executeSql(
      s"""
         |CREATE TABLE nullable_src (
         |  category STRING,
         |  shopId INT,
         |  num INT
         |) WITH (
         |  'connector' = 'values',
         |  'data-id' = '$dataId'
         |)
         |""".stripMargin)
    tEnv.executeSql(
      s"""
         |CREATE TABLE not_null_sink (
         |  category STRING,
         |  shopId INT,
         |  num INT NOT NULL
         |) WITH (
         |  'connector' = 'values',
         |  'sink-insert-only' = 'true'
         |)
         |""".stripMargin)

    // default should fail, because there are null values in the source
    try {
      tEnv.executeSql("INSERT INTO not_null_sink SELECT * FROM nullable_src").await()
      fail("Execution should fail.")
    } catch {
      case t: Throwable =>
        val exception = ExceptionUtils.findThrowableWithMessage(
          t,
          "Column 'num' is NOT NULL, however, a null value is being written into it. " +
            "You can set job configuration 'table.exec.sink.not-null-enforcer'='drop' " +
            "to suppress this exception and drop such records silently.")
        assertTrue(exception.isPresent)
    }

    // enable drop enforcer to make the query can run
    tEnv.getConfig.getConfiguration.setString("table.exec.sink.not-null-enforcer", "drop")
    tEnv.executeSql("INSERT INTO not_null_sink SELECT * FROM nullable_src").await()

    val result = TestValuesTableFactory.getResults("not_null_sink")
    val expected = List("book,1,12", "book,4,11", "fruit,3,44")
    assertEquals(expected.sorted, result.sorted)
  }

  @Test
  def testMetadataSourceAndSink(): Unit = {
    val dataId = TestValuesTableFactory.registerData(nullData4)
    // tests metadata at different locations and casting in both sources and sinks
    tEnv.executeSql(
      s"""
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
      .executeSql(
        s"""
           |INSERT INTO MetadataSink
           |SELECT category, shopId, CAST(num AS STRING)
           |FROM MetadataSource
           |""".stripMargin)
      .await()

    val result = TestValuesTableFactory.getResults("MetadataSink")
    val expected =
      List("1,book,12", "2,book,null", "3,fruit,44", "4,book,11", "4,fruit,null", "5,fruit,null")
    assertEquals(expected.sorted, result.sorted)
  }


  @Test
  def testParallelismWithDataStream(): Unit = {
    Try(innerTestSetParallelism(
      "DataStreamWithParallelism",
      1,
      index = 1)) match {
      case Success(_) => fail("this should not happen")
      case Failure(t) => {
        val exception = ExceptionUtils.findThrowableWithMessage(
          t,
          "`DataStreamSinkProvider` is not allowed to work with `ParallelismProvider`," +
            " please see document of `ParallelismProvider`")
        assertTrue(exception.isPresent)
      }
    }
  }

  @Test
  def testParallelismWithSinkFunction(): Unit = {
    val negativeParallelism = -1
    val validParallelism = 1
    val oversizedParallelism = Int.MaxValue
    val index = new AtomicInteger(1)

    Try(innerTestSetParallelism(
      "SinkFunction",
      oversizedParallelism,
      index = index.getAndIncrement))
    match {
      case Success(_) => fail("this should not happen")
      case Failure(t) => {
        val exception = ExceptionUtils.findThrowableWithMessage(
          t,
          s"Failed to wait job finish")
        assertTrue(exception.isPresent)
      }
    }

    Try(innerTestSetParallelism(
      "SinkFunction",
      negativeParallelism,
      index = index.getAndIncrement))
    match {
      case Success(_) => fail("this should not happen")
      case Failure(t) => {
        val exception = ExceptionUtils.findThrowableWithMessage(
          t,
          s"should not be less than zero or equal to zero")
        assertTrue(exception.isPresent)
      }
    }

    assertTrue(Try(innerTestSetParallelism(
      "SinkFunction",
      validParallelism,
      index = index.getAndIncrement)).isSuccess)
  }

  @Test
  def testParallelismWithOutputFormat(): Unit = {
    val negativeParallelism = -1
    val oversizedParallelism = Int.MaxValue
    val validParallelism = 1
    val index = new AtomicInteger(1)

    Try(innerTestSetParallelism(
      "OutputFormat",
      negativeParallelism,
      index = index.getAndIncrement))
    match {
      case Success(_) => fail("this should not happen")
      case Failure(t) => {
        val exception = ExceptionUtils.findThrowableWithMessage(
          t,
          s"should not be less than zero or equal to zero")
        assertTrue(exception.isPresent)
      }
    }

    Try(innerTestSetParallelism(
      "SinkFunction",
      oversizedParallelism,
      index = index.getAndIncrement))
    match {
      case Success(_) => fail("this should not happen")
      case Failure(t) => {
        val exception = ExceptionUtils.findThrowableWithMessage(
          t,
          s"Failed to wait job finish")
        assertTrue(exception.isPresent)
      }
    }

    assertTrue(Try(innerTestSetParallelism(
      "SinkFunction",
      validParallelism,
      index = index.getAndIncrement))
      .isSuccess)
  }

  @Test
  def testParallelismOnChangelogMode():Unit = {
    val dataId = TestValuesTableFactory.registerData(data1)
    val sourceTableName = s"test_para_source"
    val sinkTableWithoutPkName = s"test_para_sink_without_pk"
    val sinkTableWithPkName = s"test_para_sink_with_pk"
    val sinkParallelism = 2

    tEnv.executeSql(
      s"""
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
    tEnv.executeSql(
      s"""
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
    Try(tEnv
      .executeSql(s"INSERT INTO $sinkTableWithoutPkName SELECT * FROM $sourceTableName")
      .await()) match {
      case Failure(e) =>
        val exception = ExceptionUtils
          .findThrowableWithMessage(
            e,
            "primary key is required but no primary key is found")
        assertTrue(exception.isPresent)
    }

    tEnv.executeSql(
      s"""
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

    assertTrue(Try(tEnv
      .executeSql(s"INSERT INTO $sinkTableWithPkName SELECT * FROM $sourceTableName")
      .await()).isSuccess)

  }


  private def innerTestSetParallelism(provider: String, parallelism: Int, index: Int): Unit = {
    val dataId = TestValuesTableFactory.registerData(data1)
    val sourceTableName = s"test_para_source_${provider.toLowerCase.trim}_$index"
    val sinkTableName = s"test_para_sink_${provider.toLowerCase.trim}_$index"
    tEnv.executeSql(
      s"""
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
    tEnv.executeSql(
      s"""
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
}
