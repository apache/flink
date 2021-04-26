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

import org.apache.flink.table.api.TableException
import org.apache.flink.table.planner.factories.TestValuesTableFactory
import org.apache.flink.table.planner.factories.TestValuesTableFactory.{getRawResults, registerData}
import org.apache.flink.table.planner.runtime.utils.StreamingWithStateTestBase
import org.apache.flink.table.planner.runtime.utils.StreamingWithStateTestBase.StateBackendMode
import org.apache.flink.types.Row

import org.junit._
import org.junit.Assert.assertEquals
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

import java.time.LocalDateTime
import java.time.format.DateTimeParseException

import scala.collection.JavaConversions._

@RunWith(classOf[Parameterized])
class TemporalJoinITCase(state: StateBackendMode)
  extends StreamingWithStateTestBase(state) {

  // test data for Processing-Time temporal table join
  val procTimeOrderData = List(
    changelogRow("+I", 1L, "Euro", "no1", 12L),
    changelogRow("+I", 2L, "US Dollar", "no1", 14L),
    changelogRow("+I", 3L, "US Dollar", "no2", 18L),
    changelogRow("+I", 4L, "RMB", "no1", 40L))

  val procTimeCurrencyData = List(
    changelogRow("+I", "Euro", "no1", 114L),
    changelogRow("+I", "US Dollar", "no1", 102L),
    changelogRow("+I", "Yen", "no1", 1L),
    changelogRow("+I", "RMB", "no1", 702L),
    changelogRow("+I", "Euro", "no1", 118L),
    changelogRow("+I", "US Dollar", "no2", 106L))

  val procTimeCurrencyChangelogData = List(
    changelogRow("+I", "Euro", "no1", 114L),
    changelogRow("+I", "US Dollar", "no1", 102L),
    changelogRow("+I", "Yen", "no1", 1L),
    changelogRow("+I", "RMB", "no1", 702L),
    changelogRow("-U", "RMB", "no1", 702L),
    changelogRow("+U", "RMB", "no1", 802L),
    changelogRow("+I", "Euro", "no1", 118L),
    changelogRow("+I", "US Dollar", "no2", 106L))

  // test data for Event-Time temporal table join
  val rowTimeOrderData = List(
    changelogRow("+I", 1L, "Euro", "no1", 12L, "2020-08-15T00:01:00"),
    changelogRow("+I", 2L, "US Dollar", "no1", 1L, "2020-08-15T00:02:00"),
    changelogRow("+I", 3L, "RMB", "no1", 40L, "2020-08-15T00:03:00"),
    changelogRow("+I", 4L, "Euro", "no1", 14L, "2020-08-16T00:04:00"),
    changelogRow("-U", 2L, "US Dollar", "no1", 1L, "2020-08-16T00:03:00"),
    changelogRow("+U", 2L, "US Dollar", "no1", 18L, "2020-08-16T00:03:00"),
    changelogRow("+I", 5L, "RMB", "no1", 40L, "2020-08-16T00:03:00"),
    changelogRow("+I", 6L, "RMB", "no1", 40L, "2020-08-16T00:04:00"),
    changelogRow("-D", 6L, "RMB", "no1", 40L, "2020-08-16T00:04:00"))

  val rowTimeCurrencyDataUsingMetaTime = List(
    changelogRow("+I", "Euro", "no1", 114L, "2020-08-15T00:00:01"),
    changelogRow("+I", "US Dollar", "no1", 102L, "2020-08-15T00:00:02"),
    changelogRow("+I", "Yen", "no1", 1L, "2020-08-15T00:00:03"),
    changelogRow("+I", "RMB", "no1", 702L, "2020-08-15T00:00:04"),
    changelogRow("-U", "Euro", "no1", 114L, "2020-08-16T00:01:00"),
    changelogRow("+U", "Euro",  "no1", 118L, "2020-08-16T00:01:00"),
    changelogRow("-U", "US Dollar", "no1", 102L, "2020-08-16T00:02:00"),
    changelogRow("+U", "US Dollar",  "no1", 106L, "2020-08-16T00:02:00"),
    changelogRow("-D", "RMB", "no1", 708L, "2020-08-16T00:02:00"))

  val rowTimeCurrencyDataUsingBeforeTime = List(
    changelogRow("+I", "Euro", "no1", 114L, "2020-08-15T00:00:01"),
    changelogRow("+I", "US Dollar", "no1", 102L, "2020-08-15T00:00:02"),
    changelogRow("+I", "Yen", "no1", 1L, "2020-08-15T00:00:03"),
    changelogRow("+I", "RMB", "no1", 702L, "2020-08-15T00:00:04"),
    changelogRow("-U", "Euro", "no1", 114L, "2020-08-15T00:00:01"),
    changelogRow("+U", "Euro",  "no1", 118L, "2020-08-16T00:01:00"),
    changelogRow("-U", "US Dollar", "no1", 102L, "2020-08-15T00:00:02"),
    changelogRow("+U", "US Dollar",  "no1", 106L, "2020-08-16T00:02:00"),
    changelogRow("-D", "RMB", "no1", 702L, "2020-08-15T00:00:04"))

  val upsertSourceCurrencyData = List(
    changelogRow("+U", "Euro", "no1", 114L, "2020-08-15T00:00:01"),
    changelogRow("+U", "US Dollar", "no1", 102L, "2020-08-15T00:00:02"),
    changelogRow("+U", "Yen", "no1", 1L, "2020-08-15T00:00:03"),
    changelogRow("+U", "RMB", "no1", 702L, "2020-08-15T00:00:04"),
    changelogRow("+U", "Euro",  "no1", 118L, "2020-08-16T00:01:00"),
    changelogRow("+U", "US Dollar", "no1", 104L, "2020-08-16T00:02:00"),
    changelogRow("-D", "RMB", "no1", 702L, "2020-08-15T00:00:04"))

  @Before
  def prepare(): Unit = {
    val procTimeOrderDataId = registerData(procTimeOrderData)
    tEnv.executeSql(
      s"""
         |CREATE TABLE orders_proctime (
         |  order_id BIGINT,
         |  currency STRING,
         |  currency_no STRING,
         |  amount BIGINT,
         |  proctime as PROCTIME()
         |) WITH (
         |  'connector' = 'values',
         |  'bounded' = 'false',
         |  'data-id' = '$procTimeOrderDataId'
         |)
         |""".stripMargin)

    // register a non-lookup table
    val procTimeCurrencyDataId = registerData(procTimeCurrencyData)
    tEnv.executeSql(
      s"""
         |CREATE TABLE currency_proctime (
         |  currency STRING,
         |  currency_no STRING,
         |  rate BIGINT,
         |  proctime as PROCTIME(),
         |  PRIMARY KEY(currency, currency_no) NOT ENFORCED
         |) WITH (
         |  'connector' = 'values',
         |  'bounded' = 'false',
         |  'disable-lookup' = 'true',
         |  'data-id' = '$procTimeCurrencyDataId'
         |)
         |""".stripMargin)

    val procTimeCurrencyChangelogDataId = registerData(procTimeCurrencyChangelogData)
    tEnv.executeSql(
      s"""
         |CREATE TABLE changelog_currency_proctime (
         |  currency STRING,
         |  currency_no STRING,
         |  rate BIGINT,
         |  proctime as PROCTIME(),
         |  PRIMARY KEY(currency, currency_no) NOT ENFORCED
         |) WITH (
         |  'connector' = 'values',
         |  'bounded' = 'false',
         |  'disable-lookup' = 'true',
         |  'changelog-mode' = 'I,UA,UB,D',
         |  'data-id' = '$procTimeCurrencyChangelogDataId'
         |)
         |""".stripMargin)

    tEnv.executeSql(
      s"""
         |CREATE VIEW latest_rates AS
         |SELECT
         |  currency,
         |  currency_no,
         |  rate,
         |  proctime FROM
         |      ( SELECT *, ROW_NUMBER() OVER (PARTITION BY currency, currency_no
         |        ORDER BY proctime DESC) AS rowNum
         |        FROM currency_proctime) T
         | WHERE rowNum = 1""".stripMargin)

    createSinkTable("proctime_default_sink", None)


    val rowTimeOrderDataId = registerData(rowTimeOrderData)
    tEnv.executeSql(
      s"""
         |CREATE TABLE orders_rowtime (
         |  order_id BIGINT,
         |  currency STRING,
         |  currency_no STRING,
         |  amount BIGINT,
         |  order_time TIMESTAMP(3),
         |  WATERMARK FOR order_time AS order_time
         |) WITH (
         |  'connector' = 'values',
         |  'changelog-mode' = 'I,UA,UB,D',
         |  'data-id' = '$rowTimeOrderDataId'
         |)
         |""".stripMargin)

    val rowTimeCurrencyDataId = registerData(rowTimeCurrencyDataUsingMetaTime)
    tEnv.executeSql(
      s"""
         |CREATE TABLE versioned_currency_with_single_key (
         |  currency STRING,
         |  currency_no STRING,
         |  rate  BIGINT,
         |  currency_time TIMESTAMP(3),
         |  WATERMARK FOR currency_time AS currency_time - interval '10' SECOND,
         |  PRIMARY KEY(currency) NOT ENFORCED
         |) WITH (
         |  'connector' = 'values',
         |  'changelog-mode' = 'I,UA,UB,D',
         |  'data-id' = '$rowTimeCurrencyDataId'
         |)
         |""".stripMargin)

    val currencyDataUsingBeforeTimeId = registerData(rowTimeCurrencyDataUsingBeforeTime)
    tEnv.executeSql(
      s"""
         |CREATE TABLE currency_using_update_before_time (
         |  currency STRING,
         |  currency_no STRING,
         |  rate  BIGINT,
         |  currency_time TIMESTAMP(3),
         |  WATERMARK FOR currency_time AS currency_time - interval '10' SECOND,
         |  PRIMARY KEY(currency) NOT ENFORCED
         |) WITH (
         |  'connector' = 'values',
         |  'changelog-mode' = 'I,UA,UB,D',
         |  'data-id' = '$currencyDataUsingBeforeTimeId'
         |)
         |""".stripMargin)

    tEnv.executeSql(
      s"""
         |CREATE TABLE versioned_currency_with_multi_key (
         |  currency STRING,
         |  currency_no STRING,
         |  rate  BIGINT,
         |  currency_time TIMESTAMP(3),
         |  WATERMARK FOR currency_time AS currency_time - interval '10' SECOND,
         |  PRIMARY KEY(currency, currency_no) NOT ENFORCED
         |) WITH (
         |  'connector' = 'values',
         |  'changelog-mode' = 'I,UA,UB,D',
         |  'data-id' = '$rowTimeCurrencyDataId'
         |)
         |""".stripMargin)

    val upsertSourceDataId = registerData(upsertSourceCurrencyData)
    tEnv.executeSql(
      s"""
         |CREATE TABLE upsert_currency (
         |  currency STRING,
         |  currency_no STRING,
         |  rate  BIGINT,
         |  currency_time TIMESTAMP(3),
         |  WATERMARK FOR currency_time AS currency_time - interval '10' SECOND,
         |  PRIMARY KEY(currency) NOT ENFORCED
         |) WITH (
         |  'connector' = 'values',
         |  'changelog-mode' = 'UA,D',
         |  'data-id' = '$upsertSourceDataId'
         |)
         |""".stripMargin)

    createSinkTable("rowtime_default_sink", None)
  }

  /**
   * Because of nature of the processing time, we can not (or at least it is not that easy)
   * validate the result here. Instead of that, here we are just testing whether there are no
   * exceptions in a full blown ITCase. Actual correctness is tested in unit tests.
   */
  @Test
  def testProcTimeTemporalJoin(): Unit = {
    val sql = "INSERT INTO proctime_default_sink " +
      " SELECT o.order_id, o.currency, o.amount, o.proctime, r.rate, r.proctime " +
      " FROM orders_proctime AS o " +
      " JOIN currency_proctime FOR SYSTEM_TIME AS OF o.proctime as r " +
      " ON o.currency = r.currency and o.currency_no = r.currency_no"

    expectedException.expect(classOf[TableException])
    expectedException.expectMessage(
      "Processing-time temporal join is not supported yet.")
    tEnv.executeSql(sql).await()
  }

  @Test
  def testProcTimeLeftTemporalJoin(): Unit = {
     val sql = "INSERT INTO proctime_default_sink " +
      " SELECT o.order_id, o.currency, o.amount, o.proctime, r.rate, r.proctime " +
      " FROM orders_proctime AS o " +
      " LEFT JOIN currency_proctime FOR SYSTEM_TIME AS OF o.proctime as r " +
      " ON o.currency = r.currency and o.currency_no = r.currency_no"

    expectedException.expect(classOf[TableException])
    expectedException.expectMessage(
      "Processing-time temporal join is not supported yet.")
    tEnv.executeSql(sql).await()
  }

  @Test
  def testProcTimeTemporalJoinChangelogSource(): Unit = {
    createSinkTable("proctime_sink1", Some(
      s"""
      | currency STRING,
      | currency_no STRING,
      | rate BIGINT,
      | proctime TIMESTAMP(3)
      | """.stripMargin))

    val sql = "INSERT INTO proctime_sink1 " +
      " SELECT r.* FROM orders_proctime AS o " +
      " JOIN changelog_currency_proctime FOR SYSTEM_TIME AS OF o.proctime as r " +
      " ON o.currency = r.currency and o.currency_no = r.currency_no"

    expectedException.expect(classOf[TableException])
    expectedException.expectMessage(
      "Processing-time temporal join is not supported yet.")
    tEnv.executeSql(sql).await()
  }

  @Test
  def testProcTimeTemporalJoinWithView(): Unit = {
    val sql = "INSERT INTO proctime_default_sink " +
      " SELECT o.order_id, o.currency, o.amount, o.proctime, r.rate, r.proctime " +
      " FROM orders_proctime AS o " +
      " JOIN latest_rates FOR SYSTEM_TIME AS OF o.proctime as r " +
      " ON o.currency = r.currency and o.currency_no = r.currency_no"

    expectedException.expect(classOf[TableException])
    expectedException.expectMessage(
      "Processing-time temporal join is not supported yet.")
    tEnv.executeSql(sql).await()
  }

  @Test
  def testProcTimeLeftTemporalJoinWithView(): Unit = {
    val sql = "INSERT INTO proctime_default_sink " +
      " SELECT o.order_id, o.currency, o.amount, o.proctime, r.rate, r.proctime " +
      " FROM orders_proctime AS o " +
      " LEFT JOIN latest_rates FOR SYSTEM_TIME AS OF o.proctime as r " +
      " ON o.currency = r.currency and o.currency_no = r.currency_no"

    expectedException.expect(classOf[TableException])
    expectedException.expectMessage(
      "Processing-time temporal join is not supported yet.")
    tEnv.executeSql(sql).await()
  }

  @Test
  def testProcTimeTemporalJoinWithViewNonEqui(): Unit = {
    val sql = "INSERT INTO proctime_default_sink " +
      " SELECT o.order_id, o.currency, o.amount, o.proctime, r.rate, r.proctime " +
      " FROM orders_proctime AS o " +
      " JOIN latest_rates FOR SYSTEM_TIME AS OF o.proctime AS r " +
      " ON o.currency = r.currency and o.currency_no = r.currency_no " +
      " AND o.amount > r.rate"

    expectedException.expect(classOf[TableException])
    expectedException.expectMessage(
      "Processing-time temporal join is not supported yet.")
    tEnv.executeSql(sql).await()
  }

  @Test
  def testProcTimeLeftTemporalJoinWithViewWithPredicates(): Unit = {
    val sql = "INSERT INTO proctime_default_sink " +
      " SELECT o.order_id, o.currency, o.amount, o.proctime, r.rate, r.proctime " +
      " FROM orders_proctime AS o " +
      " LEFT JOIN latest_rates FOR SYSTEM_TIME AS OF o.proctime AS r " +
      " ON o.currency = r.currency and o.currency_no = r.currency_no" +
      " AND o.amount > r.rate"

    expectedException.expect(classOf[TableException])
    expectedException.expectMessage(
      "Processing-time temporal join is not supported yet.")
    tEnv.executeSql(sql).await()
  }

  @Test
  def testProcTimeMultiTemporalJoin(): Unit = {
    createSinkTable("proctime_sink8", None)
    val sql = "INSERT INTO proctime_sink8 " +
      " SELECT o.order_id, o.currency, o.amount, o.proctime, r.rate, r1.proctime " +
      " FROM orders_proctime AS o " +
      " JOIN latest_rates FOR SYSTEM_TIME AS OF o.proctime as r " +
      " ON o.currency = r.currency and o.currency_no = r.currency_no " +
      " JOIN currency_proctime FOR SYSTEM_TIME AS OF o.proctime as r1" +
      " ON o.currency = r1.currency and o.currency_no = r1.currency_no"

    expectedException.expect(classOf[TableException])
    expectedException.expectMessage(
      "Processing-time temporal join is not supported yet.")
    tEnv.executeSql(sql).await()
  }

  @Test
  def testEventTimeTemporalJoin(): Unit = {
    val sql = "INSERT INTO rowtime_default_sink " +
      " SELECT o.order_id, o.currency, o.amount, o.order_time, r.rate, r.currency_time " +
      " FROM orders_rowtime AS o JOIN versioned_currency_with_single_key " +
      " FOR SYSTEM_TIME AS OF o.order_time as r " +
      " ON o.currency = r.currency"

    tEnv.executeSql(sql).await()
    val rawResult = getRawResults("rowtime_default_sink")
    val expected = List(
      "+I(1,Euro,12,2020-08-15T00:01,114,2020-08-15T00:00:01)",
      "+I(2,US Dollar,1,2020-08-15T00:02,102,2020-08-15T00:00:02)",
      "+I(3,RMB,40,2020-08-15T00:03,702,2020-08-15T00:00:04)",
      "+I(4,Euro,14,2020-08-16T00:04,118,2020-08-16T00:01)",
      "-U(2,US Dollar,1,2020-08-16T00:03,106,2020-08-16T00:02)",
      "+U(2,US Dollar,18,2020-08-16T00:03,106,2020-08-16T00:02)")
    assertEquals(expected.sorted, rawResult.sorted)
  }

  @Test
  def testEventTimeTemporalJoinThatJoinkeyContainsPk(): Unit = {
    val sql = "INSERT INTO rowtime_default_sink " +
      " SELECT o.order_id, o.currency, o.amount, o.order_time, r.rate, r.currency_time " +
      " FROM orders_rowtime AS o JOIN versioned_currency_with_single_key " +
      " FOR SYSTEM_TIME AS OF o.order_time as r " +
      " ON o.currency = r.currency AND o.currency_no = r.currency_no"

    tEnv.executeSql(sql).await()
    val rawResult = getRawResults("rowtime_default_sink")
    val expected = List(
      "+I(1,Euro,12,2020-08-15T00:01,114,2020-08-15T00:00:01)",
      "+I(2,US Dollar,1,2020-08-15T00:02,102,2020-08-15T00:00:02)",
      "+I(3,RMB,40,2020-08-15T00:03,702,2020-08-15T00:00:04)",
      "+I(4,Euro,14,2020-08-16T00:04,118,2020-08-16T00:01)",
      "-U(2,US Dollar,1,2020-08-16T00:03,106,2020-08-16T00:02)",
      "+U(2,US Dollar,18,2020-08-16T00:03,106,2020-08-16T00:02)")
    assertEquals(expected.sorted, rawResult.sorted)
  }

  @Test
  def testEventTimeTemporalJoinWithFilter(): Unit = {
    tEnv.executeSql("CREATE VIEW v1 AS" +
      " SELECT * FROM versioned_currency_with_single_key WHERE rate < 115")
    val sql = "INSERT INTO rowtime_default_sink " +
      " SELECT o.order_id, o.currency, o.amount, o.order_time, r.rate, r.currency_time " +
      " FROM orders_rowtime AS o " +
      " JOIN v1 FOR SYSTEM_TIME AS OF o.order_time as r " +
      " ON o.currency = r.currency"
    tEnv.executeSql(sql).await()
    val rawResult = getRawResults("rowtime_default_sink")
    val expected = List(
      "+I(1,Euro,12,2020-08-15T00:01,114,2020-08-15T00:00:01)",
      "+I(2,US Dollar,1,2020-08-15T00:02,102,2020-08-15T00:00:02)",
      "-U(2,US Dollar,1,2020-08-16T00:03,106,2020-08-16T00:02)",
      "+U(2,US Dollar,18,2020-08-16T00:03,106,2020-08-16T00:02)")
    assertEquals(expected.sorted, rawResult.sorted)
  }

  @Test
  def testEventTimeLeftTemporalJoin(): Unit = {
    val sql = "INSERT INTO rowtime_default_sink " +
      " SELECT o.order_id, o.currency, o.amount, o.order_time, r.rate, r.currency_time " +
      " FROM orders_rowtime AS o LEFT JOIN versioned_currency_with_single_key " +
      " FOR SYSTEM_TIME AS OF o.order_time as r " +
      " ON o.currency = r.currency"
    tEnv.executeSql(sql).await()

    val rawResult = getRawResults("rowtime_default_sink")
    val expected = List(
      "+I(1,Euro,12,2020-08-15T00:01,114,2020-08-15T00:00:01)",
      "+I(2,US Dollar,1,2020-08-15T00:02,102,2020-08-15T00:00:02)",
      "+I(3,RMB,40,2020-08-15T00:03,702,2020-08-15T00:00:04)",
      "+I(4,Euro,14,2020-08-16T00:04,118,2020-08-16T00:01)",
      "-U(2,US Dollar,1,2020-08-16T00:03,106,2020-08-16T00:02)",
      "+U(2,US Dollar,18,2020-08-16T00:03,106,2020-08-16T00:02)",
      "+I(5,RMB,40,2020-08-16T00:03,null,null)",
      "+I(6,RMB,40,2020-08-16T00:04,null,null)",
      "-D(6,RMB,40,2020-08-16T00:04,null,null)")
    assertEquals(expected.sorted, rawResult.sorted)
  }

  @Test
  @Ignore("the test using update before time as changelog time which is unstable")
  def testEventTimeTemporalJoinChangelogUsingBeforeTime(): Unit = {
    val sql = "INSERT INTO rowtime_default_sink " +
      " SELECT o.order_id, o.currency, o.amount, o.order_time, r.rate, r.currency_time " +
      " FROM orders_rowtime AS o LEFT JOIN currency_using_update_before_time " +
      " FOR SYSTEM_TIME AS OF o.order_time as r " +
      " ON o.currency = r.currency"
    tEnv.executeSql(sql).await()

    val rawResult = getRawResults("rowtime_default_sink")
    val expected = List(
      "+I(1,Euro,12,2020-08-15T00:01,null,null)",
      "+I(2,US Dollar,1,2020-08-15T00:02,102,2020-08-15T00:00:02)",
      "+I(3,RMB,40,2020-08-15T00:03,702,2020-08-15T00:00:04)",
      "+I(4,Euro,14,2020-08-16T00:04,118,2020-08-16T00:01)",
      "-U(2,US Dollar,1,2020-08-16T00:03,106,2020-08-16T00:02)",
      "+U(2,US Dollar,18,2020-08-16T00:03,106,2020-08-16T00:02)",
      "+I(5,RMB,40,2020-08-16T00:03,null,null)",
      "+I(6,RMB,40,2020-08-16T00:04,null,null)",
      "-D(6,RMB,40,2020-08-16T00:04,null,null)")
    assertEquals(expected.sorted, rawResult.sorted)
  }

  @Test
  @Ignore("the test using update before time as changelog time which is unstable")
  def testEventTimeLeftTemporalJoinUpsertSource(): Unit = {
    // Note: The WatermarkAssigner of upsertSource is followed after ChangelogNormalize,
    // when the parallelism > 1 and test data doesn't cover all parallelisms, it returns
    // Long.MaxValue as final watermark until all parallelism finished.
    // This may leads the test failed because the test data doesn't cover every parallelism.
    // TODO: Remove the single parallelism once FLINK-19878 has been fixed.
    env.setParallelism(1)
    val sql = "INSERT INTO rowtime_default_sink " +
      " SELECT o.order_id, o.currency, o.amount, o.order_time, r.rate, r.currency_time " +
      " FROM orders_rowtime AS o LEFT JOIN upsert_currency " +
      " FOR SYSTEM_TIME AS OF o.order_time as r " +
      " ON o.currency = r.currency "
    tEnv.executeSql(sql).await()

    val rawResult = TestValuesTableFactory.getRawResults("rowtime_default_sink")
    val expected = List(
      "+I(1,Euro,12,2020-08-15T00:01,114,2020-08-15T00:00:01)",
      "+I(2,US Dollar,1,2020-08-15T00:02,102,2020-08-15T00:00:02)",
      "+I(3,RMB,40,2020-08-15T00:03,702,2020-08-15T00:00:04)",
      "+I(4,Euro,14,2020-08-16T00:04,118,2020-08-16T00:01)",
      "-U(2,US Dollar,1,2020-08-16T00:03,104,2020-08-16T00:02)",
      "+U(2,US Dollar,18,2020-08-16T00:03,104,2020-08-16T00:02)",
      "+I(5,RMB,40,2020-08-16T00:03,null,null)",
      "+I(6,RMB,40,2020-08-16T00:04,null,null)",
      "-D(6,RMB,40,2020-08-16T00:04,null,null)")
    assertEquals(expected.sorted, rawResult.sorted)
  }

  @Test
  def testEventTimeTemporalJoinWithMultiKeys(): Unit = {
    val sql = "INSERT INTO rowtime_default_sink " +
      " SELECT o.order_id, o.currency, o.amount, o.order_time, r.rate, r.currency_time " +
      " FROM orders_rowtime AS o JOIN versioned_currency_with_multi_key " +
      " FOR SYSTEM_TIME AS OF o.order_time as r " +
      " ON o.currency_no = r.currency_no AND o.currency = r.currency"
    tEnv.executeSql(sql).await()

    val rawResult = getRawResults("rowtime_default_sink")
    val expected = List(
      "+I(1,Euro,12,2020-08-15T00:01,114,2020-08-15T00:00:01)",
      "+I(2,US Dollar,1,2020-08-15T00:02,102,2020-08-15T00:00:02)",
      "+I(3,RMB,40,2020-08-15T00:03,702,2020-08-15T00:00:04)",
      "+I(4,Euro,14,2020-08-16T00:04,118,2020-08-16T00:01)",
      "-U(2,US Dollar,1,2020-08-16T00:03,106,2020-08-16T00:02)",
      "+U(2,US Dollar,18,2020-08-16T00:03,106,2020-08-16T00:02)")
    assertEquals(expected.sorted, rawResult.sorted)
  }

  @Test
  def testEventTimeTemporalJoinWithNonEqualCondition(): Unit = {
    val sql = "INSERT INTO rowtime_default_sink " +
      " SELECT o.order_id, o.currency, o.amount, o.order_time, r.rate, r.currency_time " +
      " FROM orders_rowtime AS o JOIN versioned_currency_with_multi_key " +
      " FOR SYSTEM_TIME AS OF o.order_time as r " +
      " ON o.currency = r.currency and o.currency_no = r.currency_no " +
      " and o.order_id < 5 and r.rate > 114"
    tEnv.executeSql(sql).await()
    val rawResult = getRawResults("rowtime_default_sink")
    val expected = List(
      "+I(3,RMB,40,2020-08-15T00:03,702,2020-08-15T00:00:04)",
      "+I(4,Euro,14,2020-08-16T00:04,118,2020-08-16T00:01)")
    assertEquals(expected.sorted, rawResult.sorted)
  }

  @Test
  def testEventTimeMultiTemporalJoin(): Unit = {
    createSinkTable("rowtime_sink1", Some(
      s"""
         |  order_id BIGINT,
         |  currency STRING,
         |  amount BIGINT,
         |  l_time TIMESTAMP(3),
         |  rate BIGINT,
         |  r_time TIMESTAMP(3),
         |  r1_rate BIGINT,
         |  r1_time TIMESTAMP(3),
         |  PRIMARY KEY(currency) NOT ENFORCED
         |""".stripMargin
    ))
    val sql = "INSERT INTO rowtime_sink1 " +
      " SELECT o.order_id, o.currency, o.amount, o.order_time, r.rate, r.currency_time," +
      " r1.rate, r1.currency_time FROM orders_rowtime AS o " +
      " LEFT JOIN versioned_currency_with_multi_key " +
      " FOR SYSTEM_TIME AS OF o.order_time as r " +
      " ON o.currency = r.currency and o.currency_no = r.currency_no " +
      " LEFT JOIN versioned_currency_with_single_key  FOR SYSTEM_TIME AS OF o.order_time as r1 " +
      " ON o.currency = r1.currency"

    tEnv.executeSql(sql).await()
    val rawResult = getRawResults("rowtime_sink1")
    val expected = List(
      "+I(1,Euro,12,2020-08-15T00:01,114,2020-08-15T00:00:01,114,2020-08-15T00:00:01)",
      "+I(2,US Dollar,1,2020-08-15T00:02,102,2020-08-15T00:00:02,102,2020-08-15T00:00:02)",
      "+I(3,RMB,40,2020-08-15T00:03,702,2020-08-15T00:00:04,702,2020-08-15T00:00:04)",
      "+I(4,Euro,14,2020-08-16T00:04,118,2020-08-16T00:01,118,2020-08-16T00:01)",
      "-U(2,US Dollar,1,2020-08-16T00:03,106,2020-08-16T00:02,106,2020-08-16T00:02)",
      "+U(2,US Dollar,18,2020-08-16T00:03,106,2020-08-16T00:02,106,2020-08-16T00:02)",
      "+I(5,RMB,40,2020-08-16T00:03,null,null,null,null)",
      "+I(6,RMB,40,2020-08-16T00:04,null,null,null,null)",
      "-D(6,RMB,40,2020-08-16T00:04,null,null,null,null)")
    assertEquals(expected.sorted, rawResult.sorted)
  }

  private def createSinkTable(tableName: String, columns: Option[String]): Unit = {
    val columnsDDL = columns match {
      case Some(cols) => cols
      case _ =>
        s"""
           |  order_id BIGINT,
           |  currency STRING,
           |  amount BIGINT,
           |  l_time TIMESTAMP(3),
           |  rate BIGINT,
           |  r_time TIMESTAMP(3),
           |  PRIMARY KEY(order_id) NOT ENFORCED
           |""".stripMargin
    }

    tEnv.executeSql(
      s"""
        |CREATE TABLE $tableName (
        | $columnsDDL
        |) WITH (
        |  'connector' = 'values',
        |  'sink-insert-only' = 'false',
        |  'changelog-mode' = 'I,UA,UB,D'
        |)
        |""".stripMargin)
  }

  private def changelogRow(kind: String, values: Any*): Row = {
    val objects = values.map {
      case l: Long => Long.box(l)
      case i: Int => Int.box(i)
      case date: String => try {
        LocalDateTime.parse(date)
      } catch {
        case _: DateTimeParseException => date
      }
      case o: Object => o
    }
    TestValuesTableFactory.changelogRow(kind, objects.toArray: _*)
  }
}
