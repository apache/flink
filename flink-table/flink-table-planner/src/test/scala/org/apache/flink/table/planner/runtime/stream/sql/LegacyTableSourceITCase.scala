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

import org.apache.flink.table.api.{DataTypes, TableSchema, Types}
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.planner.runtime.utils.{StreamingTestBase, TestData, TestingAppendSink}
import org.apache.flink.table.planner.runtime.utils.BatchTestBase.row
import org.apache.flink.table.planner.utils._

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

import scala.collection.mutable

/**
 * Note: This test class will be removed after finishing FLINK-36134. Do not add more tests here.
 */
class LegacyTableSourceITCase extends StreamingTestBase {

  @Test
  def testTableSourceWithFilterable(): Unit = {
    TestLegacyFilterableTableSource.createTemporaryTable(
      tEnv,
      TestLegacyFilterableTableSource.defaultSchema,
      "MyTable")

    val sqlQuery = "SELECT id, name FROM MyTable WHERE amount > 4 AND price < 9"
    val result = tEnv.sqlQuery(sqlQuery).toDataStream
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = Seq("5,Record_5", "6,Record_6", "7,Record_7", "8,Record_8")
    assertThat(sink.getAppendResults.sorted).isEqualTo(expected.sorted)
  }

  @Test
  def testTableSourceWithPartitionable(): Unit = {
    TestPartitionableSourceFactory.createTemporaryTable(tEnv, "PartitionableTable", true)

    val sqlQuery = "SELECT * FROM PartitionableTable WHERE part2 > 1 and id > 2 AND part1 = 'A'"
    val result = tEnv.sqlQuery(sqlQuery).toDataStream
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = Seq("3,John,A,2", "4,nosharp,A,2")
    assertThat(sink.getAppendResults.sorted).isEqualTo(expected.sorted)
  }

  @Test
  def testCsvTableSource(): Unit = {
    TestTableSourceSinks.createPersonCsvTemporaryTable(tEnv, "persons")
    val sink = new TestingAppendSink()
    tEnv
      .sqlQuery("SELECT id, `first`, `last`, score FROM persons WHERE id < 4 ")
      .toDataStream
      .addSink(sink)

    env.execute()

    val expected =
      mutable.MutableList("1,Mike,Smith,12.3", "2,Bob,Taylor,45.6", "3,Sam,Miller,7.89")
    assertThat(sink.getAppendResults.sorted).isEqualTo(expected.sorted)
  }

  @Test
  def testLookupJoinCsvTemporalTable(): Unit = {
    TestTableSourceSinks.createOrdersCsvTemporaryTable(tEnv, "orders")
    TestTableSourceSinks.createRatesCsvTemporaryTable(tEnv, "rates")

    val sql =
      """
        |SELECT o.amount, o.currency, r.rate
        |FROM (SELECT *, PROCTIME() as proc FROM orders) AS o
        |JOIN rates FOR SYSTEM_TIME AS OF o.proc AS r
        |ON o.currency = r.currency
      """.stripMargin

    val sink = new TestingAppendSink()
    tEnv.sqlQuery(sql).toDataStream.addSink(sink)

    env.execute()

    val expected = Seq(
      "2,Euro,119",
      "1,US Dollar,102",
      "50,Yen,1",
      "3,Euro,119",
      "5,US Dollar,102"
    )
    assertThat(sink.getAppendResults.sorted).isEqualTo(expected.sorted)
  }

  @Test
  def testInputFormatSource(): Unit = {
    val tableSchema = TableSchema
      .builder()
      .fields(Array("a", "b", "c"), Array(DataTypes.INT(), DataTypes.BIGINT(), DataTypes.STRING()))
      .build()
    TestInputFormatTableSource.createTemporaryTable(
      tEnv,
      tableSchema,
      TestData.smallData3,
      "MyInputFormatTable")
    val sink = new TestingAppendSink()
    tEnv.sqlQuery("SELECT a, c FROM MyInputFormatTable").toDataStream.addSink(sink)

    env.execute()

    val expected = Seq(
      "1,Hi",
      "2,Hello",
      "3,Hello world"
    )
    assertThat(sink.getAppendResults.sorted).isEqualTo(expected.sorted)
  }

  @Test
  def testDecimalSource(): Unit = {
    val tableSchema = TableSchema
      .builder()
      .fields(
        Array("a", "b", "c", "d"),
        Array(DataTypes.INT(), DataTypes.DECIMAL(5, 2), DataTypes.VARCHAR(5), DataTypes.CHAR(5)))
      .build()

    val data = Seq(
      row(1, new java.math.BigDecimal(5.1), "1", "1"),
      row(2, new java.math.BigDecimal(6.1), "12", "12"),
      row(3, new java.math.BigDecimal(7.1), "123", "123")
    )

    TestDataTypeTableSource.createTemporaryTable(tEnv, tableSchema, "MyInputFormatTable", data.seq)

    val sink = new TestingAppendSink()
    tEnv
      .sqlQuery("SELECT a, b, c, d FROM MyInputFormatTable")
      .toDataStream
      .addSink(sink)

    env.execute()

    val expected = Seq(
      "1,5.10,1,1",
      "2,6.10,12,12",
      "3,7.10,123,123"
    )
    assertThat(sink.getAppendResults.sorted).isEqualTo(expected.sorted)
  }

  /**
   * StreamTableSource must use type info in DataStream, so it will lose precision. Just support
   * default precision decimal.
   */
  @Test
  def testLegacyDecimalSourceUsingStreamTableSource(): Unit = {
    val tableSchema = new TableSchema(
      Array("a", "b", "c"),
      Array(
        Types.INT(),
        Types.DECIMAL(),
        Types.STRING()
      ))

    val data = Seq(
      row(1, new java.math.BigDecimal(5.1), "1"),
      row(2, new java.math.BigDecimal(6.1), "12"),
      row(3, new java.math.BigDecimal(7.1), "123")
    )

    TestStreamTableSource.createTemporaryTable(tEnv, tableSchema, "MyInputFormatTable", data)
    val sink = new TestingAppendSink()
    tEnv.sqlQuery("SELECT a, b, c FROM MyInputFormatTable").toDataStream.addSink(sink)

    env.execute()

    val expected = Seq(
      "1,5.099999999999999645,1",
      "2,6.099999999999999645,12",
      "3,7.099999999999999645,123"
    )
    assertThat(sink.getAppendResults.sorted).isEqualTo(expected.sorted)
  }
}
