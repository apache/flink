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
package org.apache.flink.table.planner.plan.stream.sql

import org.apache.flink.table.api.{DataTypes, TableSchema}
import org.apache.flink.table.planner.expressions.utils.Func1
import org.apache.flink.table.planner.utils._
import org.apache.flink.types.Row

import org.junit.jupiter.api.{BeforeEach, Test}

/**
 * Note: This test class will be removed after finishing FLINK-36134. Do not add more tests here.
 */
class LegacyTableSourceTest extends TableTestBase {

  private val util = streamTestUtil()

  private val tableSchema = TableSchema
    .builder()
    .fields(Array("a", "b", "c"), Array(DataTypes.INT(), DataTypes.BIGINT(), DataTypes.STRING()))
    .build()

  @BeforeEach
  def setup(): Unit = {
    TestLegacyFilterableTableSource.createTemporaryTable(
      util.tableEnv,
      TestLegacyFilterableTableSource.defaultSchema,
      "FilterableTable")

    TestPartitionableSourceFactory.createTemporaryTable(util.tableEnv, "PartitionableTable", false)
  }

  @Test
  def testBoundedStreamTableSource(): Unit = {
    TestTableSource.createTemporaryTable(util.tableEnv, isBounded = true, tableSchema, "MyTable")
    util.verifyExecPlan("SELECT * FROM MyTable")
  }

  @Test
  def testUnboundedStreamTableSource(): Unit = {
    TestTableSource.createTemporaryTable(util.tableEnv, isBounded = false, tableSchema, "MyTable")
    util.verifyExecPlan("SELECT * FROM MyTable")
  }

  @Test
  def testFilterCanPushDown(): Unit = {
    util.verifyExecPlan("SELECT * FROM FilterableTable WHERE amount > 2")
  }

  @Test
  def testFilterCannotPushDown(): Unit = {
    // TestFilterableTableSource only accept predicates with `amount`
    util.verifyExecPlan("SELECT * FROM FilterableTable WHERE price > 10")
  }

  @Test
  def testFilterPartialPushDown(): Unit = {
    util.verifyExecPlan("SELECT * FROM FilterableTable WHERE amount > 2 AND price > 10")
  }

  @Test
  def testFilterFullyPushDown(): Unit = {
    util.verifyExecPlan("SELECT * FROM FilterableTable WHERE amount > 2 AND amount < 10")
  }

  @Test
  def testFilterCannotPushDown2(): Unit = {
    util.verifyExecPlan("SELECT * FROM FilterableTable WHERE amount > 2 OR price > 10")
  }

  @Test
  def testFilterCannotPushDown3(): Unit = {
    util.verifyExecPlan("SELECT * FROM FilterableTable WHERE amount > 2 OR amount < 10")
  }

  @Test
  def testFilterPushDownUnconvertedExpression(): Unit = {
    val sqlQuery =
      """
        |SELECT * FROM FilterableTable WHERE
        |    amount > 2 AND id < 100 AND CAST(amount AS BIGINT) > 10
      """.stripMargin
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testFilterPushDownWithUdf(): Unit = {
    util.addTemporarySystemFunction("myUdf", Func1)
    util.verifyExecPlan("SELECT * FROM FilterableTable WHERE amount > 2 AND myUdf(amount) < 32")
  }

  @Test
  def testPartitionTableSource(): Unit = {
    util.verifyExecPlan(
      "SELECT * FROM PartitionableTable WHERE part2 > 1 and id > 2 AND part1 = 'A' ")
  }

  @Test
  def testPartitionTableSourceWithUdf(): Unit = {
    util.addTemporarySystemFunction("MyUdf", Func1)
    util.verifyExecPlan("SELECT * FROM PartitionableTable WHERE id > 2 AND MyUdf(part2) < 3")
  }

  @Test
  def testTimeLiteralExpressionPushDown(): Unit = {
    val schema = TableSchema
      .builder()
      .field("id", DataTypes.INT)
      .field("dv", DataTypes.DATE)
      .field("tv", DataTypes.TIME)
      .field("tsv", DataTypes.TIMESTAMP(3))
      .build()

    val row = new Row(4)
    row.setField(0, 1)
    row.setField(1, DateTimeTestUtil.localDate("2017-01-23"))
    row.setField(2, DateTimeTestUtil.localTime("14:23:02"))
    row.setField(3, DateTimeTestUtil.localDateTime("2017-01-24 12:45:01.234"))

    TestLegacyFilterableTableSource.createTemporaryTable(
      util.tableEnv,
      schema,
      "FilterableTable1",
      isBounded = false,
      List(row),
      Set("dv", "tv", "tsv"))

    val sqlQuery =
      s"""
         |SELECT id FROM FilterableTable1 WHERE
         |  tv > TIME '14:25:02' AND
         |  dv > DATE '2017-02-03' AND
         |  tsv > TIMESTAMP '2017-02-03 14:25:02.000'
      """.stripMargin
    util.verifyExecPlan(sqlQuery)
  }
}
