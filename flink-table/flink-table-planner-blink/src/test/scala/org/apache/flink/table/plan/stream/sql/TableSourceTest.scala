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

package org.apache.flink.table.plan.stream.sql

import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, SqlTimeTypeInfo, TypeInformation}
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.table.api.{TableSchema, Types}
import org.apache.flink.table.expressions.utils.Func1
import org.apache.flink.table.util._
import org.apache.flink.types.Row

import org.junit.{Before, Test}

import java.sql.{Date, Time, Timestamp}
import java.util.TimeZone

class TableSourceTest extends TableTestBase {

  private val util = streamTestUtil()

  @Before
  def setup(): Unit = {
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"))
    util.tableEnv.registerTableSource("FilterableTable", TestFilterableTableSource(false))
  }

  @Test
  def testTableSourceWithLongRowTimeField(): Unit = {
    val tableSchema = new TableSchema(
      Array("id", "rowtime", "val", "name"),
      Array(Types.INT, Types.SQL_TIMESTAMP, Types.LONG, Types.STRING))
    val returnType = new RowTypeInfo(
      Array(Types.INT, Types.LONG, Types.LONG, Types.STRING)
        .asInstanceOf[Array[TypeInformation[_]]],
      Array("id", "rowtime", "val", "name"))

    util.tableEnv.registerTableSource(
      "rowTimeT",
      new TestTableSourceWithTime[Row](false, tableSchema, returnType, Seq(), rowtime = "rowtime"))

    util.verifyPlan("SELECT rowtime, id, name, val FROM rowTimeT")
  }

  @Test
  def testTableSourceWithTimestampRowTimeField(): Unit = {
    val tableSchema = new TableSchema(
      Array("id", "rowtime", "val", "name"),
      Array(Types.INT, Types.SQL_TIMESTAMP, Types.LONG, Types.STRING))
    val returnType = new RowTypeInfo(
      Array(Types.INT, Types.SQL_TIMESTAMP, Types.LONG, Types.STRING)
        .asInstanceOf[Array[TypeInformation[_]]],
      Array("id", "rowtime", "val", "name"))

    util.tableEnv.registerTableSource(
      "rowTimeT",
      new TestTableSourceWithTime[Row](false, tableSchema, returnType, Seq(), rowtime = "rowtime"))

    util.verifyPlan("SELECT rowtime, id, name, val FROM rowTimeT")
  }

  @Test
  def testRowTimeTableSourceGroupWindow(): Unit = {
    val tableSchema = new TableSchema(
      Array("id", "rowtime", "val", "name"),
      Array(Types.INT, Types.SQL_TIMESTAMP, Types.LONG, Types.STRING))
    val returnType = new RowTypeInfo(
      Array(Types.INT, Types.SQL_TIMESTAMP, Types.LONG, Types.STRING)
        .asInstanceOf[Array[TypeInformation[_]]],
      Array("id", "rowtime", "val", "name"))

    util.tableEnv.registerTableSource(
      "rowTimeT",
      new TestTableSourceWithTime[Row](false, tableSchema, returnType, Seq(), rowtime = "rowtime"))

    val sqlQuery =
      """
        |SELECT name,
        |    TUMBLE_END(rowtime, INTERVAL '10' MINUTE),
        |    AVG(val)
        |FROM rowTimeT WHERE val > 100
        |   GROUP BY name, TUMBLE(rowtime, INTERVAL '10' MINUTE)
      """.stripMargin

    util.verifyPlan(sqlQuery)
  }

  @Test
  def testProcTimeTableSourceSimple(): Unit = {
    val tableSchema = new TableSchema(
      Array("id", "pTime", "val", "name"),
      Array(Types.INT, Types.SQL_TIMESTAMP, Types.LONG, Types.STRING))
    val returnType = new RowTypeInfo(
      Array(Types.INT, Types.LONG, Types.STRING).asInstanceOf[Array[TypeInformation[_]]],
      Array("id", "val", "name"))

    util.tableEnv.registerTableSource(
      "procTimeT",
      new TestTableSourceWithTime[Row](false, tableSchema, returnType, Seq(), proctime = "pTime"))

    util.verifyPlan("SELECT pTime, id, name, val FROM procTimeT")
  }

  @Test
  def testProjectWithRowtimeProctime(): Unit = {
    val tableSchema = new TableSchema(
      Array("id", "rtime", "val", "ptime", "name"),
      Array(Types.INT, Types.SQL_TIMESTAMP, Types.LONG, Types.SQL_TIMESTAMP, Types.STRING))
    val returnType = new RowTypeInfo(
      Array(Types.INT, Types.STRING, Types.LONG, Types.LONG)
        .asInstanceOf[Array[TypeInformation[_]]],
      Array("id", "name", "val", "rtime"))

    util.tableEnv.registerTableSource(
      "T",
      new TestProjectableTableSource(false, tableSchema, returnType, Seq(), "rtime", "ptime"))

    util.verifyPlan("SELECT name, val, id FROM T")
  }

  @Test
  def testProjectWithoutRowtime(): Unit = {
    val tableSchema = new TableSchema(
      Array("id", "rtime", "val", "ptime", "name"),
      Array(Types.INT, Types.SQL_TIMESTAMP, Types.LONG, Types.SQL_TIMESTAMP, Types.STRING))
    val returnType = new RowTypeInfo(
      Array(Types.INT, Types.STRING, Types.LONG, Types.LONG)
        .asInstanceOf[Array[TypeInformation[_]]],
      Array("id", "name", "val", "rtime"))

    util.tableEnv.registerTableSource(
      "T",
      new TestProjectableTableSource(false, tableSchema, returnType, Seq(), "rtime", "ptime"))

    util.verifyPlan("SELECT ptime, name, val, id FROM T")
  }

  def testProjectWithoutProctime(): Unit = {
    val tableSchema = new TableSchema(
      Array("id", "rtime", "val", "ptime", "name"),
      Array(Types.INT, Types.SQL_TIMESTAMP, Types.LONG, Types.SQL_TIMESTAMP, Types.STRING))
    val returnType = new RowTypeInfo(
      Array(Types.INT, Types.LONG, Types.LONG, Types.STRING)
        .asInstanceOf[Array[TypeInformation[_]]],
      Array("id", "rtime", "val", "name"))

    val util = streamTestUtil()
    util.tableEnv.registerTableSource(
      "T",
      new TestProjectableTableSource(false, tableSchema, returnType, Seq(), "rtime", "ptime"))

    util.verifyPlan("select name, val, rtime, id from T")
  }

  def testProjectOnlyProctime(): Unit = {
    val tableSchema = new TableSchema(
      Array("id", "rtime", "val", "ptime", "name"),
      Array(Types.INT, Types.SQL_TIMESTAMP, Types.LONG, Types.SQL_TIMESTAMP, Types.STRING))
    val returnType = new RowTypeInfo(
      Array(Types.INT, Types.LONG, Types.LONG, Types.STRING)
        .asInstanceOf[Array[TypeInformation[_]]],
      Array("id", "rtime", "val", "name"))

    util.tableEnv.registerTableSource(
      "T",
      new TestProjectableTableSource(false, tableSchema, returnType, Seq(), "rtime", "ptime"))

    util.verifyPlan("SELECT ptime FROM T")
  }

  def testProjectOnlyRowtime(): Unit = {
    val tableSchema = new TableSchema(
      Array("id", "rtime", "val", "ptime", "name"),
      Array(Types.INT, Types.SQL_TIMESTAMP, Types.LONG, Types.SQL_TIMESTAMP, Types.STRING))
    val returnType = new RowTypeInfo(
      Array(Types.INT, Types.LONG, Types.LONG, Types.STRING)
        .asInstanceOf[Array[TypeInformation[_]]],
      Array("id", "rtime", "val", "name"))

    util.tableEnv.registerTableSource(
      "T",
      new TestProjectableTableSource(false, tableSchema, returnType, Seq(), "rtime", "ptime"))

    util.verifyPlan("SELECT rtime FROM T")
  }

  @Test
  def testProjectWithMapping(): Unit = {
    val tableSchema = new TableSchema(
      Array("id", "rtime", "val", "ptime", "name"),
      Array(Types.INT, Types.SQL_TIMESTAMP, Types.LONG, Types.SQL_TIMESTAMP, Types.STRING))
    val returnType = new RowTypeInfo(
      Array(Types.LONG, Types.INT, Types.STRING, Types.LONG)
        .asInstanceOf[Array[TypeInformation[_]]],
      Array("p-rtime", "p-id", "p-name", "p-val"))
    val mapping = Map("rtime" -> "p-rtime", "id" -> "p-id", "val" -> "p-val", "name" -> "p-name")

    util.tableEnv.registerTableSource(
      "T",
      new TestProjectableTableSource(
        false, tableSchema, returnType, Seq(), "rtime", "ptime", mapping))

    util.verifyPlan("SELECT name, rtime, val FROM T")
  }

  @Test
  def testNestedProject(): Unit = {
    val nested1 = new RowTypeInfo(
      Array(Types.STRING, Types.INT).asInstanceOf[Array[TypeInformation[_]]],
      Array("name", "value")
    )

    val nested2 = new RowTypeInfo(
      Array(Types.INT, Types.BOOLEAN).asInstanceOf[Array[TypeInformation[_]]],
      Array("num", "flag")
    )

    val deepNested = new RowTypeInfo(
      Array(nested1, nested2).asInstanceOf[Array[TypeInformation[_]]],
      Array("nested1", "nested2")
    )

    val tableSchema = new TableSchema(
      Array("id", "deepNested", "nested", "name"),
      Array(Types.INT, deepNested, nested1, Types.STRING))

    val returnType = new RowTypeInfo(
      Array(Types.INT, deepNested, nested1, Types.STRING).asInstanceOf[Array[TypeInformation[_]]],
      Array("id", "deepNested", "nested", "name"))

    util.tableEnv.registerTableSource(
      "T",
      new TestNestedProjectableTableSource(false, tableSchema, returnType, Seq()))

    val sqlQuery =
      """
        |SELECT id,
        |    deepNested.nested1.name AS nestedName,
        |    nested.`value` AS nestedValue,
        |    deepNested.nested2.flag AS nestedFlag,
        |    deepNested.nested2.num AS nestedNum
        |FROM T
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testProjectWithoutInputRef(): Unit = {
    val tableSchema = new TableSchema(Array("id", "name"), Array(Types.INT, Types.STRING))
    val returnType = new RowTypeInfo(
      Array(Types.INT, Types.STRING).asInstanceOf[Array[TypeInformation[_]]],
      Array("id", "name"))

    util.tableEnv.registerTableSource(
      "T",
      new TestProjectableTableSource(false, tableSchema, returnType, Seq(), null, null))

    util.verifyPlan("SELECT COUNT(1) FROM T")
  }

  @Test
  def testFilterCanPushDown(): Unit = {
    util.verifyPlan("SELECT * FROM FilterableTable WHERE amount > 2")
  }

  @Test
  def testFilterCannotPushDown(): Unit = {
    // TestFilterableTableSource only accept predicates with `amount`
    util.verifyPlan("SELECT * FROM FilterableTable WHERE price > 10")
  }

  @Test
  def testFilterPartialPushDown(): Unit = {
    util.verifyPlan("SELECT * FROM FilterableTable WHERE amount > 2 AND price > 10")
  }

  @Test
  def testFilterFullyPushDown(): Unit = {
    util.verifyPlan("SELECT * FROM FilterableTable WHERE amount > 2 AND amount < 10")
  }

  @Test
  def testFilterCannotPushDown2(): Unit = {
    util.verifyPlan("SELECT * FROM FilterableTable WHERE amount > 2 OR price > 10")
  }

  @Test
  def testFilterCannotPushDown3(): Unit = {
    util.verifyPlan("SELECT * FROM FilterableTable WHERE amount > 2 OR amount < 10")
  }

  @Test
  def testFilterPushDownUnconvertedExpression(): Unit = {
    val sqlQuery =
      """
        |SELECT * FROM FilterableTable WHERE
        |    amount > 2 AND id < 100 AND CAST(amount AS BIGINT) > 10
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testFilterPushDownWithUdf(): Unit = {
    util.addFunction("myUdf", Func1)
    util.verifyPlan("SELECT * FROM FilterableTable WHERE amount > 2 AND myUdf(amount) < 32")
  }

  @Test
  def testTimeLiteralExpressionPushDown(): Unit = {
    val rowTypeInfo = new RowTypeInfo(
      Array[TypeInformation[_]](
        BasicTypeInfo.INT_TYPE_INFO,
        SqlTimeTypeInfo.DATE,
        SqlTimeTypeInfo.TIME,
        SqlTimeTypeInfo.TIMESTAMP
      ),
      Array("id", "dv", "tv", "tsv")
    )

    val row = new Row(4)
    row.setField(0, 1)
    row.setField(1, Date.valueOf("2017-01-23"))
    row.setField(2, Time.valueOf("14:23:02"))
    row.setField(3, Timestamp.valueOf("2017-01-24 12:45:01.234"))

    val tableSource = TestFilterableTableSource(
      isBatch = false, rowTypeInfo, Seq(row), Set("dv", "tv", "tsv"))
    util.tableEnv.registerTableSource("FilterableTable1", tableSource)

    val sqlQuery =
      s"""
         |SELECT id FROM FilterableTable1 WHERE
         |  tv > TIME '14:25:02' AND
         |  dv > DATE '2017-02-03' AND
         |  tsv > TIMESTAMP '2017-02-03 14:25:02.000'
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }
}
