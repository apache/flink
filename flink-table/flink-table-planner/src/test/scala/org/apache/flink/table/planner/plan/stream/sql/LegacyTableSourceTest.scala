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

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.table.api.{DataTypes, TableSchema, Types}
import org.apache.flink.table.api.internal.TableEnvironmentInternal
import org.apache.flink.table.planner.expressions.utils.Func1
import org.apache.flink.table.planner.utils._
import org.apache.flink.types.Row

import org.junit.{Before, Test}

class LegacyTableSourceTest extends TableTestBase {

  private val util = streamTestUtil()

  private val tableSchema = TableSchema
    .builder()
    .fields(Array("a", "b", "c"), Array(DataTypes.INT(), DataTypes.BIGINT(), DataTypes.STRING()))
    .build()

  @Before
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
  def testTableSourceWithLongRowTimeField(): Unit = {
    val tableSchema = new TableSchema(
      Array("id", "rowtime", "val", "name"),
      Array(Types.INT, Types.LOCAL_DATE_TIME, Types.LONG, Types.STRING))
    val returnType = new RowTypeInfo(
      Array(Types.INT, Types.LONG, Types.LONG, Types.STRING)
        .asInstanceOf[Array[TypeInformation[_]]],
      Array("id", "rowtime", "val", "name"))

    util.tableEnv
      .asInstanceOf[TableEnvironmentInternal]
      .registerTableSourceInternal(
        "rowTimeT",
        new TestTableSourceWithTime[Row](
          false,
          tableSchema,
          returnType,
          Seq(),
          rowtime = "rowtime"))

    util.verifyExecPlan("SELECT rowtime, id, name, val FROM rowTimeT")
  }

  @Test
  def testTableSourceWithTimestampRowTimeField(): Unit = {
    val tableSchema = new TableSchema(
      Array("id", "rowtime", "val", "name"),
      Array(Types.INT, Types.LOCAL_DATE_TIME, Types.LONG, Types.STRING))
    val returnType = new RowTypeInfo(
      Array(Types.INT, Types.LOCAL_DATE_TIME, Types.LONG, Types.STRING)
        .asInstanceOf[Array[TypeInformation[_]]],
      Array("id", "rowtime", "val", "name"))

    util.tableEnv
      .asInstanceOf[TableEnvironmentInternal]
      .registerTableSourceInternal(
        "rowTimeT",
        new TestTableSourceWithTime[Row](
          false,
          tableSchema,
          returnType,
          Seq(),
          rowtime = "rowtime"))

    util.verifyExecPlan("SELECT rowtime, id, name, val FROM rowTimeT")
  }

  @Test
  def testRowTimeTableSourceGroupWindow(): Unit = {
    val tableSchema = new TableSchema(
      Array("id", "rowtime", "val", "name"),
      Array(Types.INT, Types.LOCAL_DATE_TIME, Types.LONG, Types.STRING))
    val returnType = new RowTypeInfo(
      Array(Types.INT, Types.LOCAL_DATE_TIME, Types.LONG, Types.STRING)
        .asInstanceOf[Array[TypeInformation[_]]],
      Array("id", "rowtime", "val", "name"))

    util.tableEnv
      .asInstanceOf[TableEnvironmentInternal]
      .registerTableSourceInternal(
        "rowTimeT",
        new TestTableSourceWithTime[Row](
          false,
          tableSchema,
          returnType,
          Seq(),
          rowtime = "rowtime"))

    val sqlQuery =
      """
        |SELECT name,
        |    TUMBLE_END(rowtime, INTERVAL '10' MINUTE),
        |    AVG(val)
        |FROM rowTimeT WHERE val > 100
        |   GROUP BY name, TUMBLE(rowtime, INTERVAL '10' MINUTE)
      """.stripMargin

    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testLegacyRowTimeTableGroupWindow(): Unit = {
    util.tableEnv.executeSql("""
                               |CREATE TEMPORARY TABLE rowTimeT (
                               |  id INT,
                               |  val BIGINT,
                               |  name STRING,
                               |  rowtime TIMESTAMP(3)
                               |) WITH (
                               |  'connector.type' = 'TestTableSourceWithTime',
                               |  'schema.3.rowtime.timestamps.type' = 'from-field',
                               |  'schema.3.rowtime.timestamps.from' = 'rowtime',
                               |  'schema.3.rowtime.watermarks.type' = 'periodic-bounded',
                               |  'schema.3.rowtime.watermarks.delay' = '1000'
                               |)
                               |""".stripMargin)

    val sql =
      """
        |SELECT name,
        |    TUMBLE_END(rowtime, INTERVAL '10' MINUTE),
        |    AVG(val)
        |FROM rowTimeT WHERE val > 100
        |    GROUP BY name, TUMBLE(rowtime, INTERVAL '10' MINUTE)
      """.stripMargin

    util.verifyExecPlan(sql)
  }

  @Test
  def testLegacyProcTimeTableGroupWindow(): Unit = {
    util.tableEnv.executeSql("""
                               |CREATE TEMPORARY TABLE procTimeT (
                               |  id INT,
                               |  val BIGINT,
                               |  name STRING,
                               |  `proctime` TIMESTAMP_LTZ(3)
                               |) WITH (
                               |  'connector.type' = 'TestTableSourceWithTime',
                               |  'schema.3.proctime' = 'true'
                               |)
                               |""".stripMargin)

    val sql =
      """
        |SELECT name,
        |    TUMBLE_END(proctime, INTERVAL '10' MINUTE),
        |    AVG(val)
        |FROM procTimeT WHERE val > 100
        |    GROUP BY name, TUMBLE(proctime, INTERVAL '10' MINUTE)
      """.stripMargin

    util.verifyExecPlan(sql)
  }

  @Test
  def testProcTimeTableSourceSimple(): Unit = {
    val tableSchema = new TableSchema(
      Array("id", "pTime", "val", "name"),
      Array(Types.INT, Types.LOCAL_DATE_TIME, Types.LONG, Types.STRING))
    val returnType = new RowTypeInfo(
      Array(Types.INT, Types.LONG, Types.STRING).asInstanceOf[Array[TypeInformation[_]]],
      Array("id", "val", "name"))

    util.tableEnv
      .asInstanceOf[TableEnvironmentInternal]
      .registerTableSourceInternal(
        "procTimeT",
        new TestTableSourceWithTime[Row](false, tableSchema, returnType, Seq(), proctime = "pTime"))

    util.verifyExecPlan("SELECT pTime, id, name, val FROM procTimeT")
  }

  @Test
  def testProjectWithRowtimeProctime(): Unit = {
    val tableSchema = new TableSchema(
      Array("id", "rtime", "val", "ptime", "name"),
      Array(Types.INT, Types.LOCAL_DATE_TIME, Types.LONG, Types.LOCAL_DATE_TIME, Types.STRING))
    val returnType = new RowTypeInfo(
      Array(Types.INT, Types.STRING, Types.LONG, Types.LONG)
        .asInstanceOf[Array[TypeInformation[_]]],
      Array("id", "name", "val", "rtime"))

    util.tableEnv
      .asInstanceOf[TableEnvironmentInternal]
      .registerTableSourceInternal(
        "T",
        new TestLegacyProjectableTableSource(
          false,
          tableSchema,
          returnType,
          Seq(),
          "rtime",
          "ptime"))

    util.verifyExecPlan("SELECT name, val, id FROM T")
  }

  @Test
  def testProjectWithoutRowtime(): Unit = {
    val tableSchema = new TableSchema(
      Array("id", "rtime", "val", "ptime", "name"),
      Array(Types.INT, Types.LOCAL_DATE_TIME, Types.LONG, Types.LOCAL_DATE_TIME, Types.STRING))
    val returnType = new RowTypeInfo(
      Array(Types.INT, Types.STRING, Types.LONG, Types.LONG)
        .asInstanceOf[Array[TypeInformation[_]]],
      Array("id", "name", "val", "rtime"))

    util.tableEnv
      .asInstanceOf[TableEnvironmentInternal]
      .registerTableSourceInternal(
        "T",
        new TestLegacyProjectableTableSource(
          false,
          tableSchema,
          returnType,
          Seq(),
          "rtime",
          "ptime"))

    util.verifyExecPlan("SELECT ptime, name, val, id FROM T")
  }

  def testProjectWithoutProctime(): Unit = {
    val tableSchema = new TableSchema(
      Array("id", "rtime", "val", "ptime", "name"),
      Array(Types.INT, Types.LOCAL_DATE_TIME, Types.LONG, Types.LOCAL_DATE_TIME, Types.STRING))
    val returnType = new RowTypeInfo(
      Array(Types.INT, Types.LONG, Types.LONG, Types.STRING)
        .asInstanceOf[Array[TypeInformation[_]]],
      Array("id", "rtime", "val", "name"))

    val util = streamTestUtil()
    util.tableEnv
      .asInstanceOf[TableEnvironmentInternal]
      .registerTableSourceInternal(
        "T",
        new TestLegacyProjectableTableSource(
          false,
          tableSchema,
          returnType,
          Seq(),
          "rtime",
          "ptime"))

    util.verifyExecPlan("select name, val, rtime, id from T")
  }

  def testProjectOnlyProctime(): Unit = {
    val tableSchema = new TableSchema(
      Array("id", "rtime", "val", "ptime", "name"),
      Array(Types.INT, Types.LOCAL_DATE_TIME, Types.LONG, Types.LOCAL_DATE_TIME, Types.STRING))
    val returnType = new RowTypeInfo(
      Array(Types.INT, Types.LONG, Types.LONG, Types.STRING)
        .asInstanceOf[Array[TypeInformation[_]]],
      Array("id", "rtime", "val", "name"))

    util.tableEnv
      .asInstanceOf[TableEnvironmentInternal]
      .registerTableSourceInternal(
        "T",
        new TestLegacyProjectableTableSource(
          false,
          tableSchema,
          returnType,
          Seq(),
          "rtime",
          "ptime"))

    util.verifyExecPlan("SELECT ptime FROM T")
  }

  def testProjectOnlyRowtime(): Unit = {
    val tableSchema = new TableSchema(
      Array("id", "rtime", "val", "ptime", "name"),
      Array(Types.INT, Types.LOCAL_DATE_TIME, Types.LONG, Types.LOCAL_DATE_TIME, Types.STRING))
    val returnType = new RowTypeInfo(
      Array(Types.INT, Types.LONG, Types.LONG, Types.STRING)
        .asInstanceOf[Array[TypeInformation[_]]],
      Array("id", "rtime", "val", "name"))

    util.tableEnv
      .asInstanceOf[TableEnvironmentInternal]
      .registerTableSourceInternal(
        "T",
        new TestLegacyProjectableTableSource(
          false,
          tableSchema,
          returnType,
          Seq(),
          "rtime",
          "ptime"))

    util.verifyExecPlan("SELECT rtime FROM T")
  }

  @Test
  def testProjectWithMapping(): Unit = {
    val tableSchema = new TableSchema(
      Array("id", "rtime", "val", "ptime", "name"),
      Array(Types.INT, Types.LOCAL_DATE_TIME, Types.LONG, Types.LOCAL_DATE_TIME, Types.STRING))
    val returnType = new RowTypeInfo(
      Array(Types.LONG, Types.INT, Types.STRING, Types.LONG)
        .asInstanceOf[Array[TypeInformation[_]]],
      Array("p-rtime", "p-id", "p-name", "p-val"))
    val mapping = Map("rtime" -> "p-rtime", "id" -> "p-id", "val" -> "p-val", "name" -> "p-name")

    util.tableEnv
      .asInstanceOf[TableEnvironmentInternal]
      .registerTableSourceInternal(
        "T",
        new TestLegacyProjectableTableSource(
          false,
          tableSchema,
          returnType,
          Seq(),
          "rtime",
          "ptime",
          mapping))

    util.verifyExecPlan("SELECT name, rtime, val FROM T")
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

    util.tableEnv
      .asInstanceOf[TableEnvironmentInternal]
      .registerTableSourceInternal(
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
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testProjectWithoutInputRef(): Unit = {
    val tableSchema = new TableSchema(Array("id", "name"), Array(Types.INT, Types.STRING))
    val returnType = new RowTypeInfo(
      Array(Types.INT, Types.STRING).asInstanceOf[Array[TypeInformation[_]]],
      Array("id", "name"))

    util.tableEnv
      .asInstanceOf[TableEnvironmentInternal]
      .registerTableSourceInternal(
        "T",
        new TestLegacyProjectableTableSource(false, tableSchema, returnType, Seq(), null, null))

    util.verifyExecPlan("SELECT COUNT(1) FROM T")
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
    util.addFunction("myUdf", Func1)
    util.verifyExecPlan("SELECT * FROM FilterableTable WHERE amount > 2 AND myUdf(amount) < 32")
  }

  @Test
  def testPartitionTableSource(): Unit = {
    util.verifyExecPlan(
      "SELECT * FROM PartitionableTable WHERE part2 > 1 and id > 2 AND part1 = 'A' ")
  }

  @Test
  def testPartitionTableSourceWithUdf(): Unit = {
    util.addFunction("MyUdf", Func1)
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
