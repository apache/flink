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

package org.apache.flink.table.planner.catalog

import org.apache.flink.table.api.config.{ExecutionConfigOptions, TableConfigOptions}
import org.apache.flink.table.api.internal.TableEnvironmentImpl
import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment, ValidationException}
import org.apache.flink.table.catalog.{CatalogDatabaseImpl, CatalogFunctionImpl, GenericInMemoryCatalog, ObjectPath}
import org.apache.flink.table.planner.expressions.utils.Func0
import org.apache.flink.table.planner.factories.utils.TestCollectionTableFactory
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedScalarFunctions.JavaFunc0
import org.apache.flink.table.planner.runtime.utils.TableEnvUtil.{execInsertSqlAndWaitResult, execInsertTableAndWaitResult}
import org.apache.flink.table.planner.utils.DateTimeTestUtil.localDateTime
import org.apache.flink.test.util.AbstractTestBase
import org.apache.flink.types.Row
import org.apache.flink.util.FileUtils

import org.junit.Assert.{assertEquals, fail}
import org.junit.rules.ExpectedException
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.{Before, Rule, Test}

import java.io.File
import java.math.{BigDecimal => JBigDecimal}
import java.net.URI
import java.util

import scala.collection.JavaConversions._

/** Test cases for catalog table. */
@RunWith(classOf[Parameterized])
class CatalogTableITCase(isStreamingMode: Boolean) extends AbstractTestBase {
  //~ Instance fields --------------------------------------------------------

  private val settings = if (isStreamingMode) {
    EnvironmentSettings.newInstance().inStreamingMode().build()
  } else {
    EnvironmentSettings.newInstance().inBatchMode().build()
  }

  private val tableEnv: TableEnvironment = TableEnvironmentImpl.create(settings)

  var _expectedEx: ExpectedException = ExpectedException.none

  @Rule
  def expectedEx: ExpectedException = _expectedEx

  @Before
  def before(): Unit = {
    tableEnv.getConfig.getConfiguration.setBoolean(
      TableConfigOptions.TABLE_DYNAMIC_TABLE_OPTIONS_ENABLED,
      true)

    tableEnv.getConfig
      .getConfiguration
      .setInteger(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 1)
    TestCollectionTableFactory.reset()

    val func = new CatalogFunctionImpl(
      classOf[JavaFunc0].getName)
    tableEnv.getCatalog(tableEnv.getCurrentCatalog).get().createFunction(
      new ObjectPath(tableEnv.getCurrentDatabase, "myfunc"),
      func,
      true)
  }

  //~ Tools ------------------------------------------------------------------

  implicit def rowOrdering: Ordering[Row] = Ordering.by((r : Row) => {
    val builder = new StringBuilder
    0 until r.getArity foreach(idx => builder.append(r.getField(idx)))
    builder.toString()
  })

  def toRow(args: Any*):Row = {
    val row = new Row(args.length)
    0 until args.length foreach {
      i => row.setField(i, args(i))
    }
    row
  }

  //~ Tests ------------------------------------------------------------------

  private def testUdf(funcPrefix: String): Unit = {
    val sinkDDL =
      s"""
        |create table sinkT(
        |  a bigint
        |) with (
        |  'connector' = 'COLLECTION',
        |  'is-bounded' = '$isStreamingMode'
        |)
      """.stripMargin
    tableEnv.executeSql(sinkDDL)
    execInsertSqlAndWaitResult(
      tableEnv, s"insert into sinkT select ${funcPrefix}myfunc(cast(1 as bigint))")
    assertEquals(Seq(toRow(2L)), TestCollectionTableFactory.RESULT.sorted)
  }

  @Test
  def testUdfWithFullIdentifier(): Unit = {
    testUdf("default_catalog.default_database.")
  }

  @Test
  def testUdfWithDatabase(): Unit = {
    testUdf("default_database.")
  }

  @Test
  def testUdfWithNon(): Unit = {
    testUdf("")
  }

  @Test(expected = classOf[ValidationException])
  def testUdfWithWrongCatalog(): Unit = {
    testUdf("wrong_catalog.default_database.")
  }

  @Test(expected = classOf[ValidationException])
  def testUdfWithWrongDatabase(): Unit = {
    testUdf("default_catalog.wrong_database.")
  }

  @Test
  def testInsertInto(): Unit = {
    val sourceData = List(
      toRow(1, "1000", 2, new JBigDecimal("10.001")),
      toRow(2, "1", 3, new JBigDecimal("10.001")),
      toRow(3, "2000", 4, new JBigDecimal("10.001")),
      toRow(1, "2", 2, new JBigDecimal("10.001")),
      toRow(2, "3000", 3, new JBigDecimal("10.001"))
    )
    TestCollectionTableFactory.initData(sourceData)
    val sourceDDL =
      """
        |create table t1(
        |  a int,
        |  b varchar,
        |  c int,
        |  d DECIMAL(10, 3)
        |) with (
        |  'connector' = 'COLLECTION'
        |)
      """.stripMargin
    val sinkDDL =
      """
        |create table t2(
        |  a int,
        |  b varchar,
        |  c int,
        |  d DECIMAL(10, 3)
        |) with (
        |  'connector' = 'COLLECTION'
        |)
      """.stripMargin
    val query =
      """
        |insert into t2
        |select t1.a, t1.b, (t1.a + 1) as c , d from t1
      """.stripMargin
    tableEnv.executeSql(sourceDDL)
    tableEnv.executeSql(sinkDDL)
    execInsertSqlAndWaitResult(tableEnv, query)
    assertEquals(sourceData.sorted, TestCollectionTableFactory.RESULT.sorted)
  }

  @Test
  def testReadWriteCsvUsingDDL(): Unit = {
    val csvRecords = Seq(
      "2.02,Euro,2019-12-12 00:00:01.001001",
      "1.11,US Dollar,2019-12-12 00:00:02.002001",
      "50,Yen,2019-12-12 00:00:04.004001",
      "3.1,Euro,2019-12-12 00:00:05.005001",
      "5.33,US Dollar,2019-12-12 00:00:06.006001"
    )
    val tempFilePath = createTempFile(
      "csv-order-test",
      csvRecords.mkString("#"))
    val sourceDDL =
      s"""
       |CREATE TABLE T1 (
       |  price DECIMAL(10, 2),
       |  currency STRING,
       |  ts6 TIMESTAMP(6),
       |  ts AS CAST(ts6 AS TIMESTAMP(3)),
       |  WATERMARK FOR ts AS ts
       |) WITH (
       |  'connector.type' = 'filesystem',
       |  'connector.path' = '$tempFilePath',
       |  'format.type' = 'csv',
       |  'format.field-delimiter' = ',',
       |  'format.line-delimiter' = '#'
       |)
     """.stripMargin
    tableEnv.executeSql(sourceDDL)

    val sinkFilePath = getTempFilePath("csv-order-sink")
    val sinkDDL =
      s"""
        |CREATE TABLE T2 (
        |  window_end TIMESTAMP(3),
        |  max_ts TIMESTAMP(6),
        |  counter BIGINT,
        |  total_price DECIMAL(10, 2)
        |) with (
        |  'connector.type' = 'filesystem',
        |  'connector.path' = '$sinkFilePath',
        |  'format.type' = 'csv',
        |  'format.field-delimiter' = ','
        |)
      """.stripMargin
    tableEnv.executeSql(sinkDDL)

    val query =
      """
        |INSERT INTO T2
        |SELECT
        |  TUMBLE_END(ts, INTERVAL '5' SECOND),
        |  MAX(ts6),
        |  COUNT(*),
        |  MAX(price)
        |FROM T1
        |GROUP BY TUMBLE(ts, INTERVAL '5' SECOND)
      """.stripMargin
    execInsertSqlAndWaitResult(tableEnv, query)

    val expected =
      "2019-12-12 00:00:05.0,2019-12-12 00:00:04.004001,3,50.00\n" +
      "2019-12-12 00:00:10.0,2019-12-12 00:00:06.006001,2,5.33\n"
    assertEquals(expected, FileUtils.readFileUtf8(new File(new URI(sinkFilePath))))
  }

  @Test
  def testReadWriteCsvWithDynamicTableOptions(): Unit = {
    val csvRecords = Seq(
      "2.02,Euro,2019-12-12 00:00:01.001001",
      "1.11,US Dollar,2019-12-12 00:00:02.002001",
      "50,Yen,2019-12-12 00:00:04.004001",
      "3.1,Euro,2019-12-12 00:00:05.005001",
      "5.33,US Dollar,2019-12-12 00:00:06.006001"
    )
    val tempFilePath = createTempFile(
      "csv-order-test",
      csvRecords.mkString("#"))
    val sourceDDL =
      s"""
         |CREATE TABLE T1 (
         |  price DECIMAL(10, 2),
         |  currency STRING,
         |  ts6 TIMESTAMP(6),
         |  ts AS CAST(ts6 AS TIMESTAMP(3)),
         |  WATERMARK FOR ts AS ts
         |) WITH (
         |  'connector.type' = 'filesystem',
         |  'connector.path' = '$tempFilePath',
         |  'format.type' = 'csv',
         |  'format.field-delimiter' = ','
         |)
     """.stripMargin
    tableEnv.executeSql(sourceDDL)

    val sinkFilePath = getTempFilePath("csv-order-sink")
    val sinkDDL =
      s"""
         |CREATE TABLE T2 (
         |  window_end TIMESTAMP(3),
         |  max_ts TIMESTAMP(6),
         |  counter BIGINT,
         |  total_price DECIMAL(10, 2)
         |) with (
         |  'connector.type' = 'filesystem',
         |  'connector.path' = '$sinkFilePath',
         |  'format.type' = 'csv'
         |)
      """.stripMargin
    tableEnv.executeSql(sinkDDL)

    val query =
      """
        |INSERT INTO T2 /*+ OPTIONS('format.field-delimiter' = '|') */
        |SELECT
        |  TUMBLE_END(ts, INTERVAL '5' SECOND),
        |  MAX(ts6),
        |  COUNT(*),
        |  MAX(price)
        |FROM T1 /*+ OPTIONS('format.line-delimiter' = '#') */
        |GROUP BY TUMBLE(ts, INTERVAL '5' SECOND)
      """.stripMargin
    execInsertSqlAndWaitResult(tableEnv, query)

    val expected =
      "2019-12-12 00:00:05.0|2019-12-12 00:00:04.004001|3|50.00\n" +
        "2019-12-12 00:00:10.0|2019-12-12 00:00:06.006001|2|5.33\n"
    assertEquals(expected, FileUtils.readFileUtf8(new File(new URI(sinkFilePath))))
  }

  @Test
  def testInsertSourceTableExpressionFields(): Unit = {
    val sourceData = List(
      toRow(1, "1000"),
      toRow(2, "1"),
      toRow(3, "2000"),
      toRow(1, "2"),
      toRow(2, "3000")
    )
    val expected = List(
      toRow(1, "1000", 2),
      toRow(2, "1", 3),
      toRow(3, "2000", 4),
      toRow(1, "2", 2),
      toRow(2, "3000", 3)
    )
    TestCollectionTableFactory.initData(sourceData)
    val sourceDDL =
      """
        |create table t1(
        |  a int,
        |  b varchar,
        |  c as a + 1
        |) with (
        |  'connector' = 'COLLECTION'
        |)
      """.stripMargin
    val sinkDDL =
      """
        |create table t2(
        |  a int,
        |  b varchar,
        |  c int
        |) with (
        |  'connector' = 'COLLECTION'
        |)
      """.stripMargin
    val query =
      """
        |insert into t2
        |select t1.a, t1.b, t1.c from t1
      """.stripMargin
    tableEnv.executeSql(sourceDDL)
    tableEnv.executeSql(sinkDDL)
    execInsertSqlAndWaitResult(tableEnv, query)
    assertEquals(expected.sorted, TestCollectionTableFactory.RESULT.sorted)
  }

  // Test the computation expression in front of referenced columns.
  @Test
  def testInsertSourceTableExpressionFieldsBeforeReferences(): Unit = {
    val sourceData = List(
      toRow(1, "1000"),
      toRow(2, "1"),
      toRow(3, "2000"),
      toRow(2, "2"),
      toRow(2, "3000")
    )
    val expected = List(
      toRow(101, 1, "1000"),
      toRow(102, 2, "1"),
      toRow(103, 3, "2000"),
      toRow(102, 2, "2"),
      toRow(102, 2, "3000")
    )
    TestCollectionTableFactory.initData(sourceData)
    val sourceDDL =
      """
        |create table t1(
        |  c as a + 100,
        |  a int,
        |  b varchar
        |) with (
        |  'connector' = 'COLLECTION'
        |)
      """.stripMargin
    val sinkDDL =
      """
        |create table t2(
        |  c int,
        |  a int,
        |  b varchar
        |) with (
        |  'connector' = 'COLLECTION'
        |)
      """.stripMargin
    val query =
      """
        |insert into t2
        |select t1.c, t1.a, t1.b from t1
      """.stripMargin
    tableEnv.executeSql(sourceDDL)
    tableEnv.executeSql(sinkDDL)
    execInsertSqlAndWaitResult(tableEnv, query)
    assertEquals(expected.sorted, TestCollectionTableFactory.RESULT.sorted)
  }

  @Test
  def testInsertSourceTableWithFuncField(): Unit = {
    val sourceData = List(
      toRow(1, "1990-02-10 12:34:56"),
      toRow(2, "2019-09-10 09:23:41"),
      toRow(3, "2019-09-10 09:23:42"),
      toRow(1, "2019-09-10 09:23:43"),
      toRow(2, "2019-09-10 09:23:44")
    )
    val expected = List(
      toRow(1, "1990-02-10 12:34:56", localDateTime("1990-02-10 12:34:56")),
      toRow(2, "2019-09-10 09:23:41", localDateTime("2019-09-10 09:23:41")),
      toRow(3, "2019-09-10 09:23:42", localDateTime("2019-09-10 09:23:42")),
      toRow(1, "2019-09-10 09:23:43", localDateTime("2019-09-10 09:23:43")),
      toRow(2, "2019-09-10 09:23:44", localDateTime("2019-09-10 09:23:44"))
    )
    TestCollectionTableFactory.initData(sourceData)
    val sourceDDL =
      """
        |create table t1(
        |  a int,
        |  b varchar,
        |  c as to_timestamp(b)
        |) with (
        |  'connector' = 'COLLECTION'
        |)
      """.stripMargin
    val sinkDDL =
      """
        |create table t2(
        |  a int,
        |  b varchar,
        |  c timestamp(3)
        |) with (
        |  'connector' = 'COLLECTION'
        |)
      """.stripMargin
    val query =
      """
        |insert into t2
        |select t1.a, t1.b, t1.c from t1
      """.stripMargin
    tableEnv.executeSql(sourceDDL)
    tableEnv.executeSql(sinkDDL)
    execInsertSqlAndWaitResult(tableEnv, query)
    assertEquals(expected.sorted, TestCollectionTableFactory.RESULT.sorted)
  }

  @Test
  def testInsertSourceTableWithUserDefinedFuncField(): Unit = {
    val sourceData = List(
      toRow(1, "1990-02-10 12:34:56"),
      toRow(2, "2019-09-10 9:23:41"),
      toRow(3, "2019-09-10 9:23:42"),
      toRow(1, "2019-09-10 9:23:43"),
      toRow(2, "2019-09-10 9:23:44")
    )
    val expected = List(
      toRow(1, "1990-02-10 12:34:56", 1, "1990-02-10 12:34:56"),
      toRow(2, "2019-09-10 9:23:41", 2, "2019-09-10 9:23:41"),
      toRow(3, "2019-09-10 9:23:42", 3, "2019-09-10 9:23:42"),
      toRow(1, "2019-09-10 9:23:43", 1, "2019-09-10 9:23:43"),
      toRow(2, "2019-09-10 9:23:44", 2, "2019-09-10 9:23:44")
    )
    TestCollectionTableFactory.initData(sourceData)
    tableEnv.registerFunction("my_udf", Func0)
    val sourceDDL =
      """
        |create table t1(
        |  a int,
        |  `time` varchar,
        |  c as my_udf(a),
        |  d as `time`
        |) with (
        |  'connector' = 'COLLECTION'
        |)
      """.stripMargin
    val sinkDDL =
      """
        |create table t2(
        |  a int,
        |  `time` varchar,
        |  c int not null,
        |  d varchar
        |) with (
        |  'connector' = 'COLLECTION'
        |)
      """.stripMargin
    val query =
      """
        |insert into t2
        |select t1.a, t1.`time`, t1.c, t1.d from t1
      """.stripMargin
    tableEnv.executeSql(sourceDDL)
    tableEnv.executeSql(sinkDDL)
    execInsertSqlAndWaitResult(tableEnv, query)
    assertEquals(expected.sorted, TestCollectionTableFactory.RESULT.sorted)
  }

  @Test
  def testInsertSinkTableExpressionFields(): Unit = {
    val sourceData = List(
      toRow(1, "1000"),
      toRow(2, "1"),
      toRow(3, "2000"),
      toRow(1, "2"),
      toRow(2, "3000")
    )
    val expected = List(
      toRow(1, 2),
      toRow(1, 2),
      toRow(2, 3),
      toRow(2, 3),
      toRow(3, 4)
    )
    TestCollectionTableFactory.initData(sourceData)
    val sourceDDL =
      """
        |create table t1(
        |  a int,
        |  b varchar,
        |  c as a + 1
        |) with (
        |  'connector' = 'COLLECTION'
        |)
      """.stripMargin
    val sinkDDL =
      """
        |create table t2(
        |  a int,
        |  b as c - 1,
        |  c int
        |) with (
        |  'connector' = 'COLLECTION'
        |)
      """.stripMargin
    val query =
      """
        |insert into t2
        |select t1.a, t1.c from t1
      """.stripMargin
    tableEnv.executeSql(sourceDDL)
    tableEnv.executeSql(sinkDDL)
    execInsertSqlAndWaitResult(tableEnv, query)
    assertEquals(expected.sorted, TestCollectionTableFactory.RESULT.sorted)
  }

  @Test
  def testInsertSinkTableWithUnmatchedFields(): Unit = {
    val sourceData = List(
      toRow(1, "1000"),
      toRow(2, "1"),
      toRow(3, "2000"),
      toRow(1, "2"),
      toRow(2, "3000")
    )
    TestCollectionTableFactory.initData(sourceData)
    val sourceDDL =
      """
        |create table t1(
        |  a int,
        |  b varchar,
        |  c as a + 1
        |) with (
        |  'connector' = 'COLLECTION'
        |)
      """.stripMargin
    val sinkDDL =
      """
        |create table t2(
        |  a int,
        |  b as cast(a as varchar(20)) || cast(c as varchar(20)),
        |  c int
        |) with (
        |  'connector' = 'COLLECTION'
        |)
      """.stripMargin
    val query =
      """
        |insert into t2
        |select t1.a, t1.b from t1
      """.stripMargin
    tableEnv.executeSql(sourceDDL)
    tableEnv.executeSql(sinkDDL)
    expectedEx.expect(classOf[ValidationException])
    expectedEx.expectMessage("Field types of query result and registered TableSink "
      + "default_catalog.default_database.t2 do not match.")
    tableEnv.executeSql(query)
  }

  @Test
  def testInsertWithJoinedSource(): Unit = {
    val sourceData = List(
      toRow(1, 1000, 2),
      toRow(2, 1, 3),
      toRow(3, 2000, 4),
      toRow(1, 2, 2),
      toRow(2, 3000, 3)
    )

    val expected = List(
      toRow(1, 1000, 2, 1),
      toRow(1, 2, 2, 1),
      toRow(2, 1, 1, 2),
      toRow(2, 3000, 1, 2)
    )
    TestCollectionTableFactory.initData(sourceData)
    val sourceDDL =
      """
        |create table t1(
        |  a int,
        |  b int,
        |  c int
        |) with (
        |  'connector' = 'COLLECTION'
        |)
      """.stripMargin
    val sinkDDL =
      """
        |create table t2(
        |  a int,
        |  b int,
        |  c int,
        |  d int
        |) with (
        |  'connector' = 'COLLECTION'
        |)
      """.stripMargin
    val query =
      """
        |insert into t2
        |select a.a, a.b, b.a, b.b
        |  from t1 a
        |  join t1 b
        |  on a.a = b.b
      """.stripMargin
    tableEnv.executeSql(sourceDDL)
    tableEnv.executeSql(sinkDDL)
    execInsertSqlAndWaitResult(tableEnv, query)
    assertEquals(expected.sorted, TestCollectionTableFactory.RESULT.sorted)
  }

  @Test
  def testInsertWithAggregateSource(): Unit = {
    if (isStreamingMode) {
      return
    }
    val sourceData = List(
      toRow(1, 1000, 2),
      toRow(2, 1000, 3),
      toRow(3, 2000, 4),
      toRow(4, 2000, 5),
      toRow(5, 3000, 6)
    )

    val expected = List(
      toRow(3, 1000),
      toRow(5, 3000),
      toRow(7, 2000)
    )
    TestCollectionTableFactory.initData(sourceData)
    val sourceDDL =
      """
        |create table t1(
        |  a int,
        |  b int,
        |  c int
        |) with (
        |  'connector' = 'COLLECTION'
        |)
      """.stripMargin
    val sinkDDL =
      """
        |create table t2(
        |  a int,
        |  b int
        |) with (
        |  'connector' = 'COLLECTION'
        |)
      """.stripMargin
    val query =
      """
        |insert into t2
        |select sum(a), t1.b from t1 group by t1.b
      """.stripMargin
    tableEnv.executeSql(sourceDDL)
    tableEnv.executeSql(sinkDDL)
    execInsertSqlAndWaitResult(tableEnv, query)
    assertEquals(expected.sorted, TestCollectionTableFactory.RESULT.sorted)
  }

  @Test
  def testTemporaryTableMaskPermanentTableWithSameName(): Unit = {
    val sourceData = List(
      toRow(1, "1000", 2),
      toRow(2, "1", 3),
      toRow(3, "2000", 4),
      toRow(1, "2", 2),
      toRow(2, "3000", 3))

    val permanentTable =
      """
        |CREATE TABLE T1(
        |  a int,
        |  b varchar,
        |  d int
        |) with (
        |  'connector' = 'COLLECTION'
        |)
      """.stripMargin

    val temporaryTable =
      """
        |CREATE TEMPORARY TABLE T1(
        |  a int,
        |  b varchar,
        |  c int,
        |  d as c+1
        |) with (
        |  'connector' = 'COLLECTION'
        |)
      """.stripMargin

    val sinkTable =
      """
        |CREATE TABLE T2(
        |  a int,
        |  b varchar,
        |  c int
        |) with (
        |  'connector' = 'COLLECTION'
        |)
      """.stripMargin


    val permanentData = List(
      toRow(1, "1000", 2),
      toRow(2, "1", 3),
      toRow(3, "2000", 4),
      toRow(1, "2", 2),
      toRow(2, "3000", 3))

    val temporaryData = List(
      toRow(1, "1000", 3),
      toRow(2, "1", 4),
      toRow(3, "2000", 5),
      toRow(1, "2", 3),
      toRow(2, "3000", 4))

    tableEnv.executeSql(permanentTable)
    tableEnv.executeSql(temporaryTable)
    tableEnv.executeSql(sinkTable)

    TestCollectionTableFactory.initData(sourceData)

    val query = "SELECT a, b, d FROM T1"
    execInsertTableAndWaitResult(tableEnv.sqlQuery(query), "T2")
    // temporary table T1 masks permanent table T1
    assertEquals(temporaryData.sorted, TestCollectionTableFactory.RESULT.sorted)

    TestCollectionTableFactory.reset()
    TestCollectionTableFactory.initData(sourceData)

    val dropTemporaryTable =
      """
        |DROP TEMPORARY TABLE IF EXISTS T1
      """.stripMargin
    tableEnv.executeSql(dropTemporaryTable)
    execInsertTableAndWaitResult(tableEnv.sqlQuery(query), "T2")
    // now we only have permanent view T1
    assertEquals(permanentData.sorted, TestCollectionTableFactory.RESULT.sorted)
  }

  @Test
  def testDropTableWithFullPath(): Unit = {
    val ddl1 =
      """
        |create table t1(
        |  a bigint,
        |  b bigint,
        |  c varchar
        |) with (
        |  'connector' = 'COLLECTION'
        |)
      """.stripMargin
    val ddl2 =
      """
        |create table t2(
        |  a bigint,
        |  b bigint
        |) with (
        |  'connector' = 'COLLECTION'
        |)
      """.stripMargin

    tableEnv.executeSql(ddl1)
    tableEnv.executeSql(ddl2)
    assert(tableEnv.listTables().sameElements(Array[String]("t1", "t2")))
    tableEnv.executeSql("DROP TABLE default_catalog.default_database.t2")
    assert(tableEnv.listTables().sameElements(Array("t1")))
  }

  @Test
  def testDropTableWithPartialPath(): Unit = {
    val ddl1 =
      """
        |create table t1(
        |  a bigint,
        |  b bigint,
        |  c varchar
        |) with (
        | 'connector' = 'COLLECTION'
        |)
      """.stripMargin
    val ddl2 =
      """
        |create table t2(
        |  a bigint,
        |  b bigint
        |) with (
        |  'connector' = 'COLLECTION'
        |)
      """.stripMargin

    tableEnv.executeSql(ddl1)
    tableEnv.executeSql(ddl2)
    assert(tableEnv.listTables().sameElements(Array[String]("t1", "t2")))
    tableEnv.executeSql("DROP TABLE default_database.t2")
    tableEnv.executeSql("DROP TABLE t1")
    assert(tableEnv.listTables().isEmpty)
  }

  @Test(expected = classOf[ValidationException])
  def testDropTableWithInvalidPath(): Unit = {
    val ddl1 =
      """
        |create table t1(
        |  a bigint,
        |  b bigint,
        |  c varchar
        |) with (
        |  'connector' = 'COLLECTION'
        |)
      """.stripMargin

    tableEnv.executeSql(ddl1)
    assert(tableEnv.listTables().sameElements(Array[String]("t1")))
    tableEnv.executeSql("DROP TABLE catalog1.database1.t1")
  }

  @Test
  def testDropTableWithInvalidPathIfExists(): Unit = {
    val ddl1 =
      """
        |create table t1(
        |  a bigint,
        |  b bigint,
        |  c varchar
        |) with (
        |  'connector' = 'COLLECTION'
        |)
      """.stripMargin

    tableEnv.executeSql(ddl1)
    assert(tableEnv.listTables().sameElements(Array[String]("t1")))
    tableEnv.executeSql("DROP TABLE IF EXISTS catalog1.database1.t1")
    assert(tableEnv.listTables().sameElements(Array[String]("t1")))
  }

  @Test
  def testAlterTable(): Unit = {
    val ddl1 =
      """
        |create table t1(
        |  a bigint not null,
        |  b bigint,
        |  c varchar
        |) with (
        |  'connector' = 'COLLECTION',
        |  'k1' = 'v1'
        |)
      """.stripMargin
    tableEnv.executeSql(ddl1)
    tableEnv.executeSql("alter table t1 rename to t2")
    assert(tableEnv.listTables().sameElements(Array[String]("t2")))
    tableEnv.executeSql("alter table t2 set ('k1' = 'a', 'k2' = 'b')")
    val expectedProperties = new util.HashMap[String, String]()
    expectedProperties.put("connector", "COLLECTION")
    expectedProperties.put("k1", "a")
    expectedProperties.put("k2", "b")
    val properties = tableEnv.getCatalog(tableEnv.getCurrentCatalog).get()
      .getTable(new ObjectPath(tableEnv.getCurrentDatabase, "t2"))
      .getProperties
    assertEquals(expectedProperties, properties)
    val currentCatalog = tableEnv.getCurrentCatalog
    val currentDB = tableEnv.getCurrentDatabase
    tableEnv.executeSql("alter table t2 add constraint ct1 primary key(a) not enforced")
    val tableSchema1 = tableEnv.getCatalog(currentCatalog).get()
      .getTable(ObjectPath.fromString(s"${currentDB}.t2"))
      .getSchema
    assert(tableSchema1.getPrimaryKey.isPresent)
    assertEquals("CONSTRAINT ct1 PRIMARY KEY (a)",
      tableSchema1.getPrimaryKey.get().asSummaryString())
    tableEnv.executeSql("alter table t2 drop constraint ct1")
    val tableSchema2 = tableEnv.getCatalog(currentCatalog).get()
      .getTable(ObjectPath.fromString(s"${currentDB}.t2"))
      .getSchema
    assertEquals(false, tableSchema2.getPrimaryKey.isPresent)
  }

  @Test
  def testUseCatalog(): Unit = {
    tableEnv.registerCatalog("cat1", new GenericInMemoryCatalog("cat1"))
    tableEnv.registerCatalog("cat2", new GenericInMemoryCatalog("cat2"))
    tableEnv.executeSql("use catalog cat1")
    assertEquals("cat1", tableEnv.getCurrentCatalog)
    tableEnv.executeSql("use catalog cat2")
    assertEquals("cat2", tableEnv.getCurrentCatalog)
  }

  @Test
  def testUseDatabase(): Unit = {
    val catalog = new GenericInMemoryCatalog("cat1")
    tableEnv.registerCatalog("cat1", catalog)
    val catalogDB1 = new CatalogDatabaseImpl(new util.HashMap[String, String](), "db1")
    val catalogDB2 = new CatalogDatabaseImpl(new util.HashMap[String, String](), "db2")
    catalog.createDatabase("db1", catalogDB1, true)
    catalog.createDatabase("db2", catalogDB2, true)
    tableEnv.executeSql("use cat1.db1")
    assertEquals("db1", tableEnv.getCurrentDatabase)
    tableEnv.executeSql("use db2")
    assertEquals("db2", tableEnv.getCurrentDatabase)
  }

  @Test
  def testCreateDatabase(): Unit = {
    tableEnv.registerCatalog("cat1", new GenericInMemoryCatalog("default"))
    tableEnv.registerCatalog("cat2", new GenericInMemoryCatalog("default"))
    tableEnv.executeSql("use catalog cat1")
    tableEnv.executeSql("create database db1 ")
    tableEnv.executeSql("create database if not exists db1 ")
    try {
      tableEnv.executeSql("create database db1 ")
      fail("ValidationException expected")
    } catch {
      case _: ValidationException => //ignore
    }
    tableEnv.executeSql("create database cat2.db1 comment 'test_comment'" +
                         " with ('k1' = 'v1', 'k2' = 'v2')")
    val database = tableEnv.getCatalog("cat2").get().getDatabase("db1")
    assertEquals("test_comment", database.getComment)
    assertEquals(2, database.getProperties.size())
    val expectedProperty = new util.HashMap[String, String]()
    expectedProperty.put("k1", "v1")
    expectedProperty.put("k2", "v2")
    assertEquals(expectedProperty, database.getProperties)
  }

  @Test
  def testDropDatabase(): Unit = {
    tableEnv.registerCatalog("cat1", new GenericInMemoryCatalog("default"))
    tableEnv.executeSql("use catalog cat1")
    tableEnv.executeSql("create database db1")
    tableEnv.executeSql("drop database db1")
    tableEnv.executeSql("drop database if exists db1")
    try {
      tableEnv.executeSql("drop database db1")
      fail("ValidationException expected")
    } catch {
      case _: ValidationException => //ignore
    }
    tableEnv.executeSql("create database db1")
    tableEnv.executeSql("use db1")
    val ddl1 =
      """
        |create table t1(
        |  a bigint,
        |  b bigint,
        |  c varchar
        |) with (
        |  'connector' = 'COLLECTION'
        |)
      """.stripMargin
    tableEnv.executeSql(ddl1)
    val ddl2 =
      """
        |create table t2(
        |  a bigint,
        |  b bigint,
        |  c varchar
        |) with (
        |  'connector' = 'COLLECTION'
        |)
      """.stripMargin
    tableEnv.executeSql(ddl2)
    try {
      tableEnv.executeSql("drop database db1")
      fail("ValidationException expected")
    } catch {
      case _: ValidationException => //ignore
    }
    tableEnv.executeSql("drop database db1 cascade")
  }

  @Test
  def testAlterDatabase(): Unit = {
    tableEnv.registerCatalog("cat1", new GenericInMemoryCatalog("default"))
    tableEnv.executeSql("use catalog cat1")
    tableEnv.executeSql("create database db1 comment 'db1_comment' with ('k1' = 'v1')")
    tableEnv.executeSql("alter database db1 set ('k1' = 'a', 'k2' = 'b')")
    val database = tableEnv.getCatalog("cat1").get().getDatabase("db1")
    assertEquals("db1_comment", database.getComment)
    assertEquals(2, database.getProperties.size())
    val expectedProperty = new util.HashMap[String, String]()
    expectedProperty.put("k1", "a")
    expectedProperty.put("k2", "b")
    assertEquals(expectedProperty, database.getProperties)
  }
}

object CatalogTableITCase {
  @Parameterized.Parameters(name = "{0}")
  def parameters(): java.util.Collection[Boolean] = {
    util.Arrays.asList(true, false)
  }
}
