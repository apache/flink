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

import org.apache.flink.table.api._
import org.apache.flink.table.api.config.ExecutionConfigOptions
import org.apache.flink.table.api.internal.TableEnvironmentImpl
import org.apache.flink.table.catalog._
import org.apache.flink.table.planner.expressions.utils.Func0
import org.apache.flink.table.planner.factories.TestValuesCatalog
import org.apache.flink.table.planner.factories.utils.TestCollectionTableFactory
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedScalarFunctions.JavaFunc0
import org.apache.flink.table.planner.utils.DateTimeTestUtil.localDateTime
import org.apache.flink.table.utils.UserDefinedFunctions.{GENERATED_LOWER_UDF_CLASS, GENERATED_LOWER_UDF_CODE}
import org.apache.flink.test.util.AbstractTestBase
import org.apache.flink.types.Row
import org.apache.flink.util.{FileUtils, UserClassLoaderJarTestUtils}

import org.junit.{Before, Rule, Test}
import org.junit.Assert.{assertEquals, fail}
import org.junit.rules.ExpectedException
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

import java.io.File
import java.math.{BigDecimal => JBigDecimal}
import java.net.URI
import java.util
import java.util.UUID

import scala.collection.JavaConversions._
import scala.util.Random

/** Test cases for catalog table. */
@RunWith(classOf[Parameterized])
class CatalogTableITCase(isStreamingMode: Boolean) extends AbstractTestBase {
  // ~ Instance fields --------------------------------------------------------

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
    tableEnv.getConfig
      .set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, Int.box(1))
    TestCollectionTableFactory.reset()

    val func = new CatalogFunctionImpl(classOf[JavaFunc0].getName)
    tableEnv
      .getCatalog(tableEnv.getCurrentCatalog)
      .get()
      .createFunction(new ObjectPath(tableEnv.getCurrentDatabase, "myfunc"), func, true)
  }

  // ~ Tools ------------------------------------------------------------------

  implicit def rowOrdering: Ordering[Row] = Ordering.by(
    (r: Row) => {
      val builder = new StringBuilder
      (0 until r.getArity).foreach(idx => builder.append(r.getField(idx)))
      builder.toString()
    })

  def toRow(args: Any*): Row = {
    val row = new Row(args.length)
    (0 until args.length).foreach(i => row.setField(i, args(i)))
    row
  }

  def getTableOptions(tableName: String): java.util.Map[String, String] = {
    tableEnv
      .getCatalog(tableEnv.getCurrentCatalog)
      .get()
      .getTable(new ObjectPath(tableEnv.getCurrentDatabase, tableName))
      .getOptions
  }

  // ~ Tests ------------------------------------------------------------------

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
    tableEnv.executeSql(s"insert into sinkT select ${funcPrefix}myfunc(cast(1 as bigint))").await()
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
    tableEnv.executeSql(query).await()
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
    val tempFilePath = createTempFile("csv-order-test", csvRecords.mkString("#"))
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
    tableEnv.executeSql(query).await()

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
    val tempFilePath = createTempFile("csv-order-test", csvRecords.mkString("#"))
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
    tableEnv.executeSql(query).await()

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
    tableEnv.executeSql(query).await()
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
    tableEnv.executeSql(query).await()
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
    tableEnv.executeSql(query).await()
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
    tableEnv.executeSql(query).await()
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
    tableEnv.executeSql(query).await()
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
    expectedEx.expectMessage("Incompatible types for sink column 'c' at position 1.")
    tableEnv.executeSql(query).await()
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
    tableEnv.executeSql(query).await()
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
    tableEnv.executeSql(query).await()
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
    tableEnv.sqlQuery(query).executeInsert("T2").await()
    // temporary table T1 masks permanent table T1
    assertEquals(temporaryData.sorted, TestCollectionTableFactory.RESULT.sorted)

    TestCollectionTableFactory.reset()
    TestCollectionTableFactory.initData(sourceData)

    val dropTemporaryTable =
      """
        |DROP TEMPORARY TABLE IF EXISTS T1
      """.stripMargin
    tableEnv.executeSql(dropTemporaryTable)
    tableEnv.sqlQuery(query).executeInsert("T2").await()
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
  def testDropTableSameNameWithTemporaryTable(): Unit = {
    val createTable1 =
      """
        |create table t1(
        |  a bigint,
        |  b bigint,
        |  c varchar
        |) with (
        |  'connector' = 'COLLECTION'
        |)
      """.stripMargin
    val createTable2 =
      """
        |create temporary table t1(
        |  a bigint,
        |  b bigint,
        |  c varchar
        |) with (
        |  'connector' = 'COLLECTION'
        |)
      """.stripMargin
    tableEnv.executeSql(createTable1)
    tableEnv.executeSql(createTable2)

    expectedEx.expect(classOf[ValidationException])
    expectedEx.expectMessage(
      "Temporary table with identifier "
        + "'`default_catalog`.`default_database`.`t1`' exists. "
        + "Drop it first before removing the permanent table.")
    tableEnv.executeSql("drop table t1")
  }

  @Test
  def testDropViewSameNameWithTable(): Unit = {
    val createTable1 =
      """
        |create table t1(
        |  a bigint,
        |  b bigint,
        |  c varchar
        |) with (
        |  'connector' = 'COLLECTION'
        |)
      """.stripMargin
    tableEnv.executeSql(createTable1)

    expectedEx.expect(classOf[ValidationException])
    expectedEx.expectMessage(
      "View with identifier "
        + "'default_catalog.default_database.t1' does not exist.")
    tableEnv.executeSql("drop view t1")
  }

  @Test
  def testDropViewSameNameWithTableIfNotExists(): Unit = {
    val createTable1 =
      """
        |create table t1(
        |  a bigint,
        |  b bigint,
        |  c varchar
        |) with (
        |  'connector' = 'COLLECTION'
        |)
      """.stripMargin
    tableEnv.executeSql(createTable1)
    tableEnv.executeSql("drop view if exists t1")
    assert(tableEnv.listTables().sameElements(Array("t1")))
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

    // alter table rename
    tableEnv.executeSql("alter table t1 rename to t2")
    assert(tableEnv.listTables().sameElements(Array[String]("t2")))

    // alter table options
    tableEnv.executeSql("alter table t2 set ('k1' = 'a', 'k2' = 'b')")
    val expectedOptions = new util.HashMap[String, String]()
    expectedOptions.put("connector", "COLLECTION")
    expectedOptions.put("k1", "a")
    expectedOptions.put("k2", "b")
    assertEquals(expectedOptions, getTableOptions("t2"))

    tableEnv.executeSql("alter table t2 reset ('k1')")
    expectedOptions.remove("k1")
    assertEquals(expectedOptions, getTableOptions("t2"))

    // alter table add constraint
    val currentCatalog = tableEnv.getCurrentCatalog
    val currentDB = tableEnv.getCurrentDatabase
    tableEnv.executeSql("alter table t2 add constraint ct1 primary key(a) not enforced")
    val tableSchema1 = tableEnv
      .getCatalog(currentCatalog)
      .get()
      .getTable(ObjectPath.fromString(s"$currentDB.t2"))
      .getSchema
    assert(tableSchema1.getPrimaryKey.isPresent)
    assertEquals(
      "CONSTRAINT ct1 PRIMARY KEY (a)",
      tableSchema1.getPrimaryKey.get().asSummaryString())

    // alter table drop constraint
    tableEnv.executeSql("alter table t2 drop constraint ct1")
    val tableSchema2 = tableEnv
      .getCatalog(currentCatalog)
      .get()
      .getTable(ObjectPath.fromString(s"$currentDB.t2"))
      .getSchema
    assertEquals(false, tableSchema2.getPrimaryKey.isPresent)
  }

  @Test
  def testCreateTableAndShowCreateTable(): Unit = {
    val executedDDL =
      """
        |create temporary table TBL1 (
        |  a bigint not null,
        |  h string,
        |  g as 2*(a+1),
        |  b string not null,
        |  c bigint metadata virtual,
        |  e row<name string, age int, flag boolean>,
        |  f as myfunc(a),
        |  ts1 timestamp(3),
        |  ts2 timestamp_ltz(3) metadata from 'timestamp',
        |  `__source__` varchar(255),
        |  proc as proctime(),
        |  watermark for ts1 as cast(timestampadd(hour, 8, ts1) as timestamp(3)),
        |  constraint test_constraint primary key (a, b) not enforced
        |) comment 'test show create table statement'
        |partitioned by (b,h)
        |with (
        |  'connector' = 'kafka',
        |  'kafka.topic' = 'log.test'
        |)
        |""".stripMargin

    val expectedDDL =
      """ |CREATE TEMPORARY TABLE `default_catalog`.`default_database`.`TBL1` (
        |  `a` BIGINT NOT NULL,
        |  `h` VARCHAR(2147483647),
        |  `g` AS 2 * (`a` + 1),
        |  `b` VARCHAR(2147483647) NOT NULL,
        |  `c` BIGINT METADATA VIRTUAL,
        |  `e` ROW<`name` VARCHAR(2147483647), `age` INT, `flag` BOOLEAN>,
        |  `f` AS `default_catalog`.`default_database`.`myfunc`(`a`),
        |  `ts1` TIMESTAMP(3),
        |  `ts2` TIMESTAMP(3) WITH LOCAL TIME ZONE METADATA FROM 'timestamp',
        |  `__source__` VARCHAR(255),
        |  `proc` AS PROCTIME(),
        |  WATERMARK FOR `ts1` AS CAST(TIMESTAMPADD(HOUR, 8, `ts1`) AS TIMESTAMP(3)),
        |  CONSTRAINT `test_constraint` PRIMARY KEY (`a`, `b`) NOT ENFORCED
        |) COMMENT 'test show create table statement'
        |PARTITIONED BY (`b`, `h`)
        |WITH (
        |  'connector' = 'kafka',
        |  'kafka.topic' = 'log.test'
        |)
        |""".stripMargin
    tableEnv.executeSql(executedDDL)
    val row = tableEnv.executeSql("SHOW CREATE TABLE `TBL1`").collect().next()
    assertEquals(expectedDDL, row.getField(0))

    expectedEx.expect(classOf[ValidationException])
    expectedEx.expectMessage(
      "Could not execute SHOW CREATE TABLE. " +
        "Table with identifier `default_catalog`.`default_database`.`tmp` does not exist.")
    tableEnv.executeSql("SHOW CREATE TABLE `tmp`")
  }

  @Test
  def testCreateTableWithCommentAndShowCreateTable(): Unit = {
    val executedDDL =
      """
        |create temporary table TBL1 (
        |  pk1 bigint not null comment 'this is column pk1 which part of primary key',
        |  h string,
        |  a bigint,
        |  g as 2*(a+1) comment 'notice: computed column, expression ''2*(`a`+1)''.',
        |  pk2 string not null comment 'this is column pk2 which part of primary key',
        |  c bigint metadata virtual comment 'notice: metadata column, named ''c''.',
        |  e row<name string, age int, flag boolean>,
        |  f as myfunc(a),
        |  ts1 timestamp(3) comment 'notice: watermark, named ''ts1''.',
        |  ts2 timestamp_ltz(3) metadata from 'timestamp' comment 'notice: metadata column, named ''ts2''.',
        |  `__source__` varchar(255),
        |  proc as proctime(),
        |  watermark for ts1 as cast(timestampadd(hour, 8, ts1) as timestamp(3)),
        |  constraint test_constraint primary key (pk1, pk2) not enforced
        |) comment 'test show create table statement'
        |partitioned by (h)
        |with (
        |  'connector' = 'kafka',
        |  'kafka.topic' = 'log.test'
        |)
        |""".stripMargin

    val expectedDDL =
      """ |CREATE TEMPORARY TABLE `default_catalog`.`default_database`.`TBL1` (
        |  `pk1` BIGINT NOT NULL COMMENT 'this is column pk1 which part of primary key',
        |  `h` VARCHAR(2147483647),
        |  `a` BIGINT,
        |  `g` AS 2 * (`a` + 1) COMMENT 'notice: computed column, expression ''2*(`a`+1)''.',
        |  `pk2` VARCHAR(2147483647) NOT NULL COMMENT 'this is column pk2 which part of primary key',
        |  `c` BIGINT METADATA VIRTUAL COMMENT 'notice: metadata column, named ''c''.',
        |  `e` ROW<`name` VARCHAR(2147483647), `age` INT, `flag` BOOLEAN>,
        |  `f` AS `default_catalog`.`default_database`.`myfunc`(`a`),
        |  `ts1` TIMESTAMP(3) COMMENT 'notice: watermark, named ''ts1''.',
        |  `ts2` TIMESTAMP(3) WITH LOCAL TIME ZONE METADATA FROM 'timestamp' COMMENT 'notice: metadata column, named ''ts2''.',
        |  `__source__` VARCHAR(255),
        |  `proc` AS PROCTIME(),
        |  WATERMARK FOR `ts1` AS CAST(TIMESTAMPADD(HOUR, 8, `ts1`) AS TIMESTAMP(3)),
        |  CONSTRAINT `test_constraint` PRIMARY KEY (`pk1`, `pk2`) NOT ENFORCED
        |) COMMENT 'test show create table statement'
        |PARTITIONED BY (`h`)
        |WITH (
        |  'connector' = 'kafka',
        |  'kafka.topic' = 'log.test'
        |)
        |""".stripMargin
    tableEnv.executeSql(executedDDL)
    val row = tableEnv.executeSql("SHOW CREATE TABLE `TBL1`").collect().next()
    assertEquals(expectedDDL, row.getField(0).toString)
  }

  @Test
  def testCreateViewAndShowCreateTable(): Unit = {
    val isBounded = !isStreamingMode
    val createTableDDL =
      s""" |create table `source` (
         |  `id` bigint not null,
         | `group` string not null,
         | `score` double
         |) with (
         |  'connector' = 'source-only',
         |  'bounded' = '$isBounded'
         |)
         |""".stripMargin
    val createViewDDL =
      """ |create view `tmp` as
        |select `group`, avg(`score`) as avg_score
        |from `source`
        |group by `group`
        |""".stripMargin
    tableEnv.executeSql(createTableDDL)
    tableEnv.executeSql(createViewDDL)
    expectedEx.expect(classOf[TableException])
    expectedEx.expectMessage(
      "SHOW CREATE TABLE is only supported for tables, " +
        "but `default_catalog`.`default_database`.`tmp` is a view. " +
        "Please use SHOW CREATE VIEW instead.")
    tableEnv.executeSql("SHOW CREATE TABLE `tmp`")
  }

  @Test
  def testAlterViewRename(): Unit = {
    val isBounded = !isStreamingMode
    tableEnv.executeSql(s"""
                           | CREATE TABLE T (
                           |   id INT
                           | ) WITH (
                           |   'connector' = 'source-only',
                           |   'bounded' = '$isBounded'
                           | )
                           |""".stripMargin)
    tableEnv.executeSql("CREATE VIEW V AS SELECT * FROM T")

    tableEnv.executeSql("ALTER VIEW V RENAME TO V2")
    assert(tableEnv.listViews().sameElements(Array[String]("V2")))
  }

  @Test
  def testAlterViewAs(): Unit = {
    val isBounded = !isStreamingMode
    tableEnv.executeSql(s"""
                           | CREATE TABLE T (
                           |   a INT,
                           |   b INT
                           | ) WITH (
                           |   'connector' = 'source-only',
                           |   'bounded' = '$isBounded'
                           | )
                           |""".stripMargin)
    tableEnv.executeSql("CREATE VIEW V AS SELECT a FROM T")

    tableEnv.executeSql("ALTER VIEW V AS SELECT b FROM T")

    val objectPath = new ObjectPath(tableEnv.getCurrentDatabase, "V")
    val view = tableEnv
      .getCatalog(tableEnv.getCurrentCatalog)
      .get()
      .getTable(objectPath)
      .asInstanceOf[CatalogView]
    assertEquals("SELECT `b`\nFROM `T`", view.getOriginalQuery)
  }

  @Test
  def testUseCatalogAndShowCurrentCatalog(): Unit = {
    tableEnv.registerCatalog("cat1", new GenericInMemoryCatalog("cat1"))
    tableEnv.registerCatalog("cat2", new GenericInMemoryCatalog("cat2"))
    tableEnv.executeSql("use catalog cat1")
    assertEquals("cat1", tableEnv.getCurrentCatalog)
    tableEnv.executeSql("use catalog cat2")
    assertEquals("cat2", tableEnv.getCurrentCatalog)
    assertEquals("+I[cat2]", tableEnv.executeSql("show current catalog").collect().next().toString)
  }

  @Test
  def testUseDatabaseAndShowCurrentDatabase(): Unit = {
    val catalog = new GenericInMemoryCatalog("cat1")
    tableEnv.registerCatalog("cat1", catalog)
    val catalogDB1 = new CatalogDatabaseImpl(new util.HashMap[String, String](), "db1")
    val catalogDB2 = new CatalogDatabaseImpl(new util.HashMap[String, String](), "db2")
    catalog.createDatabase("db1", catalogDB1, true)
    catalog.createDatabase("db2", catalogDB2, true)
    tableEnv.executeSql("use cat1.db1")
    assertEquals("db1", tableEnv.getCurrentDatabase)
    var currentDatabase = tableEnv.executeSql("show current database").collect().next().toString
    assertEquals("+I[db1]", currentDatabase)
    tableEnv.executeSql("use db2")
    assertEquals("db2", tableEnv.getCurrentDatabase)
    currentDatabase = tableEnv.executeSql("show current database").collect().next().toString
    assertEquals("+I[db2]", currentDatabase)
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
      case _: ValidationException => // ignore
    }
    tableEnv.executeSql(
      "create database cat2.db1 comment 'test_comment'" +
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
      case _: ValidationException => // ignore
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
      case _: ValidationException => // ignore
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

  @Test
  def testLoadFunction(): Unit = {
    tableEnv.registerCatalog("cat2", new TestValuesCatalog("cat2", "default", true))
    tableEnv.executeSql("use catalog cat2")
    // test load customer function packaged in a jar
    val random = new Random();
    val udfClassName = GENERATED_LOWER_UDF_CLASS + random.nextInt(50)
    val jarPath = UserClassLoaderJarTestUtils
      .createJarFile(
        AbstractTestBase.TEMPORARY_FOLDER.newFolder(String.format("test-jar-%s", UUID.randomUUID)),
        "test-classloader-udf.jar",
        udfClassName,
        String.format(GENERATED_LOWER_UDF_CODE, udfClassName)
      )
      .toURI
      .toString
    tableEnv.executeSql(s"""create function lowerUdf as '$udfClassName' using jar '$jarPath'""")

    TestCollectionTableFactory.reset()
    TestCollectionTableFactory.initData(List(Row.of("BoB")))
    val ddl1 =
      """
        |create table t1(
        |  a varchar
        |) with (
        |  'connector' = 'COLLECTION'
        |)
      """.stripMargin
    tableEnv.executeSql(ddl1)
    assertEquals(
      "+I[bob]",
      tableEnv.executeSql("select lowerUdf(a) from t1").collect().next().toString)
  }
}

object CatalogTableITCase {
  @Parameterized.Parameters(name = "{0}")
  def parameters(): java.util.Collection[Boolean] = {
    util.Arrays.asList(true, false)
  }
}
