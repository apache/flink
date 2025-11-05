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

import org.apache.flink.core.testutils.FlinkAssertions.anyCauseMatches
import org.apache.flink.table.api._
import org.apache.flink.table.api.config.ExecutionConfigOptions
import org.apache.flink.table.api.internal.TableEnvironmentImpl
import org.apache.flink.table.catalog._
import org.apache.flink.table.planner.expressions.utils.Func0
import org.apache.flink.table.planner.factories.TestValuesCatalog
import org.apache.flink.table.planner.factories.utils.TestCollectionTableFactory
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedScalarFunctions.JavaFunc0
import org.apache.flink.table.planner.utils.DateTimeTestUtil.localDateTime
import org.apache.flink.table.planner.utils.TableITCaseBase
import org.apache.flink.table.utils.UserDefinedFunctions.{GENERATED_LOWER_UDF_CLASS, GENERATED_LOWER_UDF_CODE}
import org.apache.flink.testutils.junit.extensions.parameterized.{ParameterizedTestExtension, Parameters}
import org.apache.flink.testutils.junit.utils.TempDirUtils
import org.apache.flink.types.Row
import org.apache.flink.util.{FileUtils, UserClassLoaderJarTestUtils}

import org.assertj.core.api.Assertions.{assertThat, assertThatExceptionOfType, assertThatList, assertThatThrownBy, fail}
import org.junit.jupiter.api.{BeforeEach, TestTemplate}
import org.junit.jupiter.api.extension.ExtendWith

import java.io.File
import java.math.{BigDecimal => JBigDecimal}
import java.net.URI
import java.util
import java.util.UUID

import scala.collection.JavaConverters._
import scala.util.Random

/** Test cases for catalog table. */
@ExtendWith(Array(classOf[ParameterizedTestExtension]))
class CatalogTableITCase(isStreamingMode: Boolean) extends TableITCaseBase {
  // ~ Instance fields --------------------------------------------------------

  private val settings = if (isStreamingMode) {
    EnvironmentSettings.newInstance().inStreamingMode().build()
  } else {
    EnvironmentSettings.newInstance().inBatchMode().build()
  }

  private val tableEnv: TableEnvironment = TableEnvironmentImpl.create(settings)

  @BeforeEach
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
    assertThatList(TestCollectionTableFactory.RESULT)
      .containsExactlyInAnyOrderElementsOf(Seq(toRow(2L)).asJava)
  }

  @TestTemplate
  def testUdfWithFullIdentifier(): Unit = {
    testUdf("default_catalog.default_database.")
  }

  @TestTemplate
  def testUdfWithDatabase(): Unit = {
    testUdf("default_database.")
  }

  @TestTemplate
  def testUdfWithNon(): Unit = {
    testUdf("")
  }

  @TestTemplate
  def testUdfWithWrongCatalog(): Unit = {
    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(() => testUdf("wrong_catalog.default_database."))
  }

  @TestTemplate
  def testUdfWithWrongDatabase(): Unit = {
    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(() => testUdf("default_catalog.wrong_database."))
  }

  @TestTemplate
  def testInsertInto(): Unit = {
    val sourceData = List(
      toRow(1, "1000", 2, new JBigDecimal("10.001")),
      toRow(2, "1", 3, new JBigDecimal("10.001")),
      toRow(3, "2000", 4, new JBigDecimal("10.001")),
      toRow(1, "2", 2, new JBigDecimal("10.001")),
      toRow(2, "3000", 3, new JBigDecimal("10.001"))
    ).asJava
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
    assertThatList(sourceData).containsExactlyInAnyOrderElementsOf(
      TestCollectionTableFactory.RESULT)
  }

  @TestTemplate
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
    assertThat(expected).isEqualTo(FileUtils.readFileUtf8(new File(new URI(sinkFilePath))))
  }

  @TestTemplate
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
    assertThat(expected).isEqualTo(FileUtils.readFileUtf8(new File(new URI(sinkFilePath))))
  }

  @TestTemplate
  def testInsertSourceTableExpressionFields(): Unit = {
    val sourceData = List(
      toRow(1, "1000"),
      toRow(2, "1"),
      toRow(3, "2000"),
      toRow(1, "2"),
      toRow(2, "3000")
    ).asJava
    val expected = List(
      toRow(1, "1000", 2),
      toRow(2, "1", 3),
      toRow(3, "2000", 4),
      toRow(1, "2", 2),
      toRow(2, "3000", 3)
    ).asJava
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
    assertThatList(expected).containsExactlyInAnyOrderElementsOf(TestCollectionTableFactory.RESULT)
  }

  // Test the computation expression in front of referenced columns.
  @TestTemplate
  def testInsertSourceTableExpressionFieldsBeforeReferences(): Unit = {
    val sourceData = List(
      toRow(1, "1000"),
      toRow(2, "1"),
      toRow(3, "2000"),
      toRow(2, "2"),
      toRow(2, "3000")
    ).asJava
    val expected = List(
      toRow(101, 1, "1000"),
      toRow(102, 2, "1"),
      toRow(103, 3, "2000"),
      toRow(102, 2, "2"),
      toRow(102, 2, "3000")
    ).asJava
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
    assertThatList(expected).containsExactlyInAnyOrderElementsOf(TestCollectionTableFactory.RESULT)
  }

  @TestTemplate
  def testInsertSourceTableWithFuncField(): Unit = {
    val sourceData = List(
      toRow(1, "1990-02-10 12:34:56"),
      toRow(2, "2019-09-10 09:23:41"),
      toRow(3, "2019-09-10 09:23:42"),
      toRow(1, "2019-09-10 09:23:43"),
      toRow(2, "2019-09-10 09:23:44")
    ).asJava
    val expected = List(
      toRow(1, "1990-02-10 12:34:56", localDateTime("1990-02-10 12:34:56")),
      toRow(2, "2019-09-10 09:23:41", localDateTime("2019-09-10 09:23:41")),
      toRow(3, "2019-09-10 09:23:42", localDateTime("2019-09-10 09:23:42")),
      toRow(1, "2019-09-10 09:23:43", localDateTime("2019-09-10 09:23:43")),
      toRow(2, "2019-09-10 09:23:44", localDateTime("2019-09-10 09:23:44"))
    ).asJava
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
    assertThatList(expected).containsExactlyInAnyOrderElementsOf(TestCollectionTableFactory.RESULT)
  }

  @TestTemplate
  def testInsertSourceTableWithUserDefinedFuncField(): Unit = {
    val sourceData = List(
      toRow(1, "1990-02-10 12:34:56"),
      toRow(2, "2019-09-10 9:23:41"),
      toRow(3, "2019-09-10 9:23:42"),
      toRow(1, "2019-09-10 9:23:43"),
      toRow(2, "2019-09-10 9:23:44")
    ).asJava
    val expected = List(
      toRow(1, "1990-02-10 12:34:56", 1, "1990-02-10 12:34:56"),
      toRow(2, "2019-09-10 9:23:41", 2, "2019-09-10 9:23:41"),
      toRow(3, "2019-09-10 9:23:42", 3, "2019-09-10 9:23:42"),
      toRow(1, "2019-09-10 9:23:43", 1, "2019-09-10 9:23:43"),
      toRow(2, "2019-09-10 9:23:44", 2, "2019-09-10 9:23:44")
    ).asJava
    TestCollectionTableFactory.initData(sourceData)
    tableEnv.createTemporarySystemFunction("my_udf", Func0)
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
    assertThatList(expected).containsExactlyInAnyOrderElementsOf(TestCollectionTableFactory.RESULT)
  }

  @TestTemplate
  def testInsertSinkTableExpressionFields(): Unit = {
    val sourceData = List(
      toRow(1, "1000"),
      toRow(2, "1"),
      toRow(3, "2000"),
      toRow(1, "2"),
      toRow(2, "3000")
    ).asJava
    val expected = List(
      toRow(1, 2),
      toRow(1, 2),
      toRow(2, 3),
      toRow(2, 3),
      toRow(3, 4)
    ).asJava
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
    assertThatList(expected).containsExactlyInAnyOrderElementsOf(TestCollectionTableFactory.RESULT)
  }

  @TestTemplate
  def testInsertSinkTableWithUnmatchedFields(): Unit = {
    val sourceData = List(
      toRow(1, "1000"),
      toRow(2, "1"),
      toRow(3, "2000"),
      toRow(1, "2"),
      toRow(2, "3000")
    ).asJava
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

    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(() => tableEnv.executeSql(query).await())
      .withMessageContaining("Incompatible types for sink column 'c' at position 1.")
  }

  @TestTemplate
  def testInsertWithJoinedSource(): Unit = {
    val sourceData = List(
      toRow(1, 1000, 2),
      toRow(2, 1, 3),
      toRow(3, 2000, 4),
      toRow(1, 2, 2),
      toRow(2, 3000, 3)
    ).asJava

    val expected = List(
      toRow(1, 1000, 2, 1),
      toRow(1, 2, 2, 1),
      toRow(2, 1, 1, 2),
      toRow(2, 3000, 1, 2)
    ).asJava
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
    assertThatList(expected).containsExactlyInAnyOrderElementsOf(TestCollectionTableFactory.RESULT)
  }

  @TestTemplate
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
    ).asJava

    val expected = List(
      toRow(3, 1000),
      toRow(5, 3000),
      toRow(7, 2000)
    ).asJava
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
    assertThatList(expected).containsExactlyInAnyOrderElementsOf(TestCollectionTableFactory.RESULT)
  }

  @TestTemplate
  def testTemporaryTableMaskPermanentTableWithSameName(): Unit = {
    val sourceData = List(
      toRow(1, "1000", 2),
      toRow(2, "1", 3),
      toRow(3, "2000", 4),
      toRow(1, "2", 2),
      toRow(2, "3000", 3)).asJava

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
      toRow(2, "3000", 3)).asJava

    val temporaryData = List(
      toRow(1, "1000", 3),
      toRow(2, "1", 4),
      toRow(3, "2000", 5),
      toRow(1, "2", 3),
      toRow(2, "3000", 4)).asJava

    tableEnv.executeSql(permanentTable)
    tableEnv.executeSql(temporaryTable)
    tableEnv.executeSql(sinkTable)

    TestCollectionTableFactory.initData(sourceData)

    val query = "SELECT a, b, d FROM T1"
    tableEnv.sqlQuery(query).executeInsert("T2").await()
    // temporary table T1 masks permanent table T1
    assertThatList(temporaryData).containsExactlyInAnyOrderElementsOf(
      TestCollectionTableFactory.RESULT)

    TestCollectionTableFactory.reset()
    TestCollectionTableFactory.initData(sourceData)

    val dropTemporaryTable =
      """
        |DROP TEMPORARY TABLE IF EXISTS T1
      """.stripMargin
    tableEnv.executeSql(dropTemporaryTable)
    tableEnv.sqlQuery(query).executeInsert("T2").await()
    // now we only have permanent view T1
    assertThatList(permanentData).containsExactlyInAnyOrderElementsOf(
      TestCollectionTableFactory.RESULT)
  }

  @TestTemplate
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
    assertThat(tableEnv.listTables())
      .containsExactlyInAnyOrderElementsOf(Seq[String]("t1", "t2").asJava)
    tableEnv.executeSql("DROP TABLE default_catalog.default_database.t2")
    assertThat(tableEnv.listTables()).containsExactly("t1")
  }

  @TestTemplate
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
    assertThat(tableEnv.listTables())
      .containsExactlyInAnyOrderElementsOf(Seq[String]("t1", "t2").asJava)
    tableEnv.executeSql("DROP TABLE default_database.t2")
    tableEnv.executeSql("DROP TABLE t1")
    assertThat(tableEnv.listTables()).isEmpty()
  }

  @TestTemplate
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
    assertThat(tableEnv.listTables()).containsExactly("t1")
    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(() => tableEnv.executeSql("DROP TABLE catalog1.database1.t1"))
  }

  @TestTemplate
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
    assertThat(tableEnv.listTables()).containsExactly("t1")
    tableEnv.executeSql("DROP TABLE IF EXISTS catalog1.database1.t1")
    assertThat(tableEnv.listTables()).containsExactly("t1")
  }

  @TestTemplate
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

    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(() => tableEnv.executeSql("drop table t1"))
      .withMessageContaining(
        "Temporary table with identifier "
          + "'`default_catalog`.`default_database`.`t1`' exists. "
          + "Drop it first before removing the permanent table.")
  }

  @TestTemplate
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

    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(() => tableEnv.executeSql("drop view t1"))
      .withMessageContaining("View with identifier "
        + "'default_catalog.default_database.t1' does not exist.")
  }

  @TestTemplate
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
    assertThat(tableEnv.listTables()).containsExactly("t1")
  }

  @TestTemplate
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
    assertThat(tableEnv.listTables()).containsExactly("t2")

    // alter table options
    tableEnv.executeSql("alter table t2 set ('k1' = 'a', 'k2' = 'b')")
    val expectedOptions = new util.HashMap[String, String]()
    expectedOptions.put("connector", "COLLECTION")
    expectedOptions.put("k1", "a")
    expectedOptions.put("k2", "b")
    assertThat(expectedOptions).isEqualTo(getTableOptions("t2"))

    tableEnv.executeSql("alter table t2 reset ('k1')")
    expectedOptions.remove("k1")
    assertThat(expectedOptions).isEqualTo(getTableOptions("t2"))

    // alter table add constraint
    val currentCatalog = tableEnv.getCurrentCatalog
    val currentDB = tableEnv.getCurrentDatabase
    tableEnv.executeSql("alter table t2 add constraint ct1 primary key(a) not enforced")
    val primaryKey = tableEnv
      .getCatalog(currentCatalog)
      .get()
      .getTable(ObjectPath.fromString(s"$currentDB.t2"))
      .asInstanceOf[ResolvedCatalogTable]
      .getResolvedSchema
      .getPrimaryKey
    assertThat(primaryKey).isPresent
    assertThat(primaryKey.get().asSummaryString())
      .isEqualTo("CONSTRAINT `ct1` PRIMARY KEY (`a`) NOT ENFORCED")

    // alter table drop constraint
    tableEnv.executeSql("alter table t2 drop constraint ct1")
    val primaryKey2 = tableEnv
      .getCatalog(currentCatalog)
      .get()
      .getTable(ObjectPath.fromString(s"$currentDB.t2"))
      .asInstanceOf[ResolvedCatalogTable]
      .getResolvedSchema
      .getPrimaryKey
    assertThat(primaryKey2).isNotPresent
  }

  @TestTemplate
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
        |distributed by (a)
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
        |)
        |COMMENT 'test show create table statement'
        |DISTRIBUTED BY (`a`)
        |PARTITIONED BY (`b`, `h`)
        |WITH (
        |  'connector' = 'kafka',
        |  'kafka.topic' = 'log.test'
        |)
        |""".stripMargin
    tableEnv.executeSql(executedDDL)
    val row = tableEnv.executeSql("SHOW CREATE TABLE `TBL1`").collect().next()
    assertThat(row.getField(0)).isEqualTo(expectedDDL)

    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(() => tableEnv.executeSql("SHOW CREATE TABLE `tmp`"))
      .withMessageContaining("Could not execute SHOW CREATE TABLE. " +
        "Table with identifier `default_catalog`.`default_database`.`tmp` does not exist.")
  }

  @TestTemplate
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
        |distributed into 5 buckets
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
        |)
        |COMMENT 'test show create table statement'
        |DISTRIBUTED INTO 5 BUCKETS
        |PARTITIONED BY (`h`)
        |WITH (
        |  'connector' = 'kafka',
        |  'kafka.topic' = 'log.test'
        |)
        |""".stripMargin
    tableEnv.executeSql(executedDDL)
    val row = tableEnv.executeSql("SHOW CREATE TABLE `TBL1`").collect().next()
    assertThat(row.getField(0)).hasToString(expectedDDL)
  }

  @TestTemplate
  def testCreateTableAndShowCreateTableWithDistributionAlgorithm(): Unit = {
    val executedDDL =
      """
        |create temporary table TBL1 (
        |  a bigint not null,
        |  h string,
        |  b string not null
        |) comment 'test show create table statement'
        |distributed by range(a) into 7 buckets
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
        |  `b` VARCHAR(2147483647) NOT NULL
        |)
        |COMMENT 'test show create table statement'
        |DISTRIBUTED BY RANGE(`a`) INTO 7 BUCKETS
        |PARTITIONED BY (`b`, `h`)
        |WITH (
        |  'connector' = 'kafka',
        |  'kafka.topic' = 'log.test'
        |)
        |""".stripMargin
    tableEnv.executeSql(executedDDL)
    val row = tableEnv.executeSql("SHOW CREATE TABLE `TBL1`").collect().next()
    assertThat(row.getField(0)).isEqualTo(expectedDDL)
  }

  @TestTemplate
  def testCreateViewAndShowCreateTable(): Unit = {
    val createTableDDL =
      """ |create table `source` (
        |  `id` bigint not null,
        | `group` string not null,
        | `score` double
        |) with (
        |  'connector' = 'source-only'
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

    assertThatExceptionOfType(classOf[TableException])
      .isThrownBy(() => tableEnv.executeSql("SHOW CREATE TABLE `tmp`"))
      .withMessageContaining(
        "SHOW CREATE TABLE is only supported for tables, " +
          "but `default_catalog`.`default_database`.`tmp` is a view. " +
          "Please use SHOW CREATE VIEW instead.")

    assertThatExceptionOfType(classOf[TableException])
      .isThrownBy(() => tableEnv.executeSql("SHOW CREATE VIEW `source`"))
      .withMessageContaining(
        "SHOW CREATE VIEW is only supported for views, " +
          "but `default_catalog`.`default_database`.`source` is a table. " +
          "Please use SHOW CREATE TABLE instead.")
  }

  @TestTemplate
  def testAlterViewRename(): Unit = {
    tableEnv.executeSql("""
                          | CREATE TABLE T (
                          |   id INT
                          | ) WITH (
                          |   'connector' = 'source-only'
                          | )
                          |""".stripMargin)
    tableEnv.executeSql("CREATE VIEW V AS SELECT * FROM T")

    tableEnv.executeSql("ALTER VIEW V RENAME TO V2")
    assertThat(tableEnv.listViews()).containsExactly("V2")
  }

  @TestTemplate
  def testAlterViewAs(): Unit = {
    tableEnv.executeSql("""
                          | CREATE TABLE T (
                          |   a INT,
                          |   b INT
                          | ) WITH (
                          |   'connector' = 'source-only'
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
    assertThat(view.getOriginalQuery).isEqualTo("SELECT `b`\nFROM `T`")
  }

  @TestTemplate
  def testUseCatalogAndShowCurrentCatalog(): Unit = {
    tableEnv.registerCatalog("cat1", new GenericInMemoryCatalog("cat1"))
    tableEnv.registerCatalog("cat2", new GenericInMemoryCatalog("cat2"))
    tableEnv.executeSql("use catalog cat1")
    assertThat(tableEnv.getCurrentCatalog).isEqualTo("cat1")
    tableEnv.executeSql("use catalog cat2")
    assertThat(tableEnv.getCurrentCatalog).isEqualTo("cat2")
    assertThat(tableEnv.executeSql("show current catalog").collect().next()).hasToString("+I[cat2]")
  }

  @TestTemplate
  def testUseDatabaseAndShowCurrentDatabase(): Unit = {
    val catalog = new GenericInMemoryCatalog("cat1")
    tableEnv.registerCatalog("cat1", catalog)
    val catalogDB1 = new CatalogDatabaseImpl(new util.HashMap[String, String](), "db1")
    val catalogDB2 = new CatalogDatabaseImpl(new util.HashMap[String, String](), "db2")
    catalog.createDatabase("db1", catalogDB1, true)
    catalog.createDatabase("db2", catalogDB2, true)
    tableEnv.executeSql("use cat1.db1")
    assertThat(tableEnv.getCurrentDatabase).isEqualTo("db1")
    var currentDatabase = tableEnv.executeSql("show current database").collect().next().toString
    assertThat(currentDatabase).isEqualTo("+I[db1]")
    tableEnv.executeSql("use db2")
    assertThat(tableEnv.getCurrentDatabase).isEqualTo("db2")
    currentDatabase = tableEnv.executeSql("show current database").collect().next().toString
    assertThat(currentDatabase).isEqualTo("+I[db2]")
  }

  @TestTemplate
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
    assertThat(database.getComment).isEqualTo("test_comment")
    assertThat(database.getProperties).hasSize(2)
    val expectedProperty = new util.HashMap[String, String]()
    expectedProperty.put("k1", "v1")
    expectedProperty.put("k2", "v2")
    assertThat(database.getProperties).isEqualTo(expectedProperty)
  }

  @TestTemplate
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
    assertThatThrownBy(() => tableEnv.executeSql("drop database db1")).satisfies(
      anyCauseMatches(
        classOf[ValidationException],
        "Cannot drop a database which is currently in use."))
    tableEnv.executeSql("use `default`")
    try {
      tableEnv.executeSql("drop database db1")
      fail("ValidationException expected")
    } catch {
      case _: ValidationException => // ignore
    }
    tableEnv.executeSql("drop database db1 cascade")
  }

  @TestTemplate
  def testAlterDatabase(): Unit = {
    tableEnv.registerCatalog("cat1", new GenericInMemoryCatalog("default"))
    tableEnv.executeSql("use catalog cat1")
    tableEnv.executeSql("create database db1 comment 'db1_comment' with ('k1' = 'v1')")
    tableEnv.executeSql("alter database db1 set ('k1' = 'a', 'k2' = 'b')")
    val database = tableEnv.getCatalog("cat1").get().getDatabase("db1")
    assertThat(database.getComment).isEqualTo("db1_comment")
    assertThat(database.getProperties).hasSize(2)
    val expectedProperty = new util.HashMap[String, String]()
    expectedProperty.put("k1", "a")
    expectedProperty.put("k2", "b")
    assertThat(database.getProperties).isEqualTo(expectedProperty)
  }

  @TestTemplate
  def testLoadFunction(): Unit = {
    tableEnv.registerCatalog("cat2", new TestValuesCatalog("cat2", "default", true))
    tableEnv.executeSql("use catalog cat2")
    // test load customer function packaged in a jar
    val random = new Random()
    val udfClassName = GENERATED_LOWER_UDF_CLASS + random.nextInt(50)
    val jarPath = UserClassLoaderJarTestUtils
      .createJarFile(
        TempDirUtils.newFolder(tmpDir, String.format("test-jar-%s", UUID.randomUUID)),
        "test-classloader-udf.jar",
        udfClassName,
        String.format(GENERATED_LOWER_UDF_CODE, udfClassName)
      )
      .toURI
      .toString
    tableEnv.executeSql(s"""create function lowerUdf as '$udfClassName' using jar '$jarPath'""")

    TestCollectionTableFactory.reset()
    TestCollectionTableFactory.initData(List(Row.of("BoB")).asJava)
    val ddl1 =
      """
        |create table t1(
        |  a varchar
        |) with (
        |  'connector' = 'COLLECTION'
        |)
      """.stripMargin
    tableEnv.executeSql(ddl1)
    assertThat(tableEnv.executeSql("select lowerUdf(a) from t1").collect().next())
      .hasToString("+I[bob]")
  }

  @TestTemplate
  def testSensitiveInfoMasking(): Unit = {
    // Create a table with sensitive information
    val tableDDL =
      """
        |create table t1(
        |  a bigint,
        |  b varchar,
        |  c int
        |) with (
        |  'connector' = 'values',
        |  'password' = 'Secret1',
        |  'secret' = 'Secret2',
        |  'fs.azure.account.key' = 'Secret3',
        |  'apikey' =  'Secret4',
        |  'api-key' = 'Secret5',
        |  'auth-params' = 'Secret6',
        |  'service-key' = 'Secret7',
        |  'token' = 'Secret8',
        |  'basic-auth' = 'Secret9',
        |  'jaas.config' = 'Secret10',
        |  'http-headers' = '{"Authorization": "Secret11"}'
        |)
    """.stripMargin

    tableEnv.executeSql(tableDDL)

    // Extract a generic validation function
    def validateSensitiveInfoMasking(sql: String, tableType: String): Unit = {
      try {
        tableEnv.executeSql(sql)
        fail(s"Expected ValidationException for $tableType table")
      } catch {
        case e: ValidationException =>
          val errorMsg = e.getMessage
          // Verify sensitive information is masked
          assert(!errorMsg.contains("Secret"))

          // Verify all sensitive fields are replaced with '******'
          assert(errorMsg.contains("'password'='******'"))
          assert(errorMsg.contains("'secret'='******'"))
          assert(errorMsg.contains("'fs.azure.account.key'='******'"))
          assert(errorMsg.contains("'apikey'='******'"))
          assert(errorMsg.contains("'api-key'='******'"))
          assert(errorMsg.contains("'auth-params'='******'"))
          assert(errorMsg.contains("'service-key'='******'"))
          assert(errorMsg.contains("'token'='******'"))
          assert(errorMsg.contains("'basic-auth'='******'"))
          assert(errorMsg.contains("'jaas.config'='******'"))
          assert(errorMsg.contains("'http-headers'='******'"))
      }
    }

    // Test source table (SELECT)
    validateSensitiveInfoMasking("select * from t1", "source")

    // Test sink table (INSERT)
    validateSensitiveInfoMasking("insert into t1 select 1, 'test', 1", "sink")
  }

}

object CatalogTableITCase {
  @Parameters(name = "{0}")
  def parameters(): java.util.Collection[Boolean] = {
    util.Arrays.asList(true, false)
  }
}
