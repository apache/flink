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

import org.apache.flink.api.scala._
import org.apache.flink.core.testutils.FlinkAssertions
import org.apache.flink.core.testutils.FlinkAssertions.anyCauseMatches
import org.apache.flink.table.api._
import org.apache.flink.table.api.config.ExecutionConfigOptions
import org.apache.flink.table.planner.expressions.utils.Func0
import org.apache.flink.table.planner.factories.TestValuesTableFactory.MockedLookupTableSource
import org.apache.flink.table.planner.utils.TableTestBase

import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.Test

class TableScanTest extends TableTestBase {

  private val util = streamTestUtil()

  @Test
  def testLegacyTableSourceScan(): Unit = {
    util.addTableSource[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
    util.verifyExecPlan("SELECT * FROM MyTable")
  }

  @Test
  def testDataStreamScan(): Unit = {
    util.addDataStream[(Int, Long, String)]("DataStreamTable", 'a, 'b, 'c)
    util.verifyExecPlan("SELECT * FROM DataStreamTable")
  }

  @Test
  def testDDLTableScan(): Unit = {
    util.addTable("""
                    |CREATE TABLE src (
                    |  ts TIMESTAMP(3),
                    |  a INT,
                    |  b DOUBLE,
                    |  WATERMARK FOR ts AS ts - INTERVAL '0.001' SECOND
                    |) WITH (
                    |  'connector' = 'values'
                    |)
      """.stripMargin)
    util.verifyExecPlan("SELECT * FROM src WHERE a > 1")
  }

  @Test
  def testDDLWithComputedColumn(): Unit = {
    // Create table with field as atom expression.
    util.tableEnv.registerFunction("my_udf", Func0)
    util.addTable(s"""
                     |create table t1(
                     |  a int,
                     |  b varchar,
                     |  c as a + 1,
                     |  d as to_timestamp(b),
                     |  e as my_udf(a)
                     |) with (
                     |  'connector' = 'values'
                     |)
       """.stripMargin)
    util.verifyExecPlan("SELECT * FROM t1")
  }

  @Test
  def testDDLWithMetadataColumn(): Unit = {
    // tests reordering, skipping, and casting of metadata
    util.addTable(
      s"""
         |CREATE TABLE MetadataTable (
         |  `a` INT,
         |  `other_metadata` INT METADATA FROM 'metadata_3' VIRTUAL,
         |  `b` BIGINT,
         |  `c` INT,
         |  `metadata_1` STRING METADATA,
         |  `computed` AS UPPER(`metadata_1`)
         |) WITH (
         |  'connector' = 'values',
         |  'bounded' = 'false',
         |  'readable-metadata' = 'metadata_1:STRING, metadata_2:BOOLEAN, metadata_3:BIGINT',
         |  'writable-metadata' = 'metadata_1:STRING, metadata_2:BOOLEAN'
         |)
       """.stripMargin)
    util.verifyExecPlan("SELECT * FROM MetadataTable")
  }

  @Test
  def testDDLWithMetadataThatConflictsWithPhysicalColumn(): Unit = {
    util.addTable(s"""
                     |CREATE TABLE MetadataTable (
                     |  `timestamp` TIMESTAMP(9),
                     |  `metadata_timestamp` TIMESTAMP(0) METADATA FROM 'timestamp',
                     |  `other` STRING METADATA,
                     |  `computed_other` AS UPPER(`other`),
                     |  `computed_timestamp` AS CAST(`metadata_timestamp` AS STRING)
                     |) WITH (
                     |  'connector' = 'values',
                     |  'bounded' = 'false',
                     |  'readable-metadata' = 'timestamp:TIMESTAMP(0), other:STRING'
                     |)
       """.stripMargin)
    util.verifyExecPlan("SELECT * FROM MetadataTable")
  }

  @Test
  def testDDLWithMetadataColumnProjectionPushDown(): Unit = {
    // tests reordering, skipping, and casting of metadata
    util.addTable(
      s"""
         |CREATE TABLE MetadataTable (
         |  `a` INT,
         |  `other_metadata` INT METADATA FROM 'metadata_3' VIRTUAL,
         |  `b` BIGINT,
         |  `c` INT,
         |  `metadata_1` STRING METADATA
         |) WITH (
         |  'connector' = 'values',
         |  'bounded' = 'false',
         |  'readable-metadata' = 'metadata_1:STRING, metadata_2:BOOLEAN, metadata_3:BIGINT',
         |  'writable-metadata' = 'metadata_1:STRING, metadata_2:BOOLEAN'
         |)
       """.stripMargin)
    util.verifyExecPlan("SELECT `b`, `other_metadata` FROM MetadataTable")
  }

  @Test
  def testDDLWithWatermarkComputedColumn(): Unit = {
    // Create table with field as atom expression.
    util.tableEnv.registerFunction("my_udf", Func0)
    util.addTable(s"""
                     |create table t1(
                     |  a int,
                     |  b varchar,
                     |  c as a + 1,
                     |  d as to_timestamp(b),
                     |  e as my_udf(a),
                     |  WATERMARK FOR d AS d - INTERVAL '0.001' SECOND
                     |) with (
                     |  'connector' = 'values'
                     |)
       """.stripMargin)
    util.verifyExecPlan("SELECT * FROM t1")
  }

  @Test
  def testDDLWithComputedColumnReferRowtime(): Unit = {
    util.addTable("""
                    |CREATE TABLE src (
                    |  ts TIMESTAMP(3),
                    |  a INT,
                    |  b DOUBLE,
                    |  my_ts AS ts - INTERVAL '0.001' SECOND,
                    |  proc AS PROCTIME(),
                    |  WATERMARK FOR ts AS ts - INTERVAL '0.001' SECOND
                    |) WITH (
                    |  'connector' = 'values'
                    |)
      """.stripMargin)
    util.verifyExecPlan("SELECT * FROM src WHERE a > 1")
  }

  @Test
  def testDDLWithMultipleColumnsFromSameMetadataKey(): Unit = {
    assertThatThrownBy(() => util.tableEnv.executeSql("""
                                                        |CREATE TABLE source (
                                                        |  a INT METADATA,
                                                        |  b INT METADATA FROM 'a'
                                                        |) WITH (
                                                        |  'connector' = 'COLLECTION'
                                                        |)
                                                        |""".stripMargin)).satisfies(
      FlinkAssertions.anyCauseMatches(
        classOf[ValidationException],
        "The column `a` and `b` in the table are both from the same metadata key 'a'. " +
          "Please specify one of the columns as the metadata column and use the computed column" +
          " syntax to specify the others."
      ))
  }

  @Test
  def testDDLWithMultipleColumnsFromSameMetadataKey2(): Unit = {
    util.tableEnv.executeSql("""
                               |CREATE TABLE source (
                               |  a INT METADATA
                               |) WITH (
                               |  'connector' = 'COLLECTION'
                               |)
                               |""".stripMargin)
    assertThatThrownBy(
      () =>
        util.tableEnv.executeSql(
          """
            |CREATE TABLE like_source (
            |  b INT METADATA FROM 'a'
            |)
            |WITH (
            |  'connector' = 'COLLECTION'
            |) LIKE source (
            |  INCLUDING METADATA
            |)
            |""".stripMargin
        )).satisfies(
      anyCauseMatches(
        "The column `a` and `b` in the table are both from the same metadata key 'a'. " +
          "Please specify one of the columns as the metadata column and use the computed column" +
          " syntax to specify the others."
      ))
  }

  @Test
  def testKeywordsWithWatermarkComputedColumn(): Unit = {
    // Create table with field as atom expression.
    util.tableEnv.registerFunction("my_udf", Func0)
    util.addTable(s"""
                     |create table t1(
                     |  a int,
                     |  b varchar,
                     |  `time` time,
                     |  mytime as `time`,
                     |  `current_time` as current_time,
                     |  json_row ROW<`timestamp` TIMESTAMP(3)>,
                     |  `timestamp` AS json_row.`timestamp`,
                     |  WATERMARK FOR `timestamp` AS `timestamp`
                     |) with (
                     |  'connector' = 'values'
                     |)
       """.stripMargin)
    util.verifyExecPlan("SELECT * FROM t1")
  }

  @Test
  def testScanOnBoundedSource(): Unit = {
    util.addTable("""
                    |CREATE TABLE src (
                    |  ts TIMESTAMP(3),
                    |  a INT,
                    |  b DOUBLE
                    |) WITH (
                    |  'connector' = 'values',
                    |  'bounded' = 'true'
                    |)
      """.stripMargin)
    // pass
    util.verifyRelPlan("SELECT * FROM src WHERE a > 1", ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testFilterOnChangelogSource(): Unit = {
    util.addTable("""
                    |CREATE TABLE src (
                    |  ts TIMESTAMP(3),
                    |  a INT,
                    |  b DOUBLE
                    |) WITH (
                    |  'connector' = 'values',
                    |  'changelog-mode' = 'I,UA,UB,D'
                    |)
      """.stripMargin)
    util.verifyRelPlan("SELECT * FROM src WHERE a > 1", ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testScanOnChangelogSource(): Unit = {
    util.addTable("""
                    |CREATE TABLE src (
                    |  ts TIMESTAMP(3),
                    |  a INT,
                    |  b DOUBLE
                    |) WITH (
                    |  'connector' = 'values',
                    |  'changelog-mode' = 'I,UA,UB,D'
                    |)
      """.stripMargin)
    util.verifyRelPlan("SELECT b,a,ts FROM src", ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testUnionChangelogSourceAndAggregation(): Unit = {
    util.addTable("""
                    |CREATE TABLE changelog_src (
                    |  ts TIMESTAMP(3),
                    |  a INT,
                    |  b DOUBLE
                    |) WITH (
                    |  'connector' = 'values',
                    |  'changelog-mode' = 'I,UA,UB,D'
                    |)
      """.stripMargin)
    util.addTable("""
                    |CREATE TABLE append_src (
                    |  ts TIMESTAMP(3),
                    |  a INT,
                    |  b DOUBLE
                    |) WITH (
                    |  'connector' = 'values',
                    |  'changelog-mode' = 'I'
                    |)
      """.stripMargin)

    val query = """
                  |SELECT b, ts, a
                  |FROM (
                  |  SELECT * FROM changelog_src
                  |  UNION ALL
                  |  SELECT MAX(ts) as t, a, MAX(b) as b FROM append_src GROUP BY a
                  |)
                  |""".stripMargin
    util.verifyRelPlan(query, ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testAggregateOnChangelogSource(): Unit = {
    util.addTable("""
                    |CREATE TABLE src (
                    |  ts TIMESTAMP(3),
                    |  a INT,
                    |  b DOUBLE
                    |) WITH (
                    |  'connector' = 'values',
                    |  'changelog-mode' = 'I,UA,UB'
                    |)
      """.stripMargin)
    util.verifyRelPlan("SELECT COUNT(*) FROM src WHERE a > 1", ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testJoinOnChangelogSource(): Unit = {
    verifyJoinOnSource("I,UB,UA")
  }

  @Test
  def testJoinOnChangelogSourceWithEventsDuplicate(): Unit = {
    util.tableEnv.getConfig
      .set(ExecutionConfigOptions.TABLE_EXEC_SOURCE_CDC_EVENTS_DUPLICATE, Boolean.box(true))
    verifyJoinOnSource("I,UB,UA")
  }

  @Test
  def testJoinOnNoUpdateSource(): Unit = {
    verifyJoinOnSource("I,D")
  }

  @Test
  def testJoinOnUpsertSource(): Unit = {
    verifyJoinOnSource("UA,D")
  }

  private def verifyJoinOnSource(changelogMode: String): Unit = {
    util.addTable("""
                    |CREATE TABLE orders (
                    |  amount BIGINT,
                    |  currency_id BIGINT,
                    |  currency_name STRING
                    |) WITH (
                    | 'connector' = 'values',
                    | 'changelog-mode' = 'I'
                    |)
                    |""".stripMargin)
    util.addTable(s"""
                     |CREATE TABLE rates_history (
                     |  currency_id BIGINT,
                     |  currency_name STRING,
                     |  rate BIGINT,
                     |  PRIMARY KEY (currency_id) NOT ENFORCED
                     |) WITH (
                     |  'connector' = 'values',
                     |  'changelog-mode' = '$changelogMode'
                     |)
      """.stripMargin)

    val sql =
      """
        |SELECT o.currency_name, o.amount, r.rate, o.amount * r.rate
        |FROM orders AS o JOIN rates_history AS r
        |ON o.currency_id = r.currency_id AND o.currency_name = r.currency_name
        |""".stripMargin
    util.verifyRelPlan(sql, ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testWatermarkAndChangelogSource(): Unit = {
    util.addTable("""
                    |CREATE TABLE src (
                    |  ts TIMESTAMP(3),
                    |  a INT,
                    |  b DOUBLE,
                    |  WATERMARK FOR `ts` AS `ts` - INTERVAL '5' SECOND
                    |) WITH (
                    |  'connector' = 'values',
                    |  'changelog-mode' = 'I,UB,UA,D'
                    |)
      """.stripMargin)
    util.verifyRelPlan("SELECT * FROM src WHERE a > 1", ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testChangelogSourceWithEventsDuplicate(): Unit = {
    util.tableEnv.getConfig
      .set(ExecutionConfigOptions.TABLE_EXEC_SOURCE_CDC_EVENTS_DUPLICATE, Boolean.box(true))
    util.addTable("""
                    |CREATE TABLE src (
                    |  id STRING,
                    |  a INT,
                    |  b AS a + 1,
                    |  c STRING,
                    |  ts as to_timestamp(c),
                    |  PRIMARY KEY (id) NOT ENFORCED,
                    |  WATERMARK FOR ts AS ts - INTERVAL '1' SECOND
                    |) WITH (
                    |  'connector' = 'values',
                    |  'changelog-mode' = 'I,UB,UA,D'
                    |)
      """.stripMargin)
    // the last node should keep UB because there is a filter on the changelog stream
    util.verifyRelPlan("SELECT a, b, c FROM src WHERE a > 1", ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testScanOnUpsertSource(): Unit = {
    util.addTable("""
                    |CREATE TABLE src (
                    |  ts TIMESTAMP(3),
                    |  id1 STRING,
                    |  a INT,
                    |  id2 BIGINT,
                    |  b DOUBLE,
                    |  PRIMARY KEY (id2, id1) NOT ENFORCED
                    |) WITH (
                    |  'connector' = 'values',
                    |  'changelog-mode' = 'UA'
                    |)
      """.stripMargin)
    // projection should be pushed down, but the full primary key (id2, id1) should be kept
    util.verifyRelPlan("SELECT id1, a, b FROM src", ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testUpsertSourceWithComputedColumnAndWatermark(): Unit = {
    util.addTable("""
                    |CREATE TABLE src (
                    |  id STRING,
                    |  a INT,
                    |  b AS a + 1,
                    |  c STRING,
                    |  ts as to_timestamp(c),
                    |  PRIMARY KEY (id) NOT ENFORCED,
                    |  WATERMARK FOR ts AS ts - INTERVAL '1' SECOND
                    |) WITH (
                    |  'connector' = 'values',
                    |  'changelog-mode' = 'UA,D'
                    |)
      """.stripMargin)
    // the last node should keep UB because there is a filter on the changelog stream
    util.verifyRelPlan("SELECT a, b, c FROM src WHERE a > 1", ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testUpsertSourceWithWatermarkPushDown(): Unit = {
    util.addTable("""
                    |CREATE TABLE src (
                    |  id STRING,
                    |  a INT,
                    |  b AS a + 1,
                    |  c STRING,
                    |  ts as to_timestamp(c),
                    |  PRIMARY KEY (id) NOT ENFORCED,
                    |  WATERMARK FOR ts AS ts - INTERVAL '1' SECOND
                    |) WITH (
                    |  'connector' = 'values',
                    |  'changelog-mode' = 'UA,D',
                    |  'enable-watermark-push-down' = 'true',
                    |  'disable-lookup' = 'true'
                    |)
      """.stripMargin)
    util.verifyRelPlan("SELECT id, ts FROM src", ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testUnionUpsertSourceAndAggregation(): Unit = {
    util.addTable("""
                    |CREATE TABLE upsert_src (
                    |  ts TIMESTAMP(3),
                    |  a INT,
                    |  b DOUBLE,
                    |  PRIMARY KEY (a) NOT ENFORCED
                    |) WITH (
                    |  'connector' = 'values',
                    |  'changelog-mode' = 'UA,D'
                    |)
      """.stripMargin)
    util.addTable("""
                    |CREATE TABLE append_src (
                    |  ts TIMESTAMP(3),
                    |  a INT,
                    |  b DOUBLE
                    |) WITH (
                    |  'connector' = 'values',
                    |  'changelog-mode' = 'I'
                    |)
      """.stripMargin)

    val query =
      """
        |SELECT b, ts, a
        |FROM (
        |  SELECT * FROM upsert_src
        |  UNION ALL
        |  SELECT MAX(ts) as t, a, MAX(b) as b FROM append_src GROUP BY a
        |)
        |""".stripMargin
    util.verifyRelPlan(query, ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testAggregateOnUpsertSource(): Unit = {
    util.addTable("""
                    |CREATE TABLE src (
                    |  ts TIMESTAMP(3),
                    |  a INT,
                    |  b DOUBLE,
                    |  c STRING,
                    |  PRIMARY KEY (a) NOT ENFORCED
                    |) WITH (
                    |  'connector' = 'values',
                    |  'changelog-mode' = 'UA,D'
                    |)
      """.stripMargin)
    // the MAX and MIN should work in retract mode
    util.verifyRelPlan(
      "SELECT b, COUNT(*), MAX(ts), MIN(ts) FROM src GROUP BY b",
      ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testAggregateOnUpsertSourcePrimaryKey(): Unit = {
    util.addTable("""
                    |CREATE TABLE src (
                    |  ts TIMESTAMP(3),
                    |  a INT,
                    |  b DOUBLE,
                    |  c STRING,
                    |  PRIMARY KEY (a) NOT ENFORCED
                    |) WITH (
                    |  'connector' = 'values',
                    |  'changelog-mode' = 'UA,D'
                    |)
      """.stripMargin)
    // the MAX and MIN should work in retract mode
    util.verifyRelPlan(
      "SELECT a, COUNT(*), MAX(ts), MIN(ts) FROM src GROUP BY a",
      ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testProcTimeTemporalJoinOnUpsertSource(): Unit = {
    util.addTable("""
                    |CREATE TABLE orders (
                    |  amount BIGINT,
                    |  currency STRING,
                    |  proctime AS PROCTIME()
                    |) WITH (
                    | 'connector' = 'values',
                    | 'changelog-mode' = 'I'
                    |)
                    |""".stripMargin)
    util.addTable("""
                    |CREATE TABLE rates_history (
                    |  currency STRING PRIMARY KEY NOT ENFORCED,
                    |  rate BIGINT
                    |) WITH (
                    |  'connector' = 'values',
                    |  'changelog-mode' = 'UA,D',
                    |  'disable-lookup' = 'true'
                    |)
      """.stripMargin)

    val sql =
      """
        |SELECT o.currency, o.amount, r.rate, o.amount * r.rate
        |FROM orders AS o LEFT JOIN rates_history FOR SYSTEM_TIME AS OF o.proctime AS r
        |ON o.currency = r.currency
        |""".stripMargin
    util.verifyRelPlan(sql, ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testEventTimeTemporalJoinOnUpsertSource(): Unit = {
    util.addTable("""
                    |CREATE TABLE orders (
                    |  amount BIGINT,
                    |  currency STRING,
                    |  rowtime TIMESTAMP(3),
                    |  WATERMARK FOR rowtime AS rowtime
                    |) WITH (
                    | 'connector' = 'values',
                    | 'changelog-mode' = 'I'
                    |)
                    |""".stripMargin)
    util.addTable("""
                    |CREATE TABLE rates_history (
                    |  currency STRING PRIMARY KEY NOT ENFORCED,
                    |  rate BIGINT,
                    |  rowtime TIMESTAMP(3),
                    |  WATERMARK FOR rowtime AS rowtime
                    |) WITH (
                    |  'connector' = 'values',
                    |  'changelog-mode' = 'UA,D',
                    |  'disable-lookup' = 'true'
                    |)
      """.stripMargin)

    val sql =
      """
        |SELECT o.currency, o.amount, r.rate, o.amount * r.rate
        |FROM orders AS o LEFT JOIN rates_history FOR SYSTEM_TIME AS OF o.rowtime AS r
        |ON o.currency = r.currency
        |""".stripMargin
    util.verifyRelPlan(sql, ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testWindowAggregateOnChangelogSource(): Unit = {
    util.addTable("""
                    |CREATE TABLE src (
                    |  ts AS PROCTIME(),
                    |  a INT,
                    |  b DOUBLE
                    |) WITH (
                    |  'connector' = 'values',
                    |  'changelog-mode' = 'I,UA,UB'
                    |)
      """.stripMargin)
    val query =
      """
        |SELECT TUMBLE_START(ts, INTERVAL '10' SECOND), COUNT(*)
        |FROM src
        |GROUP BY TUMBLE(ts, INTERVAL '10' SECOND)
        |""".stripMargin
    util.verifyRelPlan(query, ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testInvalidSourceChangelogMode(): Unit = {
    util.addTable("""
                    |CREATE TABLE src (
                    |  ts TIMESTAMP(3),
                    |  a INT,
                    |  b DOUBLE
                    |) WITH (
                    |  'connector' = 'values',
                    |  'changelog-mode' = 'I,UB,D'
                    |)
      """.stripMargin)
    thrown.expect(classOf[ValidationException])
    thrown.expectMessage(
      "Invalid source for table 'default_catalog.default_database.src'. A ScanTableSource " +
        "doesn't support a changelog stream that contains UPDATE_BEFORE but no UPDATE_AFTER. " +
        "Please adapt the implementation of class 'org.apache.flink.table.planner.factories." +
        "TestValuesTableFactory$TestValuesScanLookupTableSource'.")
    util.verifyRelPlan("SELECT * FROM src WHERE a > 1", ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testMissingPrimaryKeyForUpsertSource(): Unit = {
    util.addTable("""
                    |CREATE TABLE src (
                    |  ts TIMESTAMP(3),
                    |  a INT,
                    |  b DOUBLE
                    |) WITH (
                    |  'connector' = 'values',
                    |  'changelog-mode' = 'I,UA,D'
                    |)
      """.stripMargin)
    thrown.expect(classOf[TableException])
    thrown.expectMessage(
      "Table 'default_catalog.default_database.src' produces a " +
        "changelog stream that contains UPDATE_AFTER but no UPDATE_BEFORE. " +
        "This requires defining a primary key constraint on the table.")
    util.verifyRelPlan("SELECT * FROM src WHERE a > 1", ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testMissingPrimaryKeyForEventsDuplicate(): Unit = {
    util.addTable("""
                    |CREATE TABLE src (
                    |  ts TIMESTAMP(3),
                    |  a INT,
                    |  b DOUBLE
                    |) WITH (
                    |  'connector' = 'values',
                    |  'changelog-mode' = 'I,UB,UA,D'
                    |)
      """.stripMargin)
    util.tableEnv.getConfig
      .set(ExecutionConfigOptions.TABLE_EXEC_SOURCE_CDC_EVENTS_DUPLICATE, Boolean.box(true))

    thrown.expect(classOf[TableException])
    thrown.expectMessage(
      "Configuration 'table.exec.source.cdc-events-duplicate' is enabled " +
        "which requires the changelog sources to define a PRIMARY KEY. " +
        "However, table 'default_catalog.default_database.src' doesn't have a primary key.")
    util.verifyRelPlan("SELECT * FROM src WHERE a > 1", ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testInvalidScanOnLookupSource(): Unit = {
    util.addTable(s"""
                     |CREATE TABLE src (
                     |  ts TIMESTAMP(3),
                     |  a INT,
                     |  b DOUBLE
                     |) WITH (
                     |  'connector' = 'values',
                     |  'table-source-class' = '${classOf[MockedLookupTableSource].getName}'
                     |)
      """.stripMargin)
    thrown.expect(classOf[ValidationException])
    thrown.expectMessage(
      "The specified table source `default_catalog`.`default_database`.`src` " +
        "doesn't extend ScanTableSource and can not be used as the scan source.\nHint: You can read the data " +
        "from the source as a dim table with the look up join syntax. Otherwise, please refer to the document " +
        "and change the type of the connector to a source table that supports direct reads.")
    util.verifyRelPlan("SELECT * FROM src", ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testInvalidWatermarkOutputType(): Unit = {
    thrown.expect(classOf[ValidationException])
    thrown.expectMessage(
      "Invalid data type of expression for watermark definition. " +
        "The field must be of type TIMESTAMP(p) or TIMESTAMP_LTZ(p), " +
        "the supported precision 'p' is from 0 to 3, but the watermark " +
        "expression type is CHAR(0) NOT NULL")
    util.addTable("""
                    |CREATE TABLE src (
                    |  ts TIMESTAMP(3),
                    |  a INT,
                    |  b DOUBLE,
                    |  WATERMARK FOR `ts` AS ''
                    |) WITH (
                    |  'connector' = 'values'
                    |)
      """.stripMargin)
  }
}
