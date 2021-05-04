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

package org.apache.flink.table.planner.runtime.batch.sql

import org.apache.flink.table.api.config.TableConfigOptions
import org.apache.flink.table.planner.factories.TestValuesTableFactory
import org.apache.flink.table.planner.plan.optimize.RelNodeBlockPlanBuilder
import org.apache.flink.table.planner.runtime.utils.BatchAbstractTestBase.TEMPORARY_FOLDER
import org.apache.flink.table.planner.runtime.utils.BatchTestBase.row
import org.apache.flink.table.planner.runtime.utils.{BatchTestBase, TestData}
import org.apache.flink.table.planner.utils._
import org.apache.flink.util.FileUtils

import org.junit.{Assert, Before, Test}

class TableSourceITCase extends BatchTestBase {

  @Before
  override def before(): Unit = {
    super.before()
    env.setParallelism(1) // set sink parallelism to 1
    val myTableDataId = TestValuesTableFactory.registerData(TestData.smallData3)
    tEnv.executeSql(
      s"""
        |CREATE TABLE MyTable (
        |  `a` INT,
        |  `b` BIGINT,
        |  `c` STRING
        |) WITH (
        |  'connector' = 'values',
        |  'data-id' = '$myTableDataId',
        |  'bounded' = 'true'
        |)
        |""".stripMargin)

    val filterableTableDataId = TestValuesTableFactory.registerData(
      TestLegacyFilterableTableSource.defaultRows)
    tEnv.executeSql(
      s"""
         |CREATE TABLE FilterableTable (
         |  name STRING,
         |  id BIGINT,
         |  amount INT,
         |  price DOUBLE
         |) WITH (
         |  'connector' = 'values',
         |  'data-id' = '$filterableTableDataId',
         |  'filterable-fields' = 'amount',
         |  'bounded' = 'true'
         |)
         |""".stripMargin)
    val nestedTableDataId = TestValuesTableFactory.registerData(TestData.deepNestedRow)
    tEnv.executeSql(
      s"""
         |CREATE TABLE NestedTable (
         |  id BIGINT,
         |  deepNested ROW<
         |     nested1 ROW<name STRING, `value.` INT>,
         |     `nested2.` ROW<num INT, flag BOOLEAN>>,
         |  nested ROW<name STRING, `value` INT>,
         |  name STRING,
         |  nestedItem ROW<deepArray ROW<`value` INT> ARRAY, deepMap MAP<STRING, INT>>,
         |  lower_name AS LOWER(name)
         |) WITH (
         |  'connector' = 'values',
         |  'nested-projection-supported' = 'true',
         |  'data-id' = '$nestedTableDataId',
         |  'bounded' = 'true'
         |)
         |""".stripMargin
    )
  }

  @Test
  def testSimpleProject(): Unit = {
    checkResult(
      "SELECT a, c FROM MyTable",
      Seq(
        row(1, "Hi"),
        row(2, "Hello"),
        row(3, "Hello world"))
    )
  }

  @Test
  def testSimpleProjectWithProcTime(): Unit = {
    checkResult(
      "SELECT a, c, CHAR_LENGTH(DATE_FORMAT(PROCTIME(), 'yyyy-MM-dd HH:mm')) FROM MyTable",
      Seq(
        row(1, "Hi", 16),
        row(2, "Hello", 16),
        row(3, "Hello world", 16)
      )
    )
  }

  @Test
  def testProjectWithoutInputRef(): Unit = {
    checkResult(
      "SELECT COUNT(*) FROM MyTable",
      Seq(row(3))
    )
  }

  @Test
  def testNestedProject(): Unit = {
    checkResult(
      """
        |SELECT id,
        |    deepNested.nested1.name AS nestedName,
        |    nested.`value` AS nestedValue,
        |    deepNested.`nested2.`.flag AS nestedFlag,
        |    deepNested.`nested2.`.num + deepNested.nested1.`value.` AS nestedNum,
        |    lower_name
        |FROM NestedTable
      """.stripMargin,
      Seq(row(1, "Sarah", 10000, true, 1100, "mary"),
        row(2, "Rob", 20000, false, 2200, "bob"),
        row(3, "Mike", 30000, true, 3300, "liz")
      )
    )
  }

  @Test
  def testNestedProjectWithItem(): Unit = {
    checkResult(
      """
        |SELECT nestedItem.deepArray[nestedItem.deepMap['Monday']] FROM  NestedTable
        |""".stripMargin,
      Seq(row(row(1)), row(row(1)), row(row(1)))
    )
  }

  @Test
  def testTableSourceWithFilterable(): Unit = {
    checkResult(
      "SELECT id, amount, name FROM FilterableTable WHERE amount > 4 AND price < 9",
      Seq(
        row(5, 5, "Record_5"),
        row(6, 6, "Record_6"),
        row(7, 7, "Record_7"),
        row(8, 8, "Record_8"))
    )
  }

  @Test
  def testTableSourceWithFunctionFilterable(): Unit = {
    checkResult(
      "SELECT id, amount, name FROM FilterableTable " +
        "WHERE amount > 4 AND price < 9 AND upper(name) = 'RECORD_5'",
      Seq(
        row(5, 5, "Record_5"))
    )
  }

  @Test
  def testInputFormatSource(): Unit = {
    val dataId = TestValuesTableFactory.registerData(TestData.smallData3)
    tEnv.executeSql(
      s"""
         |CREATE TABLE MyInputFormatTable (
         |  `a` INT,
         |  `b` BIGINT,
         |  `c` STRING
         |) WITH (
         |  'connector' = 'values',
         |  'data-id' = '$dataId',
         |  'bounded' = 'true',
         |  'runtime-source' = 'InputFormat'
         |)
         |""".stripMargin
    )

    checkResult(
      "SELECT a, c FROM MyInputFormatTable",
      Seq(
        row(1, "Hi"),
        row(2, "Hello"),
        row(3, "Hello world"))
    )
  }

  @Test
  def testDataStreamSource(): Unit = {
    val dataId = TestValuesTableFactory.registerData(TestData.smallData3)
    tEnv.executeSql(
      s"""
         |CREATE TABLE MyDataStreamTable (
         |  `a` INT,
         |  `b` BIGINT,
         |  `c` STRING
         |) WITH (
         |  'connector' = 'values',
         |  'data-id' = '$dataId',
         |  'bounded' = 'true',
         |  'runtime-source' = 'DataStream'
         |)
         |""".stripMargin
    )

    checkResult(
      "SELECT a, c FROM MyDataStreamTable",
      Seq(
        row(1, "Hi"),
        row(2, "Hello"),
        row(3, "Hello world"))
    )
  }

  @Test
  def testAllDataTypes(): Unit = {
    val dataId = TestValuesTableFactory.registerData(TestData.fullDataTypesData)
    tEnv.executeSql(
      s"""
         |CREATE TABLE T (
         |  `a` BOOLEAN,
         |  `b` TINYINT,
         |  `c` SMALLINT,
         |  `d` INT,
         |  `e` BIGINT,
         |  `f` FLOAT,
         |  `g` DOUBLE,
         |  `h` DECIMAL(5, 2),
         |  `i` VARCHAR(5),
         |  `j` CHAR(5),
         |  `k` DATE,
         |  `l` TIME(0),
         |  `m` TIMESTAMP(9),
         |  `n` TIMESTAMP(9) WITH LOCAL TIME ZONE,
         |  `o` ARRAY<BIGINT>,
         |  `p` ROW<f1 BIGINT, f2 STRING, f3 DOUBLE>,
         |  `q` MAP<STRING, INT>
         |) WITH (
         |  'connector' = 'values',
         |  'data-id' = '$dataId',
         |  'bounded' = 'true'
         |)
         |""".stripMargin
    )

    checkResult(
      "SELECT * FROM T",
      Seq(
        row(
          true, 127, 32767, 2147483647, 9223372036854775807L, "-1.123", "-1.123", "5.10",
          1, 1, "1969-01-01", "00:00:00.123", "1969-01-01T00:00:00.123456789",
          "1969-01-01T00:00:00.123456789Z", "[1, 2, 3]", row(1, "a", "2.3"), "{k1=1}"),
        row(
          false, -128, -32768, -2147483648, -9223372036854775808L, "3.4", "3.4", "6.10",
          12, 12, "1970-09-30", "01:01:01.123", "1970-09-30T01:01:01.123456",
          "1970-09-30T01:01:01.123456Z", "[4, 5]", row(null, "b", "4.56"), "{k2=2, k4=4}"),
        row(
          true, 0, 0, 0, 0, "0.12", "0.12", "7.10",
          123, 123, "1990-12-24", "08:10:24.123", "1990-12-24T08:10:24.123",
          "1990-12-24T08:10:24.123Z", "[6, null, 7]", row(3, null, "7.86"), "{k3=null}"),
        row(
          false, 5, 4, 123, 1234, "1.2345", "1.2345", "8.12",
          1234, 1234, "2020-05-01", "23:23:23", "2020-05-01T23:23:23",
          "2020-05-01T23:23:23Z", "[8]", row(4, "c", null), "{null=3}"),
        row(
          null, null, null, null, null, null, null, null, null, null, null, null, null,
          null, null, null, null)
      )
    )
  }

  @Test
  def testSourceProvider(): Unit = {
    val file = TEMPORARY_FOLDER.newFile()
    file.delete()
    file.createNewFile()
    FileUtils.writeFileUtf8(file, "1\n5\n6")
    tEnv.executeSql(
      s"""
         |CREATE TABLE MyFileSourceTable (
         |  `a` STRING
         |) WITH (
         |  'connector' = 'test-file',
         |  'path' = '${file.toURI}'
         |)
         |""".stripMargin
    )

    checkResult(
      "SELECT a FROM MyFileSourceTable",
      Seq(
        row("1"),
        row("5"),
        row("6"))
    )
  }

  @Test
  def testTableHint(): Unit = {
    tEnv.getConfig.getConfiguration.setBoolean(
      TableConfigOptions.TABLE_DYNAMIC_TABLE_OPTIONS_ENABLED, true)
    val resultPath = TEMPORARY_FOLDER.newFolder().getAbsolutePath
    tEnv.executeSql(
      s"""
         |CREATE TABLE MySink (
         |  `a` INT,
         |  `b` BIGINT,
         |  `c` STRING
         |) WITH (
         |  'connector' = 'filesystem',
         |  'format' = 'testcsv',
         |  'path' = '$resultPath'
         |)
       """.stripMargin)

    val stmtSet= tEnv.createStatementSet()
    stmtSet.addInsertSql(
      """
        |insert into MySink select a,b,c from MyTable
        |  /*+ OPTIONS('source.num-element-to-skip'='1') */
        |""".stripMargin)
    stmtSet.addInsertSql(
      """
        |insert into MySink select a,b,c from MyTable
        |  /*+ OPTIONS('source.num-element-to-skip'='2') */
        |""".stripMargin)
    stmtSet.execute().await()

    val result = TableTestUtil.readFromFile(resultPath)
    val expected = Seq("2,2,Hello", "3,2,Hello world", "3,2,Hello world")
    Assert.assertEquals(expected.sorted, result.sorted)
  }

  @Test
  def testTableHintWithLogicalTableScanReuse(): Unit = {
    tEnv.getConfig.getConfiguration.setBoolean(
      TableConfigOptions.TABLE_DYNAMIC_TABLE_OPTIONS_ENABLED, true)
    tEnv.getConfig.getConfiguration.setBoolean(
      RelNodeBlockPlanBuilder.TABLE_OPTIMIZER_REUSE_OPTIMIZE_BLOCK_WITH_DIGEST_ENABLED, true)
    val resultPath = TEMPORARY_FOLDER.newFolder().getAbsolutePath
    tEnv.executeSql(
      s"""
         |CREATE TABLE MySink (
         |  `a` INT,
         |  `b` BIGINT,
         |  `c` STRING
         |) WITH (
         |  'connector' = 'filesystem',
         |  'format' = 'testcsv',
         |  'path' = '$resultPath'
         |)
       """.stripMargin)

    val stmtSet = tEnv.createStatementSet()
    stmtSet.addInsertSql(
      """
        |insert into MySink
        |select a,b,c from MyTable /*+ OPTIONS('source.num-element-to-skip'='0') */
        |union all
        |select a,b,c from MyTable /*+ OPTIONS('source.num-element-to-skip'='1') */
        |""".stripMargin)
    stmtSet.addInsertSql(
      """
        |insert into MySink select a,b,c from MyTable
        |  /*+ OPTIONS('source.num-element-to-skip'='2') */
        |""".stripMargin)
    stmtSet.execute().await()

    val result = TableTestUtil.readFromFile(resultPath)
    val expected = Seq(
      "1,1,Hi", "2,2,Hello", "2,2,Hello", "3,2,Hello world", "3,2,Hello world", "3,2,Hello world")
    Assert.assertEquals(expected.sorted, result.sorted)
  }
}
