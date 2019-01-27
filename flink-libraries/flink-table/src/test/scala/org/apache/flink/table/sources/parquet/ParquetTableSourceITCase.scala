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

package org.apache.flink.table.sources.parquet

import org.apache.flink.table.api.TableConfigOptions
import org.apache.flink.table.runtime.batch.sql.BatchTestBase
import org.apache.flink.test.util.TestBaseUtils

import org.junit.Test

import scala.collection.JavaConverters._

class ParquetTableSourceITCase extends BatchTestBase {

  @Test
  def testBatchExecParquetTableSource(): Unit = {
    val expected = Seq(
      "1,Mike,Smith,12.3",
      "2,Bob,Taylor,45.6",
      "3,Sam,Miller,7.89",
      "4,Peter,Smith,0.12",
      "5,Liz,Williams,34.5",
      "6,Sally,Miller,6.78",
      "7,Alice,Smith,90.1",
      "8,Kelly,Williams,2.34").mkString("\n")

    // test ParquetVectorizedColumnRowTableSource
    val vectorColumnRowTable = CommonParquetTestData.getParquetVectorizedColumnRowTableSource
    val vectorColumnRowTableName = "vectorColumnRowTable"
    val vectorColumnRowSql = "SELECT id, `first`, `last`, score FROM vectorColumnRowTable"
    checkBatchExecParquetSource(vectorColumnRowTable,
      vectorColumnRowTableName, vectorColumnRowSql, expected)
  }

  @Test
  def testBatchExecParquetTableSourceWithLimitPushdown(): Unit = {
    val expected = Seq("1,Mike,Smith,12.3", "2,Bob,Taylor,45.6").mkString("\n")

    // test ParquetVectorizedColumnRowTableSource
    val vectorColumnRowTable = CommonParquetTestData.getParquetVectorizedColumnRowTableSource
    val vectorColumnRowTableName = "vectorColumnRowTable"
    val vectorColumnRowSql = "SELECT id, `first`, `last`, score FROM vectorColumnRowTable limit 2"
    checkBatchExecParquetSource(vectorColumnRowTable,
      vectorColumnRowTableName, vectorColumnRowSql, expected)
  }

  @Test
  def testDecimalType(): Unit = {
    conf.getConf.setInteger(TableConfigOptions.SQL_RESOURCE_DEFAULT_PARALLELISM, 1)
    val expected = Seq(
      "0.00,0,0.0000,0E-9,0,0.0,0E-10,0,0.00000,0E-18,0,0,0.000",
      "1.23,999999999,12345.6700,1E-9,2147483648,12345678.9,1E-10,999999999999999999," +
          "9876543210.98765,1E-18,9223372036854775808,99999999999999999999999999999999999999," +
          "123456789012345678901.234",
      "-1.23,-999999999,-12345.6700,-1E-9,-2147483649,-12345678.9,-1E-10,-999999999999999999," +
          "-9876543210.98765,-1E-18,-9223372036854775809,-99999999999999999999999999999999999999," +
          "-123456789012345678901.234",
      "0.00,-999999999,-12345.6700,0E-9,2147483648,0.9,-1E-10,-999999999999999999," +
          "-9876543210.98765,-1E-18,-9223372036854775809,-99999999999999999999999999999999999999," +
          "123456789012345678901.234"
    ).mkString("\n")

    val vectorColumnRowTable = CommonParquetTestData.getWithDecimalVectorizedTableSource(false)
    val vectorColumnRowTableName = "vectorColumnRowTable"
    val vectorColumnRowSql = "SELECT * FROM vectorColumnRowTable"
    checkBatchExecParquetSource(vectorColumnRowTable,
      vectorColumnRowTableName, vectorColumnRowSql, expected)
  }

  @Test
  def testDecimalTypeWithDictionary(): Unit = {
    conf.getConf.setInteger(TableConfigOptions.SQL_RESOURCE_DEFAULT_PARALLELISM, 1)
    val expected = Seq(
      "0.00,0,0.0000,0E-9,0,0.0,0E-10,0,0.00000,0E-18,0,0,0.000",
      "1.23,999999999,12345.6700,1E-9,2147483648,12345678.9,1E-10,999999999999999999," +
          "9876543210.98765,1E-18,9223372036854775808,99999999999999999999999999999999999999," +
          "123456789012345678901.234",
      "-1.23,-999999999,-12345.6700,-1E-9,-2147483649,-12345678.9,-1E-10,-999999999999999999," +
          "-9876543210.98765,-1E-18,-9223372036854775809,-99999999999999999999999999999999999999," +
          "-123456789012345678901.234",
      "0.00,-999999999,-12345.6700,0E-9,2147483648,0.9,-1E-10,-999999999999999999," +
          "-9876543210.98765,-1E-18,-9223372036854775809,-99999999999999999999999999999999999999," +
          "123456789012345678901.234"
    ).mkString("\n")

    val vectorColumnRowTable = CommonParquetTestData.getWithDecimalVectorizedTableSource(true)
    val vectorColumnRowTableName = "vectorColumnRowTable"
    val vectorColumnRowSql = "SELECT * FROM vectorColumnRowTable"
    checkBatchExecParquetSource(vectorColumnRowTable,
      vectorColumnRowTableName, vectorColumnRowSql, expected)
  }

  @Test
  def testBatchExecPushDownProject(): Unit = {

    val expected = Seq(
      "1,Mike,Smith",
      "2,Bob,Taylor",
      "3,Sam,Miller",
      "4,Peter,Smith",
      "5,Liz,Williams",
      "6,Sally,Miller",
      "7,Alice,Smith",
      "8,Kelly,Williams").mkString("\n")

    // test ParquetVectorizedColumnRowTableSource
    val vectorColumnRowTable = CommonParquetTestData.getParquetVectorizedColumnRowTableSource
    val vectorColumnRowTableName = "vectorColumnRowTable"
    val vectorColumnRowSql = "SELECT id, `first`, `last` FROM vectorColumnRowTable"
    checkBatchExecParquetSource(vectorColumnRowTable,
      vectorColumnRowTableName, vectorColumnRowSql, expected)
  }

  @Test
  def testBatchExecPushDownProjectWithSQLDate(): Unit = {

    val expected = Seq(
      "1,Mike,Smith,2017-10-13",
      "2,Bob,Taylor,2017-10-13",
      "3,Sam,Miller,2017-10-13",
      "4,Peter,Smith,2017-10-13",
      "5,Liz,Williams,2017-10-13",
      "6,Sally,Miller,2017-10-13",
      "7,Alice,Smith,2017-10-13",
      "8,Kelly,Williams,2017-10-13").mkString("\n")

    // test ParquetVectorizedColumnRowTableSource
    val vectorColumnRowTable = CommonParquetTestData
        .getWithSQLDateParquetVectorizedColumnRowTableSource
    val vectorColumnRowTableName = "vectorColumnRowTable"
    val vectorColumnRowSql = "SELECT id, `first`, `last`, `birthday` FROM vectorColumnRowTable"
    checkBatchExecParquetSource(vectorColumnRowTable,
      vectorColumnRowTableName, vectorColumnRowSql, expected)
  }

  @Test
  def testBatchExecPushDownFilter(): Unit = {
    val expected = Seq(
      "1,Mike,Smith,12.3",
      "2,Bob,Taylor,45.6",
      "3,Sam,Miller,7.89").mkString("\n")

    // test ParquetVectorizedColumnRowTableSource
    val vectorColumnRowTable = CommonParquetTestData.getParquetVectorizedColumnRowTableSource
    val vectorColumnRowTableName = "vectorColumnRowTable"
    val vectorColumnRowSql = "SELECT id, `first`, `last`, score " +
        "FROM vectorColumnRowTable WHERE id < 4"
    checkBatchExecParquetSource(vectorColumnRowTable,
      vectorColumnRowTableName, vectorColumnRowSql, expected)
  }

  @Test
  def testBatchExecPushDownFilterWithParallelism(): Unit = {
    val expected = Seq(
      "1,Mike,Smith,12.3",
      "2,Bob,Taylor,45.6",
      "3,Sam,Miller,7.89").mkString("\n")

    // test ParquetVectorizedColumnRowTableSource
    val vectorColumnRowTable = CommonParquetTestData
        .getWithParallelismParquetVectorizedColumnRowTableSource
    val vectorColumnRowTableName = "vectorColumnRowTable"
    val vectorColumnRowSql = "SELECT id, `first`, `last`, score " +
        "FROM vectorColumnRowTable WHERE id < 4"
    checkBatchExecParquetSource(vectorColumnRowTable,
      vectorColumnRowTableName, vectorColumnRowSql, expected)
  }

  /**
    * Test the case with string type filter push down
    */
  @Test
  def testBatchExecPushDownFilterWithString(): Unit = {
    val expected = Seq(
      "2,Bob,Taylor").mkString("\n")


    // test ParquetVectorizedColumnRowTableSource
    val vectorColumnRowTable = CommonParquetTestData
        .getWithParallelismParquetVectorizedColumnRowTableSource
    val vectorColumnRowTableName = "vectorColumnRowTable"
    val vectorColumnRowSql = "SELECT id, `first`, `last` " +
        "FROM vectorColumnRowTable WHERE id < 4 and `first`='Bob'"
    checkBatchExecParquetSource(vectorColumnRowTable,
      vectorColumnRowTableName, vectorColumnRowSql, expected)
  }

  @Test
  def testBatchExecPushDownFilterWithSQLDate(): Unit = {
    val expected = Seq(
      "1,Mike,Smith,12.3,2017-10-13",
      "2,Bob,Taylor,45.6,2017-10-13",
      "3,Sam,Miller,7.89,2017-10-13").mkString("\n")

    // test ParquetVectorizedColumnRowTableSource
    val vectorColumnRowTable = CommonParquetTestData
        .getWithSQLDateParquetVectorizedColumnRowTableSource
    val vectorColumnRowTableName = "vectorColumnRowTable"
    val vectorColumnRowSql = "SELECT id, `first`, `last`, score, birthday " +
        "FROM vectorColumnRowTable WHERE id < 4"
    checkBatchExecParquetSource(vectorColumnRowTable,
      vectorColumnRowTableName, vectorColumnRowSql, expected)
  }

  // Test sql time type data: Timestamp,Time and Date
  @Test
  def testBatchExecParquetTableSourceWithTimestamp(): Unit = {
    val expected = Seq(
      2,3,5).mkString("\n")

    // test ParquetVectorizedColumnRowTableSource
    val vectorColumnRowTable = CommonParquetTestSqlTimeTypeData
        .getWithSQLTimestampParquetVectorizedColumnRowTableSource
    val vectorColumnRowTableName = "vectorColumnRowTable"
    val vectorColumnRowSql =
      """
        |select id from vectorColumnRowTable
        |where ts >= TIMESTAMP '2017-07-12 11:45:00' and ts < TIMESTAMP '2017-07-14 12:14:23'
      """.stripMargin
    checkBatchExecParquetSource(vectorColumnRowTable,
      vectorColumnRowTableName, vectorColumnRowSql, expected)
  }

  @Test
  def testBatchExecParquetTableSourceWithDate(): Unit = {
    val expected = Seq(
      24,25).mkString("\n")

    // test ParquetVectorizedColumnRowTableSource
    val vectorColumnRowTable = CommonParquetTestSqlTimeTypeData
        .getWithSQLDateParquetVectorizedColumnRowTableSource
    val vectorColumnRowTableName = "vectorColumnRowTable"
    val vectorColumnRowSql =
      """
        |select id from vectorColumnRowTable
        |where date_val >= DATE '2017-04-24' and date_val < DATE '2017-04-26'
      """.stripMargin
    checkBatchExecParquetSource(vectorColumnRowTable,
      vectorColumnRowTableName, vectorColumnRowSql, expected)
  }

  private def checkBatchExecParquetSource(
      table: ParquetTableSource[_],
      name: String,
      sql: String,
      expected: String): Unit = {
    tEnv.registerTableSource(name, table)
    val results = tEnv.sqlQuery(sql).collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }
}
