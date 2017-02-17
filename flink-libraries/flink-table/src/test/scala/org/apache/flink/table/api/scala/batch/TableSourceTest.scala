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

package org.apache.flink.table.api.scala.batch

import org.apache.flink.table.api.Types
import org.apache.flink.table.api.scala._
import org.apache.flink.table.sources.CsvTableSource
import org.apache.flink.table.utils.{CommonTestData, TableTestBase}
import org.apache.flink.table.utils.TableTestUtil._
import org.junit.{Assert, Test}

class TableSourceTest extends TableTestBase {

  private val projectedFields: Array[String] = Array("last", "id", "score")
  private val noCalcFields: Array[String] = Array("id", "score", "first")

  @Test
  def testBatchProjectableSourceScanPlanTableApi(): Unit = {
    val (csvTable, tableName) = tableSource
    val util = batchTestUtil()
    val tEnv = util.tEnv

    tEnv.registerTableSource(tableName, csvTable)

    val result = tEnv
      .scan(tableName)
      .select('last.upperCase(), 'id.floor(), 'score * 2)

    val expected = unaryNode(
      "DataSetCalc",
      sourceBatchTableNode(tableName, projectedFields),
      term("select", "UPPER(last) AS _c0", "FLOOR(id) AS _c1", "*(score, 2) AS _c2")
    )

    util.verifyTable(result, expected)
  }

  @Test
  def testBatchProjectableSourceScanPlanSQL(): Unit = {
    val (csvTable, tableName) = tableSource
    val util = batchTestUtil()

    util.tEnv.registerTableSource(tableName, csvTable)

    val sqlQuery = s"SELECT last, floor(id), score * 2 FROM $tableName"

    val expected = unaryNode(
      "DataSetCalc",
      sourceBatchTableNode(tableName, projectedFields),
      term("select", "last", "FLOOR(id) AS EXPR$1", "*(score, 2) AS EXPR$2")
    )

    util.verifySql(sqlQuery, expected)
  }

  @Test
  def testBatchProjectableSourceScanNoIdentityCalc(): Unit = {
    val (csvTable, tableName) = tableSource
    val util = batchTestUtil()
    val tEnv = util.tEnv

    tEnv.registerTableSource(tableName, csvTable)

    val result = tEnv
      .scan(tableName)
      .select('id, 'score, 'first)

    val expected = sourceBatchTableNode(tableName, noCalcFields)
    util.verifyTable(result, expected)
  }

  @Test
  def testStreamProjectableSourceScanPlanTableApi(): Unit = {
    val (csvTable, tableName) = tableSource
    val util = streamTestUtil()
    val tEnv = util.tEnv

    tEnv.registerTableSource(tableName, csvTable)

    val result = tEnv
      .scan(tableName)
      .select('last, 'id.floor(), 'score * 2)

    val expected = unaryNode(
      "DataStreamCalc",
      sourceStreamTableNode(tableName, projectedFields),
      term("select", "last", "FLOOR(id) AS _c1", "*(score, 2) AS _c2")
    )

    util.verifyTable(result, expected)
  }

  @Test
  def testStreamProjectableSourceScanPlanSQL(): Unit = {
    val (csvTable, tableName) = tableSource
    val util = streamTestUtil()

    util.tEnv.registerTableSource(tableName, csvTable)

    val sqlQuery = s"SELECT last, floor(id), score * 2 FROM $tableName"

    val expected = unaryNode(
      "DataStreamCalc",
      sourceStreamTableNode(tableName, projectedFields),
      term("select", "last", "FLOOR(id) AS EXPR$1", "*(score, 2) AS EXPR$2")
    )

    util.verifySql(sqlQuery, expected)
  }

  @Test
  def testStreamProjectableSourceScanNoIdentityCalc(): Unit = {
    val (csvTable, tableName) = tableSource
    val util = streamTestUtil()
    val tEnv = util.tEnv

    tEnv.registerTableSource(tableName, csvTable)

    val result = tEnv
      .scan(tableName)
      .select('id, 'score, 'first)

    val expected = sourceStreamTableNode(tableName, noCalcFields)
    util.verifyTable(result, expected)
  }

  @Test
  def testCsvTableSourceBuilder(): Unit = {
    val source1 = CsvTableSource.builder()
      .path("/path/to/csv")
      .field("myfield", Types.STRING)
      .field("myfield2", Types.INT)
      .quoteCharacter(';')
      .fieldDelimiter("#")
      .lineDelimiter("\r\n")
      .commentPrefix("%%")
      .ignoreFirstLine()
      .ignoreParseErrors()
      .build()

    val source2 = new CsvTableSource(
      "/path/to/csv",
      Array("myfield", "myfield2"),
      Array(Types.STRING, Types.INT),
      "#",
      "\r\n",
      ';',
      true,
      "%%",
      true)

    Assert.assertEquals(source1, source2)
  }

  @Test(expected = classOf[IllegalArgumentException])
  def testCsvTableSourceBuilderWithNullPath(): Unit = {
    CsvTableSource.builder()
      .field("myfield", Types.STRING)
      // should fail, path is not defined
      .build()
  }

  @Test(expected = classOf[IllegalArgumentException])
  def testCsvTableSourceBuilderWithDuplicateFieldName(): Unit = {
    CsvTableSource.builder()
      .path("/path/to/csv")
      .field("myfield", Types.STRING)
      // should fail, field name must no be duplicate
      .field("myfield", Types.INT)
  }

  @Test(expected = classOf[IllegalArgumentException])
  def testCsvTableSourceBuilderWithEmptyField(): Unit = {
    CsvTableSource.builder()
      .path("/path/to/csv")
      // should fail, field can be empty
      .build()
  }

  def tableSource: (CsvTableSource, String) = {
    val csvTable = CommonTestData.getCsvTableSource
    val tableName = "csvTable"
    (csvTable, tableName)
  }

  def sourceBatchTableNode(sourceName: String, fields: Array[String]): String = {
    s"BatchTableSourceScan(table=[[$sourceName]], fields=[${fields.mkString(", ")}])"
  }

  def sourceStreamTableNode(sourceName: String, fields: Array[String] ): String = {
    s"StreamTableSourceScan(table=[[$sourceName]], fields=[${fields.mkString(", ")}])"
  }
}
