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

package org.apache.flink.table.api

import org.apache.flink.table.api.scala._
import org.apache.flink.table.expressions.utils._
import org.apache.flink.table.runtime.utils.CommonTestData
import org.apache.flink.table.sources.{CsvTableSource, TableSource}
import org.apache.flink.table.utils.TableTestUtil._
import org.apache.flink.table.utils.{TableTestBase, TestFilterableTableSource}
import org.junit.{Assert, Test}

class TableSourceTest extends TableTestBase {

  private val projectedFields: Array[String] = Array("last", "id", "score")
  private val noCalcFields: Array[String] = Array("id", "score", "first")

  @Test
  def testTableSourceScanToString(): Unit = {
    val (tableSource1, _) = filterableTableSource
    val (tableSource2, _) = filterableTableSource
    val util = batchTestUtil()
    val tableEnv = util.tableEnv

    tableEnv.registerTableSource("table1", tableSource1)
    tableEnv.registerTableSource("table2", tableSource2)

    val table1 = tableEnv.scan("table1").where("amount > 2")
    val table2 = tableEnv.scan("table2").where("amount > 2")
    val result = table1.unionAll(table2)

    val expected = binaryNode(
      "DataSetUnion",
      batchFilterableSourceTableNode(
        "table1",
        Array("name", "id", "amount", "price"),
        "'amount > 2"),
      batchFilterableSourceTableNode(
        "table2",
        Array("name", "id", "amount", "price"),
        "'amount > 2"),
      term("union", "name, id, amount, price")
    )
    util.verifyTable(result, expected)
  }

  // batch plan

  @Test
  def testBatchProjectableSourceScanPlanTableApi(): Unit = {
    val (tableSource, tableName) = csvTable
    val util = batchTestUtil()
    val tableEnv = util.tableEnv

    tableEnv.registerTableSource(tableName, tableSource)

    val result = tableEnv
      .scan(tableName)
      .select('last.upperCase(), 'id.floor(), 'score * 2)

    val expected = unaryNode(
      "DataSetCalc",
      batchSourceTableNode(tableName, projectedFields),
      term("select", "UPPER(last) AS _c0", "FLOOR(id) AS _c1", "*(score, 2) AS _c2")
    )

    util.verifyTable(result, expected)
  }

  @Test
  def testBatchProjectableSourceScanPlanSQL(): Unit = {
    val (tableSource, tableName) = csvTable
    val util = batchTestUtil()

    util.tableEnv.registerTableSource(tableName, tableSource)

    val sqlQuery = s"SELECT `last`, floor(id), score * 2 FROM $tableName"

    val expected = unaryNode(
      "DataSetCalc",
      batchSourceTableNode(tableName, projectedFields),
      term("select", "last", "FLOOR(id) AS EXPR$1", "*(score, 2) AS EXPR$2")
    )

    util.verifySql(sqlQuery, expected)
  }

  @Test
  def testBatchProjectableSourceScanNoIdentityCalc(): Unit = {
    val (tableSource, tableName) = csvTable
    val util = batchTestUtil()
    val tableEnv = util.tableEnv

    tableEnv.registerTableSource(tableName, tableSource)

    val result = tableEnv
      .scan(tableName)
      .select('id, 'score, 'first)

    val expected = batchSourceTableNode(tableName, noCalcFields)
    util.verifyTable(result, expected)
  }

  @Test
  def testBatchFilterableWithoutPushDown(): Unit = {
    val (tableSource, tableName) = filterableTableSource
    val util = batchTestUtil()
    val tableEnv = util.tableEnv

    tableEnv.registerTableSource(tableName, tableSource)

    val result = tableEnv
        .scan(tableName)
        .select('price, 'id, 'amount)
        .where("price * 2 < 32")

    val expected = unaryNode(
      "DataSetCalc",
      batchSourceTableNode(
        tableName,
        Array("name", "id", "amount", "price")),
      term("select", "price", "id", "amount"),
      term("where", "<(*(price, 2), 32)")
    )

    util.verifyTable(result, expected)
  }

  @Test
  def testBatchFilterablePartialPushDown(): Unit = {
    val (tableSource, tableName) = filterableTableSource
    val util = batchTestUtil()
    val tableEnv = util.tableEnv

    tableEnv.registerTableSource(tableName, tableSource)

    val result = tableEnv
      .scan(tableName)
      .where("amount > 2 && price * 2 < 32")
      .select('price, 'name.lowerCase(), 'amount)

    val expected = unaryNode(
      "DataSetCalc",
      batchFilterableSourceTableNode(
        tableName,
        Array("name", "id", "amount", "price"),
        "'amount > 2"),
      term("select", "price", "LOWER(name) AS _c1", "amount"),
      term("where", "<(*(price, 2), 32)")
    )
    util.verifyTable(result, expected)
  }

  @Test
  def testBatchFilterableFullyPushedDown(): Unit = {
    val (tableSource, tableName) = filterableTableSource
    val util = batchTestUtil()
    val tableEnv = util.tableEnv

    tableEnv.registerTableSource(tableName, tableSource)

    val result = tableEnv
        .scan(tableName)
        .select('price, 'id, 'amount)
        .where("amount > 2 && amount < 32")

    val expected = unaryNode(
      "DataSetCalc",
      batchFilterableSourceTableNode(
        tableName,
        Array("name", "id", "amount", "price"),
        "'amount > 2 && 'amount < 32"),
      term("select", "price", "id", "amount")
    )
    util.verifyTable(result, expected)
  }

  @Test
  def testBatchFilterableWithUnconvertedExpression(): Unit = {
    val (tableSource, tableName) = filterableTableSource
    val util = batchTestUtil()
    val tableEnv = util.tableEnv

    tableEnv.registerTableSource(tableName, tableSource)

    val result = tableEnv
        .scan(tableName)
        .select('price, 'id, 'amount)
        .where("amount > 2 && (amount < 32 || amount.cast(LONG) > 10)") // cast can not be converted

    val expected = unaryNode(
      "DataSetCalc",
      batchFilterableSourceTableNode(
        tableName,
        Array("name", "id", "amount", "price"),
        "'amount > 2"),
      term("select", "price", "id", "amount"),
      term("where", "OR(<(amount, 32), >(CAST(amount), 10))")
    )
    util.verifyTable(result, expected)
  }

  @Test
  def testBatchFilterableWithUDF(): Unit = {
    val (tableSource, tableName) = filterableTableSource
    val util = batchTestUtil()
    val tableEnv = util.tableEnv

    tableEnv.registerTableSource(tableName, tableSource)
    val func = Func0
    tableEnv.registerFunction("func0", func)

    val result = tableEnv
        .scan(tableName)
        .select('price, 'id, 'amount)
        .where("amount > 2 && func0(amount) < 32")

    val expected = unaryNode(
      "DataSetCalc",
      batchFilterableSourceTableNode(
        tableName,
        Array("name", "id", "amount", "price"),
        "'amount > 2"),
      term("select", "price", "id", "amount"),
      term("where", s"<(${Func0.getClass.getSimpleName}(amount), 32)")
    )

    util.verifyTable(result, expected)
  }

  // stream plan

  @Test
  def testStreamProjectableSourceScanPlanTableApi(): Unit = {
    val (tableSource, tableName) = csvTable
    val util = streamTestUtil()
    val tableEnv = util.tableEnv

    tableEnv.registerTableSource(tableName, tableSource)

    val result = tableEnv
      .scan(tableName)
      .select('last, 'id.floor(), 'score * 2)

    val expected = unaryNode(
      "DataStreamCalc",
      streamSourceTableNode(tableName, projectedFields),
      term("select", "last", "FLOOR(id) AS _c1", "*(score, 2) AS _c2")
    )

    util.verifyTable(result, expected)
  }

  @Test
  def testStreamProjectableSourceScanPlanSQL(): Unit = {
    val (tableSource, tableName) = csvTable
    val util = streamTestUtil()

    util.tableEnv.registerTableSource(tableName, tableSource)

    val sqlQuery = s"SELECT `last`, floor(id), score * 2 FROM $tableName"

    val expected = unaryNode(
      "DataStreamCalc",
      streamSourceTableNode(tableName, projectedFields),
      term("select", "last", "FLOOR(id) AS EXPR$1", "*(score, 2) AS EXPR$2")
    )

    util.verifySql(sqlQuery, expected)
  }

  @Test
  def testStreamProjectableSourceScanNoIdentityCalc(): Unit = {
    val (tableSource, tableName) = csvTable
    val util = streamTestUtil()
    val tableEnv = util.tableEnv

    tableEnv.registerTableSource(tableName, tableSource)

    val result = tableEnv
      .scan(tableName)
      .select('id, 'score, 'first)

    val expected = streamSourceTableNode(tableName, noCalcFields)
    util.verifyTable(result, expected)
  }

  @Test
  def testStreamFilterableSourceScanPlanTableApi(): Unit = {
    val (tableSource, tableName) = filterableTableSource
    val util = streamTestUtil()
    val tableEnv = util.tableEnv

    tableEnv.registerTableSource(tableName, tableSource)

    val result = tableEnv
      .scan(tableName)
      .select('price, 'id, 'amount)
      .where("amount > 2 && price * 2 < 32")

    val expected = unaryNode(
      "DataStreamCalc",
      streamFilterableSourceTableNode(
        tableName,
        Array("name", "id", "amount", "price"),
        "'amount > 2"),
      term("select", "price", "id", "amount"),
      term("where", "<(*(price, 2), 32)")
    )

    util.verifyTable(result, expected)
  }

  // csv builder

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

  // utils

  def filterableTableSource:(TableSource[_], String) = {
    val tableSource = new TestFilterableTableSource
    (tableSource, "filterableTable")
  }

  def csvTable: (CsvTableSource, String) = {
    val csvTable = CommonTestData.getCsvTableSource
    val tableName = "csvTable"
    (csvTable, tableName)
  }

  def batchSourceTableNode(sourceName: String, fields: Array[String]): String = {
    s"BatchTableSourceScan(table=[[$sourceName]], fields=[${fields.mkString(", ")}])"
  }

  def streamSourceTableNode(sourceName: String, fields: Array[String] ): String = {
    s"StreamTableSourceScan(table=[[$sourceName]], fields=[${fields.mkString(", ")}])"
  }

  def batchFilterableSourceTableNode(
      sourceName: String,
      fields: Array[String],
      exp: String): String = {
    "BatchTableSourceScan(" +
      s"table=[[$sourceName]], fields=[${fields.mkString(", ")}], source=[filter=[$exp]])"
  }

  def streamFilterableSourceTableNode(
      sourceName: String,
      fields: Array[String],
      exp: String): String = {
    "StreamTableSourceScan(" +
      s"table=[[$sourceName]], fields=[${fields.mkString(", ")}], source=[filter=[$exp]])"
  }
}
