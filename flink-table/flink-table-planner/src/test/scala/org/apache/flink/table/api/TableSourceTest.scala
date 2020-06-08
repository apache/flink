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

import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, SqlTimeTypeInfo, TypeInformation}
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.table.api.internal.TableEnvironmentInternal
import org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR
import org.apache.flink.table.descriptors.{ConnectorDescriptor, Schema}
import org.apache.flink.table.expressions.utils._
import org.apache.flink.table.runtime.utils.CommonTestData
import org.apache.flink.table.sources.{CsvTableSource, TableSource}
import org.apache.flink.table.utils.TableTestUtil._
import org.apache.flink.table.utils.{TableTestBase, TestFilterableTableSource}
import org.apache.flink.types.Row

import org.junit.{Assert, Test}

import java.sql.{Date, Time, Timestamp}
import java.util.{HashMap => JHashMap, Map => JMap}

class TableSourceTest extends TableTestBase {

  private val projectedFields: Array[String] = Array("last", "id", "score")
  private val noCalcFields: Array[String] = Array("id", "score", "first")

  @Test
  def testTableSourceScanToString(): Unit = {
    val (tableSource1, _) = filterableTableSource
    val (tableSource2, _) = filterableTableSource
    val util = batchTestUtil()
    val tableEnv = util.tableEnv

    tableEnv.asInstanceOf[TableEnvironmentInternal]
      .registerTableSourceInternal("table1", tableSource1)
    tableEnv.asInstanceOf[TableEnvironmentInternal]
      .registerTableSourceInternal("table2", tableSource2)

    val table1 = tableEnv.scan("table1").where($"amount" > 2)
    val table2 = tableEnv.scan("table2").where($"amount" > 2)
    val result = table1.unionAll(table2)

    val expected = binaryNode(
      "DataSetUnion",
      batchFilterableSourceTableNode(
        "table1",
        Array("name", "id", "amount", "price"),
        isPushedDown = true,
        "'amount > 2"),
      batchFilterableSourceTableNode(
        "table2",
        Array("name", "id", "amount", "price"),
        isPushedDown = true,
        "'amount > 2"),
      term("all", "true"),
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

    tableEnv.asInstanceOf[TableEnvironmentInternal]
      .registerTableSourceInternal(tableName, tableSource)

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

    util.tableEnv.asInstanceOf[TableEnvironmentInternal]
      .registerTableSourceInternal(tableName, tableSource)

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

    tableEnv.asInstanceOf[TableEnvironmentInternal]
      .registerTableSourceInternal(tableName, tableSource)

    val result = tableEnv
      .scan(tableName)
      .select('id, 'score, 'first)

    val expected = batchSourceTableNode(tableName, noCalcFields)
    util.verifyTable(result, expected)
  }

  @Test
  def testBatchProjectableSourceFullProjection(): Unit = {
    val (tableSource, tableName) = csvTable
    val util = batchTestUtil()
    val tableEnv = util.tableEnv

    tableEnv.asInstanceOf[TableEnvironmentInternal]
      .registerTableSourceInternal(tableName, tableSource)

    val result = tableEnv
      .scan(tableName)
      .select(1)

    val expected = unaryNode(
      "DataSetCalc",
      s"BatchTableSourceScan(table=[[default_catalog, default_database, $tableName]], " +
        s"fields=[], " +
        s"source=[CsvTableSource(read fields: )])",
      term("select", "1 AS _c0")
    )

    util.verifyTable(result, expected)
  }

  @Test
  def testBatchFilterableWithoutPushDown(): Unit = {
    val (tableSource, tableName) = filterableTableSource
    val util = batchTestUtil()
    val tableEnv = util.tableEnv

    tableEnv.asInstanceOf[TableEnvironmentInternal]
      .registerTableSourceInternal(tableName, tableSource)

    val result = tableEnv
        .scan(tableName)
        .select('price, 'id, 'amount)
        .where($"price" * 2 < 32)

    val expected = unaryNode(
      "DataSetCalc",
      batchFilterableSourceTableNode(
        tableName,
        Array("price", "id", "amount"),
        isPushedDown = true,
        ""),
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

    tableEnv.asInstanceOf[TableEnvironmentInternal]
      .registerTableSourceInternal(tableName, tableSource)

    val result = tableEnv
      .scan(tableName)
      .where($"amount" > 2 && $"price" * 2 < 32)
      .select('price, 'name.lowerCase(), 'amount)

    val expected = unaryNode(
      "DataSetCalc",
      batchFilterableSourceTableNode(
        tableName,
        Array("price", "name", "amount"),
        isPushedDown = true,
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

    tableEnv.asInstanceOf[TableEnvironmentInternal]
      .registerTableSourceInternal(tableName, tableSource)

    val result = tableEnv
        .scan(tableName)
        .select('price, 'id, 'amount)
        .where($"amount" > 2 && $"amount" < 32)

    val expected = batchFilterableSourceTableNode(
      tableName,
      Array("price", "id", "amount"),
      isPushedDown = true,
      "'amount > 2 && 'amount < 32")
    util.verifyTable(result, expected)
  }

  @Test
  def testBatchFilterableWithUnconvertedExpression(): Unit = {
    val (tableSource, tableName) = filterableTableSource
    val util = batchTestUtil()
    val tableEnv = util.tableEnv

    tableEnv.asInstanceOf[TableEnvironmentInternal]
      .registerTableSourceInternal(tableName, tableSource)

    val result = tableEnv
        .scan(tableName)
        .select('price, 'id, 'amount)
        .where($"amount" > 2 && $"id" < 1.2 &&
          ($"amount" < 32 || $"amount".cast(Types.LONG) > 10)) // cast can not be converted

    val expected = unaryNode(
      "DataSetCalc",
      batchFilterableSourceTableNode(
        tableName,
        Array("price", "id", "amount"),
        isPushedDown = true,
        "'amount > 2"),
      term("select", "price", "id", "amount"),
      term("where", "AND(<(id, 1.2E0:DOUBLE), OR(<(amount, 32), >(CAST(amount), 10)))")
    )
    util.verifyTable(result, expected)
  }

  @Test
  def testBatchFilterableWithUDF(): Unit = {
    val (tableSource, tableName) = filterableTableSource
    val util = batchTestUtil()
    val tableEnv = util.tableEnv

    tableEnv.asInstanceOf[TableEnvironmentInternal]
      .registerTableSourceInternal(tableName, tableSource)
    val func = Func0
    tableEnv.registerFunction("func0", func)

    val result = tableEnv
        .scan(tableName)
        .select('price, 'id, 'amount)
        .where($"amount" > 2 && call("func0", $"amount") < 32)

    val expected = unaryNode(
      "DataSetCalc",
      batchFilterableSourceTableNode(
        tableName,
        Array("price", "id", "amount"),
        isPushedDown = true,
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

    tableEnv.asInstanceOf[TableEnvironmentInternal]
      .registerTableSourceInternal(tableName, tableSource)

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

    util.tableEnv.asInstanceOf[TableEnvironmentInternal]
      .registerTableSourceInternal(tableName, tableSource)

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

    tableEnv.asInstanceOf[TableEnvironmentInternal]
      .registerTableSourceInternal(tableName, tableSource)

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

    tableEnv.asInstanceOf[TableEnvironmentInternal]
      .registerTableSourceInternal(tableName, tableSource)

    val result = tableEnv
      .scan(tableName)
      .select('price, 'id, 'amount)
      .where($"amount" > 2 && $"price" * 2 < 32)

    val expected = unaryNode(
      "DataStreamCalc",
      streamFilterableSourceTableNode(
        tableName,
        Array("price", "id", "amount"),
        isPushedDown = true,
        "'amount > 2"),
      term("select", "price", "id", "amount"),
      term("where", "<(*(price, 2), 32)")
    )

    util.verifyTable(result, expected)
  }

  @Test
  def testConnectToTableWithProperties(): Unit = {
    val util = streamTestUtil()
    val tableEnv = util.tableEnv

    val path = "cat.db.tab1"
    tableEnv.connect(new ConnectorDescriptor("COLLECTION", 1, false) {
      override protected def toConnectorProperties: JMap[String, String] = {
        val context = new JHashMap[String, String]()
        context.put(CONNECTOR, "COLLECTION")
        context
      }
    }).withSchema(
        new Schema()
          .schema(TableSchema.builder()
            .field("id", DataTypes.INT())
            .field("name", DataTypes.STRING())
            .build())
      ).createTemporaryTable(path)

    val result = tableEnv.from(path)

    val expected = "StreamTableSourceScan(table=[[cat, db, tab1]], fields=[id, name], " +
      "source=[CollectionTableSource(id, name)])"

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

// TODO enable this test once we expose the feature through the table environment
//  @Test
//  def testCsvTableSourceDescriptor(): Unit = {
//    val util = streamTestUtil()
//    val source1 = util.tableEnv
//      .from(
//        FileSystem()
//          .path("/path/to/csv"))
//      .withFormat(
//        Csv()
//          .field("myfield", Types.STRING)
//          .field("myfield2", Types.INT)
//          .quoteCharacter(';')
//          .fieldDelimiter("#")
//          .lineDelimiter("\r\n")
//          .commentPrefix("%%")
//          .ignoreFirstLine()
//          .ignoreParseErrors())
//        .withSchema(
//          Schema()
//          .field("myfield", Types.STRING)
//          .field("myfield2", Types.INT))
//      .toTableSource
//
//    val source2 = new CsvTableSource(
//      "/path/to/csv",
//      Array("myfield", "myfield2"),
//      Array(Types.STRING, Types.INT),
//      "#",
//      "\r\n",
//      ';',
//      true,
//      "%%",
//      true)
//
//    Assert.assertEquals(source1, source2)
//  }

  @Test
  def testTimeLiteralExpressionPushdown(): Unit = {
    val (tableSource, tableName) = filterableTableSourceTimeTypes
    val util = batchTestUtil()
    val tableEnv = util.tableEnv

    tableEnv.asInstanceOf[TableEnvironmentInternal]
      .registerTableSourceInternal(tableName, tableSource)

    val sqlQuery =
      s"""
        |SELECT id from $tableName
        |WHERE
        |  tv > TIME '14:25:02' AND
        |  dv > DATE '2017-02-03' AND
        |  tsv > TIMESTAMP '2017-02-03 14:25:02.000'
      """.stripMargin

    val result = tableEnv.sqlQuery(sqlQuery)

    val expectedFilter =
        "'tv > 14:25:02.toTime && " +
        "'dv > 2017-02-03.toDate && " +
        "'tsv > 2017-02-03 14:25:02.0.toTimestamp"
    val expected = batchFilterableSourceTableNode(
      tableName,
      Array("id"),
      isPushedDown = true,
      expectedFilter
    )
    util.verifyTable(result, expected)
  }

  // utils

  def filterableTableSource:(TableSource[_], String) = {
    val tableSource = TestFilterableTableSource()
    (tableSource, "filterableTable")
  }

  def filterableTableSourceTimeTypes:(TableSource[_], String) = {
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

    val tableSource = TestFilterableTableSource(rowTypeInfo, Seq(row), Set("dv", "tv", "tsv"))
    (tableSource, "filterableTable")
  }

  def csvTable: (CsvTableSource, String) = {
    val csvTable = CommonTestData.getCsvTableSource
    val tableName = "csvTable"
    (csvTable, tableName)
  }

  def batchSourceTableNode(sourceName: String, fields: Array[String]): String = {
    s"BatchTableSourceScan(table=[[default_catalog, default_database, $sourceName]], " +
      s"fields=[${fields.mkString(", ")}], " +
      s"source=[CsvTableSource(read fields: ${fields.mkString(", ")})])"
  }

  def streamSourceTableNode(sourceName: String, fields: Array[String] ): String = {
    s"StreamTableSourceScan(table=[[default_catalog, default_database, $sourceName]], " +
      s"fields=[${fields.mkString(", ")}], " +
      s"source=[CsvTableSource(read fields: ${fields.mkString(", ")})])"
  }

  def batchFilterableSourceTableNode(
      sourceName: String,
      fields: Array[String],
      isPushedDown: Boolean,
      exp: String)
    : String = {
    "BatchTableSourceScan(" +
      s"table=[[default_catalog, default_database, $sourceName]], fields=[${
        fields
          .mkString(", ")
      }], source=[filterPushedDown=[$isPushedDown], filter=[$exp]])"
  }

  def streamFilterableSourceTableNode(
      sourceName: String,
      fields: Array[String],
      isPushedDown: Boolean,
      exp: String)
    : String = {
    "StreamTableSourceScan(" +
      s"table=[[default_catalog, default_database, $sourceName]], fields=[${
        fields
          .mkString(", ")
      }], source=[filterPushedDown=[$isPushedDown], filter=[$exp]])"
  }

}
