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
import org.apache.flink.table.runtime.utils.CommonTestData
import org.apache.flink.table.utils.TableTestUtil._
import org.apache.flink.table.utils.TableTestBase
import org.junit.Test

/**
  * Test for external catalog query plan.
  */
class ExternalCatalogTest extends TableTestBase {
  private val table1Path: Array[String] = Array("test", "db1", "tb1")
  private val table1TopLevelPath: Array[String] = Array("test", "tb1")
  private val table1ProjectedFields: Array[String] = Array("a", "b", "c")
  private val table2Path: Array[String] = Array("test", "db2", "tb2")
  private val table2ProjectedFields: Array[String] = Array("d", "e", "g")

  @Test
  def testBatchTableApi(): Unit = {
    val util = batchTestUtil()
    val tableEnv = util.tableEnv

    tableEnv.registerExternalCatalog(
      "test",
      CommonTestData.getInMemoryTestCatalog(isStreaming = false))

    val table1 = tableEnv.scan("test", "db1", "tb1")
    val table2 = tableEnv.scan("test", "db2", "tb2")
    val result = table2
        .select('d * 2, 'e, 'g.upperCase())
        .unionAll(table1.select('a * 2, 'b, 'c.upperCase()))

    val expected = binaryNode(
      "DataSetUnion",
      unaryNode(
        "DataSetCalc",
        sourceBatchTableNode(table2Path, table2ProjectedFields),
        term("select", "*(d, 2) AS _c0", "e", "UPPER(g) AS _c2")
      ),
      unaryNode(
        "DataSetCalc",
        sourceBatchTableNode(table1Path, table1ProjectedFields),
        term("select", "*(a, 2) AS _c0", "b", "UPPER(c) AS _c2")
      ),
      term("all", "true"),
      term("union", "_c0", "e", "_c2")
    )

    util.verifyTable(result, expected)
  }

  @Test
  def testBatchSQL(): Unit = {
    val util = batchTestUtil()

    util.tableEnv.registerExternalCatalog(
      "test",
      CommonTestData.getInMemoryTestCatalog(isStreaming = false))

    val sqlQuery = "SELECT d * 2, e, g FROM test.db2.tb2 WHERE d < 3 UNION ALL " +
        "(SELECT a * 2, b, c FROM test.db1.tb1)"

    val expected = binaryNode(
      "DataSetUnion",
      unaryNode(
        "DataSetCalc",
        sourceBatchTableNode(table2Path, table2ProjectedFields),
        term("select", "*(d, 2) AS EXPR$0", "e", "g"),
        term("where", "<(d, 3)")),
      unaryNode(
        "DataSetCalc",
        sourceBatchTableNode(table1Path, table1ProjectedFields),
        term("select", "*(a, 2) AS EXPR$0", "b", "c")
      ),
      term("all", "true"),
      term("union", "EXPR$0", "e", "g"))

    util.verifySql(sqlQuery, expected)
  }

  @Test
  def testStreamTableApi(): Unit = {
    val util = streamTestUtil()
    val tableEnv = util.tableEnv

    util.tableEnv.registerExternalCatalog(
      "test",
      CommonTestData.getInMemoryTestCatalog(isStreaming = true))

    val table1 = tableEnv.scan("test", "db1", "tb1")
    val table2 = tableEnv.scan("test", "db2", "tb2")

    val result = table2.where("d < 3")
        .select('d * 2, 'e, 'g.upperCase())
        .unionAll(table1.select('a * 2, 'b, 'c.upperCase()))

    val expected = binaryNode(
      "DataStreamUnion",
      unaryNode(
        "DataStreamCalc",
        sourceStreamTableNode(table2Path, table2ProjectedFields),
        term("select", "*(d, 2) AS _c0", "e", "UPPER(g) AS _c2"),
        term("where", "<(d, 3)")
      ),
      unaryNode(
        "DataStreamCalc",
        sourceStreamTableNode(table1Path, table1ProjectedFields),
        term("select", "*(a, 2) AS _c0", "b", "UPPER(c) AS _c2")
      ),
      term("all", "true"),
      term("union all", "_c0", "e", "_c2")
    )

    util.verifyTable(result, expected)
  }

  @Test
  def testStreamSQL(): Unit = {
    val util = streamTestUtil()

    util.tableEnv.registerExternalCatalog(
      "test",
      CommonTestData.getInMemoryTestCatalog(isStreaming = true))

    val sqlQuery = "SELECT d * 2, e, g FROM test.db2.tb2 WHERE d < 3 UNION ALL " +
        "(SELECT a * 2, b, c FROM test.db1.tb1)"

    val expected = binaryNode(
      "DataStreamUnion",
      unaryNode(
        "DataStreamCalc",
        sourceStreamTableNode(table2Path, table2ProjectedFields),
        term("select", "*(d, 2) AS EXPR$0", "e", "g"),
        term("where", "<(d, 3)")),
      unaryNode(
        "DataStreamCalc",
        sourceStreamTableNode(table1Path, table1ProjectedFields),
        term("select", "*(a, 2) AS EXPR$0", "b", "c")
      ),
      term("all", "true"),
      term("union all", "EXPR$0", "e", "g"))

    util.verifySql(sqlQuery, expected)
  }

  @Test
  def testTopLevelTable(): Unit = {
    val util = batchTestUtil()
    val tableEnv = util.tableEnv

    tableEnv.registerExternalCatalog(
      "test",
      CommonTestData.getInMemoryTestCatalog(isStreaming = false))

    val table1 = tableEnv.scan("test", "tb1")
    val table2 = tableEnv.scan("test", "db2", "tb2")
    val result = table2
      .select('d * 2, 'e, 'g.upperCase())
      .unionAll(table1.select('a * 2, 'b, 'c.upperCase()))

    val expected = binaryNode(
      "DataSetUnion",
      unaryNode(
        "DataSetCalc",
        sourceBatchTableNode(table2Path, table2ProjectedFields),
        term("select", "*(d, 2) AS _c0", "e", "UPPER(g) AS _c2")
      ),
      unaryNode(
        "DataSetCalc",
        sourceBatchTableNode(table1TopLevelPath, table1ProjectedFields),
        term("select", "*(a, 2) AS _c0", "b", "UPPER(c) AS _c2")
      ),
      term("all", "true"),
      term("union", "_c0", "e", "_c2")
    )

    util.verifyTable(result, expected)
  }

  def sourceBatchTableNode(
      sourceTablePath: Array[String],
      fields: Array[String]): String = {
    s"BatchTableSourceScan(table=[[${sourceTablePath.mkString(", ")}]], " +
        s"fields=[${fields.mkString(", ")}], " +
        s"source=[CsvTableSource(read fields: ${fields.mkString(", ")})])"
  }

  def sourceStreamTableNode(sourceTablePath: Array[String], fields: Array[String]): String = {
    s"StreamTableSourceScan(table=[[${sourceTablePath.mkString(", ")}]], " +
      s"fields=[${fields.mkString(", ")}], " +
      s"source=[CsvTableSource(read fields: ${fields.mkString(", ")})])"
  }
}
