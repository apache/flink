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

package org.apache.flink.api.table

import org.apache.flink.api.scala.table._
import org.apache.flink.api.scala.utils.CommonTestData
import org.apache.flink.api.table.utils.TableTestBase
import org.apache.flink.api.table.utils.TableTestUtil._
import org.junit.Test

class ProjectableTableSourceTest extends TableTestBase {

  private val csvTable = CommonTestData.getCsvTableSource
  private val tableName = "csvTable"
  private val projectedFields: Array[String] = Array("last", "id", "score")
  private val noCalcFields: Array[String] = Array("id", "score", "first")

  @Test
  def testBatchProjectableSourceScanPlanTableApi(): Unit = {
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
    val util = batchTestUtil()
    val tEnv = util.tEnv

    tEnv.registerTableSource(tableName, csvTable)

    val result = tEnv
      .scan(tableName)
      .select(noCalcFields.mkString(", "))

    val expected = sourceBatchTableNode(tableName, noCalcFields)
    util.verifyTable(result, expected)
  }

  @Test
  def testStreamProjectableSourceScanPlanTableApi(): Unit = {
    val util = streamTestUtil()
    val tEnv = util.tEnv

    tEnv.registerTableSource(tableName, csvTable)

    val result = tEnv
      .ingest(tableName)
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
    val util = streamTestUtil()
    val tEnv = util.tEnv

    tEnv.registerTableSource(tableName, csvTable)

    val result = tEnv
      .ingest(tableName)
      .select(noCalcFields.mkString(", "))

    val expected = sourceStreamTableNode(tableName, noCalcFields)
    util.verifyTable(result, expected)
  }

  def sourceBatchTableNode(sourceName: String, fields: Array[String]): String = {
    s"BatchTableSourceScan(table=[[$sourceName]], fields=[${fields.mkString(", ")}])"
  }

  def sourceStreamTableNode(sourceName: String, fields: Array[String] ): String = {
    s"StreamTableSourceScan(table=[[$sourceName]], fields=[${fields.mkString(", ")}])"
  }
}
