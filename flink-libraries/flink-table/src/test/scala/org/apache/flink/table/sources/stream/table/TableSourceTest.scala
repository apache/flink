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

package org.apache.flink.table.sources.stream.table

import org.apache.flink.table.api.scala._
import org.apache.flink.table.sources.TableSourceTestBase
import org.apache.flink.table.utils.TableTestUtil._
import org.junit.Test

class TableSourceTest extends TableSourceTestBase {

  @Test
  def testProjectableSourceScanPlan(): Unit = {
    val (tableSource, tableName) = csvTable
    val util = streamTestUtil()
    val tEnv = util.tableEnv

    tEnv.registerTableSource(tableName, tableSource)

    val result = tEnv
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
  def testProjectableSourceScanNoIdentityCalc(): Unit = {
    val (tableSource, tableName) = csvTable
    val util = streamTestUtil()
    val tEnv = util.tableEnv

    tEnv.registerTableSource(tableName, tableSource)

    val result = tEnv
      .scan(tableName)
      .select('id, 'score, 'first)

    val expected = streamSourceTableNode(tableName, noCalcFields)
    util.verifyTable(result, expected)
  }

  @Test
  def testFilterableSourceScanPlan(): Unit = {
    val (tableSource, tableName) = filterableTableSource
    val util = streamTestUtil()
    val tEnv = util.tableEnv

    tEnv.registerTableSource(tableName, tableSource)

    val result = tEnv
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

}
