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

package org.apache.flink.table.sources

import org.apache.flink.table.api.Types
import org.apache.flink.table.utils.TableTestUtil._
import org.junit.{Assert, Test}

class TableSourceTest extends TableSourceTestBase {

  @Test
  def testTableSourceScanToString(): Unit = {
    val (tableSource1, _) = filterableTableSource
    val (tableSource2, _) = filterableTableSource
    val util = batchTestUtil()
    val tEnv = util.tableEnv

    tEnv.registerTableSource("table1", tableSource1)
    tEnv.registerTableSource("table2", tableSource2)

    val table1 = tEnv.scan("table1").where("amount > 2")
    val table2 = tEnv.scan("table2").where("amount > 2")
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

}
