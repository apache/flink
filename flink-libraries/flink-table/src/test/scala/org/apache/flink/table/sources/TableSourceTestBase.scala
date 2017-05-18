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

import org.apache.flink.table.utils.{CommonTestData, TableTestBase, TestFilterableTableSource}

class TableSourceTestBase extends TableTestBase {

  protected val projectedFields: Array[String] = Array("last", "id", "score")
  protected val noCalcFields: Array[String] = Array("id", "score", "first")

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
