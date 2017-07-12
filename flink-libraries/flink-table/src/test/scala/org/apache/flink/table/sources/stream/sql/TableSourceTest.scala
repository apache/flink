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

package org.apache.flink.table.sources.stream.sql

import org.apache.flink.table.sources.TableSourceTestBase
import org.apache.flink.table.utils.TableTestUtil._
import org.junit.Test

class TableSourceTest extends TableSourceTestBase {

  @Test
  def testProjectableSourceScanPlan(): Unit = {
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

}
