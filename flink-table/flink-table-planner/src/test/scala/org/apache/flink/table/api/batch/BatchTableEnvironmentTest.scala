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

package org.apache.flink.table.api.batch

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.utils.TableTestUtil._
import org.apache.flink.table.utils.TableTestBase
import org.junit.Test

class BatchTableEnvironmentTest extends TableTestBase {

  @Test
  def testSqlWithoutRegistering(): Unit = {
    val util = batchTestUtil()
    val table = util.addTable[(Long, Int, String)]("tableName", 'a, 'b, 'c)

    val sqlTable = util.tableEnv.sqlQuery(s"SELECT a, b, c FROM $table WHERE b > 12")

    val expected = unaryNode(
      "DataSetCalc",
      batchTableNode(table),
      term("select", "a, b, c"),
      term("where", ">(b, 12)"))

    util.verifyTable(sqlTable, expected)

    val table2 = util.addTable[(Long, Int, String)]('d, 'e, 'f)

    val sqlTable2 = util.tableEnv.sqlQuery(s"SELECT d, e, f FROM $table, $table2 WHERE c = d")

    val join = unaryNode(
      "DataSetJoin",
      binaryNode(
        "DataSetCalc",
        batchTableNode(table),
        batchTableNode(table2),
        term("select", "c")),
      term("where", "=(c, d)"),
      term("join", "c, d, e, f"),
      term("joinType", "InnerJoin"))

    val expected2 = unaryNode(
      "DataSetCalc",
      join,
      term("select", "d, e, f"))

    util.verifyTable(sqlTable2, expected2)
  }
}
