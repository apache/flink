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
import org.apache.flink.table.util.TableTestBase
import org.junit.Test

class BatchTableEnvironmentTest extends TableTestBase {

  @Test
  def testSqlWithoutRegistering(): Unit = {
    val util = batchTestUtil()
    val table = util.addTable[(Long, Int, String)]("tableName", 'a, 'b, 'c)

    val sqlTable = util.tableEnv.sqlQuery(s"SELECT a, b, c FROM $table WHERE b > 12")

    util.verifyPlan(sqlTable)
  }

  @Test
  def testSqlWithoutRegistering2(): Unit = {
    val util = batchTestUtil()
    val table = util.addTable[(Long, Int, String)]("tableName", 'a, 'b, 'c)
    val table2 = util.addTable[(Long, Int, String)]('d, 'e, 'f)

    val sqlTable2 = util.tableEnv.sqlQuery(s"SELECT d, e, f FROM $table, $table2 WHERE c = d")
    util.verifyPlan(sqlTable2)
  }
}
