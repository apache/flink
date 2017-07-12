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

package org.apache.flink.table.catalog.batch.sql

import org.apache.flink.table.catalog.utils.ExternalCatalogTestBase
import org.apache.flink.table.utils.CommonTestData
import org.apache.flink.table.utils.TableTestUtil._
import org.junit.Test

/**
  * Test for external catalog query plan.
  */
class ExternalCatalogTest extends ExternalCatalogTestBase {

  @Test
  def test(): Unit = {
    val util = batchTestUtil()

    util.tableEnv.registerExternalCatalog("test", CommonTestData.getInMemoryTestCatalog)

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
      term("union", "EXPR$0", "e", "g"))

    util.verifySql(sqlQuery, expected)
  }
}
