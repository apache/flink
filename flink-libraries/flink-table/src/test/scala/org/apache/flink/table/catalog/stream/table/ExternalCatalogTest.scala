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

package org.apache.flink.table.catalog.stream.table

import org.apache.flink.table.api.scala._
import org.apache.flink.table.catalog.utils.ExternalCatalogTestBase
import org.apache.flink.table.utils.CommonTestData
import org.apache.flink.table.utils.TableTestUtil._
import org.junit.Test

/**
  * Test for external catalog query plan.
  */
class ExternalCatalogTest extends ExternalCatalogTestBase {

  @Test
  def testStreamTableApi(): Unit = {
    val util = streamTestUtil()
    val tEnv = util.tableEnv

    util.tableEnv.registerExternalCatalog("test", CommonTestData.getInMemoryTestCatalog)

    val table1 = tEnv.scan("test", "db1", "tb1")
    val table2 = tEnv.scan("test", "db2", "tb2")

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
      term("union all", "_c0", "e", "_c2")
    )

    util.verifyTable(result, expected)
  }

}
