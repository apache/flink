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
package org.apache.flink.table.api.scala.batch.sql

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.utils.TableTestBase
import org.apache.flink.table.utils.TableTestUtil._
import org.junit.Test

class GroupingSetsTest extends TableTestBase {

  @Test
  def testGroupingSets(): Unit = {
    val util = batchTestUtil()
    util.addTable[(Int, Long, Int)]("MyTable", 'a, 'b, 'c)

    val sqlQuery = "SELECT b, c, avg(a) as a, GROUP_ID() as g FROM MyTable " +
      "GROUP BY GROUPING SETS (b, c)"

    val aggregate = unaryNode(
      "DataSetCalc",
      unaryNode(
        "DataSetAggregate",
        unaryNode(
          "DataSetExpand",
          batchTableNode(0),
          term("expand", "a", "b", "c", "i$b", "i$c")
        ),
        term("groupBy", "b, c, i$b, i$c"),
        term("select", "b", "c", "i$b", "i$c", "AVG(a) AS a")
      ),
      term("select",
        "CASE(i$b, null, b) AS b",
        "CASE(i$c, null, c) AS c",
        "a",
        "+(*(CASE(i$b, 1, 0), 2), CASE(i$c, 1, 0)) AS g") // GROUP_ID()
    )

    util.verifySql(sqlQuery, aggregate)
  }

  @Test
  def testCube(): Unit = {
    val util = batchTestUtil()
    util.addTable[(Int, Long, Int)]("MyTable", 'a, 'b, 'c)

    val sqlQuery = "SELECT b, c, avg(a) as a, GROUP_ID() as g, " +
      "GROUPING(b) as gb, GROUPING(c) as gc, " +
      "GROUPING_ID(b) as gib, GROUPING_ID(c) as gic, " +
      "GROUPING_ID(b, c) as gid " +
      "FROM MyTable " +
      "GROUP BY CUBE (b, c)"

    val expand = unaryNode(
      "DataSetExpand",
      batchTableNode(0),
      term("expand", "a", "b", "c", "i$b", "i$c")
    )

    val group = unaryNode(
      "DataSetAggregate",
      expand,
      term("groupBy", "b, c, i$b, i$c"),
      term("select", "b", "c", "i$b", "i$c", "AVG(a) AS a")
    )

    val aggregate = unaryNode(
      "DataSetCalc",
      group,
      term("select",
        "CASE(i$b, null, b) AS b",
        "CASE(i$c, null, c) AS c",
        "a",
        "+(*(CASE(i$b, 1, 0), 2), CASE(i$c, 1, 0)) AS g", // GROUP_ID()
        "CASE(i$b, 1, 0) AS gb", // GROUPING(b)
        "CASE(i$c, 1, 0) AS gc", // GROUPING(c)
        "CASE(i$b, 1, 0) AS gib", // GROUPING_ID(b)
        "CASE(i$c, 1, 0) AS gic", // GROUPING_ID(c)
        "+(*(CASE(i$b, 1, 0), 2), CASE(i$c, 1, 0)) AS gid") // GROUPING_ID(b, c)
    )

    util.verifySql(sqlQuery, aggregate)
  }

  @Test
  def testRollup(): Unit = {
    val util = batchTestUtil()
    util.addTable[(Int, Long, Int)]("MyTable", 'a, 'b, 'c)

    val sqlQuery = "SELECT b, c, avg(a) as a, GROUP_ID() as g, " +
      "GROUPING(b) as gb, GROUPING(c) as gc, " +
      "GROUPING_ID(b) as gib, GROUPING_ID(c) as gic, " +
      "GROUPING_ID(b, c) as gid " + " FROM MyTable " +
      "GROUP BY ROLLUP (b, c)"

    val expand = unaryNode(
      "DataSetExpand",
      batchTableNode(0),
      term("expand", "a", "b", "c", "i$b", "i$c")
    )

    val group = unaryNode(
      "DataSetAggregate",
      expand,
      term("groupBy", "b, c, i$b, i$c"),
      term("select", "b", "c", "i$b", "i$c", "AVG(a) AS a")
    )

    val aggregate = unaryNode(
      "DataSetCalc",
      group,
      term("select",
        "CASE(i$b, null, b) AS b",
        "CASE(i$c, null, c) AS c",
        "a",
        "+(*(CASE(i$b, 1, 0), 2), CASE(i$c, 1, 0)) AS g", // GROUP_ID()
        "CASE(i$b, 1, 0) AS gb", // GROUPING(b)
        "CASE(i$c, 1, 0) AS gc", // GROUPING(c)
        "CASE(i$b, 1, 0) AS gib", // GROUPING_ID(b)
        "CASE(i$c, 1, 0) AS gic", // GROUPING_ID(c)
        "+(*(CASE(i$b, 1, 0), 2), CASE(i$c, 1, 0)) AS gid") // GROUPING_ID(b, c)
    )

    util.verifySql(sqlQuery, aggregate)
  }
}
