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

package org.apache.flink.table.api.batch.sql

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

    val aggregate = binaryNode(
      "DataSetUnion",
      unaryNode(
        "DataSetCalc",
        unaryNode(
          "DataSetAggregate",
          batchTableNode(0),
          term("groupBy", "b"),
          term("select", "b", "AVG(a) AS a")
        ),
        term("select", "b", "null AS c", "a", "1 AS g")
      ),
      unaryNode(
        "DataSetCalc",
        unaryNode(
          "DataSetAggregate",
          batchTableNode(0),
          term("groupBy", "c"),
          term("select", "c", "AVG(a) AS a")
        ),
        term("select", "null AS b", "c", "a", "2 AS g")
      ),
      term("union", "b", "c", "a", "g")
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

    val group1 = unaryNode(
      "DataSetCalc",
      unaryNode(
        "DataSetAggregate",
        batchTableNode(0),
        term("groupBy", "b", "c"),
        term("select", "b", "c", "AVG(a) AS a")
      ),
      term("select", "b", "c", "a", "3 AS g", "1 AS gb", "1 AS gc",
        "1 AS gib", "1 AS gic", "3 AS gid")
    )

    val group2 = unaryNode(
      "DataSetCalc",
      unaryNode(
        "DataSetAggregate",
        batchTableNode(0),
        term("groupBy", "b"),
        term("select", "b", "AVG(a) AS a")
      ),
      term("select", "b", "null AS c", "a", "1 AS g", "1 AS gb", "0 AS gc",
        "1 AS gib", "0 AS gic", "2 AS gid")
    )

    val group3 = unaryNode(
      "DataSetCalc",
      unaryNode(
        "DataSetAggregate",
        batchTableNode(0),
        term("groupBy", "c"),
        term("select", "c", "AVG(a) AS a")
      ),
      term("select", "null AS b", "c", "a", "2 AS g", "0 AS gb", "1 AS gc",
        "0 AS gib", "1 AS gic", "1 AS gid")
    )

    val group4 = unaryNode(
      "DataSetCalc",
      unaryNode(
        "DataSetAggregate",
        batchTableNode(0),
        term("select", "AVG(a) AS a")
      ),
      term(
        "select", "null AS b", "null AS c", "a", "0 AS g", "0 AS gb", "0 AS gc",
        "0 AS gib", "0 AS gic", "0 AS gid")
    )

    val union = binaryNode(
      "DataSetUnion",
      binaryNode(
        "DataSetUnion",
        binaryNode(
          "DataSetUnion",
          group1,
          group2,
          term("union", "b", "c", "a", "g", "gb", "gc", "gib", "gic", "gid")
        ),
        group3,
        term("union", "b", "c", "a", "g", "gb", "gc", "gib", "gic", "gid")
      ),
      group4,
      term("union", "b", "c", "a", "g", "gb", "gc", "gib", "gic", "gid")
    )

    util.verifySql(sqlQuery, union)
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

    val group1 = unaryNode(
      "DataSetCalc",
      unaryNode(
        "DataSetAggregate",
        batchTableNode(0),
        term("groupBy", "b", "c"),
        term("select", "b", "c", "AVG(a) AS a")
      ),
      term("select", "b", "c", "a", "3 AS g", "1 AS gb", "1 AS gc",
        "1 AS gib", "1 AS gic", "3 AS gid")
    )

    val group2 = unaryNode(
      "DataSetCalc",
      unaryNode(
        "DataSetAggregate",
        batchTableNode(0),
        term("groupBy", "b"),
        term("select", "b", "AVG(a) AS a")
      ),
      term("select", "b", "null AS c", "a", "1 AS g", "1 AS gb", "0 AS gc",
        "1 AS gib", "0 AS gic", "2 AS gid")
    )

    val group3 = unaryNode(
      "DataSetCalc",
      unaryNode(
        "DataSetAggregate",
        batchTableNode(0),
        term("select", "AVG(a) AS a")
      ),
      term(
        "select", "null AS b", "null AS c", "a", "0 AS g", "0 AS gb", "0 AS gc",
        "0 AS gib", "0 AS gic", "0 AS gid")
    )

    val union = binaryNode(
      "DataSetUnion",
      binaryNode(
        "DataSetUnion",
        group1,
        group2,
        term("union", "b", "c", "a", "g", "gb", "gc", "gib", "gic", "gid")
      ),
      group3,
      term("union", "b", "c", "a", "g", "gb", "gc", "gib", "gic", "gid")
    )

    util.verifySql(sqlQuery, union)
  }
}
