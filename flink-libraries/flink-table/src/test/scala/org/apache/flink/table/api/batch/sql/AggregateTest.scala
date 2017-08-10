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
import org.apache.flink.table.utils.TableTestUtil._
import org.apache.flink.table.utils.TableTestBase
import org.junit.Test

/**
  * Test for testing aggregate plans.
  */
class AggregateTest extends TableTestBase {

  @Test
  def testAggregate(): Unit = {
    val util = batchTestUtil()
    util.addTable[(Int, Long, Int)]("MyTable", 'a, 'b, 'c)

    val sqlQuery = "SELECT avg(a), sum(b), count(c) FROM MyTable"

    val setValues = unaryNode(
      "DataSetValues",
      batchTableNode(0),
      tuples(List(null,null,null)),
      term("values","a","b","c")
    )
    val union = unaryNode(
      "DataSetUnion",
      setValues,
      term("union","a","b","c")
    )

    val aggregate = unaryNode(
      "DataSetAggregate",
      union,
      term("select",
        "AVG(a) AS EXPR$0",
        "SUM(b) AS EXPR$1",
        "COUNT(c) AS EXPR$2")
    )
    util.verifySql(sqlQuery, aggregate)
  }

  @Test
  def testAggregateWithFilter(): Unit = {
    val util = batchTestUtil()
    util.addTable[(Int, Long, Int)]("MyTable", 'a, 'b, 'c)

    val sqlQuery = "SELECT avg(a), sum(b), count(c) FROM MyTable WHERE a = 1"

    val calcNode = unaryNode(
      "DataSetCalc",
      batchTableNode(0),
      term("select", "a", "b", "c"),
      term("where", "=(a, 1)")
    )

    val setValues =  unaryNode(
        "DataSetValues",
        calcNode,
        tuples(List(null,null,null)),
        term("values","a","b","c")
    )

    val union = unaryNode(
      "DataSetUnion",
      setValues,
      term("union","a","b","c")
    )

    val aggregate = unaryNode(
      "DataSetAggregate",
      union,
      term("select",
        "AVG(a) AS EXPR$0",
        "SUM(b) AS EXPR$1",
        "COUNT(c) AS EXPR$2")
    )
    util.verifySql(sqlQuery, aggregate)
  }

  @Test
  def testAggregateWithFilterOnNestedFields(): Unit = {
    val util = batchTestUtil()
    util.addTable[(Int, Long, (Int, Long))]("MyTable", 'a, 'b, 'c)

    val sqlQuery = "SELECT avg(a), sum(b), count(c), sum(c._1) FROM MyTable WHERE a = 1"

    val calcNode = unaryNode(
      "DataSetCalc",
      batchTableNode(0),
      term("select", "CAST(1) AS a", "b", "c", "c._1 AS $f3"),
      term("where", "=(a, 1)")
    )

    val setValues =  unaryNode(
      "DataSetValues",
      calcNode,
      tuples(List(null,null,null,null)),
      term("values","a","b","c","$f3")
    )

    val union = unaryNode(
      "DataSetUnion",
      setValues,
      term("union","a","b","c","$f3")
    )

    val aggregate = unaryNode(
      "DataSetAggregate",
      union,
      term("select",
        "AVG(a) AS EXPR$0",
        "SUM(b) AS EXPR$1",
        "COUNT(c) AS EXPR$2",
        "SUM($f3) AS EXPR$3")
    )

    util.verifySql(sqlQuery, aggregate)
  }

  @Test
  def testGroupAggregate(): Unit = {
    val util = batchTestUtil()
    util.addTable[(Int, Long, Int)]("MyTable", 'a, 'b, 'c)

    val sqlQuery = "SELECT avg(a), sum(b), count(c) FROM MyTable GROUP BY a"

    val aggregate = unaryNode(
        "DataSetAggregate",
        batchTableNode(0),
        term("groupBy", "a"),
        term("select",
          "a",
          "AVG(a) AS EXPR$0",
          "SUM(b) AS EXPR$1",
          "COUNT(c) AS EXPR$2")
    )
    val expected = unaryNode(
        "DataSetCalc",
        aggregate,
        term("select",
          "EXPR$0",
          "EXPR$1",
          "EXPR$2")
    )
    util.verifySql(sqlQuery, expected)
  }

  @Test
  def testGroupAggregateWithFilter(): Unit = {
    val util = batchTestUtil()
    util.addTable[(Int, Long, Int)]("MyTable", 'a, 'b, 'c)

    val sqlQuery = "SELECT avg(a), sum(b), count(c) FROM MyTable WHERE a = 1 GROUP BY a"

    val calcNode = unaryNode(
      "DataSetCalc",
      batchTableNode(0),
      term("select","a", "b", "c") ,
      term("where","=(a, 1)")
    )

    val aggregate = unaryNode(
        "DataSetAggregate",
        calcNode,
        term("groupBy", "a"),
        term("select",
          "a",
          "AVG(a) AS EXPR$0",
          "SUM(b) AS EXPR$1",
          "COUNT(c) AS EXPR$2")
    )
    val expected = unaryNode(
        "DataSetCalc",
        aggregate,
        term("select",
          "EXPR$0",
          "EXPR$1",
          "EXPR$2")
    )
    util.verifySql(sqlQuery, expected)
  }

  @Test
  def testGroupingSets(): Unit = {
    val util = batchTestUtil()
    util.addTable[(Int, Long, Int)]("MyTable", 'a, 'b, 'c)

    val sqlQuery = "SELECT b, c, avg(a) as a, GROUP_ID() as g FROM MyTable " +
      "GROUP BY GROUPING SETS (b, c)"

    val aggregate = unaryNode(
      "DataSetCalc",
      binaryNode(
        "DataSetUnion",
        unaryNode(
          "DataSetAggregate",
          batchTableNode(0),
          term("groupBy", "b"),
          term("select", "b", "AVG(a) AS c")
        ),
        unaryNode(
          "DataSetAggregate",
          batchTableNode(0),
          term("groupBy", "c"),
          term("select", "c AS b", "AVG(a) AS c")
        ),
        term("union", "b", "c", "i$b", "i$c", "a")
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

    val group1 = unaryNode(
      "DataSetAggregate",
      batchTableNode(0),
      term("groupBy", "b, c"),
      term("select", "b", "c",
           "AVG(a) AS i$b")
    )

    val group2 = unaryNode(
      "DataSetAggregate",
      batchTableNode(0),
      term("groupBy", "b"),
      term("select", "b",
           "AVG(a) AS c")
    )

    val group3 = unaryNode(
      "DataSetAggregate",
      batchTableNode(0),
      term("groupBy", "c"),
      term("select", "c AS b",
           "AVG(a) AS c")
    )

    val group4 = unaryNode(
      "DataSetAggregate",
      batchTableNode(0),
      term("select",
           "AVG(a) AS b")
    )

    val union1 = binaryNode(
      "DataSetUnion",
      group1, group2,
      term("union", "b", "c", "i$b", "i$c", "a")
    )

    val union2 = binaryNode(
      "DataSetUnion",
      union1, group3,
      term("union", "b", "c", "i$b", "i$c", "a")
    )

    val union3 = binaryNode(
      "DataSetUnion",
      union2, group4,
      term("union", "b", "c", "i$b", "i$c", "a")
    )

    val aggregate = unaryNode(
      "DataSetCalc",
      union3,
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

    val group1 = unaryNode(
      "DataSetAggregate",
      batchTableNode(0),
      term("groupBy", "b, c"),
      term("select", "b", "c",
           "AVG(a) AS i$b")
    )

    val group2 = unaryNode(
      "DataSetAggregate",
      batchTableNode(0),
      term("groupBy", "b"),
      term("select", "b",
           "AVG(a) AS c")
    )

    val group3 = unaryNode(
      "DataSetAggregate",
      batchTableNode(0),
      term("select",
           "AVG(a) AS b")
    )

    val union1 = binaryNode(
      "DataSetUnion",
      group1, group2,
      term("union", "b", "c", "i$b", "i$c", "a")
    )

    val union2 = binaryNode(
      "DataSetUnion",
      union1, group3,
      term("union", "b", "c", "i$b", "i$c", "a")
    )

    val aggregate = unaryNode(
      "DataSetCalc",
      union2,
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
