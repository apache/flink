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

/**
  * Test for testing aggregate plans.
  */
class AggregationsTest extends TableTestBase {

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

}
