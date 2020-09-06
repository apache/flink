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
import org.apache.flink.table.api._
import org.apache.flink.table.utils.TableTestBase
import org.apache.flink.table.utils.TableTestUtil._

import org.junit.Test

/**
  * Test for testing aggregate plans.
  */
class AggregateTest extends TableTestBase {

  @Test
  def testAggregate(): Unit = {
    val util = batchTestUtil()
    val table = util.addTable[(Int, Long, Int)]("MyTable", 'a, 'b, 'c)

    val sqlQuery = "SELECT avg(a), sum(b), count(c) FROM MyTable"

    val aggregate = unaryNode(
      "DataSetAggregate",
      batchTableNode(table),
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
    val table = util.addTable[(Int, Long, Int)]("MyTable", 'a, 'b, 'c)

    val sqlQuery = "SELECT avg(a), sum(b), count(c) FROM MyTable WHERE a = 1"

    val calcNode = unaryNode(
      "DataSetCalc",
      batchTableNode(table),
      term("select", "CAST(1) AS a", "b", "c"),
      term("where", "=(a, 1)")
    )

    val aggregate = unaryNode(
      "DataSetAggregate",
      calcNode,
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
    val table = util.addTable[(Int, Long, (Int, Long))]("MyTable", 'a, 'b, 'c)

    val sqlQuery = "SELECT avg(a), sum(b), count(c), sum(c._1) FROM MyTable WHERE a = 1"

    val calcNode = unaryNode(
      "DataSetCalc",
      batchTableNode(table),
      term("select", "CAST(1) AS a", "b", "c", "c._1 AS $f3"),
      term("where", "=(a, 1)")
    )

    val aggregate = unaryNode(
      "DataSetAggregate",
      calcNode,
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
    val table = util.addTable[(Int, Long, Int)]("MyTable", 'a, 'b, 'c)

    val sqlQuery = "SELECT avg(a), sum(b), count(c) FROM MyTable GROUP BY a"

    val aggregate = unaryNode(
        "DataSetAggregate",
        batchTableNode(table),
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
    val table = util.addTable[(Int, Long, Int)]("MyTable", 'a, 'b, 'c)

    val sqlQuery = "SELECT avg(a), sum(b), count(c) FROM MyTable WHERE a = 1 GROUP BY a"

    val calcNode = unaryNode(
      "DataSetCalc",
      batchTableNode(table),
      term("select","CAST(1) AS a", "b", "c") ,
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
