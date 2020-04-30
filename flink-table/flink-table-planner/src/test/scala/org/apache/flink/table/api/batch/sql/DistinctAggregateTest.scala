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

class DistinctAggregateTest extends TableTestBase {

  @Test
  def testSingleDistinctAggregate(): Unit = {
    val util = batchTestUtil()
    val table = util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)

    val sqlQuery = "SELECT COUNT(DISTINCT a) FROM MyTable"

    val expected = unaryNode(
      "DataSetAggregate",
      unaryNode(
        "DataSetDistinct",
        unaryNode(
          "DataSetCalc",
          batchTableNode(table),
          term("select", "a")
        ),
        term("distinct", "a")
      ),
      term("select", "COUNT(a) AS EXPR$0")
    )

    util.verifySql(sqlQuery, expected)
  }

  @Test
  def testMultiDistinctAggregateOnSameColumn(): Unit = {
    val util = batchTestUtil()
    val table = util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)

    val sqlQuery = "SELECT COUNT(DISTINCT a), SUM(DISTINCT a), MAX(DISTINCT a) FROM MyTable"

    val expected = unaryNode(
      "DataSetAggregate",
      unaryNode(
        "DataSetDistinct",
        unaryNode(
          "DataSetCalc",
          batchTableNode(table),
          term("select", "a")
        ),
        term("distinct", "a")
      ),
      term("select", "COUNT(a) AS EXPR$0", "SUM(a) AS EXPR$1", "MAX(a) AS EXPR$2")
    )

    util.verifySql(sqlQuery, expected)
  }

  @Test
  def testSingleDistinctAggregateAndOneOrMultiNonDistinctAggregate(): Unit = {
    val util = batchTestUtil()
    val table = util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)

    // case 0x00: DISTINCT on COUNT and Non-DISTINCT on others
    val sqlQuery0 = "SELECT COUNT(DISTINCT a), SUM(b) FROM MyTable"

    val expected0 = unaryNode(
      "DataSetAggregate",
      unaryNode(
        "DataSetAggregate",
        unaryNode(
          "DataSetCalc",
          batchTableNode(table),
          term("select", "a", "b")
        ),
        term("groupBy", "a"),
        term("select", "a", "SUM(b) AS EXPR$1")
      ),
      term("select", "COUNT(a) AS EXPR$0", "SUM(EXPR$1) AS EXPR$1")
    )

    util.verifySql(sqlQuery0, expected0)

    // case 0x01: Non-DISTINCT on COUNT and DISTINCT on others
    val sqlQuery1 = "SELECT COUNT(a), SUM(DISTINCT b) FROM MyTable"

    val expected1 = unaryNode(
      "DataSetAggregate",
      unaryNode(
        "DataSetAggregate",
        unaryNode(
          "DataSetCalc",
          batchTableNode(table),
          term("select", "a", "b")
        ),
        term("groupBy", "b"),
        term("select", "b", "COUNT(a) AS EXPR$0")
      ),
      term("select", "$SUM0(EXPR$0) AS EXPR$0", "SUM(b) AS EXPR$1")
    )

    util.verifySql(sqlQuery1, expected1)
  }

  @Test
  def testMultiDistinctAggregateOnDifferentColumn(): Unit = {
    val util = batchTestUtil()
    val table = util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)

    val sqlQuery = "SELECT COUNT(DISTINCT a), SUM(DISTINCT b) FROM MyTable"

    val expected = binaryNode(
      "DataSetSingleRowJoin",
      unaryNode(
        "DataSetAggregate",
        unaryNode(
          "DataSetDistinct",
          unaryNode(
            "DataSetCalc",
            batchTableNode(table),
            term("select", "a")
          ),
          term("distinct", "a")
        ),
        term("select", "COUNT(a) AS EXPR$0")
      ),
      unaryNode(
        "DataSetAggregate",
        unaryNode(
          "DataSetDistinct",
          unaryNode(
            "DataSetCalc",
            batchTableNode(table),
            term("select", "b")
          ),
          term("distinct", "b")
        ),
        term("select", "SUM(b) AS EXPR$1")
      ),
      term("where", "true"),
      term("join", "EXPR$0", "EXPR$1"),
      term("joinType", "NestedLoopInnerJoin")
    )

    util.verifySql(sqlQuery, expected)
  }

  @Test
  def testMultiDistinctAndNonDistinctAggregateOnDifferentColumn(): Unit = {
    val util = batchTestUtil()
    val table = util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)

    val sqlQuery = "SELECT COUNT(DISTINCT a), SUM(DISTINCT b), COUNT(c) FROM MyTable"

    val expected = unaryNode(
      "DataSetCalc",
      binaryNode(
        "DataSetSingleRowJoin",
        binaryNode(
          "DataSetSingleRowJoin",
          unaryNode(
            "DataSetAggregate",
            unaryNode(
              "DataSetCalc",
              batchTableNode(table),
              term("select", "c")
            ),
            term("select", "COUNT(c) AS EXPR$2")
          ),
          unaryNode(
            "DataSetAggregate",
            unaryNode(
              "DataSetDistinct",
              unaryNode(
                "DataSetCalc",
                batchTableNode(table),
                term("select", "a")
              ),
              term("distinct", "a")
            ),
            term("select", "COUNT(a) AS EXPR$0")
          ),
          term("where", "true"),
          term("join", "EXPR$2, EXPR$0"),
          term("joinType", "NestedLoopInnerJoin")
        ),
        unaryNode(
          "DataSetAggregate",
          unaryNode(
            "DataSetDistinct",
            unaryNode(
              "DataSetCalc",
              batchTableNode(table),
              term("select", "b")
            ),
            term("distinct", "b")
          ),
          term("select", "SUM(b) AS EXPR$1")
        ),
        term("where", "true"),
        term("join", "EXPR$2", "EXPR$0, EXPR$1"),
        term("joinType", "NestedLoopInnerJoin")
      ),
      term("select", "EXPR$0, EXPR$1, EXPR$2")
    )

    util.verifySql(sqlQuery, expected)
  }

  @Test
  def testSingleDistinctAggregateWithGrouping(): Unit = {
    val util = batchTestUtil()
    val table = util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)

    val sqlQuery = "SELECT a, COUNT(a), SUM(DISTINCT b) FROM MyTable GROUP BY a"

    val expected = unaryNode(
      "DataSetAggregate",
      unaryNode(
        "DataSetAggregate",
        unaryNode(
          "DataSetCalc",
          batchTableNode(table),
          term("select", "a", "b")
        ),
        term("groupBy", "a", "b"),
        term("select", "a", "b", "COUNT(a) AS EXPR$1")
      ),
      term("groupBy", "a"),
      term("select", "a", "$SUM0(EXPR$1) AS EXPR$1", "SUM(b) AS EXPR$2")
    )

    util.verifySql(sqlQuery, expected)
  }

  @Test
  def testSingleDistinctAggregateWithGroupingAndCountStar(): Unit = {
    val util = batchTestUtil()
    val table = util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)

    val sqlQuery = "SELECT a, COUNT(*), SUM(DISTINCT b) FROM MyTable GROUP BY a"

    val expected = unaryNode(
      "DataSetAggregate",
      unaryNode(
        "DataSetAggregate",
        unaryNode(
          "DataSetCalc",
          batchTableNode(table),
          term("select", "a", "b")
        ),
        term("groupBy", "a", "b"),
        term("select", "a", "b", "COUNT(*) AS EXPR$1")
      ),
      term("groupBy", "a"),
      term("select", "a", "$SUM0(EXPR$1) AS EXPR$1", "SUM(b) AS EXPR$2")
    )

    util.verifySql(sqlQuery, expected)
  }

  @Test
  def testTwoDistinctAggregateWithGroupingAndCountStar(): Unit = {
    val util = batchTestUtil()
    val table = util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)

    val sqlQuery = "SELECT a, COUNT(*), SUM(DISTINCT b), COUNT(DISTINCT b) FROM MyTable GROUP BY a"

    val expected = unaryNode(
      "DataSetCalc",
      binaryNode(
        "DataSetJoin",
        unaryNode(
          "DataSetAggregate",
          unaryNode(
            "DataSetCalc",
            batchTableNode(table),
            term("select", "a")
          ),
          term("groupBy", "a"),
          term("select", "a", "COUNT(*) AS EXPR$1")
        ),
        unaryNode(
          "DataSetAggregate",
          unaryNode(
            "DataSetDistinct",
            unaryNode(
              "DataSetCalc",
              batchTableNode(table),
              term("select", "a", "b")
            ),
            term("distinct", "a, b")
          ),
          term("groupBy", "a"),
          term("select", "a, SUM(b) AS EXPR$2, COUNT(b) AS EXPR$3")
        ),
        term("where", "IS NOT DISTINCT FROM(a, a0)"),
        term("join", "a, EXPR$1, a0, EXPR$2, EXPR$3"),
        term("joinType", "InnerJoin")
      ),
      term("select", "a, EXPR$1, EXPR$2, EXPR$3")
    )

    util.verifySql(sqlQuery, expected)
  }

  @Test
  def testTwoDifferentDistinctAggregateWithGroupingAndCountStar(): Unit = {
    val util = batchTestUtil()
    val table = util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)

    val sqlQuery = "SELECT a, COUNT(*), SUM(DISTINCT b), COUNT(DISTINCT c) FROM MyTable GROUP BY a"

    val expected = unaryNode(
      "DataSetCalc",
      binaryNode(
        "DataSetJoin",
        unaryNode(
          "DataSetCalc",
          binaryNode(
            "DataSetJoin",
            unaryNode(
              "DataSetAggregate",
              unaryNode(
                "DataSetCalc",
                batchTableNode(table),
                term("select", "a")
              ),
              term("groupBy", "a"),
              term("select", "a, COUNT(*) AS EXPR$1")
            ),
            unaryNode(
              "DataSetAggregate",
              unaryNode(
                "DataSetDistinct",
                unaryNode(
                  "DataSetCalc",
                  batchTableNode(table),
                  term("select", "a", "b")
                ),
                term("distinct", "a, b")
              ),
              term("groupBy", "a"),
              term("select", "a, SUM(b) AS EXPR$2")
            ),
            term("where", "IS NOT DISTINCT FROM(a, a0)"),
            term("join", "a, EXPR$1, a0, EXPR$2"),
            term("joinType", "InnerJoin")
          ),
          term("select", "a, EXPR$1, EXPR$2")
        ),
        unaryNode(
          "DataSetAggregate",
          unaryNode(
            "DataSetDistinct",
            unaryNode(
              "DataSetCalc",
              batchTableNode(table),
              term("select", "a", "c")
            ),
            term("distinct", "a, c")
          ),
          term("groupBy", "a"),
          term("select", "a, COUNT(c) AS EXPR$3")
        ),
        term("where", "IS NOT DISTINCT FROM(a, a0)"),
        term("join", "a, EXPR$1, EXPR$2, a0, EXPR$3"),
        term("joinType", "InnerJoin")
      ),
      term("select", "a, EXPR$1, EXPR$2, EXPR$3")
    )

    util.verifySql(sqlQuery, expected)
  }
}
