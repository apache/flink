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

class SingleRowJoinTest extends TableTestBase {

  @Test
  def testSingleRowCrossJoin(): Unit = {
    val util = batchTestUtil()
    util.addTable[(Int, Int)]("A", 'a1, 'a2)

    val query =
      "SELECT a1, asum " +
      "FROM A, (SELECT sum(a1) + sum(a2) AS asum FROM A)"

    val expected =
      binaryNode(
        "DataSetSingleRowJoin",
        unaryNode(
          "DataSetCalc",
          batchTableNode(0),
          term("select", "a1")
        ),
        unaryNode(
          "DataSetCalc",
          unaryNode(
            "DataSetAggregate",
            batchTableNode(0),
            term("select", "SUM(a1) AS $f0", "SUM(a2) AS $f1")
          ),
          term("select", "+($f0, $f1) AS asum")
        ),
        term("where", "true"),
        term("join", "a1", "asum"),
        term("joinType", "NestedLoopInnerJoin")
      )

    util.verifySql(query, expected)
  }

  @Test
  def testSingleRowEquiJoin(): Unit = {
    val util = batchTestUtil()
    util.addTable[(Int, String)]("A", 'a1, 'a2)

    val query =
      "SELECT a1, a2 " +
      "FROM A, (SELECT count(a1) AS cnt FROM A) " +
      "WHERE a1 = cnt"

    val expected =
      unaryNode(
        "DataSetCalc",
        binaryNode(
          "DataSetSingleRowJoin",
          batchTableNode(0),
          unaryNode(
            "DataSetAggregate",
            unaryNode(
              "DataSetCalc",
              batchTableNode(0),
              term("select", "a1")
            ),
            term("select", "COUNT(a1) AS cnt")
          ),
          term("where", "=(CAST(a1), cnt)"),
          term("join", "a1", "a2", "cnt"),
          term("joinType", "NestedLoopInnerJoin")
        ),
        term("select", "a1", "a2")
      )

    util.verifySql(query, expected)
  }

  @Test
  def testSingleRowNotEquiJoin(): Unit = {
    val util = batchTestUtil()
    util.addTable[(Int, String)]("A", 'a1, 'a2)

    val query =
      "SELECT a1, a2 " +
      "FROM A, (SELECT count(a1) AS cnt FROM A) " +
      "WHERE a1 < cnt"

    val expected =
      unaryNode(
        "DataSetCalc",
        binaryNode(
          "DataSetSingleRowJoin",
          batchTableNode(0),
          unaryNode(
            "DataSetAggregate",
            unaryNode(
              "DataSetCalc",
              batchTableNode(0),
              term("select", "a1")
            ),
            term("select", "COUNT(a1) AS cnt")
          ),
          term("where", "<(a1, cnt)"),
          term("join", "a1", "a2", "cnt"),
          term("joinType", "NestedLoopInnerJoin")
        ),
        term("select", "a1", "a2")
      )

    util.verifySql(query, expected)
  }

  @Test
  def testSingleRowJoinWithComplexPredicate(): Unit = {
    val util = batchTestUtil()
    util.addTable[(Int, Long)]("A", 'a1, 'a2)
    util.addTable[(Int, Long)]("B", 'b1, 'b2)

    val query =
      "SELECT a1, a2, b1, b2 " +
        "FROM A, (SELECT min(b1) AS b1, max(b2) AS b2 FROM B) " +
        "WHERE a1 < b1 AND a2 = b2"

    val expected = binaryNode(
      "DataSetSingleRowJoin",
      batchTableNode(0),
      unaryNode(
        "DataSetAggregate",
        batchTableNode(1),
        term("select", "MIN(b1) AS b1", "MAX(b2) AS b2")
      ),
      term("where", "AND(<(a1, b1)", "=(a2, b2))"),
      term("join", "a1", "a2", "b1", "b2"),
      term("joinType", "NestedLoopInnerJoin")
    )

    util.verifySql(query, expected)
  }

  @Test
  def testRightSingleLeftJoinEqualPredicate(): Unit = {
    val util = batchTestUtil()
    util.addTable[(Long, Int)]("A", 'a1, 'a2)
    util.addTable[(Int, Int)]("B", 'b1, 'b2)

    val queryLeftJoin =
      "SELECT a2 " +
        "FROM A " +
        "  LEFT JOIN " +
        "(SELECT COUNT(*) AS cnt FROM B) AS x " +
        "  ON a1 = cnt"

    val expected =
      unaryNode(
        "DataSetCalc",
        unaryNode(
          "DataSetSingleRowJoin",
          batchTableNode(0),
          term("where", "=(a1, cnt)"),
          term("join", "a1", "a2", "cnt"),
          term("joinType", "NestedLoopLeftJoin")
        ),
        term("select", "a2")
      ) + "\n" +
        unaryNode(
          "DataSetAggregate",
          unaryNode(
            "DataSetCalc",
            batchTableNode(1),
            term("select", "0 AS $f0")),
          term("select", "COUNT(*) AS cnt")
        )

    util.verifySql(queryLeftJoin, expected)
  }

  @Test
  def testRightSingleLeftJoinNotEqualPredicate(): Unit = {
    val util = batchTestUtil()
    util.addTable[(Long, Int)]("A", 'a1, 'a2)
    util.addTable[(Int, Int)]("B", 'b1, 'b2)

    val queryLeftJoin =
      "SELECT a2 " +
        "FROM A " +
        "  LEFT JOIN " +
        "(SELECT COUNT(*) AS cnt FROM B) AS x " +
        "  ON a1 > cnt"

    val expected =
      unaryNode(
        "DataSetCalc",
        unaryNode(
          "DataSetSingleRowJoin",
          batchTableNode(0),
          term("where", ">(a1, cnt)"),
          term("join", "a1", "a2", "cnt"),
          term("joinType", "NestedLoopLeftJoin")
        ),
        term("select", "a2")
      ) + "\n" +
        unaryNode(
          "DataSetAggregate",
          unaryNode(
            "DataSetCalc",
            batchTableNode(1),
            term("select", "0 AS $f0")),
          term("select", "COUNT(*) AS cnt")
        )

    util.verifySql(queryLeftJoin, expected)
  }

  @Test
  def testLeftSingleRightJoinEqualPredicate(): Unit = {
    val util = batchTestUtil()
    util.addTable[(Long, Long)]("A", 'a1, 'a2)
    util.addTable[(Long, Long)]("B", 'b1, 'b2)

    val queryRightJoin =
      "SELECT a1 " +
        "FROM (SELECT COUNT(*) AS cnt FROM B) " +
        "  RIGHT JOIN " +
        "A " +
        "  ON cnt = a2"

    val expected =
      unaryNode(
        "DataSetCalc",
        unaryNode(
          "DataSetSingleRowJoin",
          "",
          term("where", "=(cnt, a2)"),
          term("join", "cnt", "a1", "a2"),
          term("joinType", "NestedLoopRightJoin")
        ),
        term("select", "a1")
      ) + unaryNode(
        "DataSetAggregate",
        unaryNode(
          "DataSetCalc",
          batchTableNode(1),
          term("select", "0 AS $f0")),
        term("select", "COUNT(*) AS cnt")
      ) + "\n" +
        batchTableNode(0)

    util.verifySql(queryRightJoin, expected)
  }

  @Test
  def testLeftSingleRightJoinNotEqualPredicate(): Unit = {
    val util = batchTestUtil()
    util.addTable[(Long, Long)]("A", 'a1, 'a2)
    util.addTable[(Long, Long)]("B", 'b1, 'b2)

    val queryRightJoin =
      "SELECT a1 " +
        "FROM (SELECT COUNT(*) AS cnt FROM B) " +
        "  RIGHT JOIN " +
        "A " +
        "  ON cnt < a2"

    val expected =
      unaryNode(
        "DataSetCalc",
        unaryNode(
          "DataSetSingleRowJoin",
          "",
          term("where", "<(cnt, a2)"),
          term("join", "cnt", "a1", "a2"),
          term("joinType", "NestedLoopRightJoin")
        ),
        term("select", "a1")
      ) +
        unaryNode(
          "DataSetAggregate",
          unaryNode(
            "DataSetCalc",
            batchTableNode(1),
            term("select", "0 AS $f0")),
          term("select", "COUNT(*) AS cnt")
        ) + "\n" +
        batchTableNode(0)

    util.verifySql(queryRightJoin, expected)
  }

  @Test
  def testSingleRowJoinInnerJoin(): Unit = {
    val util = batchTestUtil()
    util.addTable[(Int, Int)]("A", 'a1, 'a2)
    val query =
      "SELECT a2, sum(a1) " +
        "FROM A " +
        "GROUP BY a2 " +
        "HAVING sum(a1) > (SELECT sum(a1) * 0.1 FROM A)"

    val expected =
      unaryNode(
        "DataSetCalc",
        unaryNode(
          "DataSetSingleRowJoin",
          unaryNode(
            "DataSetAggregate",
            batchTableNode(0),
            term("groupBy", "a2"),
            term("select", "a2", "SUM(a1) AS EXPR$1")
          ),
          term("where", ">(EXPR$1, EXPR$0)"),
          term("join", "a2", "EXPR$1", "EXPR$0"),
          term("joinType", "NestedLoopInnerJoin")
        ),
        term("select", "a2", "EXPR$1")
      ) + "\n" +
        unaryNode(
          "DataSetCalc",
          unaryNode(
            "DataSetAggregate",
            unaryNode(
              "DataSetCalc",
              batchTableNode(0),
              term("select", "a1")
            ),
            term("select", "SUM(a1) AS $f0")
          ),
          term("select", "*($f0, 0.1) AS EXPR$0")
        )

    util.verifySql(query, expected)
  }
}
