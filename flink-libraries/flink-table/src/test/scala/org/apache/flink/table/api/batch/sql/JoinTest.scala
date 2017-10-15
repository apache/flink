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

class JoinTest extends TableTestBase {

  @Test
  def testLeftOuterJoinEquiPred(): Unit = {
    val util = batchTestUtil()
    util.addTable[(Int, Long, String)]("t", 'a, 'b, 'c)
    util.addTable[(Long, String, Int)]("s", 'x, 'y, 'z)

    val query = "SELECT b, y FROM t LEFT OUTER JOIN s ON a = z"
    val result = util.tableEnv.sqlQuery(query)

    val expected = unaryNode(
      "DataSetCalc",
      binaryNode(
        "DataSetJoin",
        unaryNode(
          "DataSetCalc",
          batchTableNode(0),
          term("select", "a", "b")
        ),
        unaryNode(
          "DataSetCalc",
          batchTableNode(1),
          term("select", "y", "z")
        ),
        term("where", "=(a, z)"),
        term("join", "a", "b", "y", "z"),
        term("joinType", "LeftOuterJoin")
      ),
      term("select", "b", "y")
    )

    util.verifyTable(result, expected)
  }

  @Test
  def testLeftOuterJoinEquiAndLocalPred(): Unit = {
    val util = batchTestUtil()
    util.addTable[(Int, Long, String)]("t", 'a, 'b, 'c)
    util.addTable[(Long, String, Int)]("s", 'x, 'y, 'z)

    val query = "SELECT b, y FROM t LEFT OUTER JOIN s ON a = z AND b < 2"
    val result = util.tableEnv.sqlQuery(query)

    val expected = unaryNode(
      "DataSetCalc",
      binaryNode(
        "DataSetJoin",
        unaryNode(
          "DataSetCalc",
          batchTableNode(0),
          term("select", "a", "b", "<(b, 2) AS $f3")
        ),
        unaryNode(
          "DataSetCalc",
          batchTableNode(1),
          term("select", "y", "z")
        ),
        term("where", "AND(=(a, z), $f3)"),
        term("join", "a", "b", "$f3", "y", "z"),
        term("joinType", "LeftOuterJoin")
      ),
      term("select", "b", "y")
    )

    util.verifyTable(result, expected)
  }

  @Test
  def testLeftOuterJoinEquiAndNonEquiPred(): Unit = {
    val util = batchTestUtil()
    util.addTable[(Int, Long, String)]("t", 'a, 'b, 'c)
    util.addTable[(Long, String, Int)]("s", 'x, 'y, 'z)

    val query = "SELECT b, y FROM t LEFT OUTER JOIN s ON a = z AND b < x"
    val result = util.tableEnv.sqlQuery(query)

    val expected = unaryNode(
      "DataSetCalc",
      binaryNode(
        "DataSetJoin",
        unaryNode(
          "DataSetCalc",
          batchTableNode(0),
          term("select", "a", "b")
        ),
        batchTableNode(1),
        term("where", "AND(=(a, z), <(b, x))"),
        term("join", "a", "b", "x", "y", "z"),
        term("joinType", "LeftOuterJoin")
      ),
      term("select", "b", "y")
    )

    util.verifyTable(result, expected)
  }

  @Test
  def testRightOuterJoinEquiPred(): Unit = {
    val util = batchTestUtil()
    util.addTable[(Int, Long, String)]("t", 'a, 'b, 'c)
    util.addTable[(Long, String, Int)]("s", 'x, 'y, 'z)

    val query = "SELECT b, y FROM t RIGHT OUTER JOIN s ON a = z"
    val result = util.tableEnv.sqlQuery(query)

    val expected = unaryNode(
      "DataSetCalc",
      binaryNode(
        "DataSetJoin",
        unaryNode(
          "DataSetCalc",
          batchTableNode(0),
          term("select", "a", "b")
        ),
        unaryNode(
          "DataSetCalc",
          batchTableNode(1),
          term("select", "y", "z")
        ),
        term("where", "=(a, z)"),
        term("join", "a", "b", "y", "z"),
        term("joinType", "RightOuterJoin")
      ),
      term("select", "b", "y")
    )

    util.verifyTable(result, expected)
  }

  @Test
  def testRightOuterJoinEquiAndLocalPred(): Unit = {
    val util = batchTestUtil()
    util.addTable[(Int, Long, String)]("t", 'a, 'b, 'c)
    util.addTable[(Long, String, Int)]("s", 'x, 'y, 'z)

    val query = "SELECT b, x FROM t RIGHT OUTER JOIN s ON a = z AND x < 2"
    val result = util.tableEnv.sqlQuery(query)

    val expected = unaryNode(
      "DataSetCalc",
      binaryNode(
        "DataSetJoin",
        unaryNode(
          "DataSetCalc",
          batchTableNode(0),
          term("select", "a", "b")
        ),
        unaryNode(
          "DataSetCalc",
          batchTableNode(1),
          term("select", "x", "z", "<(x, 2) AS $f3")
        ),
        term("where", "AND(=(a, z), $f3)"),
        term("join", "a", "b", "x", "z", "$f3"),
        term("joinType", "RightOuterJoin")
      ),
      term("select", "b", "x")
    )

    util.verifyTable(result, expected)
  }

  @Test
  def testRightOuterJoinEquiAndNonEquiPred(): Unit = {
    val util = batchTestUtil()
    util.addTable[(Int, Long, String)]("t", 'a, 'b, 'c)
    util.addTable[(Long, String, Int)]("s", 'x, 'y, 'z)

    val query = "SELECT b, y FROM t RIGHT OUTER JOIN s ON a = z AND b < x"
    val result = util.tableEnv.sqlQuery(query)

    val expected = unaryNode(
      "DataSetCalc",
      binaryNode(
        "DataSetJoin",
        unaryNode(
          "DataSetCalc",
          batchTableNode(0),
          term("select", "a", "b")
        ),
        batchTableNode(1),
        term("where", "AND(=(a, z), <(b, x))"),
        term("join", "a", "b", "x", "y", "z"),
        term("joinType", "RightOuterJoin")
      ),
      term("select", "b", "y")
    )

    util.verifyTable(result, expected)
  }

  @Test
  def testFullOuterJoinEquiPred(): Unit = {
    val util = batchTestUtil()
    util.addTable[(Int, Long, String)]("t", 'a, 'b, 'c)
    util.addTable[(Long, String, Int)]("s", 'x, 'y, 'z)

    val query = "SELECT b, y FROM t FULL OUTER JOIN s ON a = z"
    val result = util.tableEnv.sqlQuery(query)

    val expected = unaryNode(
      "DataSetCalc",
      binaryNode(
        "DataSetJoin",
        unaryNode(
          "DataSetCalc",
          batchTableNode(0),
          term("select", "a", "b")
        ),
        unaryNode(
          "DataSetCalc",
          batchTableNode(1),
          term("select", "y", "z")
        ),
        term("where", "=(a, z)"),
        term("join", "a", "b", "y", "z"),
        term("joinType", "FullOuterJoin")
      ),
      term("select", "b", "y")
    )

    util.verifyTable(result, expected)
  }

  @Test
  def testFullOuterJoinEquiAndLocalPred(): Unit = {
    val util = batchTestUtil()
    util.addTable[(Int, Long, String)]("t", 'a, 'b, 'c)
    util.addTable[(Long, String, Int)]("s", 'x, 'y, 'z)

    val query = "SELECT b, y FROM t FULL OUTER JOIN s ON a = z AND b < 2 AND z > 5"
    val result = util.tableEnv.sqlQuery(query)

    val expected = unaryNode(
      "DataSetCalc",
      binaryNode(
        "DataSetJoin",
        unaryNode(
          "DataSetCalc",
          batchTableNode(0),
          term("select", "a", "b", "<(b, 2) AS $f3")
        ),
        unaryNode(
          "DataSetCalc",
          batchTableNode(1),
          term("select", "y", "z", ">(z, 5) AS $f3")
        ),
        term("where", "AND(=(a, z), $f3, $f30)"),
        term("join", "a", "b", "$f3", "y", "z", "$f30"),
        term("joinType", "FullOuterJoin")
      ),
      term("select", "b", "y")
    )

    util.verifyTable(result, expected)
  }

  @Test
  def testFullOuterJoinEquiAndNonEquiPred(): Unit = {
    val util = batchTestUtil()
    util.addTable[(Int, Long, String)]("t", 'a, 'b, 'c)
    util.addTable[(Long, String, Int)]("s", 'x, 'y, 'z)

    val query = "SELECT b, y FROM t FULL OUTER JOIN s ON a = z AND b < x"
    val result = util.tableEnv.sqlQuery(query)

    val expected = unaryNode(
      "DataSetCalc",
      binaryNode(
        "DataSetJoin",
        unaryNode(
          "DataSetCalc",
          batchTableNode(0),
          term("select", "a", "b")
        ),
        batchTableNode(1),
        term("where", "AND(=(a, z), <(b, x))"),
        term("join", "a", "b", "x", "y", "z"),
        term("joinType", "FullOuterJoin")
      ),
      term("select", "b", "y")
    )

    util.verifyTable(result, expected)
  }

}
