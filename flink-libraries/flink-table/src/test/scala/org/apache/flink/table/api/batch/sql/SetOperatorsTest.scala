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

import org.apache.flink.api.java.typeutils.{GenericTypeInfo, RowTypeInfo}
import org.apache.flink.api.scala._
import org.apache.flink.table.api.Types
import org.apache.flink.table.api.scala._
import org.apache.flink.table.runtime.utils.CommonTestData.NonPojo
import org.apache.flink.table.utils.TableTestUtil._
import org.apache.flink.table.utils.TableTestBase
import org.junit.{Ignore, Test}

class SetOperatorsTest extends TableTestBase {

  @Test
  def testMinusWithNestedTypes(): Unit = {
    val util = batchTestUtil()
    val t = util.addTable[(Long, (Int, String), Array[Boolean])]("MyTable", 'a, 'b, 'c)

    val expected = binaryNode(
      "DataSetMinus",
      batchTableNode(0),
      batchTableNode(0),
      term("minus", "a", "b", "c")
    )

    val result = t.minus(t)

    util.verifyTable(result, expected)
  }

  @Test
  def testExists(): Unit = {
    val util = batchTestUtil()
    util.addTable[(Long, Int, String)]("A", 'a_long, 'a_int, 'a_string)
    util.addTable[(Long, Int, String)]("B", 'b_long, 'b_int, 'b_string)

    val expected = unaryNode(
      "DataSetCalc",
      binaryNode(
        "DataSetJoin",
        batchTableNode(0),
        unaryNode(
          "DataSetCalc",
          unaryNode(
            "DataSetAggregate",
            unaryNode(
              "DataSetCalc",
              batchTableNode(1),
              term("select", "b_long AS b_long3", "true AS $f0"),
              term("where", "IS NOT NULL(b_long)")
            ),
            term("groupBy", "b_long3"),
            term("select", "b_long3", "MIN($f0) AS $f1")
          ),
          term("select", "b_long3")
        ),
        term("where", "=(a_long, b_long3)"),
        term("join", "a_long", "a_int", "a_string", "b_long3"),
        term("joinType", "InnerJoin")
      ),
      term("select", "a_int", "a_string")
    )

    util.verifySql(
      "SELECT a_int, a_string FROM A WHERE EXISTS(SELECT * FROM B WHERE a_long = b_long)",
      expected
    )
  }

  @Test
  def testNotIn(): Unit = {
    val util = batchTestUtil()
    util.addTable[(Int, Long, String)]("A", 'a, 'b, 'c)

    val expected = unaryNode(
      "DataSetCalc",
      binaryNode(
        "DataSetJoin",
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
                term("select", "b"),
                term("where", "OR(=(b, 6), =(b, 1))")
              ),
              term("select", "COUNT(*) AS $f0", "COUNT(b) AS $f1")
            ),
            term("where", "true"),
            term("join", "a", "b", "c", "$f0", "$f1"),
            term("joinType", "NestedLoopInnerJoin")
          ),
          term("select", "a AS $f0", "b AS $f1", "c AS $f2", "$f0 AS $f3", "$f1 AS $f4", "b AS $f5")
        ),
        unaryNode(
          "DataSetAggregate",
          unaryNode(
            "DataSetCalc",
            batchTableNode(0),
            term("select", "b AS $f0", "true AS $f1"),
            term("where", "OR(=(b, 6), =(b, 1))")
          ),
          term("groupBy", "$f0"),
          term("select", "$f0", "MIN($f1) AS $f1")
        ),
        term("where", "=($f5, $f00)"),
        term("join", "$f0", "$f1", "$f2", "$f3", "$f4", "$f5", "$f00", "$f10"),
        term("joinType", "LeftOuterJoin")
      ),
      term("select", "$f0 AS a", "$f2 AS c"),
      term("where", "OR(=($f3, 0), AND(IS NULL($f10), >=($f4, $f3), IS NOT NULL($f5)))")
    )

    util.verifySql(
      "SELECT a, c FROM A WHERE b NOT IN (SELECT b FROM A WHERE b = 6 OR b = 1)",
      expected
    )
  }

  @Test
  def testInWithFields(): Unit = {
    val util = batchTestUtil()
    util.addTable[(Int, Long, Int, String, Long)]("A", 'a, 'b, 'c, 'd, 'e)

    val expected = unaryNode(
      "DataSetCalc",
      batchTableNode(0),
      term("select", "a", "b", "c", "d", "e"),
      term("where", "OR(=(a, c), =(a, CAST(b)), =(a, 5))")
    )

    util.verifySql(
      "SELECT a, b, c, d, e FROM A WHERE a IN (c, b, 5)",
      expected
    )
  }

  @Test
  @Ignore // Calcite bug
  def testNotInWithFilter(): Unit = {
    val util = batchTestUtil()
    util.addTable[(Int, Long, String)]("A", 'a, 'b, 'c)
    util.addTable[(Int, Long, Int, String, Long)]("B", 'a, 'b, 'c, 'd, 'e)

    val expected = "FAIL"

    util.verifySql(
      "SELECT d FROM B WHERE d NOT IN (SELECT a FROM A) AND d < 5",
      expected
    )
  }

  @Test
  def testUnionNullableTypes(): Unit = {
    val util = batchTestUtil()
    util.addTable[((Int, String), (Int, String), Int)]("A", 'a, 'b, 'c)

    val expected = binaryNode(
      "DataSetUnion",
      unaryNode(
        "DataSetCalc",
        batchTableNode(0),
        term("select", "a")
      ),
      unaryNode(
        "DataSetCalc",
        batchTableNode(0),
        term("select", "CASE(>(c, 0), b, null) AS EXPR$0")
      ),
      term("union", "a")
    )

    util.verifySql(
      "SELECT a FROM A UNION ALL SELECT CASE WHEN c > 0 THEN b ELSE NULL END FROM A",
      expected
    )
  }

  @Test
  def testUnionAnyType(): Unit = {
    val util = batchTestUtil()
    val typeInfo = Types.ROW(
      new GenericTypeInfo(classOf[NonPojo]),
      new GenericTypeInfo(classOf[NonPojo]))
    util.addJavaTable(typeInfo, "A", "a, b")

    val expected = binaryNode(
      "DataSetUnion",
      unaryNode(
        "DataSetCalc",
        batchTableNode(0),
        term("select", "a")
      ),
      unaryNode(
        "DataSetCalc",
        batchTableNode(0),
        term("select", "b")
      ),
      term("union", "a")
    )

    util.verifyJavaSql("SELECT a FROM A UNION ALL SELECT b FROM A", expected)
  }
}
