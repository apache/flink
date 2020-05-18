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

package org.apache.flink.table.api.batch.table

import org.apache.flink.api.java.typeutils.GenericTypeInfo
import org.apache.flink.api.scala._
import org.apache.flink.table.api.Expressions.$
import org.apache.flink.table.api._
import org.apache.flink.table.runtime.utils.CommonTestData.NonPojo
import org.apache.flink.table.utils.TableTestBase
import org.apache.flink.table.utils.TableTestUtil._

import org.junit.Test

import java.sql.Timestamp

class SetOperatorsTest extends TableTestBase {

  @Test
  def testInWithFilter(): Unit = {
    val util = batchTestUtil()
    val t = util.addTable[((Int, Int), String, (Int, Int))]("A", 'a, 'b, 'c)

    val elements = t.where('b === "two").select('a).as("a1")
    val in = t.select($"*").where('c.in(elements))

    val expected = unaryNode(
      "DataSetCalc",
      binaryNode(
        "DataSetJoin",
        batchTableNode(t),
        unaryNode(
          "DataSetDistinct",
          unaryNode(
            "DataSetCalc",
            batchTableNode(t),
            term("select", "a AS a1"),
            term("where", "=(b, 'two')")
          ),
          term("distinct", "a1")
        ),
        term("where", "=(c, a1)"),
        term("join", "a", "b", "c", "a1"),
        term("joinType", "InnerJoin")
      ),
      term("select", "a", "b", "c")
    )

    util.verifyTable(in, expected)
  }

  @Test
  def testInWithProject(): Unit = {
    val util = batchTestUtil()
    val t = util.addTable[(Int, Timestamp, String)]("A", 'a, 'b, 'c)

    val in = t.select('b.in(Timestamp.valueOf("1972-02-22 07:12:00.333"))).as("b2")

    val expected = unaryNode(
      "DataSetCalc",
      batchTableNode(t),
      term("select", "IN(b, 1972-02-22 07:12:00.333:TIMESTAMP(3)) AS b2")
    )

    util.verifyTable(in, expected)
  }

  @Test
  def testUnionNullableTypes(): Unit = {
    val util = batchTestUtil()
    val t = util.addTable[((Int, String), (Int, String), Int)]("A", 'a, 'b, 'c)

    val in = t.select('a)
      .unionAll(
        t.select(('c > 0) ? ('b, nullOf(createTypeInformation[(Int, String)]))))

    val expected = binaryNode(
      "DataSetUnion",
      unaryNode(
        "DataSetCalc",
        batchTableNode(t),
        term("select", "a")
      ),
      unaryNode(
        "DataSetCalc",
        batchTableNode(t),
        term("select", "CASE(>(c, 0), b, null:RecordType:peek_no_expand(INTEGER _1, " +
          "VARCHAR(65536) _2)) AS _c0")
      ),
      term("all", "true"),
      term("union", "a")
    )

    util.verifyTable(in, expected)
  }

  @Test
  def testUnionAnyType(): Unit = {
    val util = batchTestUtil()
    val typeInfo = Types.ROW(
      new GenericTypeInfo(classOf[NonPojo]),
      new GenericTypeInfo(classOf[NonPojo]))
    val t = util.addJavaTable(typeInfo, "A", $("a"), $("b"))

    val in = t.select('a).unionAll(t.select('b))

    val expected = binaryNode(
      "DataSetUnion",
      unaryNode(
        "DataSetCalc",
        batchTableNode(t),
        term("select", "a")
      ),
      unaryNode(
        "DataSetCalc",
        batchTableNode(t),
        term("select", "b")
      ),
      term("all", "true"),
      term("union", "a")
    )

    util.verifyJavaTable(in, expected)
  }

  @Test
  def testFilterUnionTranspose(): Unit = {
    val util = batchTestUtil()
    val left = util.addTable[(Int, Long, String)]("left", 'a, 'b, 'c)
    val right = util.addTable[(Int, Long, String)]("right", 'a, 'b, 'c)

    val result = left.unionAll(right)
      .where('a > 0)
      .groupBy('b)
      .select('a.sum as 'a, 'b as 'b, 'c.count as 'c)

    val expected = unaryNode(
      "DataSetCalc",
      unaryNode(
        "DataSetAggregate",
        binaryNode(
          "DataSetUnion",
          unaryNode(
            "DataSetCalc",
            batchTableNode(left),
            term("select", "a", "b", "c"),
            term("where", ">(a, 0)")
          ),
          unaryNode(
            "DataSetCalc",
            batchTableNode(right),
            term("select", "a", "b", "c"),
            term("where", ">(a, 0)")
          ),
          term("all", "true"),
          term("union", "a", "b", "c")
        ),
        term("groupBy", "b"),
        term("select", "b", "SUM(a) AS EXPR$0", "COUNT(c) AS EXPR$1")
      ),
      term("select", "EXPR$0 AS a", "b", "EXPR$1 AS c")
    )

    util.verifyTable(result, expected)
  }

  @Test
  def testFilterMinusTranspose(): Unit = {
    val util = batchTestUtil()
    val left = util.addTable[(Int, Long, String)]("left", 'a, 'b, 'c)
    val right = util.addTable[(Int, Long, String)]("right", 'a, 'b, 'c)

    val result = left.minusAll(right)
      .where('a > 0)
      .groupBy('b)
      .select('a.sum as 'a, 'b as 'b, 'c.count as 'c)

    val expected = unaryNode(
      "DataSetCalc",
      unaryNode(
        "DataSetAggregate",
        binaryNode(
          "DataSetMinus",
          unaryNode(
            "DataSetCalc",
            batchTableNode(left),
            term("select", "a", "b", "c"),
            term("where", ">(a, 0)")
          ),
          unaryNode(
            "DataSetCalc",
            batchTableNode(right),
            term("select", "a", "b", "c"),
            term("where", ">(a, 0)")
          ),
          term("minus", "a", "b", "c")
        ),
        term("groupBy", "b"),
        term("select", "b", "SUM(a) AS EXPR$0", "COUNT(c) AS EXPR$1")
      ),
      term("select", "EXPR$0 AS a", "b", "EXPR$1 AS c")
    )

    util.verifyTable(result, expected)
  }

  @Test
  def testProjectUnionTranspose(): Unit = {
    val util = batchTestUtil()
    val left = util.addTable[(Int, Long, String)]("left", 'a, 'b, 'c)
    val right = util.addTable[(Int, Long, String)]("right", 'a, 'b, 'c)

    val result = left.select('a, 'b, 'c)
                 .unionAll(right.select('a, 'b, 'c))
                 .select('b, 'c)

    val expected = binaryNode(
      "DataSetUnion",
      unaryNode(
        "DataSetCalc",
        batchTableNode(left),
        term("select", "b", "c")
      ),
      unaryNode(
        "DataSetCalc",
        batchTableNode(right),
        term("select", "b", "c")
      ),
      term("all", "true"),
      term("union", "b", "c")
    )

    util.verifyTable(result, expected)

  }

  @Test
  def testProjectMinusTranspose(): Unit = {
    val util = batchTestUtil()
    val left = util.addTable[(Int, Long, String)]("left", 'a, 'b, 'c)
    val right = util.addTable[(Int, Long, String)]("right", 'a, 'b, 'c)

    val result = left.select('a, 'b, 'c)
                 .minusAll(right.select('a, 'b, 'c))
                 .select('b, 'c)

    val expected = binaryNode(
      "DataSetMinus",
      unaryNode(
        "DataSetCalc",
        batchTableNode(left),
        term("select", "b", "c")
      ),
      unaryNode(
        "DataSetCalc",
        batchTableNode(right),
        term("select", "b", "c")
      ),
      term("minus", "b", "c")
    )

    util.verifyTable(result, expected)

  }
}
