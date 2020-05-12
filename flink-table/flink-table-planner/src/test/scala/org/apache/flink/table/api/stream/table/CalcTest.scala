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

package org.apache.flink.table.api.stream.table

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.Tumble
import org.apache.flink.table.expressions.utils.{Func1, Func23, Func24}
import org.apache.flink.table.utils.TableTestBase
import org.apache.flink.table.utils.TableTestUtil._
import org.junit.Test

class CalcTest extends TableTestBase {

  // ----------------------------------------------------------------------------------------------
  // Tests for all the situations when we can do fields projection. Like selecting few fields
  // from a large field count source.
  // ----------------------------------------------------------------------------------------------

  @Test
  def testSelectFromWindow(): Unit = {
    val util = streamTestUtil()
    val sourceTable =
      util.addTable[(Int, Long, String, Double)]("MyTable", 'a, 'b, 'c, 'd, 'rowtime.rowtime)
    val resultTable = sourceTable
        .window(Tumble over 5.millis on 'rowtime as 'w)
        .groupBy('w)
        .select('c.upperCase().count, 'a.sum)

    val expected =
      unaryNode(
        "DataStreamGroupWindowAggregate",
        unaryNode(
          "DataStreamCalc",
          streamTableNode(sourceTable),
          term("select", "a", "rowtime", "UPPER(c) AS $f5")
        ),
        term("window", "TumblingGroupWindow('w, 'rowtime, 5.millis)"),
        term("select", "COUNT($f5) AS EXPR$0", "SUM(a) AS EXPR$1")
      )

    util.verifyTable(resultTable, expected)
  }

  @Test
  def testSelectFromGroupedWindow(): Unit = {
    val util = streamTestUtil()
    val sourceTable =
      util.addTable[(Int, Long, String, Double)]("MyTable", 'a, 'b, 'c, 'd, 'rowtime.rowtime)
    val resultTable = sourceTable
        .window(Tumble over 5.millis on 'rowtime as 'w)
        .groupBy('w, 'b)
        .select('c.upperCase().count, 'a.sum, 'b)

    val expected = unaryNode(
        "DataStreamCalc",
        unaryNode(
          "DataStreamGroupWindowAggregate",
          unaryNode(
            "DataStreamCalc",
            streamTableNode(sourceTable),
            term("select", "a", "b", "rowtime", "UPPER(c) AS $f5")
          ),
          term("groupBy", "b"),
          term("window", "TumblingGroupWindow('w, 'rowtime, 5.millis)"),
          term("select", "b", "COUNT($f5) AS EXPR$0", "SUM(a) AS EXPR$1")
        ),
        term("select", "EXPR$0", "EXPR$1", "b")
    )

    util.verifyTable(resultTable, expected)
  }

  @Test
  def testMultiFilter(): Unit = {
    val util = streamTestUtil()
    val sourceTable = util.addTable[(Int, Long, String, Double)]("MyTable", 'a, 'b, 'c, 'd)
    val resultTable = sourceTable.select('a, 'b)
      .filter('a > 0)
      .filter('b < 2)
      .filter(('a % 2) === 1)

    val expected = unaryNode(
      "DataStreamCalc",
      streamTableNode(sourceTable),
      term("select", "a", "b"),
      term("where", "AND(AND(>(a, 0), <(b, 2)), =(MOD(a, 2), 1))")
    )

    util.verifyTable(resultTable, expected)
  }

  @Test
  def testIn(): Unit = {
    val util = streamTestUtil()
    val sourceTable = util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
    val resultTable = sourceTable.select('a, 'b, 'c)
      .where((1 to 30).map($"b" === _).reduce((ex1, ex2) => ex1 || ex2) && ($"c" === "xx"))

    val expected = unaryNode(
      "DataStreamCalc",
      streamTableNode(sourceTable),
      term("select", "a", "b", "c"),
      term("where", s"AND(IN(b, ${(1 to 30).mkString(", ")}), =(c, 'xx'))")
    )

    util.verifyTable(resultTable, expected)
  }

  @Test
  def testNotIn(): Unit = {
    val util = streamTestUtil()
    val sourceTable = util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
    val resultTable = sourceTable.select('a, 'b, 'c)
      .where((1 to 30).map($"b" !== _).reduce((ex1, ex2) => ex1 && ex2) || ($"c" !== "xx"))

    val expected = unaryNode(
      "DataStreamCalc",
      streamTableNode(sourceTable),
      term("select", "a", "b", "c"),
      term("where", s"OR(NOT IN(b, ${(1 to 30).mkString(", ")}), <>(c, 'xx'))")
    )

    util.verifyTable(resultTable, expected)
  }

  @Test
  def testAddColumns(): Unit = {
    val util = streamTestUtil()
    val sourceTable = util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
    val resultTable = sourceTable
      .addColumns("concat(c, 'sunny') as kid")
      .addColumns('a + 2, 'b as 'b2)
      .addOrReplaceColumns(concat('c, "_kid") as 'kid, concat('c, "kid") as 'kid)
      .addOrReplaceColumns("concat(c, '_kid_last') as kid")
      .addColumns("'literal_value'")

    val expected = unaryNode(
      "DataStreamCalc",
      streamTableNode(sourceTable),
      term(
        "select", "a", "b", "c", "CONCAT(c, '_kid_last') AS kid", "+(a, 2) AS _c4, b AS b2",
        "'literal_value' AS _c6")
    )
    util.verifyTable(resultTable, expected)
  }

  @Test
  def testRenameColumns(): Unit = {
    val util = streamTestUtil()
    val sourceTable = util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
    val resultTable = sourceTable.renameColumns('a as 'a2, 'b as 'b2).select('a2, 'b2)

    val expected = unaryNode(
      "DataStreamCalc",
      streamTableNode(sourceTable),
      term("select", "a AS a2", "b AS b2")
    )
    util.verifyTable(resultTable, expected)
  }

  @Test
  def testDropColumns(): Unit = {
    val util = streamTestUtil()
    val sourceTable = util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
    val resultTable = sourceTable.dropColumns('a, 'b)

    val expected = unaryNode(
      "DataStreamCalc",
      streamTableNode(sourceTable),
      term("select", "c")
    )
    util.verifyTable(resultTable, expected)
  }

  @Test
  def testSimpleMap(): Unit = {
    val util = streamTestUtil()

    val sourceTable = util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
    val resultTable = sourceTable.map(Func23('a, 'b, 'c))

    val expected = unaryNode(
      "DataStreamCalc",
      streamTableNode(sourceTable),
      term("select", "Func23$(a, b, c).f0 AS _c0, Func23$(a, b, c).f1 AS _c1, " +
        "Func23$(a, b, c).f2 AS _c2, Func23$(a, b, c).f3 AS _c3")
    )

    util.verifyTable(resultTable, expected)
  }

  @Test
  def testScalarResult(): Unit = {
    val util = streamTestUtil()

    val sourceTable = util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
    val resultTable = sourceTable.map(Func1('a))

    val expected = unaryNode(
      "DataStreamCalc",
      streamTableNode(sourceTable),
      term("select", "Func1$(a) AS _c0")
    )

    util.verifyTable(resultTable, expected)
  }

  @Test
  def testMultiMap(): Unit = {
    val util = streamTestUtil()

    val sourceTable = util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
    val resultTable = sourceTable
      .map(Func23('a, 'b, 'c))
      .map(Func24('_c0, '_c1, '_c2, '_c3))

    val expected = unaryNode(
      "DataStreamCalc",
      streamTableNode(sourceTable),
      term("select",
           "Func24$(Func23$(a, b, c).f0, Func23$(a, b, c).f1, " +
             "Func23$(a, b, c).f2, Func23$(a, b, c).f3).f0 AS _c0, " +
             "Func24$(Func23$(a, b, c).f0, Func23$(a, b, c).f1, " +
             "Func23$(a, b, c).f2, Func23$(a, b, c).f3).f1 AS _c1, " +
             "Func24$(Func23$(a, b, c).f0, Func23$(a, b, c).f1, " +
             "Func23$(a, b, c).f2, Func23$(a, b, c).f3).f2 AS _c2, " +
             "Func24$(Func23$(a, b, c).f0, Func23$(a, b, c).f1, " +
             "Func23$(a, b, c).f2, Func23$(a, b, c).f3).f3 AS _c3")
    )

    util.verifyTable(resultTable, expected)
  }
}


