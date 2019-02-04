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
import org.apache.flink.table.expressions.{Upper, WindowReference}
import org.apache.flink.table.plan.logical.TumblingGroupWindow
import org.apache.flink.table.utils.TableTestUtil._
import org.apache.flink.table.utils.TableTestBase
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
        .select(Upper('c).count, 'a.sum)

    val expected =
      unaryNode(
        "DataStreamGroupWindowAggregate",
        unaryNode(
          "DataStreamCalc",
          streamTableNode(0),
          term("select", "c", "a", "rowtime", "UPPER(c) AS $f3")
        ),
        term("window",
          TumblingGroupWindow(
            WindowReference("w"),
            'rowtime,
            5.millis)),
        term("select", "COUNT($f3) AS TMP_0", "SUM(a) AS TMP_1")
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
        .select(Upper('c).count, 'a.sum, 'b)

    val expected = unaryNode(
        "DataStreamCalc",
        unaryNode(
          "DataStreamGroupWindowAggregate",
          unaryNode(
            "DataStreamCalc",
            streamTableNode(0),
            term("select", "c", "a", "b", "rowtime", "UPPER(c) AS $f4")
          ),
          term("groupBy", "b"),
          term("window",
            TumblingGroupWindow(
              WindowReference("w"),
              'rowtime,
              5.millis)),
          term("select", "b", "COUNT($f4) AS TMP_0", "SUM(a) AS TMP_1")
        ),
        term("select", "TMP_0", "TMP_1", "b")
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
      streamTableNode(0),
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
      .where(s"${(1 to 30).map("b = " + _).mkString(" || ")} && c = 'xx'")

    val expected = unaryNode(
      "DataStreamCalc",
      streamTableNode(0),
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
      .where(s"${(1 to 30).map("b != " + _).mkString(" && ")} || c != 'xx'")

    val expected = unaryNode(
      "DataStreamCalc",
      streamTableNode(0),
      term("select", "a", "b", "c"),
      term("where", s"OR(NOT IN(b, ${(1 to 30).mkString(", ")}), <>(c, 'xx'))")
    )

    util.verifyTable(resultTable, expected)
  }
}


