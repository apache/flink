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
import org.apache.flink.table.api.{Over, Slide, Table}
import org.apache.flink.table.functions.ScalarFunction
import org.apache.flink.table.functions.aggfunctions.CountAggFunction
import org.apache.flink.table.runtime.utils.JavaUserDefinedAggFunctions.{CountDistinct, WeightedAvg}
import org.apache.flink.table.utils.TableTestUtil.{binaryNode, streamTableNode, term, unaryNode}
import org.apache.flink.table.utils.{TableFunc0, TableTestBase}
import org.junit.Test

/**
  * Tests for column functions which includes tests for different column functions.
  */
class ColumnFunctionsTest extends TableTestBase {

  val util = streamTestUtil()

  private def verifyAll(tab1: Table, tab2: Table, expected: String): Unit = {
    util.verifyTable(tab1, expected)
    this.verifyTableEquals(tab1, tab2)
  }

  @Test
  def testStar(): Unit = {

    val t = util.addTable[(Double, Long)]('double, 'long)

    util.tableEnv.registerFunction("TestFunc", TestFunc)
    val tab1 = t.select(TestFunc(withColumns('*)))
    val tab2 = t.select("TestFunc(withColumns(*))")

    val expected =
      unaryNode(
        "DataStreamCalc",
        streamTableNode(t),
        term("select", "TestFunc$(double, long) AS _c0")
      )

    verifyAll(tab1, tab2, expected)
  }

  @Test
  def testColumnRange(): Unit = {
    val t = util.addTable[(Int, Long, String, Int, Long, String)]('a, 'b, 'c, 'd, 'e, 'f)

    val tab1 = t.select(withColumns('b to 'c), 'a, withColumns(5 to 6, 'd))
    val tab2 = t.select("withColumns(b to c), a, withColumns(5 to 6, d)")

    val expected =
      unaryNode(
        "DataStreamCalc",
        streamTableNode(t),
        term("select", "b", "c", "a", "e", "f", "d")
      )

    verifyAll(tab1, tab2, expected)
  }

  @Test
  def testColumnWithoutRange(): Unit = {
    val t = util.addTable[(Int, Long, String, Int, Long, String)]('a, 'b, 'c, 'd, 'e, 'f)

    val tab1 = t.select(withColumns(1, 'b, 'c), 'f)
    val tab2 = t.select("withColumns(1, b, c), f")

    val expected =
      unaryNode(
        "DataStreamCalc",
        streamTableNode(t),
        term("select", "a", "b", "c", "f")
      )

    verifyAll(tab1, tab2, expected)
  }

  @Test
  def testInverseSelection(): Unit = {
    val t = util.addTable[(Int, Long, String, Int, Long, String)]('a, 'b, 'c, 'd, 'e, 'f)

    val tab1 = t
      .select(withoutColumns(1, 'b))
      .select(withoutColumns(1 to 2))

    val tab2 = t
      .select("withoutColumns(1, b)")
      .select("withoutColumns(1 to 2)")

    val expected =
      unaryNode(
        "DataStreamCalc",
        streamTableNode(t),
        term("select", "e", "f")
      )

    verifyAll(tab1, tab2, expected)
  }

  @Test
  def testColumnFunctionsInUDF(): Unit = {
    val t = util.addTable[(Int, Long, String, String)]('int, 'long, 'string1, 'string2)

    val tab1 = t.select(concat(withColumns('string1 to 'string2)))
    val tab2 = t.select("concat(withColumns(string1 to string2))")

    val expected =
      unaryNode(
        "DataStreamCalc",
        streamTableNode(t),
        term("select", "CONCAT(string1, string2) AS _c0")
      )

    verifyAll(tab1, tab2, expected)
  }

  @Test
  def testJoin(): Unit = {
    val t1 = util.addTable[(Int, Long, String)]('int1, 'long1, 'string1)
    val t2 = util.addTable[(Int, Long, String)]('int2, 'long2, 'string2)

    val tab1 = t1.join(t2, withColumns(1) === withColumns(4))
    val tab2 = t1.join(t2, "withColumns(1) === withColumns(4)")

    val expected =
      binaryNode(
        "DataStreamJoin",
        streamTableNode(t1),
        streamTableNode(t2),
        term("where", "=(int1, int2)"),
        term("join", "int1", "long1", "string1", "int2", "long2", "string2"),
        term("joinType", "InnerJoin")
      )

    verifyAll(tab1, tab2, expected)
  }

  @Test
  def testJoinLateral(): Unit = {
    val t = util.addTable[(Double, Long, String)]('int, 'long, 'string)
    val func0 = new TableFunc0
    util.tableEnv.registerFunction("func0", func0)

    val tab1 = t.joinLateral(func0(withColumns('string)))
    val tab2 = t.joinLateral("func0(withColumns(string))")

    val expected =
      unaryNode(
        "DataStreamCorrelate",
        streamTableNode(t),
        term("invocation", func0.functionIdentifier() + "($2)"),
        term("correlate", "table(TableFunc0(string))"),
        term("select", "int", "long", "string", "name", "age"),
        term("rowType",
          "RecordType(DOUBLE int, BIGINT long, VARCHAR(65536) string, VARCHAR(65536) name," +
            " INTEGER age)"),
        term("joinType", "INNER")
      )

    util.verifyTable(tab1, expected)
    util.verify2Tables(tab1, tab2)
  }

  @Test
  def testFilter(): Unit = {
    val t = util.addTable[(Int, Long, String, String)]('int, 'long, 'string1, 'string2)

    val tab1 = t.where(concat(withColumns('string1 to 'string2)) === "a")
    val tab2 = t.where("concat(withColumns(string1 to string2)) = 'a'")

    val expected =
      unaryNode(
        "DataStreamCalc",
        streamTableNode(t),
        term("select", "int", "long", "string1", "string2"),
        term("where", "=(CONCAT(string1, string2), 'a')")
      )

    verifyAll(tab1, tab2, expected)
  }

  @Test
  def testGroupBy(): Unit = {
    val t = util.addTable[(Int, Long, String, Int, Long, String)]('a, 'b, 'c, 'd, 'e, 'f)

    val tab1 = t
      .groupBy(withColumns(1), 'b)
      .select('a, 'b, withColumns('c).count)

    val tab2 = t
      .groupBy("withColumns(1), b")
      .select("a, b, withColumns(c).count")

    val expected =
      unaryNode(
        "DataStreamGroupAggregate",
        unaryNode(
          "DataStreamCalc",
          streamTableNode(t),
          term("select", "a", "b", "c")
        ),
        term("groupBy", "a", "b"),
        term("select", "a", "b", "COUNT(c) AS EXPR$0")
      )

    verifyAll(tab1, tab2, expected)
  }

  @Test
  def testWindowGroupBy(): Unit = {
    val t = util.addTable[(Int, Long, String, Int)]('a, 'b.rowtime, 'c, 'd)

    val tab1 = t
      .window(Slide over 3.milli every 10.milli on withColumns('b) as 'w)
      .groupBy(withColumns('a, 'b), 'w)
      .select(withColumns(1 to 2), withColumns('c).count as 'c)

    val tab2 = t
      .window(Slide.over("3.milli").every("10.milli").on("withColumns(b)").as("w"))
      .groupBy("withColumns(a, b), w")
      .select("withColumns(1 to 2), withColumns(c).count as c")

    val expected =
      unaryNode(
        "DataStreamCalc",
        unaryNode(
          "DataStreamGroupWindowAggregate",
          unaryNode(
            "DataStreamCalc",
            streamTableNode(t),
            term("select", "a", "CAST(b) AS b", "c")
          ),
          term("groupBy", "a", "b"),
          term("window", "SlidingGroupWindow('w, 'b, 3.millis, 10.millis)"),
          term("select", "a", "b", "COUNT(c) AS EXPR$0")
        ),
        term("select", "a", "b", "EXPR$0 AS c")
      )

    verifyAll(tab1, tab2, expected)
  }

  @Test
  def testOver(): Unit = {
    val table = util.addTable[(Long, Int, String)]('a, 'b, 'c, 'proctime.proctime)

    val countFun = new CountAggFunction
    val weightAvgFun = new WeightedAvg
    val countDist = new CountDistinct

    util.tableEnv.registerFunction("countFun", countFun)
    util.tableEnv.registerFunction("weightAvgFun", weightAvgFun)
    util.tableEnv.registerFunction("countDist", countDist)

    val tab1 = table
      .window(
        Over partitionBy withColumns('c) orderBy 'proctime preceding UNBOUNDED_ROW as 'w)
      .select('c,
        countFun(withColumns('b)) over 'w as 'mycount,
        weightAvgFun(withColumns('a to 'b)) over 'w as 'wAvg,
        countDist('a) over 'w as 'countDist)
      .select('c, 'mycount, 'wAvg, 'countDist)

    val tab2 = table
      .window(
        Over.partitionBy("withColumns(c)")
          .orderBy("proctime")
          .preceding("UNBOUNDED_ROW")
          .as("w"))
      .select("c, countFun(withColumns(b)) over w as mycount, " +
        "weightAvgFun(withColumns(a to b)) over w as wAvg, countDist(a) over w as countDist")
      .select('c, 'mycount, 'wAvg, 'countDist)

    val expected =
      unaryNode(
        "DataStreamCalc",
        unaryNode(
          "DataStreamOverAggregate",
          streamTableNode(table),
          term("partitionBy", "c"),
          term("orderBy", "proctime"),
          term("rows", "BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"),
          term("select", "a", "b", "c", "proctime", "CountAggFunction(b) AS w0$o0",
            "WeightedAvg(a, b) AS w0$o1", "CountDistinct(a) AS w0$o2")
        ),
        term("select", "c", "w0$o0 AS mycount", "w0$o1 AS wAvg", "w0$o2 AS countDist")
      )
    verifyAll(tab1, tab2, expected)
  }

  @Test
  def testAddColumns(): Unit = {
    val t = util.addTable[(Double, Long, String)]('a, 'b, 'c)

    util.tableEnv.registerFunction("TestFunc", TestFunc)
    val tab1 = t.addColumns(TestFunc(withColumns('a, 'b)) as 'd)
    val tab2 = t.addColumns("TestFunc(withColumns(a, b)) as d")

    val expected =
      unaryNode(
        "DataStreamCalc",
        streamTableNode(t),
        term("select", "a", "b", "c", "TestFunc$(a, b) AS d")
      )

    verifyAll(tab1, tab2, expected)
  }

  @Test
  def testRenameColumns(): Unit = {
    val t = util.addTable[(Double, Long, String)]('a, 'b, 'c)

    val tab1 = t.renameColumns(withColumns('a) as 'd).select("d, b")
    val tab2 = t.renameColumns("withColumns(a) as d").select('d, 'b)

    val expected =
      unaryNode(
        "DataStreamCalc",
        streamTableNode(t),
        term("select", "a AS d", "b")
      )

    verifyAll(tab1, tab2, expected)
  }

  @Test
  def testDropColumns(): Unit = {
    val t = util.addTable[(Double, Long, String)]('a, 'b, 'c)

    val tab1 = t.dropColumns(withColumns('a to 'b))
    val tab2 = t.dropColumns("withColumns(a to b)")

    val expected =
      unaryNode(
        "DataStreamCalc",
        streamTableNode(t),
        term("select", "c")
      )

    verifyAll(tab1, tab2, expected)
  }
}

object TestFunc extends ScalarFunction {
  def eval(a: Double, b: Long): Double = {
    a
  }
}
