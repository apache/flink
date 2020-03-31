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

package org.apache.flink.table.plan

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.runtime.utils.JavaUserDefinedScalarFunctions.{BooleanPythonScalarFunction, PythonScalarFunction}
import org.apache.flink.table.utils.TableTestUtil._
import org.apache.flink.table.utils.TableTestBase
import org.junit.Test

class PythonCalcSplitRuleTest extends TableTestBase {

  @Test
  def testPythonFunctionAsInputOfJavaFunction(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Int, Int, Int)]("MyTable", 'a, 'b, 'c)
    util.tableEnv.registerFunction("pyFunc1", new PythonScalarFunction("pyFunc1"))

    val resultTable = table
      .select("pyFunc1(a, b) + 1")

    val expected = unaryNode(
      "DataStreamCalc",
      unaryNode(
        "DataStreamPythonCalc",
        streamTableNode(table),
        term("select", "pyFunc1(a, b) AS f0")
      ),
      term("select", "+(f0, 1) AS _c0")
    )

    util.verifyTable(resultTable, expected)
  }

  @Test
  def testPythonFunctionMixedWithJavaFunction(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Int, Int, Int)]("MyTable", 'a, 'b, 'c)
    util.tableEnv.registerFunction("pyFunc1", new PythonScalarFunction("pyFunc1"))

    val resultTable = table
      .select("pyFunc1(a, b), c + 1")

    val expected = unaryNode(
      "DataStreamCalc",
      unaryNode(
        "DataStreamPythonCalc",
        streamTableNode(table),
        term("select", "c", "pyFunc1(a, b) AS f0")
      ),
      term("select", "f0 AS _c0", "+(c, 1) AS _c1")
    )

    util.verifyTable(resultTable, expected)
  }

  @Test
  def testPythonFunctionMixedWithJavaFunctionInWhereClause(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Int, Int, Int)]("MyTable", 'a, 'b, 'c)
    util.tableEnv.registerFunction("pyFunc1", new PythonScalarFunction("pyFunc1"))
    util.tableEnv.registerFunction("pyFunc2", new PythonScalarFunction("pyFunc2"))

    val resultTable = table
      .where("pyFunc2(a, c) > 0")
      .select("pyFunc1(a, b), c + 1")

    val expected = unaryNode(
      "DataStreamCalc",
      unaryNode(
        "DataStreamPythonCalc",
        unaryNode(
          "DataStreamCalc",
          unaryNode(
            "DataStreamPythonCalc",
            streamTableNode(table),
            term("select", "a", "b", "c", "pyFunc2(a, c) AS f0")
          ),
          term("select", "c", "a", "b"),
          term("where", ">(f0, 0)")
        ),
        term("select", "c", "pyFunc1(a, b) AS f0")
      ),
      term("select", "f0 AS _c0", "+(c, 1) AS _c1")
    )

    util.verifyTable(resultTable, expected)
  }

  @Test
  def testPythonFunctionInWhereClause(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Int, Int, Int)]("MyTable", 'a, 'b, 'c)
    util.tableEnv.registerFunction("pyFunc1", new PythonScalarFunction("pyFunc1"))
    util.tableEnv.registerFunction("pyFunc2", new BooleanPythonScalarFunction("pyFunc2"))

    val resultTable = table
      .where("pyFunc2(a, c)")
      .select("pyFunc1(a, b)")

    val expected = unaryNode(
      "DataStreamPythonCalc",
      unaryNode(
        "DataStreamCalc",
        unaryNode(
          "DataStreamPythonCalc",
          streamTableNode(table),
          term("select", "a", "b", "pyFunc2(a, c) AS f0")
        ),
        term("select", "a", "b"),
        term("where", "f0")
      ),
      term("select", "pyFunc1(a, b) AS _c0")
    )

    util.verifyTable(resultTable, expected)
  }

  @Test
  def testChainingPythonFunction(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Int, Int, Int)]("MyTable", 'a, 'b, 'c)
    util.tableEnv.registerFunction("pyFunc1", new PythonScalarFunction("pyFunc1"))
    util.tableEnv.registerFunction("pyFunc2", new PythonScalarFunction("pyFunc2"))
    util.tableEnv.registerFunction("pyFunc3", new PythonScalarFunction("pyFunc3"))

    val resultTable = table
      .select("pyFunc3(pyFunc2(a + pyFunc1(a, c), b), c)")

    val expected = unaryNode(
      "DataStreamPythonCalc",
      unaryNode(
        "DataStreamCalc",
        unaryNode(
          "DataStreamPythonCalc",
          streamTableNode(table),
          term("select", "b", "c", "a", "pyFunc1(a, c) AS f0")
        ),
        term("select", "b", "c", "+(a, f0) AS f0")
      ),
      term("select", "pyFunc3(pyFunc2(f0, b), c) AS _c0")
    )

    util.verifyTable(resultTable, expected)
  }

  @Test
  def testOnlyOnePythonFunction(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Int, Int, Int)]("MyTable", 'a, 'b, 'c)
    util.tableEnv.registerFunction("pyFunc1", new PythonScalarFunction("pyFunc1"))

    val resultTable = table
      .select("pyFunc1(a, b)")

    val expected = unaryNode(
      "DataStreamPythonCalc",
      streamTableNode(table),
      term("select", "pyFunc1(a, b) AS _c0")
      )

    util.verifyTable(resultTable, expected)
  }

  @Test
  def testOnlyOnePythonFunctionInWhereClause(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Int, Int, Int)]("MyTable", 'a, 'b, 'c)
    util.tableEnv.registerFunction("pyFunc1", new BooleanPythonScalarFunction("pyFunc1"))

    val resultTable = table
      .where("pyFunc1(a, c)")
      .select("a, b")

    val expected = unaryNode(
      "DataStreamCalc",
      unaryNode(
        "DataStreamPythonCalc",
        streamTableNode(table),
        term("select", "a", "b", "pyFunc1(a, c) AS f0")
      ),
      term("select", "a", "b"),
      term("where", "f0")
    )

    util.verifyTable(resultTable, expected)
  }

  @Test
  def testFieldNameUniquify(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Int, Int, Int)]("MyTable", 'f0, 'f1, 'f2)
    util.tableEnv.registerFunction("pyFunc1", new PythonScalarFunction("pyFunc1"))

    val resultTable = table
      .select("pyFunc1(f1, f2), f0 + 1")

    val expected = unaryNode(
      "DataStreamCalc",
      unaryNode(
        "DataStreamPythonCalc",
        streamTableNode(table),
        term("select", "f0", "pyFunc1(f1, f2) AS f00")
        ),
      term("select", "f00 AS _c0", "+(f0, 1) AS _c1")
      )

    util.verifyTable(resultTable, expected)
  }

  @Test
  def testLiteral(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Int, Int, Int)]("MyTable", 'a, 'b, 'c)
    util.tableEnv.registerFunction("pyFunc1", new BooleanPythonScalarFunction("pyFunc1"))

    val resultTable = table.select("a, b, pyFunc1(a, c), 1")

    val expected = unaryNode(
      "DataStreamCalc",
      unaryNode(
        "DataStreamPythonCalc",
        streamTableNode(table),
        term("select", "a", "b", "pyFunc1(a, c) AS f0")
      ),
      term("select", "a", "b", "f0 AS _c2", "1 AS _c3")
    )

    util.verifyTable(resultTable, expected)
  }

  @Test
  def testReorderPythonCalc(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Int, Int, Int)]("MyTable", 'a, 'b, 'c)
    util.tableEnv.registerFunction("pyFunc1", new BooleanPythonScalarFunction("pyFunc1"))

    val resultTable = table.select("a, pyFunc1(a, c), b")

    val expected = unaryNode(
      "DataStreamCalc",
      unaryNode(
        "DataStreamPythonCalc",
        streamTableNode(table),
        term("select", "a", "b", "pyFunc1(a, c) AS f0")
      ),
      term("select", "a", "f0 AS _c1", "b")
    )

    util.verifyTable(resultTable, expected)
  }
}
