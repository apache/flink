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
import org.apache.flink.table.api._
import org.apache.flink.table.runtime.utils.JavaUserDefinedScalarFunctions.{BooleanPandasScalarFunction, BooleanPythonScalarFunction, PandasScalarFunction, PythonScalarFunction}
import org.apache.flink.table.utils.TableTestBase
import org.apache.flink.table.utils.TableTestUtil._

import org.junit.Test

class PythonCalcSplitRuleTest extends TableTestBase {

  @Test
  def testPythonFunctionAsInputOfJavaFunction(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Int, Int, Int)]("MyTable", 'a, 'b, 'c)
    util.tableEnv.registerFunction("pyFunc1", new PythonScalarFunction("pyFunc1"))

    val resultTable = table
      .select(call("pyFunc1", $"a", $"b") + 1)

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
      .select(call("pyFunc1", $"a", $"b") , $"c" + 1)

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
      .where(call("pyFunc2", $"a", $"c") > 0)
      .select(call("pyFunc1", $"a", $"b"), $"c" + 1)

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
      .where(call("pyFunc2", $"a", $"c"))
      .select(call("pyFunc1", $"a", $"b"))

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
      .select(
        call(
          "pyFunc3",
          call(
            "pyFunc2",
            $"a" + call("pyFunc1", $"a", $"c"
            ),
            $"b"
          ),
          $"c"
        )
      )

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
      .select(call("pyFunc1", $"a", $"b"))

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
      .where(call("pyFunc1", $"a", $"c"))
      .select($"a", $"b")

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
      .select(call("pyFunc1", $"f1", $"f2"), $"f0" + 1)

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

    val resultTable = table.select($"a", $"b", call("pyFunc1", $"a", $"c"), 1)

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

    val resultTable = table.select($"a", call("pyFunc1", $"a", $"c"), $"b")

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

  @Test
  def testPandasFunctionAsInputOfJavaFunction(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Int, Int, Int)]("MyTable", 'a, 'b, 'c)
    util.tableEnv.registerFunction("pandasFunc1", new PandasScalarFunction("pandasFunc1"))

    val resultTable = table.select(call("pandasFunc1", $"a", $"b") + 1)

    val expected = unaryNode(
      "DataStreamCalc",
      unaryNode(
        "DataStreamPythonCalc",
        streamTableNode(table),
        term("select", "pandasFunc1(a, b) AS f0")
      ),
      term("select", "+(f0, 1) AS _c0")
    )

    util.verifyTable(resultTable, expected)
  }

  @Test
  def testPandasFunctionMixedWithJavaFunction(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Int, Int, Int)]("MyTable", 'a, 'b, 'c)
    util.tableEnv.registerFunction("pandasFunc1", new PandasScalarFunction("pandasFunc1"))

    val resultTable = table.select(call("pandasFunc1", $"a", $"b"), $"c" + 1)

    val expected = unaryNode(
      "DataStreamCalc",
      unaryNode(
        "DataStreamPythonCalc",
        streamTableNode(table),
        term("select", "c", "pandasFunc1(a, b) AS f0")
      ),
      term("select", "f0 AS _c0", "+(c, 1) AS _c1")
    )

    util.verifyTable(resultTable, expected)
  }

  @Test
  def testPandasFunctionMixedWithJavaFunctionInWhereClause(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Int, Int, Int)]("MyTable", 'a, 'b, 'c)
    util.tableEnv.registerFunction("pandasFunc1", new PandasScalarFunction("pandasFunc1"))
    util.tableEnv.registerFunction("pandasFunc2", new PandasScalarFunction("pandasFunc2"))

    val resultTable = table
      .where(call("pandasFunc2", $"a", $"c") > 0)
      .select(call("pandasFunc1", $"a", $"b"), $"c" + 1)

    val expected = unaryNode(
      "DataStreamCalc",
      unaryNode(
        "DataStreamPythonCalc",
        unaryNode(
          "DataStreamCalc",
          unaryNode(
            "DataStreamPythonCalc",
            streamTableNode(table),
            term("select", "a", "b", "c", "pandasFunc2(a, c) AS f0")
          ),
          term("select", "c", "a", "b"),
          term("where", ">(f0, 0)")
        ),
        term("select", "c", "pandasFunc1(a, b) AS f0")
      ),
      term("select", "f0 AS _c0", "+(c, 1) AS _c1")
    )

    util.verifyTable(resultTable, expected)
  }

  @Test
  def testPandasFunctionInWhereClause(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Int, Int, Int)]("MyTable", 'a, 'b, 'c)
    util.tableEnv.registerFunction("pandasFunc1", new PandasScalarFunction("pandasFunc1"))
    util.tableEnv.registerFunction("pandasFunc2", new BooleanPandasScalarFunction("pandasFunc2"))

    val resultTable = table
      .where(call("pandasFunc2", $"a", $"c"))
      .select(call("pandasFunc1", $"a", $"b"))

    val expected = unaryNode(
      "DataStreamPythonCalc",
      unaryNode(
        "DataStreamCalc",
        unaryNode(
          "DataStreamPythonCalc",
          streamTableNode(table),
          term("select", "a", "b", "pandasFunc2(a, c) AS f0")
        ),
        term("select", "a", "b"),
        term("where", "f0")
      ),
      term("select", "pandasFunc1(a, b) AS _c0")
    )

    util.verifyTable(resultTable, expected)
  }

  @Test
  def testChainingPandasFunction(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Int, Int, Int)]("MyTable", 'a, 'b, 'c)
    util.tableEnv.registerFunction("pandasFunc1", new PandasScalarFunction("pandasFunc1"))
    util.tableEnv.registerFunction("pandasFunc2", new PandasScalarFunction("pandasFunc2"))
    util.tableEnv.registerFunction("pandasFunc3", new PandasScalarFunction("pandasFunc3"))

    val resultTable = table
      .select(
        call(
          "pandasFunc3",
          call(
            "pandasFunc2",
            $"a" + call("pandasFunc1", $"a", $"c"
            ),
            $"b"
          ),
          $"c"
        )
      )

    val expected = unaryNode(
      "DataStreamPythonCalc",
      unaryNode(
        "DataStreamCalc",
        unaryNode(
          "DataStreamPythonCalc",
          streamTableNode(table),
          term("select", "b", "c", "a", "pandasFunc1(a, c) AS f0")
        ),
        term("select", "b", "c", "+(a, f0) AS f0")
      ),
      term("select", "pandasFunc3(pandasFunc2(f0, b), c) AS _c0")
    )

    util.verifyTable(resultTable, expected)
  }

  @Test
  def testOnlyOnePandasFunction(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Int, Int, Int)]("MyTable", 'a, 'b, 'c)
    util.tableEnv.registerFunction("pandasFunc1", new PandasScalarFunction("pandasFunc1"))

    val resultTable = table.select(call("pandasFunc1", $"a", $"b"))

    val expected = unaryNode(
      "DataStreamPythonCalc",
      streamTableNode(table),
      term("select", "pandasFunc1(a, b) AS _c0")
    )

    util.verifyTable(resultTable, expected)
  }

  @Test
  def testOnlyOnePandasFunctionInWhereClause(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Int, Int, Int)]("MyTable", 'a, 'b, 'c)
    util.tableEnv.registerFunction("pandasFunc1", new BooleanPandasScalarFunction("pandasFunc1"))

    val resultTable = table.where(call("pandasFunc1", $"a", $"c")).select($"a", $"b")

    val expected = unaryNode(
      "DataStreamCalc",
      unaryNode(
        "DataStreamPythonCalc",
        streamTableNode(table),
        term("select", "a", "b", "pandasFunc1(a, c) AS f0")
      ),
      term("select", "a", "b"),
      term("where", "f0")
    )

    util.verifyTable(resultTable, expected)
  }

  @Test
  def testPandasFunctionMixedWithGeneralPythonFunction(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Int, Int, Int)]("MyTable", 'a, 'b, 'c)
    util.tableEnv.registerFunction("pyFunc1", new PythonScalarFunction("pyFunc1"))
    util.tableEnv.registerFunction("pandasFunc1", new PandasScalarFunction("pandasFunc1"))

    val resultTable = table.select(
      call("pandasFunc1", $"a", $"b"),
      call("pyFunc1", $"a", $"c") + 1,
      $"a" + 1)

    val expected = unaryNode(
      "DataStreamCalc",
      unaryNode(
        "DataStreamPythonCalc",
        unaryNode(
          "DataStreamPythonCalc",
          streamTableNode(table),
          term("select", "a", "c", "pandasFunc1(a, b) AS f0")
        ),
        term("select", "a", "f0", "pyFunc1(a, c) AS f1")
      ),
      term("select",  "f0 AS _c0", "+(f1, 1) AS _c1", "+(a, 1) AS _c2")
    )

    util.verifyTable(resultTable, expected)
  }

  @Test
  def testPandasFunctionNotChainingWithGeneralPythonFunction(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Int, Int, Int)]("MyTable", 'a, 'b, 'c)
    util.tableEnv.registerFunction("pyFunc1", new PythonScalarFunction("pyFunc1"))
    util.tableEnv.registerFunction("pandasFunc1", new PandasScalarFunction("pandasFunc1"))

    val resultTable = table
      .select(
        call(
          "pyFunc1",
          $"a",
          call(
            "pandasFunc1", $"a", $"b"
          )
        ) + 1
      )

    val expected = unaryNode(
      "DataStreamCalc",
      unaryNode(
        "DataStreamPythonCalc",
        unaryNode(
          "DataStreamPythonCalc",
          streamTableNode(table),
          term("select", "a", "pandasFunc1(a, b) AS f0")
        ),
        term("select", "pyFunc1(a, f0) AS f0")
      ),
      term("select",  "+(f0, 1) AS _c0")
    )

    util.verifyTable(resultTable, expected)
  }

  @Test
  def testPythonFunctionWithCompositeInputs(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Int, Int, (Int, Int))]("MyTable", 'a, 'b, 'c)
    util.tableEnv.registerFunction("pyFunc1", new PythonScalarFunction("pyFunc1"))

    val resultTable = table.select('a, 'b, 'c.flatten())
      .select($"a", call("pyFunc1", $"a", $"c$$_1"), $"b")

    val expected = unaryNode(
      "DataStreamCalc",
      unaryNode(
        "DataStreamPythonCalc",
        unaryNode(
          "DataStreamCalc",
          streamTableNode(table),
          term("select", "a", "b", "c._1 AS f0")
        ),
        term("select", "a", "b", "pyFunc1(a, f0) AS f0")
      ),
      term("select", "a", "f0 AS _c1", "b")
    )

    util.verifyTable(resultTable, expected)
  }

  @Test
  def testChainingPythonFunctionWithCompositeInputs(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Int, Int, (Int, Int))]("MyTable", 'a, 'b, 'c)
    util.tableEnv.registerFunction("pyFunc1", new PythonScalarFunction("pyFunc1"))

    val resultTable = table.select('a, 'b, 'c.flatten())
      .select(
        $"a",
        call(
          "pyFunc1",
          $"a",
          call("pyFunc1", $"b", $"c$$_1")),
        $"b")

    val expected = unaryNode(
      "DataStreamCalc",
      unaryNode(
        "DataStreamPythonCalc",
        unaryNode(
          "DataStreamCalc",
          streamTableNode(table),
          term("select", "a", "b", "c._1 AS f0")
        ),
        term("select", "a", "b", "pyFunc1(a, pyFunc1(b, f0)) AS f0")
      ),
      term("select", "a", "f0 AS _c1", "b")
    )

    util.verifyTable(resultTable, expected)
  }

  @Test
  def testPandasFunctionWithCompositeInputs(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Int, Int, (Int, Int))]("MyTable", 'a, 'b, 'c)
    util.tableEnv.registerFunction("pandasFunc1", new PandasScalarFunction("pandasFunc1"))

    val resultTable = table.select('a, 'b, 'c.flatten())
      .select(call("pandasFunc1", $"a", $"c$$_1"))

    val expected = unaryNode(
      "DataStreamPythonCalc",
      unaryNode(
        "DataStreamCalc",
        streamTableNode(table),
        term("select", "a", "c._1 AS f0")
      ),
      term("select", "pandasFunc1(a, f0) AS _c0")
    )

    util.verifyTable(resultTable, expected)
  }
}
