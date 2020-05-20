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
import org.apache.flink.table.runtime.utils.JavaUserDefinedScalarFunctions.PythonScalarFunction
import org.apache.flink.table.utils.TableTestUtil.{streamTableNode, term, unaryNode}
import org.apache.flink.table.utils.{MockPythonTableFunction, TableFunc1, TableTestBase}
import org.junit.Test

class PythonCorrelateSplitRuleTest extends TableTestBase {

  @Test
  def testPythonTableFunctionWithJavaFunc(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Int, Int, Int)]("MyTable", 'a, 'b, 'c)
    val pyFunc = new PythonScalarFunction("pyFunc")
    val tableFunc = new MockPythonTableFunction

    val resultTable = table.joinLateral(tableFunc('a * 'a, pyFunc('b, 'c)) as('x, 'y))

    val expected = unaryNode(
        "DataStreamCalc",
        unaryNode(
          "DataStreamPythonCorrelate",
          unaryNode(
            "DataStreamCalc",
            streamTableNode(table),
            term("select", "a", "b", "c", "*(a, a) AS f0")),
          term("invocation",
               s"${tableFunc.functionIdentifier}($$3, " +
                 s"${pyFunc.functionIdentifier}($$1, $$2))"),
          term("correlate", s"table(${tableFunc.getClass.getSimpleName}(f0, " +
            s"${pyFunc.toString}(b, c)))"),
          term("select", "a", "b", "c", "f0", "x", "y"),
          term("rowType",
               "RecordType(INTEGER a, INTEGER b, INTEGER c, INTEGER f0, INTEGER x, INTEGER y)"),
          term("joinType", "INNER")
          ),
        term("select", "a", "b", "c", "x", "y")
        )

    util.verifyTable(resultTable, expected)
  }

  @Test
  def testJavaTableFunctionWithPythonCalc(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Int, Int, String)]("MyTable", 'a, 'b, 'c)
    val pyFunc = new PythonScalarFunction("pyFunc")
    val tableFunc = new TableFunc1

    val result = table.joinLateral(tableFunc(pyFunc('c)) as 'x)

    val expected = unaryNode(
      "DataStreamCalc",
      unaryNode(
        "DataStreamCorrelate",
        unaryNode(
          "DataStreamPythonCalc",
          streamTableNode(table),
          term("select", "a", "b", "c", "pyFunc(c) AS f0")),
        term("invocation",
             s"${tableFunc.functionIdentifier}($$3)"),
        term("correlate", s"table(TableFunc1(f0))"),
        term("select", "a", "b", "c", "f0", "x"),
        term("rowType",
             "RecordType(INTEGER a, INTEGER b, VARCHAR(65536) c, VARCHAR(65536) f0, " +
               "VARCHAR(65536) x)"),
        term("joinType", "INNER")
        ),
      term("select", "a", "b", "c", "x")
      )

    util.verifyTable(result, expected)
  }

  @Test
  def testPythonTableFunctionWithCompositeInputs(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Int, Int, Int, (Int, Int))]("MyTable", 'a, 'b, 'c, 'd)
    val tableFunc = new MockPythonTableFunction
    util.addFunction("tableFunc", tableFunc)

    val pyFunc = new PythonScalarFunction("pyFunc")
    util.addFunction("pyFunc", pyFunc)
    val resultTable = table.select('a, 'b, 'c, 'd.flatten())
      .joinLateral(call("tableFunc", $"a" * $"d$$_1", call("pyFunc", $"d$$_2", $"c")))

    val expected = unaryNode(
      "DataStreamCalc",
      unaryNode(
        "DataStreamPythonCorrelate",
        unaryNode(
          "DataStreamCalc",
          streamTableNode(table),
          term("select", "a", "b", "c", "d._1 AS d$_1", "d._2 AS d$_2", "*(a, d._1) AS f0")
        ),
        term("invocation",
          s"${tableFunc.functionIdentifier()}($$5, " +
            s"${pyFunc.functionIdentifier()}($$4, $$2))"),
        term("correlate", s"table(${tableFunc.getClass.getSimpleName}(f0, " +
          s"pyFunc(d$$_2, c)))"),
        term("select", "a, b, c, d$_1, d$_2, f0, f00, f1"),
        term("rowType","RecordType(INTEGER a, INTEGER b, INTEGER c, INTEGER d$_1, " +
          "INTEGER d$_2, INTEGER f0, INTEGER f00, INTEGER f1)"),
        term("joinType", "INNER")
      ),
      term("select", "a", "b", "c", "d$_1", "d$_2", "f00 AS f0", "f1")
    )

    util.verifyTable(resultTable, expected)
  }

  @Test
  def testJavaTableFunctionWithPythonCalcCompositeInputs(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Int, Int, String, (String, String))]("MyTable", 'a, 'b, 'c, 'd)
    val tableFunc = new TableFunc1
    util.addFunction("tableFunc", tableFunc)

    val pyFunc = new PythonScalarFunction("pyFunc")
    util.addFunction("pyFunc", pyFunc)
    val result = table.select('a, 'b, 'c, 'd.flatten())
      .joinLateral(call("tableFunc", call("pyFunc", $"d$$_1")))

    val expected = unaryNode(
      "DataStreamCalc",
      unaryNode(
        "DataStreamCorrelate",
        unaryNode(
          "DataStreamPythonCalc",
          unaryNode(
            "DataStreamCalc",
            streamTableNode(table),
            term("select", "a", "b", "c", "d._1 AS f0", "d._2 AS f1", "d._1 AS f2")),
          term("select", "a", "b", "c", "f0 AS d$_1", "f1 AS d$_2", "pyFunc(f2) AS f0")),
        term("invocation",
          s"${tableFunc.functionIdentifier}($$5)"),
        term("correlate", s"table(TableFunc1(f0))"),
        term("select", "a", "b", "c", "d$_1", "d$_2", "f0", "f00"),
        term("rowType",
          "RecordType(INTEGER a, INTEGER b, VARCHAR(65536) c, VARCHAR(65536) d$_1, " +
            "VARCHAR(65536) d$_2, VARCHAR(65536) f0, VARCHAR(65536) f00)"),
        term("joinType", "INNER")
      ),
      term("select", "a", "b", "c", "d$_1", "d$_2", "f00 AS f0")
    )

    util.verifyTable(result, expected)
  }
}
