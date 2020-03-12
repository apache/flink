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
import org.apache.flink.table.utils.{MockPythonTableFunction, TableTestBase}
import org.apache.flink.table.utils.TableTestUtil.{streamTableNode, term, unaryNode}
import org.junit.Test

class CalcPythonCorrelateTransposeRuleTest extends TableTestBase {
  @Test
  def testPythonTableFunctionWithCondition(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Int, Int, Int)]("MyTable", 'a, 'b, 'c)
    val scalarFunc = new PythonScalarFunction("pyFunc")
    val tableFunc = new MockPythonTableFunction()

    val resultTable = table.joinLateral(
      tableFunc('a * 'a, 'b) as('x, 'y),
      'x === 'a && scalarFunc('x, 'x) === 2 && 'y + 1 === 'y * 'y)
    val expected = unaryNode(
      "DataStreamCalc",
      unaryNode(
        "DataStreamPythonCalc",
        unaryNode(
          "DataStreamPythonCorrelate",
          unaryNode(
            "DataStreamCalc",
            streamTableNode(table),
            term("select", "a, b, c, *(a, a) AS f0")),
          term("invocation", s"${tableFunc.functionIdentifier}" +
            s"($$3, $$1)"),
          term("correlate", s"table(${tableFunc.getClass.getSimpleName}" +
            s"(f0, b))"),
          term("select", "a, b, c, f0, x, y"),
          term("rowType",
               "RecordType(INTEGER a, INTEGER b, INTEGER c, INTEGER f0, INTEGER x, INTEGER y)"),
          term("joinType", "INNER")),
        term("select", "a, b, c, x, y, pyFunc(x, x) AS f0")),
      term("select", "a, b, c, x, y"),
      term("where", "AND(AND(=(f0, 2), =(+(y, 1), *(y, y))), =(x, a))"))
    util.verifyTable(resultTable, expected)
  }

}
