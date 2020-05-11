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
import org.apache.flink.table.utils.TableTestBase
import org.apache.flink.table.utils.TableTestUtil._
import org.junit.Test

class SplitPythonConditionFromJoinRuleTest extends TableTestBase {

  @Test
  def testPythonFunctionInJoinCondition(): Unit = {
    val util = streamTestUtil()
    val left = util.addTable[(Int, Int, Int)]("leftTable", 'a, 'b, 'c)
    val right = util.addTable[(Int, Int, Int)]("rightTable", 'd, 'e, 'f)
    util.tableEnv.registerFunction("pyFunc", new PythonScalarFunction("pyFunc"))

    val result = left
      .join(right, $"a" === $"d" && call("pyFunc", $"a", $"d") === $"b")
      .select($"a", $"b", $"d")

    val expected = unaryNode(
      "DataStreamCalc",
      unaryNode(
        "DataStreamPythonCalc",
        binaryNode(
          "DataStreamJoin",
          unaryNode(
            "DataStreamCalc",
            streamTableNode(left),
            term("select", "a", "b")
          ),
          unaryNode(
            "DataStreamCalc",
            streamTableNode(right),
            term("select", "d")
          ),
          term("where", "=(a, d)"),
          term("join", "a, b, d"),
          term("joinType", "InnerJoin")
        ),
        term("select", "a", "b", "d", "pyFunc(a, d) AS f0")
      ),
      term("select", "a", "b", "d"),
      term("where", "=(f0, b)")
    )
    util.verifyTable(result, expected)
  }

  @Test
  def testPythonFunctionInAboveFilter(): Unit = {
    val util = streamTestUtil()
    val left = util.addTable[(Int, Int, Int)]("leftTable", 'a, 'b, 'c)
    val right = util.addTable[(Int, Int, Int)]("rightTable", 'd, 'e, 'f)
    util.tableEnv.registerFunction("pyFunc", new PythonScalarFunction("pyFunc"))

    val result = left
      .join(right, $"a" === $"d" && call("pyFunc", $"a", $"d") === ($"a" + $"b"))
      .select($"a", $"b", $"d")
      .filter(call("pyFunc", $"a", $"b") === ($"b" * $"d"))
      .select($"a" + 1, $"b", $"d")

    val expected = unaryNode(
      "DataStreamCalc",
      unaryNode(
        "DataStreamPythonCalc",
        binaryNode(
          "DataStreamJoin",
          unaryNode(
            "DataStreamCalc",
            streamTableNode(left),
            term("select", "a", "b")),
          unaryNode(
            "DataStreamCalc",
            streamTableNode(right),
            term("select", "d")
          ),
          term("where", "=(a, d)"),
          term("join", "a, b, d"),
          term("joinType", "InnerJoin")
        ),
        term("select", "a", "b", "d", "pyFunc(a, d) AS f0", "pyFunc(a, b) AS f1")
      ),
      term("select", "+(a, 1) AS _c0, b, d"),
      term("where", "AND(=(f0, +(a, b)), =(f1, *(b, d)))")
    )

    util.verifyTable(result, expected)
  }
}
