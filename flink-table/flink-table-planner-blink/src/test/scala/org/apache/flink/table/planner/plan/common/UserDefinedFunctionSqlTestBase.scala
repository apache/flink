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

package org.apache.flink.table.planner.plan.common

import org.apache.flink.api.scala._
import org.apache.flink.table.api.{Table, _}
import org.apache.flink.table.planner.expressions.utils.{Func0, Func13}
import org.apache.flink.table.planner.plan.utils.JavaUserDefinedAggFunctions.{VarSum3AggFunction, VarSumAggFunction}
import org.apache.flink.table.planner.utils.{TableFunc1, TableFunc5, TableTestBase, TableTestUtil}

import org.junit.{Before, Test}


abstract class UserDefinedFunctionSqlTestBase extends TableTestBase {

  protected val util: TableTestUtil = getTableTestUtil

  protected def getTableTestUtil: TableTestUtil

  private var table: Table = _

  @Before
  def setup(): Unit = {
    table = util.addTableSource[(Int, Int, String)]("MyTable", 'a, 'b, 'c)
  }

  @Test
  def testTableFunction(): Unit = {
    val func1 = new TableFunc1
    util.addFunction("table_func1", func1)
    val func5 = new TableFunc5(",")
    util.addFunction("table_func5", func5)

    val query =
      """
        |SELECT * FROM (
        |    SELECT * FROM MyTable, LATERAL TABLE(table_func1(c)) AS T(d)
        |) t, LATERAL TABLE(table_func5(c)) AS T(f)
      """.stripMargin
    util.verifyPlan(query)
  }

  @Test
  def testScalarFunction(): Unit = {
    util.addFunction("func0", Func0)
    val func13 = new Func13("test")
    util.addFunction("func13", func13)

    val query = "SELECT func0(a) as a1, func13(c) as c1 FROM MyTable"
    util.verifyPlan(query)
  }

  @Test
  def testAggregateFunction(): Unit = {
    val varSum = new VarSumAggFunction
    util.addFunction("var_sum", varSum)
    val varSum3 = new VarSum3AggFunction(10L)
    util.addFunction("var_sum3", varSum3)

    val query = "SELECT var_sum(a) as a1, var_sum3(a) as a2 FROM MyTable GROUP BY b"
    util.verifyPlan(query)
  }

}
