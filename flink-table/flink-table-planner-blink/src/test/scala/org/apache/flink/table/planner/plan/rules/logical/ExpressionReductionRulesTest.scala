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

package org.apache.flink.table.planner.plan.rules.logical

import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.functions.ScalarFunction
import org.apache.flink.table.functions.python.{PythonEnv, PythonFunction}
import org.apache.flink.table.planner.expressions.utils.{Func1, RichFunc1}
import org.apache.flink.table.planner.utils.TableTestBase

import org.junit.Test

/**
  * Test for [[org.apache.flink.table.planner.codegen.ExpressionReducer]].
  */
class ExpressionReductionRulesTest extends TableTestBase {

  private val util = batchTestUtil()
  util.addTableSource[(Int, Long, Int)]("MyTable", 'a, 'b, 'c)

  @Test
  def testExpressionReductionWithUDF(): Unit = {
    util.addFunction("MyUdf", Func1)
    util.verifyRelPlan("SELECT MyUdf(1) FROM MyTable")
  }

  @Test
  def testExpressionReductionWithRichUDF(): Unit = {
    util.addFunction("MyUdf", new RichFunc1)
    util.getTableEnv.getConfig.addJobParameter("int.value", "10")
    util.verifyRelPlan("SELECT myUdf(1) FROM MyTable")
  }

  @Test
  def testExpressionReductionWithRichUDFAndInvalidOpen(): Unit = {
    util.addFunction("MyUdf", new RichFunc1)
    // FunctionContext.getCachedFile will fail during expression reduction
    // it will be executed during runtime though
    util.getTableEnv.getConfig.addJobParameter("fail-for-cached-file", "true")
    util.verifyRelPlan("SELECT myUdf(1 + 1) FROM MyTable")
  }

  @Test
  def testExpressionReductionWithPythonUDF(): Unit = {
    util.addFunction("PyUdf", DeterministicPythonFunc)
    util.addFunction("MyUdf", Func1)
    util.verifyExecPlan("SELECT PyUdf(), MyUdf(1) FROM MyTable")
  }
}

@SerialVersionUID(1L)
object DeterministicPythonFunc extends ScalarFunction with PythonFunction {

  def eval(): Long = 1

  override def getSerializedPythonFunction: Array[Byte] = null

  override def getPythonEnv: PythonEnv = null
}
