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

package org.apache.flink.table.codegen

import java.util

import org.apache.calcite.rex.{RexBuilder, RexNode}
import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.calcite.{FlinkTypeFactory, FlinkTypeSystem}
import org.apache.flink.table.functions.utils.ScalarSqlFunction
import org.apache.flink.table.functions.{FunctionLanguage, ScalarFunction}
import org.junit.Test
import org.junit.Assert.assertEquals

class ExpressionReducerTest {

  @Test
  def testSkipPythonFunctions(): Unit = {
    val typeFactory = new FlinkTypeFactory(new FlinkTypeSystem)
    val rexBuilder = new RexBuilder(typeFactory)

    val constExprs = new util.ArrayList[RexNode]

    constExprs.add(rexBuilder.makeCall(new ScalarSqlFunction(
      "normalFunc1", "normalFunc1", new NormalScalarFunction, typeFactory)))

    constExprs.add(rexBuilder.makeCall(new ScalarSqlFunction(
      "pythonFunc1", "pythonFunc1", new MockedPythonScalarFunction, typeFactory)))

    constExprs.add(rexBuilder.makeCall(
      new ScalarSqlFunction(
      "normalFunc2", "normalFunc2", new NormalScalarFunction, typeFactory),
      rexBuilder.makeBigintLiteral(java.math.BigDecimal.valueOf(2))
    ))

    constExprs.add(rexBuilder.makeCall(
      new ScalarSqlFunction(
        "pythonFunc2", "pythonFunc2", new MockedPythonScalarFunction, typeFactory),
      rexBuilder.makeBigintLiteral(java.math.BigDecimal.valueOf(2))
    ))

    val reducer = new ExpressionReducer(new TableConfig())

    val reducedExprs = new util.ArrayList[RexNode]()

    reducer.reduce(rexBuilder, constExprs, reducedExprs)

    assertEquals(constExprs.size(), reducedExprs.size())

    val expectedExprs = new util.ArrayList[RexNode]()

    expectedExprs.add(rexBuilder.makeLiteral(
      java.math.BigDecimal.valueOf(1), constExprs.get(0).getType, true))
    expectedExprs.add(constExprs.get(1))
    expectedExprs.add(rexBuilder.makeLiteral(
      java.math.BigDecimal.valueOf(2), constExprs.get(2).getType, true))
    expectedExprs.add(constExprs.get(3))

    assertEquals(expectedExprs, reducedExprs)
  }
}

class NormalScalarFunction extends ScalarFunction {
  def eval(): Long = 1

  def eval(a: Long): Long = a
}

class MockedPythonScalarFunction extends ScalarFunction {

  override def getLanguage: FunctionLanguage = FunctionLanguage.PYTHON

  def eval(): Long = {
    throw new RuntimeException("This method should not be called!")
  }

  def eval(a: Long): Long = {
    throw new RuntimeException("This method should not be called!")
  }
}
