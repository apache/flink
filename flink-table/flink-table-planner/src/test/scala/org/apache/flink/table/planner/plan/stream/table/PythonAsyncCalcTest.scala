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
package org.apache.flink.table.planner.plan.stream.table

import org.apache.flink.table.api._
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedScalarFunctions.{AsyncPythonScalarFunction, PythonScalarFunction}
import org.apache.flink.table.planner.utils.TableTestBase

import org.junit.jupiter.api.{BeforeEach, Test}

/** Test for Python Async Calc execution plan. */
class PythonAsyncCalcTest extends TableTestBase {
  private val util = streamTestUtil()

  @BeforeEach
  def setup(): Unit = {
    util.addTableSource[(Int, Int, Int)]("MyTable", 'a, 'b, 'c)
    util.addTemporarySystemFunction("asyncFunc1", new AsyncPythonScalarFunction("asyncFunc1"))
    util.addTemporarySystemFunction("asyncFunc2", new AsyncPythonScalarFunction("asyncFunc2"))
    util.addTemporarySystemFunction("pyFunc1", new PythonScalarFunction("pyFunc1"))
  }

  @Test
  def testAsyncPythonFunctionMixedWithJavaFunction(): Unit = {
    val sqlQuery = "SELECT asyncFunc1(a, b), c + 1 FROM MyTable"
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testAsyncPythonFunctionOnly(): Unit = {
    val sqlQuery = "SELECT asyncFunc1(a, b) FROM MyTable"
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testMultipleAsyncPythonFunctions(): Unit = {
    val sqlQuery = "SELECT asyncFunc1(a, b), asyncFunc2(b, c) FROM MyTable"
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testAsyncPythonFunctionWithWhereClause(): Unit = {
    val sqlQuery = "SELECT asyncFunc1(a, b), c FROM MyTable WHERE asyncFunc2(a, c) > 0"
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testAsyncPythonFunctionMixedWithSyncPythonFunction(): Unit = {
    val sqlQuery = "SELECT asyncFunc1(a, b), pyFunc1(b, c) FROM MyTable"
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testChainingAsyncPythonFunctions(): Unit = {
    val sqlQuery = "SELECT asyncFunc2(asyncFunc1(a, b), c) FROM MyTable"
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testAsyncPythonFunctionAsInputOfJavaFunction(): Unit = {
    val sqlQuery = "SELECT asyncFunc1(a, b) + 1 FROM MyTable"
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testAsyncPythonFunctionInWhereClauseOnly(): Unit = {
    val sqlQuery = "SELECT a, b FROM MyTable WHERE asyncFunc1(a, c) > 0"
    util.verifyExecPlan(sqlQuery)
  }
}
