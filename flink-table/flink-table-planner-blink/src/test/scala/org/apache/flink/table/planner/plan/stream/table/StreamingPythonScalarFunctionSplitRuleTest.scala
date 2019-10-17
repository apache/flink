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

import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.{FunctionLanguage, ScalarFunction}
import org.apache.flink.table.planner.utils.TableTestBase
import org.junit.Test

class StreamingPythonScalarFunctionSplitRuleTest extends TableTestBase {

  @Test
  def testPythonFunctionAsInputOfJavaFunction(): Unit = {
    val util = streamTestUtil()
    util.addTableSource[(Int, Int, Int)]("MyTable", 'a, 'b, 'c)
    util.addFunction("pyFunc1", new PythonScalarFunction("pyFunc1"))

    val sqlQuery = "SELECT pyFunc1(a, b) + 1 FROM MyTable"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testPythonFunctionMixedWithJavaFunction(): Unit = {
    val util = streamTestUtil()
    util.addTableSource[(Int, Int, Int)]("MyTable", 'a, 'b, 'c)
    util.addFunction("pyFunc1", new PythonScalarFunction("pyFunc1"))

    val sqlQuery = "SELECT pyFunc1(a, b), c + 1 FROM MyTable"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testPythonFunctionMixedWithJavaFunctionInWhereClause(): Unit = {
    val util = streamTestUtil()
    util.addTableSource[(Int, Int, Int)]("MyTable", 'a, 'b, 'c)
    util.addFunction("pyFunc1", new PythonScalarFunction("pyFunc1"))
    util.addFunction("pyFunc2", new PythonScalarFunction("pyFunc2"))

    val sqlQuery = "SELECT pyFunc1(a, b), c + 1 FROM MyTable WHERE pyFunc2(a, c) > 0"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testPythonFunctionInWhereClause(): Unit = {
    val util = streamTestUtil()
    util.addTableSource[(Int, Int, Int)]("MyTable", 'a, 'b, 'c)
    util.addFunction("pyFunc1", new PythonScalarFunction("pyFunc1"))
    util.addFunction("pyFunc2", new BooleanPythonScalarFunction("pyFunc2"))

    val sqlQuery = "SELECT pyFunc1(a, b) FROM MyTable WHERE pyFunc2(a, c)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testChainingPythonFunction(): Unit = {
    val util = streamTestUtil()
    util.addTableSource[(Int, Int, Int)]("MyTable", 'a, 'b, 'c)
    util.addFunction("pyFunc1", new PythonScalarFunction("pyFunc1"))
    util.addFunction("pyFunc2", new PythonScalarFunction("pyFunc2"))
    util.addFunction("pyFunc3", new PythonScalarFunction("pyFunc3"))

    val sqlQuery = "SELECT pyFunc3(pyFunc2(a + pyFunc1(a, c), b), c) FROM MyTable"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testOnlyOnePythonFunction(): Unit = {
    val util = streamTestUtil()
    util.addTableSource[(Int, Int, Int)]("MyTable", 'a, 'b, 'c)
    util.addFunction("pyFunc1", new PythonScalarFunction("pyFunc1"))

    val sqlQuery = "SELECT pyFunc1(a, b) FROM MyTable"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testOnlyOnePythonFunctionInWhereClause(): Unit = {
    val util = streamTestUtil()
    util.addTableSource[(Int, Int, Int)]("MyTable", 'a, 'b, 'c)
    util.addFunction("pyFunc1", new BooleanPythonScalarFunction("pyFunc1"))

    val sqlQuery = "SELECT a, b FROM MyTable WHERE pyFunc1(a, c)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testFieldNameUniquify(): Unit = {
    val util = streamTestUtil()
    util.addTableSource[(Int, Int, Int)]("MyTable", 'f0, 'f1, 'f2)
    util.addFunction("pyFunc1", new PythonScalarFunction("pyFunc1"))

    val sqlQuery = "SELECT pyFunc1(f1, f2), f0 + 1 FROM MyTable"
    util.verifyPlan(sqlQuery)
  }
}

class PythonScalarFunction(name: String) extends ScalarFunction {
  def eval(i: Int, j: Int): Int = i + j

  override def getResultType(signature: Array[Class[_]]): TypeInformation[_] =
    BasicTypeInfo.INT_TYPE_INFO

  override def getLanguage: FunctionLanguage = FunctionLanguage.PYTHON

  override def toString: String = name

}

class BooleanPythonScalarFunction(name: String) extends ScalarFunction {
  def eval(i: Int, j: Int): Boolean = i + j > 1

  override def getResultType(signature: Array[Class[_]]): TypeInformation[_] =
    BasicTypeInfo.BOOLEAN_TYPE_INFO

  override def getLanguage: FunctionLanguage = FunctionLanguage.PYTHON

  override def toString: String = name
}
