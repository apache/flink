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

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedScalarFunctions.PythonScalarFunction
import org.apache.flink.table.planner.utils.TableTestBase
import org.junit.{Before, Test}

class PythonCalcTest extends TableTestBase {
  private val util = streamTestUtil()

  @Before
  def setup(): Unit = {
    util.addTableSource[(Int, Int, Int)]("MyTable", 'a, 'b, 'c)
    util.addFunction("pyFunc1", new PythonScalarFunction("pyFunc1"))
  }

  @Test
  def testPythonFunctionMixedWithJavaFunction(): Unit = {
    val sqlQuery = "SELECT pyFunc1(a, b), c + 1 FROM MyTable"
    util.verifyPlan(sqlQuery)
  }
}
