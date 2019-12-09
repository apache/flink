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
package org.apache.flink.table.planner.plan.stream.table.validation

import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.planner.expressions.utils._
import org.apache.flink.table.planner.plan.utils.JavaUserDefinedAggFunctions.WeightedAvg
import org.apache.flink.table.planner.utils.{ObjectTableFunction, TableFunc1, TableFunc2, TableTestBase}

import org.junit.Assert.{assertTrue, fail}
import org.junit.Test

class CorrelateValidationTest extends TableTestBase {

  @Test
  def testRegisterFunctionException(): Unit ={
    val util = streamTestUtil()
    val t = util.addTableSource[(Int, Long, String)]('a, 'b, 'c)

    // check scala object is forbidden
    expectExceptionThrown(
      util.addFunction("func3", ObjectTableFunction), "Scala object")
    expectExceptionThrown(t.joinLateral(ObjectTableFunction('a, 1)), "Scala object")
  }

  @Test
  def testInvalidTableFunction(): Unit = {
    val util = streamTestUtil()
    val t = util.addTableSource[(Int, Long, String)]("MyTable", 'a, 'b, 'c)

    //=================== check scala object is forbidden =====================
    // Scala table environment register
    expectExceptionThrown(util.addFunction("udtf", ObjectTableFunction), "Scala object")
    // Java table environment register
    expectExceptionThrown(
      util.addFunction("udtf", ObjectTableFunction), "Scala object")
    // Scala Table API directly call
    expectExceptionThrown(t.joinLateral(ObjectTableFunction('a, 1)), "Scala object")


    //============ throw exception when table function is not registered =========
    // Java Table API call
    expectExceptionThrown(
      t.joinLateral("nonexist(a)"), "Undefined function: nonexist")
    // SQL API call
    expectExceptionThrown(
      util.tableEnv.sqlQuery("SELECT * FROM MyTable, LATERAL TABLE(nonexist(a))"),
      "No match found for function signature nonexist(<NUMERIC>)")


    //========= throw exception when the called function is a scalar function ====
    util.addFunction("func0", Func0)

    // Java Table API call
    expectExceptionThrown(
      t.joinLateral("func0(a)"),
      "only accepts a string expression which defines a table function call")
    // SQL API call
    // NOTE: it doesn't throw an exception but an AssertionError, maybe a Calcite bug
    expectExceptionThrown(
      util.tableEnv.sqlQuery("SELECT * FROM MyTable, LATERAL TABLE(func0(a))"),
      null,
      classOf[AssertionError])

    //========== throw exception when the parameters is not correct ===============
    // Java Table API call
    util.addFunction("func2", new TableFunc2)
    expectExceptionThrown(
      t.joinLateral("func2(c, c)"),
      "Given parameters of function 'func2' do not match any signature")
    // SQL API call
    expectExceptionThrown(
      util.tableEnv.sqlQuery("SELECT * FROM MyTable, LATERAL TABLE(func2(c, c))"),
      "Given parameters of function 'func2' do not match any signature.")
  }

  /**
    * Due to the improper translation of TableFunction left outer join (see CALCITE-2004), the
    * join predicate can only be empty or literal true (the restriction should be removed in
    * FLINK-7865).
    */
  @Test (expected = classOf[ValidationException])
  def testLeftOuterJoinWithPredicates(): Unit = {
    val util = streamTestUtil()
    val table = util.addTableSource[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
    val function = new TableFunc1
    util.addFunction("func1", function)

    val result = table.leftOuterJoinLateral(function('c) as 's, 'c === 's)
      .select('c, 's).where('a > 10)

    util.verifyPlan(result)
  }

  @Test(expected = classOf[ValidationException])
  def testInvalidMapFunctionTypeAggregation(): Unit = {
    val util = streamTestUtil()
    util.addTableSource[(Int)](
      "MyTable", 'int)
      .flatMap('int.sum) // do not support AggregateFunction as input
  }

  @Test(expected = classOf[ValidationException])
  def testInvalidMapFunctionTypeUDAGG(): Unit = {
    val util = streamTestUtil()

    val weightedAvg = new WeightedAvg
    util.addTableSource[(Int)](
      "MyTable", 'int)
      .flatMap(weightedAvg('int, 'int)) // do not support AggregateFunction as input
  }

  @Test(expected = classOf[ValidationException])
  def testInvalidMapFunctionTypeUDAGG2(): Unit = {
    val util = streamTestUtil()

    util.addFunction("weightedAvg", new WeightedAvg)
    util.addTableSource[(Int)](
      "MyTable", 'int)
      .flatMap("weightedAvg(int, int)") // do not support AggregateFunction as input
  }

  @Test(expected = classOf[ValidationException])
  def testInvalidMapFunctionTypeScalarFunction(): Unit = {
    val util = streamTestUtil()

    util.addTableSource[(String)](
      "MyTable", 'string)
      .flatMap(Func15('string)) // do not support ScalarFunction as input
  }

  @Test(expected = classOf[ValidationException])
  def testInvalidFlatMapFunctionTypeFieldReference(): Unit = {
    val util = batchTestUtil()

    util.addTableSource[(String)](
      "MyTable", 'string)
      .flatMap('string) // Only TableFunction can be used in flatMap
  }

  // ----------------------------------------------------------------------------------------------

  private def expectExceptionThrown(
      function: => Unit,
      keywords: String,
      clazz: Class[_ <: Throwable] = classOf[ValidationException])
    : Unit = {
    try {
      function
      fail(s"Expected a $clazz, but no exception is thrown.")
    } catch {
      case e if e.getClass == clazz =>
        if (keywords != null) {
          assertTrue(
            s"The exception message '${e.getMessage}' doesn't contain keyword '$keywords'",
            e.getMessage.contains(keywords))
        }
      case e: Throwable => fail(s"Expected throw ${clazz.getSimpleName}, but is $e.")
    }
  }
}
