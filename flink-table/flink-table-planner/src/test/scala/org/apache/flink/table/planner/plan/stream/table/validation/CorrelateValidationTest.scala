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
import org.apache.flink.table.planner.expressions.utils._
import org.apache.flink.table.planner.plan.utils.JavaUserDefinedAggFunctions.WeightedAvg
import org.apache.flink.table.planner.utils.{ObjectTableFunction, TableFunc1, TableFunc2, TableTestBase}

import org.assertj.core.api.Assertions.assertThatExceptionOfType
import org.assertj.core.api.ThrowableAssert.ThrowingCallable
import org.junit.jupiter.api.Test

class CorrelateValidationTest extends TableTestBase {

  @Test
  def testRegisterFunctionException(): Unit = {
    val util = streamTestUtil()
    val t = util.addTableSource[(Int, Long, String)]('a, 'b, 'c)

    // check scala object is forbidden
    expectExceptionThrown(
      util.addTemporarySystemFunction("func3", ObjectTableFunction),
      "Could not register temporary system function 'func3' due to implementation errors.")
    expectExceptionThrown(t.joinLateral(ObjectTableFunction('a, 1)), "Scala object")
  }

  @Test
  def testInvalidTableFunction(): Unit = {
    val util = streamTestUtil()
    val t = util.addTableSource[(Int, Long, String)]("MyTable", 'a, 'b, 'c)

    // =================== check scala object is forbidden =====================
    // Scala table environment register
    expectExceptionThrown(
      util.addTemporarySystemFunction("udtf", ObjectTableFunction),
      "Could not register temporary system function 'udtf' due to implementation errors.")
    // Java table environment register
    expectExceptionThrown(
      util.addTemporarySystemFunction("udtf", ObjectTableFunction),
      "Could not register temporary system function 'udtf' due to implementation errors.")
    // Scala Table API directly call
    expectExceptionThrown(t.joinLateral(ObjectTableFunction('a, 1)), "Scala object")

    // ============ throw exception when table function is not registered =========
    // Java Table API call
    expectExceptionThrown(t.joinLateral(call("nonexist", $"a")), "Undefined function: nonexist")
    // SQL API call
    expectExceptionThrown(
      util.tableEnv.sqlQuery("SELECT * FROM MyTable, LATERAL TABLE(nonexist(a))"),
      "No match found for function signature nonexist(<NUMERIC>)")

    // ========= throw exception when the called function is a scalar function ====
    util.addTemporarySystemFunction("func0", Func0)

    // SQL API call
    expectExceptionThrown(
      util.tableEnv.sqlQuery("SELECT * FROM MyTable, LATERAL TABLE(func0(a))"),
      null,
      classOf[ValidationException])

    // ========== throw exception when the parameters is not correct ===============
    // Java Table API call
    util.addTemporarySystemFunction("func2", new TableFunc2)
    expectExceptionThrown(
      t.joinLateral(call("func2", $"c", $"c")),
      "Invalid function call:\nfunc2(STRING, STRING)")
    // SQL API call
    expectExceptionThrown(
      util.tableEnv.sqlQuery("SELECT * FROM MyTable, LATERAL TABLE(func2(c, c))"),
      "No match found for function signature func2(<CHARACTER>, <CHARACTER>).")
  }

  /**
   * Due to the improper translation of TableFunction left outer join (see CALCITE-2004), the join
   * predicate can only be empty or literal true (the restriction should be removed in FLINK-7865).
   */
  @Test
  def testLeftOuterJoinWithPredicates(): Unit = {
    val util = streamTestUtil()
    val table = util.addTableSource[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
    val function = new TableFunc1
    util.addTemporarySystemFunction("func1", function)

    expectExceptionThrown(
      {
        val result = table
          .leftOuterJoinLateral(function('c).as('s), 'c === 's)
          .select('c, 's)
          .where('a > 10)
        util.verifyExecPlan(result)
      },
      null)
  }

  @Test
  def testInvalidMapFunctionTypeAggregation(): Unit = {
    val util = streamTestUtil()
    expectExceptionThrown(
      util
        .addTableSource[(Int)]("MyTable", 'int)
        // do not support AggregateFunction as input
        .flatMap('int.sum),
      null)
  }

  @Test
  def testInvalidMapFunctionTypeUDAGG(): Unit = {
    val util = streamTestUtil()
    val weightedAvg = new WeightedAvg

    expectExceptionThrown(
      util
        .addTableSource[(Int)]("MyTable", 'int)
        // do not support AggregateFunction as input
        .flatMap(weightedAvg('int, 'int)),
      null)
  }

  @Test
  def testInvalidMapFunctionTypeUDAGG2(): Unit = {
    val util = streamTestUtil()

    util.addTemporarySystemFunction("weightedAvg", new WeightedAvg)

    expectExceptionThrown(
      util
        .addTableSource[(Int)]("MyTable", 'int)
        // do not support AggregateFunction as input
        .flatMap(call("weightedAvg", $"int", $"int")),
      null)
  }

  @Test
  def testInvalidMapFunctionTypeScalarFunction(): Unit = {
    val util = streamTestUtil()

    expectExceptionThrown(
      util
        .addTableSource[(String)]("MyTable", 'string)
        // do not support ScalarFunction as input
        .flatMap(Func15('string)),
      null)
  }

  @Test
  def testInvalidFlatMapFunctionTypeFieldReference(): Unit = {
    val util = batchTestUtil()

    expectExceptionThrown(
      util
        .addTableSource[(String)]("MyTable", 'string)
        // Only TableFunction can be used in flatMap
        .flatMap('string),
      null)
  }

  // ----------------------------------------------------------------------------------------------

  private def expectExceptionThrown(
      function: => Unit,
      keywords: String,
      clazz: Class[_ <: Throwable] = classOf[ValidationException]): Unit = {
    val callable: ThrowingCallable = () => function
    if (keywords != null) {
      assertThatExceptionOfType(clazz)
        .isThrownBy(callable)
        .withMessageContaining(keywords)
    } else {
      assertThatExceptionOfType(clazz)
        .isThrownBy(callable)
    }
  }
}
