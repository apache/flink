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
import org.apache.flink.table.planner.utils.{TableFunc0, TableTestBase}
import org.apache.flink.types.Row

import org.assertj.core.api.Assertions.assertThatExceptionOfType
import org.assertj.core.api.ThrowableAssert.ThrowingCallable
import org.junit.jupiter.api.Test

class AggregateValidationTest extends TableTestBase {
  private val util = scalaStreamTestUtil()

  @Test
  def testGroupingOnNonExistentField(): Unit = {
    val table = util.addTableSource[(Long, Int, String)]('a, 'b, 'c)

    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(
        () =>
          table
            // must fail. '_foo is not a valid field
            .groupBy('foo)
            .select('a.avg))
  }

  @Test
  def testGroupingInvalidSelection(): Unit = {
    val table = util.addTableSource[(Long, Int, String)]('a, 'b, 'c)

    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(
        () =>
          table
            .groupBy('a, 'b)
            // must fail. 'c is not a grouping key or aggregation
            .select('c))
  }

  @Test
  def testInvalidAggregationInSelection(): Unit = {
    val table = util.addTableSource[(Long, Int, String)]('a, 'b, 'c)

    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(
        () =>
          table
            .groupBy('a)
            .aggregate('b.sum.as('d))
            // must fail. Cannot use AggregateFunction in select right after aggregate
            .select('d.sum))
  }

  @Test
  def testInvalidWindowPropertiesInSelection(): Unit = {
    val table = util.addTableSource[(Long, Int, String)]('a, 'b, 'c)

    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(
        () =>
          table
            .groupBy('a)
            .aggregate('b.sum.as('d))
            // must fail. Cannot use window properties in select right after aggregate
            .select('d.start))
  }

  @Test
  def testTableFunctionInSelection(): Unit = {
    val table = util.addTableSource[(Long, Int, String)]('a, 'b, 'c)

    util.addTemporarySystemFunction("func", new TableFunc0)
    val resultTable = table
      .groupBy('a)
      .aggregate('b.sum.as('d))
      .select(call("func", "abc"))

    util.verifyExecPlan(resultTable)
  }

  @Test
  def testInvalidScalarFunctionInAggregate(): Unit = {
    val table = util.addTableSource[(Long, Int, String)]('a, 'b, 'c)

    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(
        () =>
          table
            .groupBy('a)
            // must fail. Only AggregateFunction can be used in aggregate
            .aggregate('c.upperCase.as('d))
            .select('a, 'd))
  }

  @Test
  def testInvalidTableFunctionInAggregate(): Unit = {
    val table = util.addTableSource[(Long, Int, String)]('a, 'b, 'c)

    util.addTemporarySystemFunction("func", new TableFunc0)
    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(
        () =>
          table
            .groupBy('a)
            // must fail. Only AggregateFunction can be used in aggregate
            .aggregate(call("func", $"c").as("d"))
            .select('a, 'd))
  }

  @Test
  def testIllegalArgumentForListAgg(): Unit = {
    util.addTableSource[(Long, Int, String, String)]("T", 'a, 'b, 'c, 'd)
    // If there are two parameters, second one must be character literal.
    expectExceptionThrown(
      "SELECT listagg(c, d) FROM T GROUP BY a",
      "Supported form(s): 'LISTAGG(<CHARACTER>)'\n'LISTAGG(<CHARACTER>, <CHARACTER_LITERAL>)",
      classOf[ValidationException]
    )
  }

  @Test
  def testIllegalArgumentForListAgg1(): Unit = {
    util.addTableSource[(Long, Int, String, String)]("T", 'a, 'b, 'c, 'd)
    // If there are two parameters, second one must be character literal.
    expectExceptionThrown(
      "SELECT LISTAGG(c, 1) FROM T GROUP BY a",
      "Supported form(s): 'LISTAGG(<CHARACTER>)'\n'LISTAGG(<CHARACTER>, <CHARACTER_LITERAL>)",
      classOf[ValidationException]
    )
  }

  // ----------------------------------------------------------------------------------------------

  private def expectExceptionThrown(
      sql: String,
      keywords: String,
      clazz: Class[_ <: Throwable] = classOf[ValidationException]): Unit = {
    val callable: ThrowingCallable = () => util.tableEnv.toDataStream(util.tableEnv.sqlQuery(sql))
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
