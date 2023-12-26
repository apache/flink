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
import org.apache.flink.table.planner.utils.{EmptyTableAggFunc, TableTestBase}

import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test

import java.sql.Timestamp

class TableAggregateValidationTest extends TableTestBase {

  @Test
  def testInvalidParameterNumber(): Unit = {
    val util = streamTestUtil()
    val table = util.addTableSource[(Long, Int, String)]('a, 'b, 'c)

    val func = new EmptyTableAggFunc

    assertThatThrownBy(
      () =>
        table
          .groupBy('c)
          // must fail. func does not take 3 parameters
          .flatAggregate(call(func, 'a, 'b, 'c))
          .select('_1, '_2, '_3))
      .hasMessageContaining("Invalid function call:\nEmptyTableAggFunc(BIGINT, INT, STRING)")
      .isInstanceOf[ValidationException]
  }

  @Test
  def testInvalidParameterType(): Unit = {
    val util = streamTestUtil()
    val table = util.addTableSource[(Long, Int, String)]('a, 'b, 'c)

    val func = new EmptyTableAggFunc

    assertThatThrownBy(
      () =>
        table
          .groupBy('c)
          // must fail. func take 2 parameters of type Long and Int
          .flatAggregate(call(func, 'a, 'c))
          .select('_1, '_2, '_3))
      .hasMessageContaining("Invalid function call:\nEmptyTableAggFunc(BIGINT, STRING)")
      .isInstanceOf[ValidationException]
  }

  @Test
  def testInvalidWithWindowProperties(): Unit = {
    val util = streamTestUtil()
    val table = util.addTableSource[(Long, Int, Timestamp)]('a, 'b, 'c)

    val func = new EmptyTableAggFunc

    assertThatThrownBy(
      () =>
        table
          .groupBy('b)
          .flatAggregate(call(func, 'a, 'b).as('x, 'y))
          .select('x.start, 'y))
      .hasMessageContaining("Window properties can only be used on windowed tables.")
      .isInstanceOf[ValidationException]
  }

  @Test
  def testInvalidWithAggregation(): Unit = {
    val util = streamTestUtil()
    val table = util.addTableSource[(Long, Int, Timestamp)]('a, 'b, 'c)

    val func = new EmptyTableAggFunc

    assertThatThrownBy(
      () =>
        table
          .groupBy('b)
          .flatAggregate(call(func, 'a, 'b).as('x, 'y))
          .select('x.count))
      .hasMessageContaining("Aggregate functions are not supported in the " +
        "select right after the aggregate or flatAggregate operation.")
      .isInstanceOf[ValidationException]
  }

  @Test
  def testInvalidParameterWithAgg(): Unit = {
    val util = streamTestUtil()
    val table = util.addTableSource[(Long, Int, Timestamp)]('a, 'b, 'c)

    val func = new EmptyTableAggFunc

    assertThatThrownBy(
      () =>
        table
          .groupBy('b)
          // must fail. func take agg function as input
          .flatAggregate(func('a.sum, 'c))
          .select('_1, '_2, '_3))
      .hasMessageContaining(
        "It's not allowed to use an aggregate function as input of another aggregate function")
      .isInstanceOf[ValidationException]
  }

  @Test
  def testInvalidAliasWithWrongNumber(): Unit = {
    val util = streamTestUtil()
    val table = util.addTableSource[(Long, Int, Timestamp)]('a, 'b, 'c)

    val func = new EmptyTableAggFunc

    assertThatThrownBy(
      () =>
        table
          .groupBy('b)
          // must fail. alias with wrong number of fields
          .flatAggregate(call(func, 'a, 'b).as('a, 'b, 'c))
          .select('*))
      .hasMessageContaining(
        "List of column aliases must have same degree as " +
          "table; the returned table of function 'EmptyTableAggFunc' has 2 columns, " +
          "whereas alias list has 3 columns")
      .isInstanceOf[ValidationException]
  }

  @Test
  def testAliasWithNameConflict(): Unit = {
    val util = streamTestUtil()
    val table = util.addTableSource[(Long, Int, Timestamp)]('a, 'b, 'c)

    val func = new EmptyTableAggFunc

    assertThatThrownBy(
      () =>
        table
          .groupBy('b)
          // must fail. alias with name conflict
          .flatAggregate(call(func, 'a, 'b).as('a, 'b))
          .select('*))
      .hasMessageContaining("Ambiguous column name: b")
      .isInstanceOf[ValidationException]
  }
}
