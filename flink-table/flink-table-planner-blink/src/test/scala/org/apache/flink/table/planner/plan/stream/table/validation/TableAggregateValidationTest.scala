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

import java.sql.Timestamp

import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.planner.utils.{EmptyTableAggFunc, TableTestBase}
import org.junit.Test

class TableAggregateValidationTest extends TableTestBase {

  @Test
  def testInvalidParameterNumber(): Unit = {
    expectedException.expect(classOf[ValidationException])
    expectedException.expectMessage(
      "Invalid function call:\nEmptyTableAggFunc(BIGINT, INT, STRING)")

    val util = streamTestUtil()
    val table = util.addTableSource[(Long, Int, String)]('a, 'b, 'c)

    val func = new EmptyTableAggFunc
    table
      .groupBy('c)
      // must fail. func does not take 3 parameters
      .flatAggregate(call(func, 'a, 'b, 'c))
      .select('_1, '_2, '_3)
  }

  @Test
  def testInvalidParameterType(): Unit = {
    expectedException.expect(classOf[ValidationException])
    expectedException.expectMessage(
      "Invalid function call:\nEmptyTableAggFunc(BIGINT, STRING)")

    val util = streamTestUtil()
    val table = util.addTableSource[(Long, Int, String)]('a, 'b, 'c)

    val func = new EmptyTableAggFunc
    table
      .groupBy('c)
      // must fail. func take 2 parameters of type Long and Timestamp or Long Int
      .flatAggregate(call(func, 'a, 'c))
      .select('_1, '_2, '_3)
  }

  @Test
  def testInvalidWithWindowProperties(): Unit = {
    expectedException.expect(classOf[ValidationException])
    expectedException.expectMessage("Window properties can only be used on windowed tables.")

    val util = streamTestUtil()
    val table = util.addTableSource[(Long, Int, Timestamp)]('a, 'b, 'c)

    val func = new EmptyTableAggFunc
    table
      .groupBy('b)
      .flatAggregate(call(func, 'a, 'b) as ('x, 'y))
      .select('x.start, 'y)
  }

  @Test
  def testInvalidWithAggregation(): Unit = {
    expectedException.expect(classOf[ValidationException])
    expectedException.expectMessage("Aggregate functions are not supported in the " +
      "select right after the aggregate or flatAggregate operation.")

    val util = streamTestUtil()
    val table = util.addTableSource[(Long, Int, Timestamp)]('a, 'b, 'c)

    val func = new EmptyTableAggFunc
    table
      .groupBy('b)
      .flatAggregate(call(func, 'a, 'b) as ('x, 'y))
      .select('x.count)
  }

  @Test
  def testInvalidParameterWithAgg(): Unit = {
    expectedException.expect(classOf[ValidationException])
    expectedException.expectMessage(
      "It's not allowed to use an aggregate function as input of another aggregate function")

    val util = streamTestUtil()
    val table = util.addTableSource[(Long, Int, Timestamp)]('a, 'b, 'c)

    val func = new EmptyTableAggFunc
    table
      .groupBy('b)
      // must fail. func take agg function as input
      .flatAggregate(func('a.sum, 'c))
      .select('_1, '_2, '_3)
  }

  @Test
  def testInvalidAliasWithWrongNumber(): Unit = {
    expectedException.expect(classOf[ValidationException])
    expectedException.expectMessage("List of column aliases must have same degree as " +
      "table; the returned table of function 'EmptyTableAggFunc' has 2 columns, " +
      "whereas alias list has 3 columns")

    val util = streamTestUtil()
    val table = util.addTableSource[(Long, Int, Timestamp)]('a, 'b, 'c)

    val func = new EmptyTableAggFunc
    table
      .groupBy('b)
      // must fail. alias with wrong number of fields
      .flatAggregate(call(func, 'a, 'b) as ('a, 'b, 'c))
      .select('*)
  }

  @Test
  def testAliasWithNameConflict(): Unit = {
    expectedException.expect(classOf[ValidationException])
    expectedException.expectMessage("Ambiguous column name: b")

    val util = streamTestUtil()
    val table = util.addTableSource[(Long, Int, Timestamp)]('a, 'b, 'c)

    val func = new EmptyTableAggFunc
    table
      .groupBy('b)
      // must fail. alias with name conflict
      .flatAggregate(call(func, 'a, 'b) as ('a, 'b))
      .select('*)
  }
}
