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

package org.apache.flink.table.api.stream.table.validation

import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.utils.{CountMinMax, TableFunc0, TableTestBase}

import org.junit.Test

class AggregateValidationTest extends TableTestBase {

  @Test(expected = classOf[ValidationException])
  def testGroupingOnNonExistentField(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('a, 'b, 'c)

    val ds = table
      // must fail. '_foo is not a valid field
      .groupBy('_foo)
      .select('a.avg)
  }

  @Test(expected = classOf[ValidationException])
  def testGroupingInvalidSelection(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('a, 'b, 'c)

    table
      .groupBy('a, 'b)
      // must fail. 'c is not a grouping key or aggregation
      .select('c)
  }

  @Test(expected = classOf[ValidationException])
  def testInvalidAggregationInSelection(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('a, 'b, 'c)

    table
      .groupBy('a)
      .aggregate('b.sum as 'd)
      // must fail. Cannot use AggregateFunction in select right after aggregate
      .select('d.sum)
  }

  @Test(expected = classOf[ValidationException])
  def testInvalidWindowPropertiesInSelection(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('a, 'b, 'c)

    table
      .groupBy('a)
      .aggregate('b.sum as 'd)
      // must fail. Cannot use window properties in select right after aggregate
      .select('d.start)
  }

  @Test(expected = classOf[UnsupportedOperationException])
  def testTableFunctionInSelection(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('a, 'b, 'c)

    util.tableEnv.registerFunction("func", new TableFunc0)
    val resultTable = table
      .groupBy('a)
      .aggregate('b.sum as 'd)
      // must fail. Cannot use TableFunction in select after aggregate
      .select("func('abc')")

    util.verifyTable(resultTable, "")
  }

  @Test(expected = classOf[ValidationException])
  def testInvalidScalarFunctionInAggregate(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('a, 'b, 'c)

    table
      .groupBy('a)
      // must fail. Only AggregateFunction can be used in aggregate
      .aggregate('c.upperCase as 'd)
      .select('a, 'd)
  }

  @Test(expected = classOf[ValidationException])
  def testInvalidTableFunctionInAggregate(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('a, 'b, 'c)

    util.tableEnv.registerFunction("func", new TableFunc0)
    table
      .groupBy('a)
      // must fail. Only AggregateFunction can be used in aggregate
      .aggregate(call("func", $"c") as "d")
      .select('a, 'd)
  }

  @Test(expected = classOf[ExpressionParserException])
  def testMultipleAggregateExpressionInAggregate(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('a, 'b, 'c)

    util.tableEnv.registerFunction("func", new TableFunc0)
    table
      .groupBy('a)
      // must fail. Only one AggregateFunction can be used in aggregate
      .aggregate("sum(c), count(b)")
  }

  @Test
  def testInvalidAlias(): Unit = {
    expectedException.expect(classOf[ValidationException])
    expectedException.expectMessage("List of column aliases must have same degree as " +
      "table; the returned table of function 'minMax(b)' has 3 columns, " +
      "whereas alias list has 2 columns")

    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('a, 'b, 'c)
    val minMax = new CountMinMax

    util.tableEnv.registerFunction("minMax", minMax)
    table
      .groupBy('a)
      // must fail. Invalid alias length
      .aggregate(call("minMax", $"b") as ("x", "y"))
      .select("x, y")
  }
}
