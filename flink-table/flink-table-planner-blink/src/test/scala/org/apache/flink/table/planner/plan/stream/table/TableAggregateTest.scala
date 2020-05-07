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
import org.apache.flink.table.planner.expressions.utils.Func0
import org.apache.flink.table.planner.utils.{EmptyTableAggFunc, EmptyTableAggFuncWithIntResultType, TableTestBase}
import org.junit.Test

class TableAggregateTest extends TableTestBase {

  val util = streamTestUtil()
  val table = util.addTableSource[(Long, Int, Long, Long)]('a, 'b, 'c, 'd.rowtime, 'e.proctime)
  val emptyFunc = new EmptyTableAggFunc

  @Test
  def testTableAggregateWithGroupBy(): Unit = {
    val resultTable = table
      .groupBy('b % 5 as 'bb)
      .flatAggregate(emptyFunc('a, 'b) as ('x, 'y))
      .select('bb, 'x + 1, 'y)

    util.verifyPlan(resultTable)
  }

  @Test
  def testTableAggregateWithoutGroupBy(): Unit = {
    val resultTable = table
      .flatAggregate(emptyFunc('a, 'b))
      .select(Func0('f0) as 'a, 'f1 as 'b)

    util.verifyPlan(resultTable)
  }

  @Test
  def testTableAggregateWithTimeIndicator(): Unit = {

    val resultTable = table
      .flatAggregate(emptyFunc('d, 'e))
      .select('f0 as 'a, 'f1 as 'b)

    util.verifyPlan(resultTable)
  }

  @Test
  def testTableAggregateWithSelectStar(): Unit = {

    val resultTable = table
      .flatAggregate(emptyFunc('b))
      .select($"*")

    util.verifyPlan(resultTable)
  }

  @Test
  def testTableAggregateWithAlias(): Unit = {

    val resultTable = table
      .flatAggregate(emptyFunc('b) as ('a, 'b))
      .select('a, 'b)

    util.verifyPlan(resultTable)
  }

  @Test
  def testTableAggregateWithIntResultType(): Unit = {

    val table = util.addTableSource[(Long, Int, Long, Long)]('f0, 'f1, 'f2, 'd.rowtime, 'e.proctime)
    val func = new EmptyTableAggFuncWithIntResultType

    val resultTable = table
      .groupBy('f0)
      .flatAggregate(func('f1))
      .select('f0, 'f0_0)

    util.verifyPlan(resultTable)
  }

  @Test
  def testJavaRegisterFunction(): Unit = {

    val util = javaStreamTestUtil()
    val table = util.addTableSource[(Int, Long, Long)]('a, 'b, 'c)

    val func = new EmptyTableAggFunc
    util.addFunction("func", func)

    val resultTable = table
      .groupBy($"c")
      .flatAggregate("func(a)")
      .select($"*")

    util.verifyPlan(resultTable)
  }
}
