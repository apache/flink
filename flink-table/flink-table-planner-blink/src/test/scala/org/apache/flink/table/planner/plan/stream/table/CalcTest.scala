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
import org.apache.flink.table.api._
import org.apache.flink.table.planner.expressions.utils.{Func1, Func23, Func24}
import org.apache.flink.table.planner.utils.TableTestBase

import org.junit.Test

class CalcTest extends TableTestBase {

  // ----------------------------------------------------------------------------------------------
  // Tests for all the situations when we can do fields projection. Like selecting few fields
  // from a large field count source.
  // ----------------------------------------------------------------------------------------------

  @Test
  def testSelectFromWindow(): Unit = {
    val util = streamTestUtil()
    val sourceTable =
      util.addDataStream[(Int, Long, String, Double)](
        "MyTable", 'a, 'b, 'c, 'd, 'rowtime.rowtime)
    val resultTable = sourceTable
        .window(Tumble over 5.millis on 'rowtime as 'w)
        .groupBy('w)
        .select('c.upperCase().count, 'a.sum)

    util.verifyExecPlan(resultTable)
  }

  @Test
  def testSelectFromGroupedWindow(): Unit = {
    val util = streamTestUtil()
    val sourceTable =
      util.addDataStream[(Int, Long, String, Double)](
        "MyTable", 'a, 'b, 'c, 'd, 'rowtime.rowtime)
    val resultTable = sourceTable
        .window(Tumble over 5.millis on 'rowtime as 'w)
        .groupBy('w, 'b)
        .select('c.upperCase().count, 'a.sum, 'b)

    util.verifyExecPlan(resultTable)
  }

  @Test
  def testMultiFilter(): Unit = {
    val util = streamTestUtil()
    val sourceTable = util.addTableSource[(Int, Long, String, Double)]("MyTable", 'a, 'b, 'c, 'd)
    val resultTable = sourceTable.select('a, 'b)
      .filter('a > 0)
      .filter('b < 2)
      .filter(('a % 2) === 1)

    util.verifyExecPlan(resultTable)
  }

  @Test
  def testIn(): Unit = {
    val util = streamTestUtil()
    val sourceTable = util.addTableSource[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
    val resultTable = sourceTable.select('a, 'b, 'c)
      .where((1 to 30).map($"b" === _).reduce((ex1, ex2) => ex1 || ex2) && ($"c" === "xx"))

    util.verifyExecPlan(resultTable)
  }

  @Test
  def testNotIn(): Unit = {
    val util = streamTestUtil()
    val sourceTable = util.addTableSource[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
    val resultTable = sourceTable.select('a, 'b, 'c)
      .where((1 to 30).map($"b" !== _).reduce((ex1, ex2) => ex1 && ex2) || ($"c" !== "xx"))

    util.verifyExecPlan(resultTable)
  }

  @Test
  def testAddColumns(): Unit = {
    val util = streamTestUtil()
    val sourceTable = util.addTableSource[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
    val resultTable = sourceTable
      .addColumns("concat(c, 'sunny') as kid")
      .addColumns('a + 2, 'b as 'b2)
      .addOrReplaceColumns(concat('c, "_kid") as 'kid, concat('c, "kid") as 'kid)
      .addOrReplaceColumns("concat(c, '_kid_last') as kid")
      .addColumns("'literal_value'")

    util.verifyExecPlan(resultTable)
  }

  @Test
  def testRenameColumns(): Unit = {
    val util = streamTestUtil()
    val sourceTable = util.addTableSource[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
    val resultTable = sourceTable.renameColumns('a as 'a2, 'b as 'b2).select('a2, 'b2)

    util.verifyExecPlan(resultTable)
  }

  @Test
  def testDropColumns(): Unit = {
    val util = streamTestUtil()
    val sourceTable = util.addTableSource[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
    val resultTable = sourceTable.dropColumns('a, 'b)

    util.verifyExecPlan(resultTable)
  }

  @Test
  def testSimpleMap(): Unit = {
    val util = streamTestUtil()

    val sourceTable = util.addTableSource[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
    val resultTable = sourceTable.map(Func23('a, 'b, 'c))

    util.verifyExecPlan(resultTable)
  }

  @Test
  def testScalarResult(): Unit = {
    val util = streamTestUtil()

    val sourceTable = util.addTableSource[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
    val resultTable = sourceTable.map(Func1('a))

    util.verifyExecPlan(resultTable)
  }

  @Test
  def testMultiMap(): Unit = {
    val util = streamTestUtil()

    val sourceTable = util.addTableSource[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
    val resultTable = sourceTable
      .map(Func23('a, 'b, 'c))
      .map(Func24('_c0, '_c1, '_c2, '_c3))

    util.verifyExecPlan(resultTable)
  }
}


