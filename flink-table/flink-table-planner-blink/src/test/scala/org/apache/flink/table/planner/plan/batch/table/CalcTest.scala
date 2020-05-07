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

package org.apache.flink.table.planner.plan.batch.table

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.ScalarFunction
import org.apache.flink.table.planner.plan.batch.table.CalcTest.{MyHashCode, TestCaseClass, WC, giveMeCaseClass}
import org.apache.flink.table.planner.utils.TableTestBase

import org.junit.Test

class CalcTest extends TableTestBase {

  @Test
  def testMultipleFlatteningsTable(): Unit = {
    val util = batchTestUtil()
    val table = util.addTableSource[((Int, Long), (String, Boolean), String)]("MyTable", 'a, 'b, 'c)

    val result = table.select('a.flatten(), 'c, 'b.flatten())

    util.verifyPlan(result)
  }

  @Test
  def testNestedFlattening(): Unit = {
    val util = batchTestUtil()
    val table = util
      .addTableSource[((((String, TestCaseClass), Boolean), String), String)]("MyTable", 'a, 'b)

    val result = table.select('a.flatten(), 'b.flatten())

    util.verifyPlan(result)
  }

  @Test
  def testScalarFunctionAccess(): Unit = {
    val util = batchTestUtil()
    val table = util
      .addTableSource[(String, Int)]("MyTable", 'a, 'b)

    val result = table.select(
      giveMeCaseClass().get("my"),
      giveMeCaseClass().get("clazz"),
      giveMeCaseClass().flatten())

    util.verifyPlan(result)
  }

  // ----------------------------------------------------------------------------------------------
  // Tests for all the situations when we can do fields projection. Like selecting few fields
  // from a large field count source.
  // ----------------------------------------------------------------------------------------------

  @Test
  def testSimpleSelect(): Unit = {
    val util = batchTestUtil()
    val sourceTable = util.addTableSource[(Int, Long, String, Double)]("MyTable", 'a, 'b, 'c, 'd)
    val resultTable = sourceTable.select('a, 'b)

    util.verifyPlan(resultTable)
  }

  @Test
  def testSelectAllFields(): Unit = {
    val util = batchTestUtil()
    val sourceTable = util.addTableSource[(Int, Long, String, Double)]("MyTable", 'a, 'b, 'c, 'd)
    val resultTable1 = sourceTable.select('*)
    val resultTable2 = sourceTable.select('a, 'b, 'c, 'd)

    verifyTableEquals(resultTable1, resultTable2)
  }

  @Test
  def testSelectAggregation(): Unit = {
    val util = batchTestUtil()
    val sourceTable = util.addTableSource[(Int, Long, String, Double)]("MyTable", 'a, 'b, 'c, 'd)
    val resultTable = sourceTable.select('a.sum, 'b.max)

    util.verifyPlan(resultTable)
  }

  @Test
  def testSelectFunction(): Unit = {
    val util = batchTestUtil()
    val sourceTable = util.addTableSource[(Int, Long, String, Double)]("MyTable", 'a, 'b, 'c, 'd)

    util.tableEnv.registerFunction("hashCode", MyHashCode)

    val resultTable = sourceTable.select(call("hashCode", $"c"), $"b")

    util.verifyPlan(resultTable)
  }

  @Test
  def testSelectFromGroupedTable(): Unit = {
    val util = batchTestUtil()
    val sourceTable = util.addTableSource[(Int, Long, String, Double)]("MyTable", 'a, 'b, 'c, 'd)
    val resultTable = sourceTable.groupBy('a, 'c).select('a)

    util.verifyPlan(resultTable)
  }

  @Test
  def testSelectAllFieldsFromGroupedTable(): Unit = {
    val util = batchTestUtil()
    val sourceTable = util.addTableSource[(Int, Long, String, Double)]("MyTable", 'a, 'b, 'c, 'd)
    val resultTable = sourceTable.groupBy('a, 'c).select('a, 'c)

    util.verifyPlan(resultTable)
  }

  @Test
  def testSelectAggregationFromGroupedTable(): Unit = {
    val util = batchTestUtil()
    val sourceTable = util.addTableSource[(Int, Long, String, Double)]("MyTable", 'a, 'b, 'c, 'd)
    val resultTable = sourceTable.groupBy('c).select('a.sum)

    util.verifyPlan(resultTable)
  }

  @Test
  def testSelectFromGroupedTableWithNonTrivialKey(): Unit = {
    val util = batchTestUtil()
    val sourceTable = util.addTableSource[(Int, Long, String, Double)]("MyTable", 'a, 'b, 'c, 'd)
    val resultTable = sourceTable.groupBy('c.upperCase() as 'k).select('a.sum)

    util.verifyPlan(resultTable)
  }

  @Test
  def testSelectFromGroupedTableWithFunctionKey(): Unit = {
    val util = batchTestUtil()
    val sourceTable = util.addTableSource[(Int, Long, String, Double)]("MyTable", 'a, 'b, 'c, 'd)
    val resultTable = sourceTable.groupBy(MyHashCode('c) as 'k).select('a.sum)

    util.verifyPlan(resultTable)
  }

  @Test
  def testSelectFromAggregatedPojoTable(): Unit = {
    val util = batchTestUtil()
    val sourceTable = util.addTableSource[WC]("MyTable", 'word, 'frequency)
    val resultTable = sourceTable
      .groupBy('word)
      .select('word, 'frequency.sum as 'frequency)
      .filter('frequency === 2)

    util.verifyPlan(resultTable)
  }

  @Test
  def testMultiFilter(): Unit = {
    val util = batchTestUtil()
    val sourceTable = util.addTableSource[(Int, Long, String, Double)]("MyTable", 'a, 'b, 'c, 'd)
    val resultTable = sourceTable.select('a, 'b)
      .filter('a > 0)
      .filter('b < 2)
      .filter(('a % 2) === 1)

    util.verifyPlan(resultTable)
  }
}

object CalcTest {

  case class TestCaseClass(my: String, clazz: Int)

  @SerialVersionUID(1L)
  object giveMeCaseClass extends ScalarFunction {
    def eval(): TestCaseClass = {
      TestCaseClass("hello", 42)
    }

    override def getResultType(argTypes: Array[Class[_]]): TypeInformation[TestCaseClass] = {
      createTypeInformation[TestCaseClass]
    }
  }

  @SerialVersionUID(1L)
  object MyHashCode extends ScalarFunction {
    def eval(s: String): Int = s.hashCode()
  }

  case class WC(word: String, frequency: Long)
}
