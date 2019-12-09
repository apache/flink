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
package org.apache.flink.table.planner.plan.batch.sql.agg

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{TableException, Types}
import org.apache.flink.table.planner.plan.utils.JavaUserDefinedAggFunctions.{VarSum1AggFunction, VarSum2AggFunction}
import org.apache.flink.table.planner.utils.{BatchTableTestUtil, TableTestBase}
import org.apache.flink.table.runtime.typeutils.DecimalTypeInfo

import org.junit.Test

abstract class AggregateTestBase extends TableTestBase {

  protected val util: BatchTableTestUtil = batchTestUtil()
  util.addTableSource("MyTable",
    Array[TypeInformation[_]](
      Types.BYTE, Types.SHORT, Types.INT, Types.LONG, Types.FLOAT, Types.DOUBLE, Types.BOOLEAN,
      Types.STRING, Types.LOCAL_DATE, Types.LOCAL_TIME, Types.LOCAL_DATE_TIME,
      DecimalTypeInfo.of(30, 20), DecimalTypeInfo.of(10, 5)),
    Array("byte", "short", "int", "long", "float", "double", "boolean",
      "string", "date", "time", "timestamp", "decimal3020", "decimal105"))
  util.addTableSource[(Int, Long, String)]("MyTable1", 'a, 'b, 'c)

  @Test
  def testAvg(): Unit = {
    util.verifyPlanWithType(
      """
        |SELECT AVG(`byte`),
        |       AVG(`short`),
        |       AVG(`int`),
        |       AVG(`long`),
        |       AVG(`float`),
        |       AVG(`double`),
        |       AVG(`decimal3020`),
        |       AVG(`decimal105`)
        |FROM MyTable
      """.stripMargin)
  }

  @Test
  def testSum(): Unit = {
    util.verifyPlanWithType(
      """
        |SELECT SUM(`byte`),
        |       SUM(`short`),
        |       SUM(`int`),
        |       SUM(`long`),
        |       SUM(`float`),
        |       SUM(`double`),
        |       SUM(`decimal3020`),
        |       SUM(`decimal105`)
        |FROM MyTable
      """.stripMargin)
  }

  @Test
  def testCount(): Unit = {
    util.verifyPlanWithType(
      """
        |SELECT COUNT(`byte`),
        |       COUNT(`short`),
        |       COUNT(`int`),
        |       COUNT(`long`),
        |       COUNT(`float`),
        |       COUNT(`double`),
        |       COUNT(`decimal3020`),
        |       COUNT(`decimal105`),
        |       COUNT(`boolean`),
        |       COUNT(`date`),
        |       COUNT(`time`),
        |       COUNT(`timestamp`),
        |       COUNT(`string`)
        |FROM MyTable
      """.stripMargin)
  }

  @Test
  def testCountStart(): Unit = {
    util.verifyPlanWithType("SELECT COUNT(*) FROM MyTable")
  }


  @Test
  def testCannotCountOnMultiFields(): Unit = {
    val sql = "SELECT b, COUNT(a, c) FROM MyTable1 GROUP BY b"
    thrown.expect(classOf[TableException])
    thrown.expectMessage("We now only support the count of one field")
    util.verifyPlan(sql)
  }

  @Test
  def testMinWithFixLengthType(): Unit = {
    util.verifyPlanWithType(
      """
        |SELECT MIN(`byte`),
        |       MIN(`short`),
        |       MIN(`int`),
        |       MIN(`long`),
        |       MIN(`float`),
        |       MIN(`double`),
        |       MIN(`decimal3020`),
        |       MIN(`decimal105`),
        |       MIN(`boolean`),
        |       MIN(`date`),
        |       MIN(`time`),
        |       MIN(`timestamp`)
        |FROM MyTable
      """.stripMargin)
  }

  @Test
  def testMinWithVariableLengthType(): Unit = {
    util.verifyPlanWithType("SELECT MIN(`string`) FROM MyTable")
  }

  @Test
  def testMaxWithFixLengthType(): Unit = {
    util.verifyPlanWithType(
      """
        |SELECT MAX(`byte`),
        |       MAX(`short`),
        |       MAX(`int`),
        |       MAX(`long`),
        |       MAX(`float`),
        |       MAX(`double`),
        |       MAX(`decimal3020`),
        |       MAX(`decimal105`),
        |       MAX(`boolean`),
        |       MAX(`date`),
        |       MAX(`time`),
        |       MAX(`timestamp`)
        |FROM MyTable
      """.stripMargin)
  }

  @Test
  def testMaxWithVariableLengthType(): Unit = {
    util.verifyPlanWithType("SELECT MAX(`string`) FROM MyTable")
  }

  @Test
  def testAggregateWithoutFunction(): Unit = {
    util.verifyPlan("SELECT a, b FROM MyTable1 GROUP BY a, b")
  }

  @Test
  def testAggregateWithoutGroupBy(): Unit = {
    util.verifyPlan("SELECT AVG(a), SUM(b), COUNT(c) FROM MyTable1")
  }

  @Test
  def testAggregateWithFilter(): Unit = {
    util.verifyPlan("SELECT AVG(a), SUM(b), COUNT(c) FROM MyTable1 WHERE a = 1")
  }

  @Test
  def testAggregateWithFilterOnNestedFields(): Unit = {
    util.addTableSource[(Int, Long, (Int, Long))]("MyTable2", 'a, 'b, 'c)
    util.verifyPlan("SELECT AVG(a), SUM(b), COUNT(c), SUM(c._1) FROM MyTable2 WHERE a = 1")
  }

  @Test
  def testGroupAggregate(): Unit = {
    util.verifyPlan("SELECT a, SUM(b), COUNT(c) FROM MyTable1 GROUP BY a")
  }

  @Test
  def testGroupAggregateWithFilter(): Unit = {
    util.verifyPlan("SELECT a, SUM(b), count(c) FROM MyTable1 WHERE a = 1 GROUP BY a")
  }

  @Test
  def testAggNotSupportMerge(): Unit = {
    util.addFunction("var_sum", new VarSum2AggFunction)
    util.verifyPlan("SELECT b, var_sum(a) FROM MyTable1 GROUP BY b")
  }

  @Test
  def testPojoAccumulator(): Unit = {
    util.addFunction("var_sum", new VarSum1AggFunction)
    util.verifyPlan("SELECT b, var_sum(a) FROM MyTable1 GROUP BY b")
  }

  // TODO supports group sets
}
