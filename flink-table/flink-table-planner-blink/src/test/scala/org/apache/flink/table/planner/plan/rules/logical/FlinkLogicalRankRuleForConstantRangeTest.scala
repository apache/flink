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

package org.apache.flink.table.planner.plan.rules.logical

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.planner.plan.optimize.program.FlinkBatchProgram
import org.apache.flink.table.planner.utils.TableTestBase

import org.junit.Test

/**
  * Test for [[FlinkLogicalRankRuleForConstantRange]].
  */
class FlinkLogicalRankRuleForConstantRangeTest extends TableTestBase {

  private val util = batchTestUtil()
  util.addTableSource[(Int, String, Long)]("MyTable", 'a, 'b, 'c)
  util.buildBatchProgram(FlinkBatchProgram.PHYSICAL)

  @Test
  def testRowNumberFunc(): Unit = {
    // can not be converted to Rank
    val sqlQuery =
      """
        |SELECT * FROM (
        | SELECT a, b, ROW_NUMBER() OVER (PARTITION BY b ORDER BY a) rn FROM MyTable) t
        |WHERE rn <= 2
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testWithoutFilter(): Unit = {
    // can not be converted to Rank
    util.verifyPlan("SELECT a, b, RANK() OVER (PARTITION BY b ORDER BY a) rk FROM MyTable")
  }

  @Test
  def testRankValueFilterWithUpperValue(): Unit = {
    val sqlQuery =
      """
        |SELECT * FROM (
        | SELECT a, b, RANK() OVER (PARTITION BY b ORDER BY a) rk FROM MyTable) t
        |WHERE rk <= 2 AND a > 10
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testRankValueFilterWithRange(): Unit = {
    val sqlQuery =
      """
        |SELECT * FROM (
        | SELECT a, b, RANK() OVER (PARTITION BY b, c ORDER BY a) rk FROM MyTable) t
        |WHERE rk <= 2 AND rk > -2
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testRankValueFilterWithLowerValue(): Unit = {
    // can not be converted to Rank
    val sqlQuery =
      """
        |SELECT * FROM (
        | SELECT a, b, RANK() OVER (PARTITION BY b ORDER BY a, c) rk FROM MyTable) t
        |WHERE rk > 2
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testRankValueFilterWithEquals(): Unit = {
    val sqlQuery =
      """
        |SELECT * FROM (
        | SELECT a, b, RANK() OVER (PARTITION BY b ORDER BY a, c) rk FROM MyTable) t
        |WHERE rk = 2
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testRankValueFilterWithVariableField1(): Unit = {
    // can not be converted to Rank
    val sqlQuery =
      """
        |SELECT * FROM (
        | SELECT a, b, RANK() OVER (PARTITION BY b ORDER BY c) rk FROM MyTable) t
        |WHERE rk < a
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testRankValueFilterWithVariableField2(): Unit = {
    // can not be converted to Rank
    val sqlQuery =
      """
        |SELECT * FROM (
        | SELECT a, b, RANK() OVER (PARTITION BY b ORDER BY c) rk FROM MyTable) t
        |WHERE rk > a
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testRankValueFilterWithVariableField3(): Unit = {
    // can not be converted to Rank
    val sqlQuery =
      """
        |SELECT * FROM (
        | SELECT a, b, RANK() OVER (PARTITION BY b ORDER BY c) rk FROM MyTable) t
        |WHERE rk < a and b > 5
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testRankValueFilterWithVariableField4(): Unit = {
    // can not be converted to Rank
    val sqlQuery =
      """
        |SELECT * FROM (
        | SELECT a, b, RANK() OVER (PARTITION BY a ORDER BY c) rk FROM MyTable) t
        |WHERE rk = b
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testWithoutPartitionBy(): Unit = {
    val sqlQuery =
      """
        |SELECT * FROM (
        | SELECT a, b, RANK() OVER (ORDER BY a) rk FROM MyTable) t
        |WHERE rk < 10
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testMultiSameRankFunctionsWithSameGroup(): Unit = {
    val sqlQuery =
      """
        |SELECT * FROM (
        | SELECT a, b,
        |        RANK() OVER (PARTITION BY b ORDER BY a) rk1,
        |        RANK() OVER (PARTITION BY b ORDER BY a) rk2 FROM MyTable) t
        |WHERE rk1 < 10
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testMultiSameRankFunctionsWithDiffGroup(): Unit = {
    // can not be converted to Rank
    val sqlQuery =
      """
        |SELECT * FROM (
        | SELECT a, b,
        |        RANK() OVER (PARTITION BY b ORDER BY a) rk1,
        |        RANK() OVER (PARTITION BY c ORDER BY a) rk2 FROM MyTable) t
        |WHERE rk1 < 10
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testMultiDiffRankFunctions(): Unit = {
    // can not be converted to Rank
    val sqlQuery =
      """
        |SELECT * FROM (
        | SELECT a, b,
        |        RANK() OVER (PARTITION BY b ORDER BY a) rk,
        |        ROW_NUMBER() OVER (PARTITION BY b ORDER BY a) rn FROM MyTable) t
        |WHERE rk < 10
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testDuplicateRankFunctionColumnName(): Unit = {
    util.addTableSource[(Int, Long, String)]("MyTable2", 'a, 'b, 'rk)
    val sqlQuery =
      """
        |SELECT * FROM (
        | SELECT a, b, RANK() OVER (PARTITION BY b ORDER BY a) rk FROM MyTable2) t
        |WHERE rk < 10
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }
}
