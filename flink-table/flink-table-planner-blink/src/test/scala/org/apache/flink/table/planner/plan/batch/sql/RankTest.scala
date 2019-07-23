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
package org.apache.flink.table.planner.plan.batch.sql

import org.apache.flink.api.scala._
import org.apache.flink.table.api.ValidationException
import org.apache.flink.table.api.scala._
import org.apache.flink.table.planner.utils.TableTestBase

import org.junit.Test

class RankTest extends TableTestBase {

  private val util = batchTestUtil()
  util.addTableSource[(Int, String, Long)]("MyTable", 'a, 'b, 'c)

  @Test(expected = classOf[RuntimeException])
  def testRowNumberWithoutOrderBy(): Unit = {
    val sqlQuery =
      """
        |SELECT ROW_NUMBER() over (partition by a) FROM MyTable
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }

  @Test(expected = classOf[RuntimeException])
  def testRowNumberWithMultiGroups(): Unit = {
    val sqlQuery =
      """
        |SELECT ROW_NUMBER() over (partition by a order by b) as a,
        |       ROW_NUMBER() over (partition by b) as b
        |       FROM MyTable
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }

  @Test(expected = classOf[ValidationException])
  def testRankWithoutOrderBy(): Unit = {
    val sqlQuery =
      """
        |SELECT RANK() over (partition by a) FROM MyTable
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }

  @Test(expected = classOf[ValidationException])
  def testRankWithMultiGroups(): Unit = {
    val sqlQuery =
      """
        |SELECT RANK() over (partition by a order by b) as a,
        |       RANK() over (partition by b) as b
        |       FROM MyTable
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }

  @Test(expected = classOf[ValidationException])
  def testDenseRankWithoutOrderBy(): Unit = {
    val sqlQuery =
      """
        |SELECT dense_rank() over (partition by a) FROM MyTable
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }

  @Test(expected = classOf[ValidationException])
  def testDenseRankWithMultiGroups(): Unit = {
    val sqlQuery =
      """
        |SELECT DENSE_RANK() over (partition by a order by b) as a,
        |       DENSE_RANK() over (partition by b) as b
        |       FROM MyTable
      """.stripMargin
    util.verifyPlan(sqlQuery)
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

  @Test
  def testRankFunctionInMiddle(): Unit = {
    val sqlQuery =
      """
        |SELECT * FROM (
        | SELECT a, RANK() OVER (PARTITION BY a ORDER BY a) rk, b, c FROM MyTable) t
        |WHERE rk < 10
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }
}
