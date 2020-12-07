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
import org.apache.flink.table.api._
import org.apache.flink.table.planner.plan.optimize.program._
import org.apache.flink.table.planner.utils.TableTestBase

import org.junit.{Before, Test}

/**
  * Test for [[WindowGroupReorderRule]].
  */
class WindowGroupReorderRuleTest extends TableTestBase {
  private val util = batchTestUtil()

  @Before
  def setup(): Unit = {
    util.buildBatchProgram(FlinkBatchProgram.LOGICAL)
    util.addTableSource[(Int, Int, String)]("MyTable", 'a, 'b, 'c)
  }

  @Test
  def testSamePartitionKeysWithSameOrderKeysPrefix(): Unit = {
    val sqlQuery =
      """
        |SELECT a,
        |    RANK() OVER (PARTITION BY b ORDER BY c, a DESC),
        |    RANK() OVER (PARTITION BY b ORDER BY c, b)
        |FROM MyTable
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testSamePartitionKeysWithDiffOrderKeys1(): Unit = {
    val sqlQuery =
      """
        |SELECT
        |    SUM(a) OVER (PARTITION BY b ORDER BY c),
        |    MAX(a) OVER (PARTITION BY b ORDER BY b),
        |    AVG(a) OVER (PARTITION BY b ORDER BY a),
        |    RANK() OVER (PARTITION BY b ORDER BY c),
        |    MIN(a) OVER (PARTITION BY b ORDER BY b)
        |FROM MyTable
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testSamePartitionKeysWithDiffOrderKeys2(): Unit = {
    val sqlQuery =
      """
        |SELECT
        |    SUM(a) OVER (PARTITION BY b ORDER BY c),
        |    MAX(a) OVER (PARTITION BY b ORDER BY a),
        |    AVG(a) OVER (PARTITION BY b ORDER BY a, c),
        |    RANK() OVER (PARTITION BY b ORDER BY a, b),
        |    MIN(a) OVER (PARTITION BY b ORDER BY b)
        |FROM MyTable
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testSamePartitionKeysWithSameOrderKeysDiffDirection1(): Unit = {
    val sqlQuery =
      """
        |SELECT
        |    SUM(a) OVER (PARTITION BY b ORDER BY a ASC),
        |    MAX(a) OVER (PARTITION BY b ORDER BY a ASC),
        |    AVG(a) OVER (PARTITION BY b ORDER BY a DESC),
        |    RANK() OVER (PARTITION BY b ORDER BY a ASC),
        |    MIN(a) OVER (PARTITION BY b ORDER BY a DESC)
        |FROM MyTable
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testSamePartitionKeysWithSameOrderKeysDiffDirection2(): Unit = {
    val sqlQuery =
      """
        |SELECT
        |    RANK() OVER (PARTITION BY b ORDER BY a DESC),
        |    RANK() OVER (PARTITION BY b ORDER BY a ASC)
        |FROM MyTable
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testSamePartitionKeysWithSameOrderKeysWithEmptyOrder(): Unit = {
    val sqlQuery =
      """
        |SELECT
        |    SUM(a) OVER (PARTITION BY b ORDER BY a),
        |    MIN(a) OVER (PARTITION BY b),
        |    MAX(a) OVER (PARTITION BY b ORDER BY a)
        |FROM MyTable
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testDiffPartitionKeysWithSameOrderKeys(): Unit = {
    val sqlQuery =
      """
        |SELECT
        |    SUM(a) OVER (PARTITION BY b ORDER BY a),
        |    MAX(a) OVER (PARTITION BY b ORDER BY a),
        |    AVG(a) OVER (PARTITION BY c ORDER BY a),
        |    RANK() OVER (PARTITION BY b ORDER BY a),
        |    MIN(a) OVER (PARTITION BY c ORDER BY a)
        |FROM MyTable
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testDiffPartitionKeysWithDiffOrderKeys1(): Unit = {
    val sqlQuery =
      """
        |SELECT
        |    SUM(a) OVER (PARTITION BY b ORDER BY a),
        |    MAX(a) OVER (PARTITION BY b ORDER BY c),
        |    AVG(a) OVER (PARTITION BY c ORDER BY a),
        |    RANK() OVER (PARTITION BY b ORDER BY a),
        |    MIN(a) OVER (PARTITION BY c ORDER BY a)
        |FROM MyTable
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testDiffPartitionKeysWithDiffOrderKeys2(): Unit = {
    val sqlQuery =
      """
        |SELECT
        |    SUM(a) OVER (PARTITION BY b ORDER BY c),
        |    MAX(a) OVER (PARTITION BY c ORDER BY a),
        |    MIN(a) OVER (ORDER BY c, a),
        |    RANK() OVER (PARTITION BY b ORDER BY c),
        |    AVG(a) OVER (ORDER BY b)
        |FROM MyTable
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testMultiOverWindowRangeType(): Unit = {
    val sqlQuery =
      """
        |SELECT
        |    COUNT(*) OVER (PARTITION BY c ORDER BY a RANGE BETWEEN -1 PRECEDING AND 10 FOLLOWING),
        |    SUM(a) OVER (PARTITION BY c ORDER BY a),
        |    RANK() OVER (PARTITION BY c ORDER BY a, c),
        |    SUM(a) OVER (PARTITION BY c ORDER BY a RANGE BETWEEN 1 PRECEDING AND 10 FOLLOWING),
        |    COUNT(*) OVER (PARTITION BY c ORDER BY c ROWS BETWEEN 1 PRECEDING AND 10 FOLLOWING)
        | FROM MyTable
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }
}
