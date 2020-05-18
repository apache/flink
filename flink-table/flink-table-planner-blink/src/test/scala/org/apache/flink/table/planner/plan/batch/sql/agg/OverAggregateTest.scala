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

import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedScalarFunctions.OverAgg0
import org.apache.flink.table.planner.utils.TableTestBase

import org.junit.Test

import java.sql.Timestamp


class OverAggregateTest extends TableTestBase {

  private val util = batchTestUtil()
  util.addTableSource[(Int, Long, String)]("MyTable", 'a, 'b, 'c)

  @Test
  def testOverWindowWithoutPartitionByOrderBy(): Unit = {
    util.verifyPlan("SELECT c, COUNT(*) OVER () FROM MyTable")
  }

  @Test
  def testOverWindowWithoutFrame(): Unit = {
    util.verifyPlan("SELECT c, COUNT(*) OVER (PARTITION BY c) FROM MyTable")
  }

  @Test
  def testOverWindowWithoutPartitionBy(): Unit = {
    util.verifyPlan("SELECT c, SUM(a) OVER (ORDER BY b) FROM MyTable")
  }

  @Test
  def testDiffPartitionKeysWithSameOrderKeys(): Unit = {
    val sqlQuery =
      """
        |SELECT
        |    SUM(a) OVER (PARTITION BY b ORDER BY a),
        |    MAX(a) OVER (PARTITION BY c ORDER BY a)
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
  def testSamePartitionKeysWithSameOrderKeys(): Unit = {
    val sqlQuery =
      """
        |SELECT
        |    SUM(a) OVER (PARTITION BY b ORDER BY a),
        |    MAX(a) OVER (PARTITION BY b ORDER BY a)
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
  def testSamePartitionKeysWithSameOrderKeysPrefix(): Unit = {
    // use order by c, b instead of order by c to avoid calcite reorder OVER
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
  def testOverWindow0PrecedingAnd0Following(): Unit = {
    val sqlQuery =
      """
        |SELECT c,
        |    COUNT(*) OVER (PARTITION BY c ORDER BY a ROWS BETWEEN 0 PRECEDING AND 0 FOLLOWING)
        |FROM MyTable
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testOverWindowCurrentRowAnd0Following(): Unit = {
    val sqlQuery =
      """
        |SELECT c,
        |    COUNT(*) OVER (PARTITION BY c ORDER BY a ROWS BETWEEN CURRENT ROW AND 0 FOLLOWING)
        |FROM MyTable
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testOverWindow0PrecedingAndCurrentRow(): Unit = {
    val sqlQuery =
      """
        |SELECT c,
        |    COUNT(*) OVER (PARTITION BY c ORDER BY a ROWS BETWEEN 0 PRECEDING AND CURRENT ROW)
        |FROM MyTable
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }

  @Test(expected = classOf[ValidationException])
  def testOverWindowRangeProhibitType(): Unit = {
    val sqlQuery =
      """
        |SELECT
        |    COUNT(*) OVER (PARTITION BY c ORDER BY c RANGE BETWEEN -1 PRECEDING AND 10 FOLLOWING)
        |FROM MyTable
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testOverWindowRangeType(): Unit = {
    val sqlQuery =
      """
        |SELECT
        |    COUNT(*) OVER (PARTITION BY c ORDER BY a RANGE BETWEEN -1 PRECEDING AND 10 FOLLOWING)
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

  @Test(expected = classOf[ValidationException])
  def testRowsWindowWithNegative(): Unit = {
    val sqlQuery =
      """
        |SELECT
        |    COUNT(*) OVER (PARTITION BY c ORDER BY a ROWS BETWEEN -1 PRECEDING AND 10 FOLLOWING)
        |FROM MyTable
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testRangeWindowWithNegative1(): Unit = {
    val sqlQuery =
      """
        |SELECT
        |    COUNT(*) OVER (PARTITION BY c ORDER BY a RANGE BETWEEN -1 PRECEDING AND 10 FOLLOWING)
        |FROM MyTable
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testRangeWindowWithNegative2(): Unit = {
    val sqlQuery =
      """
        |SELECT
        |    COUNT(*) OVER (PARTITION BY c ORDER BY a RANGE BETWEEN -1 FOLLOWING AND 10 FOLLOWING)
        |FROM MyTable
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testRankRangeWindowWithCompositeOrders(): Unit = {
    util.verifyPlan("SELECT RANK() OVER (PARTITION BY c ORDER BY a, c) FROM MyTable")
  }

  @Test
  def testRankRangeWindowWithConstants1(): Unit = {
    util.verifyPlan("SELECT COUNT(1) OVER () FROM MyTable")
  }

  @Test
  def testRankRangeWindowWithConstants2(): Unit = {
    val sqlQuery =
      """
        |SELECT
        |    SUM(a) OVER (PARTITION BY c ORDER BY a RANGE BETWEEN -1 FOLLOWING AND 10 FOLLOWING),
        |    COUNT(1) OVER (PARTITION BY c ORDER BY a RANGE BETWEEN -1 FOLLOWING and 10 FOLLOWING)
        |FROM MyTable
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }

  @Test(expected = classOf[RuntimeException])
  def testDistinct(): Unit = {
    val sqlQuery =
      """
        |SELECT SUM(DISTINCT a)
        |    OVER (PARTITION BY c ORDER BY a RANGE BETWEEN -1 FOLLOWING AND 10 FOLLOWING)
        |FROM MyTable
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }

  /**
    * OVER clause is necessary for [[OverAgg0]] window function.
    */
  @Test(expected = classOf[ValidationException])
  def testInvalidOverAggregation(): Unit = {
    util.addFunction("overAgg", new OverAgg0)
    util.verifyPlan("SELECT overAgg(b, a) FROM MyTable")
  }

  /**
    * OVER clause is necessary for [[OverAgg0]] window function.
    */
  @Test(expected = classOf[ValidationException])
  def testInvalidOverAggregation2(): Unit = {
    util.addTableSource[(Int, Long, String, Timestamp)]("T", 'a, 'b, 'c, 'ts)
    util.addFunction("overAgg", new OverAgg0)

    util.verifyPlan("SELECT overAgg(b, a) FROM T GROUP BY TUMBLE(ts, INTERVAL '2' HOUR)")
  }
}
