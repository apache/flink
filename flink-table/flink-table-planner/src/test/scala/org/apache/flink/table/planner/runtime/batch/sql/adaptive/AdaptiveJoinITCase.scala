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
package org.apache.flink.table.planner.runtime.batch.sql.adaptive

import org.apache.flink.api.common.typeinfo.BasicTypeInfo.{LONG_TYPE_INFO, STRING_TYPE_INFO}
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.table.api.config.{ExecutionConfigOptions, OptimizerConfigOptions}
import org.apache.flink.table.planner.runtime.utils.AdaptiveBatchTestBase
import org.apache.flink.table.planner.runtime.utils.AdaptiveBatchTestBase.row
import org.apache.flink.types.Row

import org.junit.jupiter.api.{BeforeEach, Test}

import scala.collection.JavaConversions._
import scala.util.Random

/** IT cases for adaptive join. */
trait AdaptiveJoinITCase extends AdaptiveBatchTestBase {

  @BeforeEach
  override def before(): Unit = {
    super.before()
  }

  @Test
  def testWithShuffleHashJoin(): Unit = {
    tEnv.getConfig
      .set(ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "NestedLoopJoin,SortMergeJoin")
    testSimpleJoin()
  }

  @Test
  def testWithShuffleMergeJoin(): Unit = {
    tEnv.getConfig
      .set(ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "NestedLoopJoin,ShuffleHashJoin")
    testSimpleJoin()
  }

  @Test
  def testWithBroadcastJoin(): Unit = {
    tEnv.getConfig.set(
      ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS,
      "SortMergeJoin,NestedLoopJoin")
    tEnv.getConfig.set(
      OptimizerConfigOptions.TABLE_OPTIMIZER_BROADCAST_JOIN_THRESHOLD,
      Long.box(Long.MaxValue))
    testSimpleJoin()
  }

  @Test
  def testShuffleJoinWithForwardForConsecutiveHash(): Unit = {
    tEnv.getConfig.set(
      OptimizerConfigOptions.TABLE_OPTIMIZER_MULTIPLE_INPUT_ENABLED,
      Boolean.box(false))
    val sql =
      """
        |WITH
        |  r AS (SELECT * FROM T1, T2, T3 WHERE a1 = a2 and a1 = a3)
        |SELECT sum(b1), count(c1) FROM r group by a1
        |""".stripMargin
    checkResult(sql)
  }

  @Test
  def testJoinWithUnionInput(): Unit = {
    val sql =
      """
        |SELECT * FROM
        |  (SELECT a FROM (SELECT a1 as a FROM T1) UNION ALL (SELECT a2 as a FROM T2)) Y
        |  LEFT JOIN T ON T.a = Y.a
        |""".stripMargin
    checkResult(sql)
  }

  @Test
  def testJoinWithMultipleInput(): Unit = {
    val sql =
      """
        |SELECT * FROM
        |  (SELECT a FROM T1 JOIN T ON a = a1) t1
        |  INNER JOIN
        |  (SELECT d2 FROM T JOIN T2 ON d2 = a) t2
        |ON t1.a = t2.d2
        |""".stripMargin
    checkResult(sql)
  }

  @Test
  def testSimpleJoin(): Unit = {
    // inner join
    val sql1 = "SELECT * FROM T1, T2 WHERE a1 = a2"
    checkResult(sql1)

    // left join
    val sql2 = "SELECT * FROM T1 LEFT JOIN T2 on a1 = a2"
    checkResult(sql2)

    // right join
    val sql3 = "SELECT * FROM T1 RIGHT JOIN T2 on a1 = a2"
    checkResult(sql3)

    // semi join
    val sql4 = "SELECT * FROM T1 WHERE a1 IN (SELECT a2 FROM T2)"
    checkResult(sql4)

    // anti join
    val sql5 = "SELECT * FROM T1 WHERE a1 NOT IN (SELECT a2 FROM T2 where a2 = a1)"
    checkResult(sql5)
  }

  def checkResult(sql: String): Unit
}

object AdaptiveJoinITCase {

  def generateRandomData: Seq[Row] = {
    generateRandomData(Random.nextInt(30), 0)
  }

  def generateRandomData(numRows: Int, skewedFactor: Double): Seq[Row] = {
    val data = new java.util.ArrayList[Row]()
    lazy val strs = Seq("adaptive", "join", "itcase")
    for (x <- 0 until numRows) {
      // Used to generate skewed data, records with random number less than skewedFactor will have the same id.
      val id = if (Random.nextDouble() < skewedFactor) {
        0
      } else {
        x
      }
      data.add(row(id.toLong, Random.nextLong(), strs(Random.nextInt(3)), Random.nextLong()))
    }
    data
  }

  lazy val rowType =
    new RowTypeInfo(LONG_TYPE_INFO, LONG_TYPE_INFO, STRING_TYPE_INFO, LONG_TYPE_INFO)
  lazy val nullables: Array[Boolean] = Array(true, true, true, true)
}
