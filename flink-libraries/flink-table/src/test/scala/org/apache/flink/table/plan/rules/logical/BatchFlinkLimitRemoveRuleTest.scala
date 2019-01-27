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

package org.apache.flink.table.plan.rules.logical

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.util.TableTestBase
import org.junit.Test

class BatchFlinkLimitRemoveRuleTest extends TableTestBase {
  private val batchUtil = batchTestUtil()
  batchUtil.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)

  @Test
  def testSimpleLimitZero(): Unit = {
    val sql =
      """
        |SELECT * FROM MyTable LIMIT 0
      """.stripMargin
    batchUtil.verifyPlan(sql)
  }

  @Test
  def testLimitZeroWithOrderBy(): Unit = {
    val sql =
      """
        |SELECT * FROM MyTable ORDER BY a LIMIT 0
      """.stripMargin
    batchUtil.verifyPlan(sql)
  }

  @Test
  def testLimitZeroWithSelect(): Unit = {
    val sql =
      """
        |SELECT * FROM (SELECT a FROM MyTable LIMIT 0)
      """.stripMargin
    batchUtil.verifyPlan(sql)
  }

  @Test
  def testLimitZeroWithIn(): Unit = {
    val sql =
      """
        |SELECT * FROM MyTable WHERE a IN
        |(SELECT a FROM MyTable LIMIT 0)
      """.stripMargin
    batchUtil.verifyPlan(sql)
  }

  @Test
  def testLimitZeroWithExists(): Unit = {
    val sql =
      """
        |SELECT * FROM MyTable WHERE exists
        |(SELECT a FROM MyTable LIMIT 0)
      """.stripMargin
    batchUtil.verifyPlan(sql)
  }

  @Test
  def testLimitZeroWithJoin(): Unit = {
    val sql =
      """
        |SELECT * FROM MyTable INNER JOIN (SELECT * FROM MyTable Limit 0) ON TRUE
      """.stripMargin
    batchUtil.verifyPlan(sql)
  }
}
