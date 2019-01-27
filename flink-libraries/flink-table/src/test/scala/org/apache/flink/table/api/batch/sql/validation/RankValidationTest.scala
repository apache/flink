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

package org.apache.flink.table.api.batch.sql.validation

import org.apache.flink.api.scala._
import org.apache.flink.table.api.ValidationException
import org.apache.flink.table.api.scala._
import org.apache.flink.table.util.TableTestBase
import org.junit.Test

class RankValidationTest extends TableTestBase {
  private val batchUtil = batchTestUtil()
  batchUtil.addTable[(Int, Long, String)]("sourceTable", 'a, 'b, 'c)

  @Test(expected = classOf[RuntimeException])
  def testRowNumberWithoutOrderBy(): Unit = {
    val sqlQuery =
      """
        |SELECT ROW_NUMBER() over (partition by a) FROM Table6
      """.stripMargin
    batchUtil.verifyPlan(sqlQuery)
  }

  @Test(expected = classOf[RuntimeException])
  def testRowNumberWithMultiGroups(): Unit = {
    val sqlQuery =
      """
        |SELECT ROW_NUMBER() over (partition by a order by b) as a,
        |       ROW_NUMBER() over (partition by b) as b
        |       FROM Table6
      """.stripMargin
    batchUtil.verifyPlan(sqlQuery)
  }

  @Test(expected = classOf[ValidationException])
  def testRankWithoutOrderBy(): Unit = {
    val sqlQuery =
      """
        |SELECT RANK() over (partition by a) FROM Table6
      """.stripMargin
    batchUtil.verifyPlan(sqlQuery)
  }

  @Test(expected = classOf[ValidationException])
  def testRankWithMultiGroups(): Unit = {
    val sqlQuery =
      """
        |SELECT RANK() over (partition by a order by b) as a,
        |       RANK() over (partition by b) as b
        |       FROM Table6
      """.stripMargin
    batchUtil.verifyPlan(sqlQuery)
  }

  @Test(expected = classOf[ValidationException])
  def testDenseRankWithoutOrderBy(): Unit = {
    val sqlQuery =
      """
        |SELECT dense_rank() over (partition by a) FROM Table6
      """.stripMargin
    batchUtil.verifyPlan(sqlQuery)
  }

  @Test(expected = classOf[ValidationException])
  def testDenseRankWithMultiGroups(): Unit = {
    val sqlQuery =
      """
        |SELECT DENSE_RANK() over (partition by a order by b) as a,
        |       DENSE_RANK() over (partition by b) as b
        |       FROM Table6
      """.stripMargin
    batchUtil.verifyPlan(sqlQuery)
  }
}
