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
import org.apache.flink.table.planner.utils.TableTestBase

import org.junit.jupiter.api.{BeforeEach, Test}

/** Test for [[FlinkProjectWindowTransposeRule]]. */
class FlinkProjectWindowTransposeRuleTest extends TableTestBase {
  private val util = streamTestUtil()

  @BeforeEach
  def setup(): Unit = {
    util.addTableSource[(String, Int, String)]("T1", 'a, 'b, 'c)
    util.addTableSource[(String, Int, String)]("T2", 'd, 'e, 'f)
  }

  @Test
  def testProjectPushDown(): Unit = {
    val sql =
      """
        |SELECT a FROM (
        |  SELECT a, f,
        |      ROW_NUMBER() OVER (PARTITION BY f ORDER BY c DESC) as rank_num
        |  FROM  T1, T2
        |  WHERE T1.a = T2.d
        |)
        |WHERE rank_num = 1
      """.stripMargin

    util.verifyRelPlan(sql)
  }
}
