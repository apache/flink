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

package org.apache.flink.table.planner.plan.rules.physical.batch

import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.planner.utils.TableTestBase

import org.junit.{Before, Test}


abstract class EnforceLocalAggRuleTestBase extends TableTestBase {
  protected val util = batchTestUtil()

  @Before
  def setup(): Unit = {
    util.addTableSource[(Int, Long, String, Int)]("t", 'a, 'b, 'c, 'd)
  }

  @Test
  def testRollupWithCount(): Unit = {
    util.verifyRelPlan("SELECT COUNT(*) FROM t GROUP BY ROLLUP (b, c)")
  }

  @Test
  def testRollupWithAvg(): Unit = {
    util.verifyRelPlan("SELECT AVG(a) FROM t GROUP BY ROLLUP (b, c)")
  }

  @Test
  def testCubeWithCount(): Unit = {
    util.verifyRelPlan("SELECT COUNT(a) FROM t GROUP BY CUBE (b, c)")
  }

  @Test
  def testCubeWithAvg(): Unit = {
    util.verifyRelPlan("SELECT AVG(d) FROM t GROUP BY CUBE (a, c)")
  }

  @Test
  def testGroupSetsWithCount(): Unit = {
    util.verifyRelPlan("SELECT COUNT(*) FROM t GROUP BY GROUPING SETS ((b, c), (b, d))")
  }

  @Test
  def testGroupSetsWithAvg(): Unit = {
    util.verifyRelPlan("SELECT AVG(a) FROM t GROUP BY GROUPING SETS ((b, c), (b, d))")
  }

}
