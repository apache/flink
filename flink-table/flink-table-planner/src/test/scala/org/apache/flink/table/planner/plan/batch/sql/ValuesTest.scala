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

import org.apache.flink.table.planner.utils.TableTestBase

import org.junit.Test

class ValuesTest extends TableTestBase {

  private val util = batchTestUtil()

  @Test
  def testNullValues(): Unit = {
    util.verifyExecPlan("SELECT * FROM (VALUES CAST(NULL AS INT))")
  }

  @Test
  def testSingleRow(): Unit = {
    util.verifyExecPlan("SELECT * FROM (VALUES (1, 2, 3)) AS T(a, b, c)")
  }

  @Test
  def testMultiRows(): Unit = {
    util.verifyExecPlan("SELECT * FROM (VALUES (1, 2), (3, CAST(NULL AS INT)), (4, 5)) AS T(a, b)")
  }

  @Test
  def testDiffTypes(): Unit = {
    util.verifyRelPlanWithType("SELECT * FROM (VALUES (1, 2.0), (3, CAST(4 AS BIGINT))) AS T(a, b)")
  }

  @Test
  def testEmptyValuesWithSort(): Unit = {
    util.verifyExecPlan("SELECT * FROM (VALUES 1, 2, 3) AS T (a) WHERE a = 1 and a = 2 ORDER BY a")
  }

}
