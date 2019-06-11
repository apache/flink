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
import org.apache.flink.table.api.SqlParserException
import org.apache.flink.table.api.scala._
import org.apache.flink.table.planner.utils.TableTestBase

import org.junit.Test

class LimitTest extends TableTestBase {

  private val util = batchTestUtil()
  util.addTableSource[(Int, Long, String)]("MyTable", 'a, 'b, 'c)

  @Test
  def testLimitWithoutOffset(): Unit = {
    util.verifyPlan("SELECT * FROM MyTable LIMIT 5")
  }

  @Test
  def testLimit0WithoutOffset(): Unit = {
    util.verifyPlan("SELECT * FROM MyTable LIMIT 0")
  }

  @Test(expected = classOf[SqlParserException])
  def testNegativeLimitWithoutOffset(): Unit = {
    util.verifyPlan("SELECT * FROM MyTable LIMIT -1")
  }

  @Test
  def testLimitWithOffset(): Unit = {
    util.verifyPlan("SELECT a, c FROM MyTable LIMIT 10 OFFSET 1")
  }

  @Test
  def testLimitWithOffset0(): Unit = {
    util.verifyPlan("SELECT a, c FROM MyTable LIMIT 10 OFFSET 0")
  }

  @Test
  def testLimit0WithOffset0(): Unit = {
    util.verifyPlan("SELECT a, c FROM MyTable LIMIT 0 OFFSET 0")
  }

  @Test
  def testLimit0WithOffset(): Unit = {
    util.verifyPlan("SELECT a, c FROM MyTable LIMIT 0 OFFSET 10")
  }

  @Test(expected = classOf[SqlParserException])
  def testLimitWithNegativeOffset(): Unit = {
    util.verifyPlan("SELECT a, c FROM MyTable LIMIT 10 OFFSET -1")
  }

  @Test
  def testFetchWithOffset(): Unit = {
    util.verifyPlan("SELECT a, c FROM MyTable OFFSET 10 ROWS FETCH NEXT 10 ROWS ONLY")
  }

  @Test
  def testFetchWithoutOffset(): Unit = {
    util.verifyPlan("SELECT a, c FROM MyTable FETCH FIRST 10 ROWS ONLY")
  }

  @Test
  def testFetch0WithoutOffset(): Unit = {
    util.verifyPlan("SELECT a, c FROM MyTable FETCH FIRST 0 ROWS ONLY")
  }

  @Test
  def testOnlyOffset(): Unit = {
    util.verifyPlan("SELECT a, c FROM MyTable OFFSET 10 ROWS")
  }

}
