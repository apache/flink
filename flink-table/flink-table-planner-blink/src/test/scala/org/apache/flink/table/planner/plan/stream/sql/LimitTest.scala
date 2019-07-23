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

package org.apache.flink.table.planner.plan.stream.sql

import org.apache.flink.api.scala._
import org.apache.flink.table.api.SqlParserException
import org.apache.flink.table.api.scala._
import org.apache.flink.table.planner.utils.TableTestBase

import org.junit.Test

class LimitTest extends TableTestBase {

  private val util = streamTestUtil()
  util.addDataStream[(Int, String, Long)](
    "MyTable", 'a, 'b, 'c, 'proctime.proctime, 'rowtime.rowtime)

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

  @Test
  def testLimitWithRowTime(): Unit = {
    util.verifyPlan("SELECT rowtime, c FROM MyTable LIMIT 2")
  }

  @Test
  def testLimit0WithRowTime(): Unit = {
    util.verifyPlan("SELECT rowtime, c FROM MyTable LIMIT 0")
  }

  @Test
  def testOffsetWithRowTime(): Unit = {
    util.verifyPlan("SELECT rowtime, c FROM MyTable OFFSET 2")
  }

  @Test
  def testLimit0WithProcessingTime(): Unit = {
    util.verifyPlan("SELECT proctime, c FROM MyTable LIMIT 0")
  }

  @Test
  def testLimitWithProcessingTime(): Unit = {
    util.verifyPlan("SELECT proctime, c FROM MyTable LIMIT 2")
  }

  @Test
  def testOffsetWithProcessingTime(): Unit = {
    util.verifyPlan("SELECT proctime, c FROM MyTable OFFSET 2")
  }

  @Test
  def testLimit0WithRowTimeSecond(): Unit = {
    util.verifyPlan("SELECT c, rowtime FROM MyTable LIMIT 0")
  }

  @Test
  def testLimitWithRowTimeSecond(): Unit = {
    util.verifyPlan("SELECT c, rowtime FROM MyTable LIMIT 2")
  }

  @Test
  def testOffsetWithRowTimeSecond(): Unit = {
    util.verifyPlan("SELECT c, rowtime FROM MyTable OFFSET 2")
  }

  @Test
  def testLimit0WithProcessingTimeSecond(): Unit = {
    util.verifyPlan("SELECT c, proctime FROM MyTable LIMIT 0")
  }

  @Test
  def testLimitWithProcessingTimeSecond(): Unit = {
    util.verifyPlan("SELECT c, proctime FROM MyTable LIMIT 2")
  }

  @Test
  def testOffsetWithProcessingTimeSecond(): Unit = {
    util.verifyPlan("SELECT c, proctime FROM MyTable OFFSET 2")
  }

  @Test
  def testLimit0WithRowTimeDesc(): Unit = {
    util.verifyPlan("SELECT rowtime desc, c FROM MyTable LIMIT 0")
  }

  @Test
  def testLimitWithRowTimeDesc(): Unit = {
    util.verifyPlan("SELECT rowtime desc, c FROM MyTable LIMIT 2")
  }

  @Test
  def testOffsetWithRowTimeDesc(): Unit = {
    util.verifyPlan("SELECT rowtime desc, c FROM MyTable OFFSET 2")
  }

  @Test
  def testLimit0WithProcessingTimeDesc(): Unit = {
    util.verifyPlan("SELECT proctime desc, c FROM MyTable LIMIT 0")
  }

  @Test
  def testLimitWithProcessingTimeDesc(): Unit = {
    util.verifyPlan("SELECT proctime desc, c FROM MyTable LIMIT 2")
  }

  @Test
  def testOffsetWithProcessingTimeDesc(): Unit = {
    util.verifyPlan("SELECT proctime desc, c FROM MyTable OFFSET 2")
  }

  @Test
  def testLimit0WithRowTimeDescSecond(): Unit = {
    util.verifyPlan("SELECT c, rowtime desc FROM MyTable LIMIT 0")
  }

  @Test
  def testLimitWithRowTimeDescSecond(): Unit = {
    util.verifyPlan("SELECT c, rowtime desc FROM MyTable LIMIT 2")
  }

  @Test
  def testOffsetWithRowTimeDescSecond(): Unit = {
    util.verifyPlan("SELECT c, rowtime desc FROM MyTable OFFSET 2")
  }

  @Test
  def testLimit0WithProcessingTimeDescSecond(): Unit = {
    util.verifyPlan("SELECT c, proctime desc FROM MyTable LIMIT 0")
  }

  @Test
  def testLimitWithProcessingTimeDescSecond(): Unit = {
    util.verifyPlan("SELECT c, proctime desc FROM MyTable LIMIT 2")
  }

  @Test
  def testOffsetWithProcessingTimeDescSecond(): Unit = {
    util.verifyPlan("SELECT c, proctime desc FROM MyTable OFFSET 2")
  }
}
