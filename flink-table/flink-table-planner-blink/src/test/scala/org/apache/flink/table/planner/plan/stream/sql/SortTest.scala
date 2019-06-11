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
import org.apache.flink.table.api.scala._
import org.apache.flink.table.planner.utils.TableTestBase

import org.junit.Test

class SortTest extends TableTestBase {

  private val util = streamTestUtil()
  util.addDataStream[(Int, String, Long)](
    "MyTable", 'a, 'b, 'c, 'proctime.proctime, 'rowtime.rowtime)

  @Test
  def testSortProcessingTime(): Unit = {
    // be converted to TemporalSort
    util.verifyPlan("SELECT a FROM MyTable ORDER BY proctime, c")
  }

  @Test
  def testSortRowTime(): Unit = {
    // be converted to TemporalSort
    util.verifyPlan("SELECT a FROM MyTable ORDER BY rowtime, c")
  }

  @Test
  def testSortProcessingTimeDesc(): Unit = {
    util.verifyPlan("SELECT a FROM MyTable ORDER BY proctime desc, c")
  }

  @Test
  def testSortRowTimeDesc(): Unit = {
    util.verifyPlan("SELECT a FROM MyTable ORDER BY rowtime desc, c")
  }

  @Test
  def testSortProcessingTimeSecond(): Unit = {
    util.verifyPlan("SELECT a FROM MyTable ORDER BY c, proctime")
  }

  @Test
  def testSortRowTimeSecond(): Unit = {
    util.verifyPlan("SELECT a FROM MyTable ORDER BY c, rowtime")
  }

  @Test
  def testSortProcessingTimeSecondDesc(): Unit = {
    util.verifyPlan("SELECT a FROM MyTable ORDER BY c, proctime desc")
  }

  @Test
  def testSortRowTimeSecondDesc(): Unit = {
    util.verifyPlan("SELECT a FROM MyTable ORDER BY c, rowtime desc")
  }

  @Test
  def testSortWithoutTime(): Unit = {
    util.verifyPlan("SELECT a FROM MyTable ORDER BY c")
  }
}
