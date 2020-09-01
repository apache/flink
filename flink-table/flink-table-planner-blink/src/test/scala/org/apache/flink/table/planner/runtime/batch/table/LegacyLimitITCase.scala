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

package org.apache.flink.table.planner.runtime.batch.table

import org.apache.flink.table.api.TableSchema
import org.apache.flink.table.planner.runtime.utils.BatchTestBase
import org.apache.flink.table.planner.runtime.utils.TestData._
import org.apache.flink.table.planner.utils.TestLegacyLimitableTableSource
import org.junit.Assert.assertEquals
import org.junit._

class LegacyLimitITCase extends BatchTestBase {

  @Before
  override def before(): Unit = {
    super.before()

    TestLegacyLimitableTableSource.createTemporaryTable(
      tEnv, data3, new TableSchema(Array("a", "b", "c"), type3.getFieldTypes), "LimitTable")
  }

  @Test
  def testFetch(): Unit = {
    assertEquals(
      executeQuery(tEnv.from("LimitTable").fetch(5)).size,
      5)
  }

  @Test
  def testOffset(): Unit = {
    assertEquals(
      executeQuery(tEnv.from("LimitTable").offset(5)).size,
      16)
  }

  @Test
  def testOffsetAndFetch(): Unit = {
    assertEquals(
      executeQuery(tEnv.from("LimitTable").limit(5, 5)).size,
      5)
  }
}
