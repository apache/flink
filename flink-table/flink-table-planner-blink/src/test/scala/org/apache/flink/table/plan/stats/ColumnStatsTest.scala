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

package org.apache.flink.table.plan.stats

import org.junit.Test

class ColumnStatsTest {

  @Test
  def testValidColumnStats(): Unit = {
    ColumnStats(null, null, null, null, null, null)
    ColumnStats(0L, 0L, 0D, 0, 0, 0)
    ColumnStats(100L, 10L, 6.12345, 10, "abc", "xyz")
  }

  @Test(expected = classOf[IllegalArgumentException])
  def testNdvIsNegative(): Unit = {
    ColumnStats(-1L, null, null, null, null, null)
  }

  @Test(expected = classOf[IllegalArgumentException])
  def testNullCountIsNegative(): Unit = {
    ColumnStats(null, -1L, null, null, null, null)
  }

  @Test(expected = classOf[IllegalArgumentException])
  def testAvgLenIsNegative(): Unit = {
    ColumnStats(null, null, -1D, null, null, null)
  }

  @Test(expected = classOf[IllegalArgumentException])
  def testMaxLenIsNegative(): Unit = {
    ColumnStats(null, null, null, -1, null, null)
  }

  @Test(expected = classOf[IllegalArgumentException])
  def testMaxIsNotComparable(): Unit = {
    ColumnStats(null, null, null, null, Array(1, 2, 3), null)
  }

  @Test(expected = classOf[IllegalArgumentException])
  def testMinIsNotComparable(): Unit = {
    ColumnStats(null, null, null, null, null, Array(1, 2, 3))
  }

  @Test(expected = classOf[IllegalArgumentException])
  def testClassOfMaxMinIsDiff(): Unit = {
    ColumnStats(null, null, null, null, 5L, 2D)
  }
}
