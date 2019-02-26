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
import org.junit.Assert.assertEquals

import java.util

import scala.collection.JavaConversions._

class TableStatsTest {

  @Test
  def testValidTableStats(): Unit = {
    TableStats(null)
    TableStats(1000L, Map[String, ColumnStats]("a" -> ColumnStats(100L, 0L, 4D, 4, null, null)))
  }

  @Test(expected = classOf[IllegalArgumentException])
  def testNdvIsRowCount(): Unit = {
    TableStats(-1L, new util.HashMap())
  }

  @Test(expected = classOf[IllegalArgumentException])
  def testColumnStatsIsNull(): Unit = {
    TableStats(1L, null)
  }

  @Test
  def testTableStatsBuilder(): Unit = {
    assertEquals(TableStats.UNKNOWN, TableStats.builder().build())
    assertEquals(TableStats(100L), TableStats.builder().rowCount(100L).build())
    val columnStats = Map[String, ColumnStats]("a" -> ColumnStats(100L, 0L, 4D, 4, null, null))
    assertEquals(TableStats(null, columnStats), TableStats.builder().colStats(columnStats).build())
    assertEquals(TableStats(100L, columnStats),
      TableStats.builder().rowCount(100L).colStats(columnStats).build())
    assertEquals(TableStats(200L, columnStats),
      TableStats.builder().tableStats(TableStats(1L, columnStats)).rowCount(200L).build())
  }
}
