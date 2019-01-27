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

package org.apache.flink.table.runtime.batch.sql.agg

import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.plan.stats.{ColumnStats, TableStats}
import org.apache.flink.table.runtime.batch.sql.BatchTestBase
import org.apache.flink.table.runtime.batch.sql.BatchTestBase._
import org.apache.flink.table.runtime.batch.sql.TestData._

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

import org.junit.{Before, Test}

abstract class BaseSplitCompleteAggRuleITCase extends BatchTestBase {

  def prepareAggOp(tableEnv: TableEnvironment): Unit

  @Before
  def before(): Unit = {
    registerCollection("T1", smallData3, type3, "a, b, c", nullablesOfSmallData3)
    tEnv.alterSkewInfo("T1", Map("a" -> List[AnyRef](new Integer(1)).asJava))
    val colStats = Map[java.lang.String, ColumnStats](
      "a" -> ColumnStats(90L, 0L, 8D, 8, 100, 1),
      "b" -> ColumnStats(90L, 0L, 32D, 32, 100D, 0D),
      "c" -> ColumnStats(90L, 0L, 64D, 64, null, null)
    )
    val tableStats = TableStats(100L, colStats)
    tEnv.alterTableStats("T1", Option(tableStats))
    prepareAggOp(tEnv)
  }

  @Test
  def testMultiDistinctAgg(): Unit = {
    checkResult(
      "SELECT MAX(DISTINCT a), SUM(DISTINCT b), MIN(DISTINCT c) FROM (VALUES (1, 2, 3)) T(a, b, c)",
      Seq(row(1, 2, 3))
    )
  }

  @Test
  def testSkewCausedByTableScan(): Unit = {
    checkResult(
      "SELECT SUM(b) FROM T1 GROUP BY a",
      Seq(row(1), row(2), row(2))
     )
  }

  @Test
  def testGroupingSets(): Unit = {
    val sqlQuery =
      """
        |SELECT a, c, avg(b) as a FROM T1
        |GROUP BY GROUPING SETS ((a), (a, c))
      """.stripMargin
    val expected = Seq(
      row(1, "Hi", 1.0),
      row(1, null, 1.0),
      row(2, "Hello", 2.0),
      row(2, null, 2.0),
      row(3, "Hello world", 2.0),
      row(3, null, 2.0))
    checkResult(sqlQuery, expected)
  }

}
