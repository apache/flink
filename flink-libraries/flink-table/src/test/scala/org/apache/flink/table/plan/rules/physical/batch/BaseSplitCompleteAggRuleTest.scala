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

package org.apache.flink.table.plan.rules.physical.batch

import org.apache.flink.table.api.{AggPhaseEnforcer, TableConfigOptions, TableEnvironment}
import org.apache.flink.table.api.AggPhaseEnforcer._
import org.apache.flink.table.plan.stats.{ColumnStats, TableStats}
import org.apache.flink.table.runtime.utils.CommonTestData
import org.apache.flink.table.util.{BatchTableTestUtil, TableTestBase}

import java.util

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.{Before, Test}

@RunWith(classOf[Parameterized])
abstract class BaseSplitCompleteAggRuleTest(aggStrategy: AggPhaseEnforcer)
  extends TableTestBase {

  private var util: BatchTableTestUtil = _

  def prepareAggOp(tableEnv: TableEnvironment): Unit

  @Before
  def before(): Unit = {
    util = batchTestUtil()
    util.addTable("T1", CommonTestData.get3Source(Array("a", "b", "c")))
    util.tableEnv.alterSkewInfo("T1", Map("a" -> List[AnyRef](new Integer(1)).asJava))
    val colStats = Map[java.lang.String, ColumnStats](
      "a" -> ColumnStats(90L, 0L, 8D, 8, 100, 1),
      "b" -> ColumnStats(90L, 0L, 32D, 32, 100D, 0D),
      "c" -> ColumnStats(90L, 0L, 64D, 64, null, null)
    )
    val tableStats = TableStats(100L, colStats)
    util.tableEnv.alterTableStats("T1", Option(tableStats))
    // difference between T1 and T2 is t1 has skew info
    util.addTable("T2", CommonTestData.get3Source(Array("a", "b", "c")))
    util.tableEnv.alterTableStats("T2", Option(tableStats))
    util.tableEnv.getConfig.getConf.setString(
      TableConfigOptions.SQL_OPTIMIZER_AGG_PHASE_ENFORCER, aggStrategy.toString)
    prepareAggOp(util.tableEnv)
  }

  @Test
  def testMultiDistinctAgg(): Unit = {
    val sqlQuery =
      """
        |SELECT MAX(DISTINCT a), SUM(DISTINCT b), MIN(DISTINCT c)
        |FROM (VALUES (1, 2, 3)) T(a, b, c)
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testSkewCausedByTableScan(): Unit = {
    val sqlQuery = "SELECT SUM(b) FROM T1 GROUP BY a"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testNotSkewOnTableScan(): Unit = {
    val sqlQuery = "SELECT SUM(b) FROM T2 GROUP BY a"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testGroupingSets(): Unit = {
    val sqlQuery =
      """
        |SELECT a, c, avg(b) as a FROM T1
        |GROUP BY GROUPING SETS ((a), (a, c))
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }
}

object BaseSplitCompleteAggRuleTest {

  @Parameterized.Parameters(name = "aggStrategy={0}")
  def parameters(): util.Collection[AggPhaseEnforcer] =
    Seq(AggPhaseEnforcer.ONE_PHASE, AggPhaseEnforcer.TWO_PHASE, AggPhaseEnforcer.NONE)

}
