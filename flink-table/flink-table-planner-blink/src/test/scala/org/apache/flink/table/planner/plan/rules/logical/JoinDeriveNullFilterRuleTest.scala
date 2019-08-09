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

package org.apache.flink.table.planner.plan.rules.logical

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.api.Types
import org.apache.flink.table.plan.stats.{ColumnStats, TableStats}
import org.apache.flink.table.planner.plan.optimize.program.{FlinkBatchProgram, FlinkHepRuleSetProgramBuilder, HEP_RULES_EXECUTION_TYPE}
import org.apache.flink.table.planner.plan.stats.FlinkStatistic
import org.apache.flink.table.planner.utils.{TableConfigUtils, TableTestBase}

import org.apache.calcite.plan.hep.HepMatchOrder
import org.apache.calcite.tools.RuleSets
import org.junit.{Before, Test}

import scala.collection.JavaConversions._


/**
  * Test for [[JoinDeriveNullFilterRule]].
  */
class JoinDeriveNullFilterRuleTest extends TableTestBase {

  private val util = batchTestUtil()

  @Before
  def setup(): Unit = {
    util.buildBatchProgram(FlinkBatchProgram.DEFAULT_REWRITE)
    val calciteConfig = TableConfigUtils.getCalciteConfig(util.tableEnv.getConfig)
    calciteConfig.getBatchProgram.get.addLast(
      "rules",
      FlinkHepRuleSetProgramBuilder.newBuilder
        .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
        .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
        .add(RuleSets.ofList(JoinDeriveNullFilterRule.INSTANCE))
        .build()
    )

    util.tableEnv.getConfig.getConfiguration.setLong(
      JoinDeriveNullFilterRule.TABLE_OPTIMIZER_JOIN_NULL_FILTER_THRESHOLD, 2000000)
    util.addTableSource("MyTable1",
      Array[TypeInformation[_]](Types.INT, Types.LONG, Types.STRING, Types.INT, Types.LONG),
      Array("a1", "b1", "c1", "d1", "e1"),
      FlinkStatistic.builder().tableStats(new TableStats(1000000000, Map(
        "a1" -> new ColumnStats(null, 10000000L, 4.0, 4, null, null),
        "c1" -> new ColumnStats(null, 5000000L, 10.2, 16, null, null),
        "e1" -> new ColumnStats(null, 500000L, 8.0, 8, null, null)
      ))).build())
    util.addTableSource("MyTable2",
      Array[TypeInformation[_]](Types.INT, Types.LONG, Types.STRING, Types.INT, Types.LONG),
      Array("a2", "b2", "c2", "d2", "e2"),
      FlinkStatistic.builder().tableStats(new TableStats(2000000000, Map(
        "b2" -> new ColumnStats(null, 10000000L, 8.0, 8, null, null),
        "c2" -> new ColumnStats(null, 3000000L, 18.6, 32, null, null),
        "e2" -> new ColumnStats(null, 1500000L, 8.0, 8, null, null)
      ))).build())
  }

  @Test
  def testInnerJoin_NoneEquiJoinKeys(): Unit = {
    util.verifyPlan("SELECT * FROM MyTable1 JOIN MyTable2 ON a1 > a2")
  }

  @Test
  def testInnerJoin_NullCountOnLeftJoinKeys(): Unit = {
    util.verifyPlan("SELECT * FROM MyTable1 JOIN MyTable2 ON a1 = a2")
  }

  @Test
  def testInnerJoin_NullCountOnRightJoinKeys(): Unit = {
    util.verifyPlan("SELECT * FROM MyTable1 JOIN MyTable2 ON b1 = b2")
  }

  @Test
  def testInnerJoin_NullCountOnLeftRightJoinKeys(): Unit = {
    util.verifyPlan("SELECT * FROM MyTable1 JOIN MyTable2 ON c1 = c2")
  }

  @Test
  def testInnerJoin_NoNullCount(): Unit = {
    util.verifyPlan("SELECT * FROM MyTable1 JOIN MyTable2 ON d1 = d2")
  }

  @Test
  def testInnerJoin_NullCountLessThanThreshold(): Unit = {
    util.verifyPlan("SELECT * FROM MyTable1 JOIN MyTable2 ON e1 = e2")
  }

  @Test
  def testLeftJoin(): Unit = {
    util.verifyPlan("SELECT * FROM MyTable1 LEFT JOIN MyTable2 ON c1 = c2")
  }

  @Test
  def testRightJoin(): Unit = {
    util.verifyPlan("SELECT * FROM MyTable1 RIGHT JOIN MyTable2 ON c1 = c2")
  }

  @Test
  def testFullJoin(): Unit = {
    util.verifyPlan("SELECT * FROM MyTable1 FULL JOIN MyTable2 ON c1 = c2")
  }
}
