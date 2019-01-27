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

package org.apache.flink.table.plan.batch.table

import org.apache.flink.table.api.TableConfigOptions
import org.apache.flink.table.api.scala._
import org.apache.flink.table.calcite.CalciteConfig
import org.apache.flink.table.plan.optimize.FlinkBatchPrograms
import org.apache.flink.table.plan.rules.logical.PushLimitIntoTableSourceScanRule
import org.apache.flink.table.plan.stats.TableStats
import org.apache.flink.table.runtime.utils.CommonTestData
import org.apache.flink.table.sinks.csv.CsvTableSink
import org.apache.flink.table.util.{BatchTableTestUtil, TableTestBase}

import org.apache.calcite.tools.RuleSets
import org.junit.{Before, Test}

class SubplanReuseTest extends TableTestBase {

  private var util: BatchTableTestUtil = _

  @Before
  def before(): Unit = {
    util = batchTestUtil()
    val programs = FlinkBatchPrograms.buildPrograms(util.tableEnv.getConfig.getConf)
    programs.getFlinkRuleSetProgram(FlinkBatchPrograms.LOGICAL)
      .get.remove(RuleSets.ofList(PushLimitIntoTableSourceScanRule.INSTANCE))
    val calciteConfig = CalciteConfig.createBuilder(util.tableEnv.getConfig.getCalciteConfig)
      .replaceBatchPrograms(programs).build()
    util.tableEnv.getConfig.setCalciteConfig(calciteConfig)

    util.tableEnv.getConfig.getConf.setBoolean(
      TableConfigOptions.SQL_OPTIMIZER_REUSE_SUB_PLAN_ENABLED, true)
    util.addTable("x", CommonTestData.get3Source(Array("a", "b", "c")))
    util.addTable("y", CommonTestData.get3Source(Array("d", "e", "f")))
    util.tableEnv.alterTableStats("x", Some(TableStats(100L)))
    util.tableEnv.alterTableStats("y", Some(TableStats(100L)))
  }

  @Test
  def testSubplanReuse_MultiSinks(): Unit = {
    util.tableEnv.getConfig.setSubsectionOptimization(true)
    util.tableEnv.getConfig.getConf.setBoolean(
      TableConfigOptions.SQL_OPTIMIZER_REUSE_TABLE_SOURCE_ENABLED, true)
    val table = util.tableEnv.scan("x")
    table.filter('a >= 50).writeToSink(new CsvTableSink("/tmp/1.csv"))
    table.filter('a < 50).writeToSink(new CsvTableSink("/tmp/2.csv"))
    util.verifyPlan()
  }

}
