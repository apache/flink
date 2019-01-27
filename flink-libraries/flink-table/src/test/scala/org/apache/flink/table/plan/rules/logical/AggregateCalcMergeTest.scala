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
package org.apache.flink.table.plan.rules.logical

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.calcite.CalciteConfig
import org.apache.flink.table.plan.optimize._
import org.apache.flink.table.util.TableTestBase

import org.apache.calcite.plan.hep.HepMatchOrder
import org.apache.calcite.rel.rules._
import org.apache.calcite.tools.RuleSets
import org.junit.{Before, Test}

class AggregateCalcMergeTest extends TableTestBase {
  private val util = batchTestUtil()

  @Before
  def setup(): Unit = {
    val programs = new FlinkChainedPrograms[BatchOptimizeContext]()
    programs.addLast(
      "rules",
      FlinkHepRuleSetProgramBuilder.newBuilder
        .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
        .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
        .add(RuleSets.ofList(
          AggregateReduceGroupingRule.INSTANCE,
          FilterCalcMergeRule.INSTANCE,
          ProjectCalcMergeRule.INSTANCE,
          FilterToCalcRule.INSTANCE,
          ProjectToCalcRule.INSTANCE,
          FlinkCalcMergeRule.INSTANCE,
          AggregateCalcMergeRule.INSTANCE)
        ).build())
    val calciteConfig = CalciteConfig.createBuilder(util.tableEnv.getConfig.getCalciteConfig)
      .replaceBatchPrograms(programs).build()
    util.tableEnv.getConfig.setCalciteConfig(calciteConfig)

    util.addTable[(Int, Int, String)]("MyTable", 'a, 'b, 'c)
  }

  @Test
  def testAggregateCalcMerge_WithoutFilter1(): Unit = {
    util.verifyPlan("SELECT a, count(c) as c FROM (SELECT a, c FROM MyTable) t GROUP BY a")
  }

  @Test
  def testAggregateCalcMerge_WithoutFilter2(): Unit = {
    util.verifyPlan("SELECT a, count(c) as c FROM (SELECT c, a FROM MyTable) t GROUP BY a")
  }

  @Test
  def testAggregateCalcMerge_WithoutFilter3(): Unit = {
    util.verifyPlan("SELECT a, count(c) as c FROM" +
      " (SELECT a, substr(c, 1, 5) as c FROM MyTable) t GROUP BY a")
  }

  @Test
  def testAggregateCalcMerge_Filter(): Unit = {
    util.verifyPlan("SELECT a, count(c) as c FROM" +
      " (SELECT a, c FROM MyTable where a > 1) t GROUP BY a")
  }

}
