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

import org.apache.flink.table.planner.expressions.utils.Func1
import org.apache.flink.table.planner.plan.optimize.program.{FlinkBatchProgram, FlinkHepRuleSetProgramBuilder, HEP_RULES_EXECUTION_TYPE}
import org.apache.flink.table.planner.utils.{TableConfigUtils, TableTestBase, TestPartitionableTableSource}

import org.apache.calcite.plan.hep.HepMatchOrder
import org.apache.calcite.tools.RuleSets
import org.junit.{Before, Test}

/**
  * Test for [[PushPartitionIntoTableSourceScanRule]].
  */
class PushPartitionIntoTableSourceScanRuleTest extends TableTestBase {
  private val util = batchTestUtil()

  @Before
  def setup(): Unit = {
    util.buildBatchProgram(FlinkBatchProgram.DEFAULT_REWRITE)
    val calciteConfig = TableConfigUtils.getCalciteConfig(util.tableEnv.getConfig)
    calciteConfig.getBatchProgram.get.addLast(
      "rules",
      FlinkHepRuleSetProgramBuilder.newBuilder
        .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_COLLECTION)
        .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
        .add(RuleSets.ofList(PushPartitionIntoTableSourceScanRule.INSTANCE))
        .build()
    )

    util.tableEnv.registerTableSource("MyTable", new TestPartitionableTableSource(true))
  }

  @Test
  def testNoPartitionFieldPredicate(): Unit = {
    util.verifyPlan("SELECT * FROM MyTable WHERE id > 2")
  }

  @Test
  def testOnlyPartitionFieldPredicate1(): Unit = {
    util.verifyPlan("SELECT * FROM MyTable WHERE part1 = 'A'")
  }

  @Test
  def testOnlyPartitionFieldPredicate2(): Unit = {
    util.verifyPlan("SELECT * FROM MyTable WHERE part2 > 1")
  }

  @Test
  def testOnlyPartitionFieldPredicate3(): Unit = {
    util.verifyPlan("SELECT * FROM MyTable WHERE part1 = 'A' AND part2 > 1")
  }

  @Test
  def testOnlyPartitionFieldPredicate4(): Unit = {
    util.verifyPlan("SELECT * FROM MyTable WHERE part1 = 'A' OR part2 > 1")
  }

  @Test
  def testPartitionFieldPredicateAndOtherPredicate(): Unit = {
    util.verifyPlan("SELECT * FROM MyTable WHERE id > 2 AND part1 = 'A'")
  }

  @Test
  def testPartitionFieldPredicateOrOtherPredicate(): Unit = {
    util.verifyPlan("SELECT * FROM MyTable WHERE id > 2 OR part1 = 'A'")
  }

  @Test
  def testPartialPartitionFieldPredicatePushDown(): Unit = {
    util.verifyPlan("SELECT * FROM MyTable WHERE (id > 2 OR part1 = 'A') AND part2 > 1")
  }

  @Test
  def testWithUdf(): Unit = {
    util.addFunction("MyUdf", Func1)
    util.verifyPlan("SELECT * FROM MyTable WHERE id > 2 AND MyUdf(part2) < 3")
  }

}
