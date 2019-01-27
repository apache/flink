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
import org.apache.flink.table.plan.nodes.FlinkConventions
import org.apache.flink.table.plan.nodes.logical.{FlinkLogicalAggregate, FlinkLogicalCalc, FlinkLogicalExpand, FlinkLogicalNativeTableScan, FlinkLogicalSemiJoin, FlinkLogicalSink, FlinkLogicalTableSourceScan, FlinkLogicalValues}
import org.apache.flink.table.plan.optimize.FlinkBatchPrograms.SUBQUERY_REWRITE
import org.apache.flink.table.plan.optimize._
import org.apache.flink.table.plan.rules.FlinkBatchExecRuleSets
import org.apache.flink.table.util.{BatchTableTestUtil, TableTestBase}

import org.apache.calcite.plan.hep.HepMatchOrder
import org.apache.calcite.rel.rules.{FilterCalcMergeRule, FilterToCalcRule, ProjectCalcMergeRule, ProjectToCalcRule, ReduceExpressionsRule}
import org.apache.calcite.tools.RuleSets
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.{Before, Test}

@RunWith(classOf[Parameterized])
class FlinkAggregateRemoveRuleTest(fieldsNullable: Boolean) extends TableTestBase {
  val util: BatchTableTestUtil = nullableBatchTestUtil(fieldsNullable)

  @Before
  def setup(): Unit = {
    val programs = new FlinkChainedPrograms[BatchOptimizeContext]()
    programs.addLast(
      // rewrite sub-queries to joins
      SUBQUERY_REWRITE,
      FlinkGroupProgramBuilder.newBuilder[BatchOptimizeContext]
        .addProgram(FlinkHepRuleSetProgramBuilder.newBuilder
          .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
          .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
          .add(FlinkBatchExecRuleSets.SEMI_JOIN_RULES)
          .build(), "rewrite sub-queries to semi-join")
        .build())

    programs.addLast(
      "rules",
      // use volcano planner because
      // rel.getCluster.getPlanner is volcano planner used in FlinkAggregateRemoveRule
      FlinkVolcanoProgramBuilder.newBuilder
        .add(RuleSets.ofList(
          ReduceExpressionsRule.FILTER_INSTANCE,
          FlinkAggregateExpandDistinctAggregatesRule.INSTANCE,
          FilterCalcMergeRule.INSTANCE,
          ProjectCalcMergeRule.INSTANCE,
          FilterToCalcRule.INSTANCE,
          ProjectToCalcRule.INSTANCE,
          FlinkCalcMergeRule.INSTANCE,
          FlinkAggregateRemoveRule.INSTANCE,
          DecomposeGroupingSetsRule.INSTANCE,
          AggregateReduceGroupingRule.INSTANCE,
          FlinkLogicalAggregate.BATCH_CONVERTER,
          FlinkLogicalCalc.CONVERTER,
          FlinkLogicalValues.CONVERTER,
          FlinkLogicalSemiJoin.CONVERTER,
          FlinkLogicalExpand.CONVERTER,
          FlinkLogicalTableSourceScan.CONVERTER,
          FlinkLogicalNativeTableScan.CONVERTER,
          FlinkLogicalSink.CONVERTER))
        .setTargetTraits(Array(FlinkConventions.LOGICAL))
        .build())
    val calciteConfig = CalciteConfig.createBuilder(util.tableEnv.getConfig.getCalciteConfig)
      .replaceBatchPrograms(programs).build()
    util.tableEnv.getConfig.setCalciteConfig(calciteConfig)

    util.addTable[(Int, Int, String)]("MyTable1", 'a, 'b, 'c)
    util.addTable[(Int, Int, String)]("MyTable2", Set(Set("a")), 'a, 'b, 'c)
    util.addTable[(Int, Int, String, String)]("MyTable3", Set(Set("a")), 'a, 'b, 'c, 'd)
  }

  @Test
  def testAggRemove_GroupKeyIsNotUnique(): Unit = {
    // can not remove agg
    util.verifyPlan("SELECT a, MAX(c) from MyTable1 GROUP BY a")
  }

  @Test
  def testAggRemove_WithoutFilter1(): Unit = {
    util.verifyPlan("SELECT a, b + 1, c, s FROM (" +
      "SELECT a, MIN(b) AS b, SUM(b) AS s, MAX(c) AS c FROM MyTable2 GROUP BY a)")
  }

  @Test
  def testAggRemove_WithoutFilter2(): Unit = {
    util.verifyPlan("SELECT a, SUM(b) AS s FROM MyTable2 GROUP BY a")
  }

  @Test
  def testAggRemove_WithoutGroupBy1(): Unit = {
    // can not remove agg
    util.verifyPlan("SELECT MAX(a), SUM(b), MIN(c) FROM MyTable2")
  }

  @Test
  def testAggRemove_WithoutGroupBy2(): Unit = {
    util.verifyPlan("SELECT MAX(a), SUM(b), MIN(c) FROM (VALUES (1, 2, 3)) T(a, b, c)")
  }

  @Test
  def testAggRemove_WithoutGroupBy3(): Unit = {
    // can not remove agg
    util.verifyPlan("SELECT * FROM MyTable2 WHERE EXISTS (SELECT SUM(a) FROM MyTable1 WHERE 1=2)")
  }

  @Test
  def testAggRemove_WithoutGroupBy4(): Unit = {
    // can not remove agg
    util.verifyPlan("SELECT SUM(a) FROM (SELECT a FROM MyTable2 WHERE 1=2)")
  }

  @Test
  def testAggRemove_WithoutAggCall(): Unit = {
    util.verifyPlan("SELECT a, b FROM MyTable2 GROUP BY a, b")
  }

  @Test
  def testAggRemove_WithFilter(): Unit = {
    // can not remove agg
    util.verifyPlan("SELECT a, MIN(c) FILTER (WHERE b > 0), MAX(b) FROM MyTable2 GROUP BY a")
  }

  @Test
  def testAggRemove_Count(): Unit = {
    // can not remove agg
    util.verifyPlan("SELECT a, COUNT(c) FROM MyTable2 GROUP BY a")
  }

  @Test
  def testAggRemove_CountStar(): Unit = {
    // can not remove agg
    util.verifyPlan("SELECT a, COUNT(*) FROM MyTable2 GROUP BY a")
  }

  @Test
  def testAggRemove_GroupSets1(): Unit = {
    // a is unique
    util.verifyPlan("SELECT a, SUM(b) AS s FROM MyTable3 GROUP BY GROUPING SETS((a, c), (a, d))")
  }

  @Test
  def testAggRemove_GroupSets2(): Unit = {
    // can not remove agg
    util.verifyPlan("SELECT a, SUM(b) AS s FROM MyTable3 GROUP BY GROUPING SETS((a, c), (a), ())")
  }

  @Test
  def testAggRemove_Rollup(): Unit = {
    // can not remove agg
    util.verifyPlan("SELECT a, SUM(b) AS s FROM MyTable3 GROUP BY ROLLUP(a, c, d)")
  }

  @Test
  def testAggRemove_Cube(): Unit = {
    // can not remove agg
    util.verifyPlan("SELECT a, SUM(b) AS s FROM MyTable3 GROUP BY CUBE(a, c, d)")
  }

  @Test
  def testAggRemove_SingleDistinctAgg1(): Unit = {
    util.verifyPlan("SELECT a, COUNT(DISTINCT c) FROM MyTable2 GROUP BY a")
  }

  @Test
  def testAggRemove_SingleDistinctAgg2(): Unit = {
    util.verifyPlan("SELECT a, COUNT(DISTINCT c) FROM MyTable2 GROUP BY a, b")
  }

  @Test
  def testAggRemove_SingleDistinctAgg_WithNonDistinctAgg1(): Unit = {
    util.verifyPlan("SELECT a, COUNT(DISTINCT c), SUM(c) FROM MyTable2 GROUP BY a")
  }

  @Test
  def testAggRemove_SingleDistinctAgg_WithNonDistinctAgg2(): Unit = {
    util.verifyPlan("SELECT a, COUNT(DISTINCT c), SUM(c) FROM MyTable2 GROUP BY a, b")
  }

  @Test
  def testAggRemove_SingleDistinctAgg_WithNonDistinctAgg3(): Unit = {
    util.verifyPlan("SELECT a, COUNT(DISTINCT c), SUM(b) FROM MyTable3 GROUP BY a")
  }

  @Test
  def testAggRemove_SingleDistinctAgg_WithNonDistinctAgg4(): Unit = {
    util.verifyPlan("SELECT a, COUNT(DISTINCT c), SUM(b) FROM MyTable3 GROUP BY a, d")
  }

  @Test
  def testAggRemove_MultiDistinctAggs1(): Unit = {
    util.verifyPlan("SELECT a, COUNT(DISTINCT b), SUM(DISTINCT b) FROM MyTable2 GROUP BY a")
  }

  @Test
  def testAggRemove_MultiDistinctAggs2(): Unit = {
    util.verifyPlan("SELECT a, COUNT(DISTINCT c), SUM(DISTINCT b) FROM MyTable3 GROUP BY a, d")
  }

  @Test
  def testAggRemove_MultiDistinctAggs3(): Unit = {
    util.verifyPlan(
      "SELECT a, SUM(DISTINCT b), MAX(DISTINCT b), MIN(DISTINCT c) FROM MyTable2 GROUP BY a")
  }

  @Test
  def testAggRemove_MultiDistinctAggs_WithNonDistinctAgg1(): Unit = {
    util.verifyPlan("SELECT a, COUNT(DISTINCT c), SUM(b) FROM MyTable3 GROUP BY a, d")
  }

}

object FlinkAggregateRemoveRuleTest {
  @Parameterized.Parameters(name = "{0}")
  def parameters(): java.util.Collection[Boolean] = {
    java.util.Arrays.asList(true, false)
  }
}
