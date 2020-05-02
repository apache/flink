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
import org.apache.flink.api.scala._
import org.apache.flink.table.api.Types
import org.apache.flink.table.api.scala._
import org.apache.flink.table.planner.plan.nodes.FlinkConventions
import org.apache.flink.table.planner.plan.nodes.logical.{FlinkLogicalAggregate, FlinkLogicalCalc, FlinkLogicalExpand, FlinkLogicalJoin, FlinkLogicalSink, FlinkLogicalLegacyTableSourceScan, FlinkLogicalValues}
import org.apache.flink.table.planner.plan.optimize.program._
import org.apache.flink.table.planner.plan.rules.FlinkBatchRuleSets
import org.apache.flink.table.planner.plan.stats.FlinkStatistic
import org.apache.flink.table.planner.utils.TableTestBase

import com.google.common.collect.ImmutableSet
import org.apache.calcite.plan.hep.HepMatchOrder
import org.apache.calcite.rel.rules.{FilterCalcMergeRule, FilterToCalcRule, ProjectCalcMergeRule, ProjectToCalcRule, ReduceExpressionsRule}
import org.apache.calcite.tools.RuleSets
import org.junit.{Before, Test}

/**
  * Test for [[FlinkAggregateRemoveRule]].
  */
class FlinkAggregateRemoveRuleTest extends TableTestBase {
  private val util = batchTestUtil()

  @Before
  def setup(): Unit = {
    val programs = new FlinkChainedProgram[BatchOptimizeContext]()
    programs.addLast(
      // rewrite sub-queries to joins
      "subquery_rewrite",
      FlinkGroupProgramBuilder.newBuilder[BatchOptimizeContext]
        .addProgram(FlinkHepRuleSetProgramBuilder.newBuilder
          .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
          .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
          .add(FlinkBatchRuleSets.SEMI_JOIN_RULES)
          .build(), "rewrite sub-queries to semi/anti join")
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
          FlinkLogicalJoin.CONVERTER,
          FlinkLogicalValues.CONVERTER,
          FlinkLogicalExpand.CONVERTER,
          FlinkLogicalLegacyTableSourceScan.CONVERTER,
          FlinkLogicalSink.CONVERTER))
        .setRequiredOutputTraits(Array(FlinkConventions.LOGICAL))
        .build())
    util.replaceBatchProgram(programs)

    util.addTableSource[(Int, Int, String)]("MyTable1", 'a, 'b, 'c)
    util.addTableSource("MyTable2",
      Array[TypeInformation[_]](Types.INT, Types.INT, Types.STRING),
      Array("a", "b", "c"),
      FlinkStatistic.builder().uniqueKeys(ImmutableSet.of(ImmutableSet.of("a"))).build()
    )
    util.addTableSource("MyTable3",
      Array[TypeInformation[_]](Types.INT, Types.INT, Types.STRING, Types.STRING),
      Array("a", "b", "c", "d"),
      FlinkStatistic.builder().uniqueKeys(ImmutableSet.of(ImmutableSet.of("a"))).build()
    )
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
    util.verifyPlan("SELECT a, COUNT(DISTINCT b), SUM(b) FROM MyTable2 GROUP BY a")
  }

  @Test
  def testAggRemove_SingleDistinctAgg_WithNonDistinctAgg2(): Unit = {
    util.verifyPlan("SELECT a, COUNT(DISTINCT b), SUM(b) FROM MyTable2 GROUP BY a, c")
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
