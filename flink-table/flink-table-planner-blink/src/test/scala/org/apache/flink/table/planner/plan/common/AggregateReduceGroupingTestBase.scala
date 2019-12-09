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
package org.apache.flink.table.planner.plan.common

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.api.Types
import org.apache.flink.table.plan.stats.TableStats
import org.apache.flink.table.planner.plan.optimize.program.FlinkBatchProgram
import org.apache.flink.table.planner.plan.rules.logical.FlinkAggregateRemoveRule
import org.apache.flink.table.planner.plan.stats.FlinkStatistic
import org.apache.flink.table.planner.utils.{BatchTableTestUtil, TableTestBase}

import com.google.common.collect.ImmutableSet
import org.apache.calcite.tools.RuleSets
import org.junit.{Before, Test}

abstract class AggregateReduceGroupingTestBase extends TableTestBase {
  protected val util: BatchTableTestUtil = batchTestUtil()

  @Before
  def setup(): Unit = {
    util.addTableSource("T1",
      Array[TypeInformation[_]](Types.INT, Types.INT, Types.STRING, Types.STRING),
      Array("a1", "b1", "c1", "d1"),
      FlinkStatistic.builder()
        .tableStats(new TableStats(100000000))
        .uniqueKeys(ImmutableSet.of(ImmutableSet.of("a1")))
        .build()
    )
    util.addTableSource("T2",
      Array[TypeInformation[_]](Types.INT, Types.INT, Types.STRING),
      Array("a2", "b2", "c2"),
      FlinkStatistic.builder()
        .tableStats(new TableStats(100000000))
        .uniqueKeys(ImmutableSet.of(ImmutableSet.of("b2"), ImmutableSet.of("a2", "b2")))
        .build()
    )
    util.addTableSource("T3",
      Array[TypeInformation[_]](Types.INT, Types.INT, Types.STRING, Types.LONG),
      Array("a3", "b3", "c3", "d3"),
      FlinkStatistic.builder()
        .tableStats(new TableStats(1000))
        .build()
    )
    util.addTableSource("T4",
      Array[TypeInformation[_]](Types.INT, Types.INT, Types.STRING, Types.LOCAL_DATE_TIME),
      Array("a4", "b4", "c4", "d4"),
      FlinkStatistic.builder()
        .tableStats(new TableStats(100000000))
        .uniqueKeys(ImmutableSet.of(ImmutableSet.of("a4")))
        .build()
    )
  }

  @Test
  def testAggWithoutAggCall(): Unit = {
    val programs = util.getBatchProgram()
    programs.getFlinkRuleSetProgram(FlinkBatchProgram.LOGICAL)
      .get.remove(RuleSets.ofList(FlinkAggregateRemoveRule.INSTANCE))
    // to prevent the agg from removing
    util.replaceBatchProgram(programs)
    util.verifyPlan("SELECT a1, b1, c1 FROM T1 GROUP BY a1, b1, c1")
  }

  @Test
  def testAggWithoutReduceGrouping(): Unit = {
    util.verifyPlan("SELECT a3, b3, count(c3) FROM T3 GROUP BY a3, b3")
  }

  @Test
  def testSingleAggOnTableWithUniqueKey(): Unit = {
    util.verifyPlan("SELECT a1, b1, count(c1) FROM T1 GROUP BY a1, b1")
  }

  @Test
  def testSingleAggOnTableWithoutUniqueKey(): Unit = {
    util.verifyPlan("SELECT a3, b3, count(c3) FROM T3 GROUP BY a3, b3")
  }

  @Test
  def testSingleAggOnTableWithUniqueKeys(): Unit = {
    util.verifyPlan("SELECT  b2, c2, avg(a2) FROM T2 GROUP BY b2, c2")
  }

  @Test
  def testSingleAggWithConstantGroupKey(): Unit = {
    util.verifyPlan("SELECT a1, b1, count(c1) FROM T1 GROUP BY a1, b1, 1, true")
  }

  @Test
  def testSingleAggOnlyConstantGroupKey(): Unit = {
    util.verifyPlan("SELECT count(c1) FROM T1 GROUP BY 1, true")
  }

  @Test
  def testMultiAggs1(): Unit = {
    util.verifyPlan("SELECT a1, b1, c1, d1, m, COUNT(*) FROM " +
      "(SELECT a1, b1, c1, COUNT(d1) AS d1, MAX(d1) AS m FROM T1 GROUP BY a1, b1, c1) t " +
      "GROUP BY a1, b1, c1, d1, m")
  }

  @Test
  def testMultiAggs2(): Unit = {
    util.verifyPlan("SELECT a3, b3, c, s, a, COUNT(*) FROM " +
      "(SELECT a3, b3, COUNT(c3) AS c, SUM(d3) AS s, AVG(d3) AS a FROM T3 GROUP BY a3, b3) t " +
      "GROUP BY a3, b3, c, s, a")
  }

  @Test
  def testAggOnInnerJoin1(): Unit = {
    util.verifyPlan("SELECT a1, b1, a2, b2, COUNT(c1) FROM " +
      "(SELECT * FROM T1, T2 WHERE a1 = b2) t GROUP BY a1, b1, a2, b2")
  }

  @Test
  def testAggOnInnerJoin2(): Unit = {
    util.verifyPlan("SELECT a2, b2, a3, b3, COUNT(c2), AVG(d3) FROM " +
      "(SELECT * FROM T2, T3 WHERE b2 = a3) t GROUP BY a2, b2, a3, b3")
  }

  @Test
  def testAggOnInnerJoin3(): Unit = {
    util.verifyPlan("SELECT a1, b1, a2, b2, a3, b3, COUNT(c1) FROM " +
      "(SELECT * FROM T1, T2, T3 WHERE a1 = b2 AND a1 = a3) t GROUP BY a1, b1, a2, b2, a3, b3")
  }

  @Test
  def testAggOnLeftJoin1(): Unit = {
    util.verifyPlan("SELECT a1, b1, a2, b2, COUNT(c1) FROM " +
      "(SELECT * FROM T1 LEFT JOIN T2 ON a1 = b2) t GROUP BY a1, b1, a2, b2")
  }

  @Test
  def testAggOnLeftJoin2(): Unit = {
    util.verifyPlan("SELECT a1, b1, a3, b3, COUNT(c1) FROM " +
      "(SELECT * FROM T1 LEFT JOIN T3 ON a1 = a3) t GROUP BY a1, b1, a3, b3")
  }

  @Test
  def testAggOnLeftJoin3(): Unit = {
    util.verifyPlan("SELECT a3, b3, a1, b1, COUNT(c1) FROM " +
      "(SELECT * FROM T3 LEFT JOIN T1 ON a1 = a3) t GROUP BY a3, b3, a1, b1")
  }

  @Test
  def testAggOnRightJoin1(): Unit = {
    util.verifyPlan("SELECT a1, b1, a2, b2, COUNT(c1) FROM " +
      "(SELECT * FROM T1 RIGHT JOIN T2 ON a1 = b2) t GROUP BY a1, b1, a2, b2")
  }

  @Test
  def testAggOnRightJoin2(): Unit = {
    util.verifyPlan("SELECT a1, b1, a3, b3, COUNT(c1) FROM " +
      "(SELECT * FROM T1 RIGHT JOIN T3 ON a1 = a3) t GROUP BY a1, b1, a3, b3")
  }

  @Test
  def testAggOnRightJoin3(): Unit = {
    util.verifyPlan("SELECT a3, b3, a1, b1, COUNT(c1) FROM " +
      "(SELECT * FROM T3 RIGHT JOIN T1 ON a1 = a3) t GROUP BY a3, b3, a1, b1")
  }

  @Test
  def testAggOnFullJoin1(): Unit = {
    util.verifyPlan("SELECT a1, b1, a2, b2, COUNT(c1) FROM " +
      "(SELECT * FROM T1 FULL OUTER JOIN T2 ON a1 = b2) t GROUP BY a1, b1, a2, b2")
  }

  @Test
  def testAggOnFullJoin2(): Unit = {
    util.verifyPlan("SELECT a1, b1, a3, b3, COUNT(c1) FROM " +
      "(SELECT * FROM T1 FULL OUTER JOIN T3 ON a1 = a3) t GROUP BY a1, b1, a3, b3")
  }

  @Test
  def testAggOnOver(): Unit = {
    util.verifyPlan("SELECT a1, b1, c, COUNT(d1) FROM " +
      "(SELECT a1, b1, d1, COUNT(*) OVER (PARTITION BY c1) AS c FROM T1) t GROUP BY a1, b1, c")
  }

  @Test
  def testAggOnWindow1(): Unit = {
    util.verifyPlan("SELECT a4, b4, COUNT(c4) FROM T4 " +
      "GROUP BY a4, b4, TUMBLE(d4, INTERVAL '15' MINUTE)")
  }

  @Test
  def testAggOnWindow2(): Unit = {
    util.verifyPlan("SELECT a4, c4, COUNT(b4), AVG(b4) FROM T4 " +
      "GROUP BY a4, c4, TUMBLE(d4, INTERVAL '15' MINUTE)")
  }

  @Test
  def testAggOnWindow3(): Unit = {
    util.verifyPlan("SELECT a4, c4, s, COUNT(b4) FROM " +
      "(SELECT a4, c4, VAR_POP(b4) AS b4, " +
      "TUMBLE_START(d4, INTERVAL '15' MINUTE) AS s, " +
      "TUMBLE_END(d4, INTERVAL '15' MINUTE) AS e FROM T4 " +
      "GROUP BY a4, c4, TUMBLE(d4, INTERVAL '15' MINUTE)) t GROUP BY a4, c4, s")
  }

  @Test
  def testAggOnWindow4(): Unit = {
    util.verifyPlan("SELECT a4, c4, e, COUNT(b4) FROM " +
      "(SELECT a4, c4, VAR_POP(b4) AS b4, " +
      "TUMBLE_START(d4, INTERVAL '15' MINUTE) AS s, " +
      "TUMBLE_END(d4, INTERVAL '15' MINUTE) AS e FROM T4 " +
      "GROUP BY a4, c4, TUMBLE(d4, INTERVAL '15' MINUTE)) t GROUP BY a4, c4, e")
  }

  @Test
  def testAggOnWindow5(): Unit = {
    util.verifyPlan("SELECT a4, b4, c4, COUNT(*) FROM " +
      "(SELECT a4, c4, VAR_POP(b4) AS b4, " +
      "TUMBLE_START(d4, INTERVAL '15' MINUTE) AS s, " +
      "TUMBLE_END(d4, INTERVAL '15' MINUTE) AS e FROM T4 " +
      "GROUP BY a4, c4, TUMBLE(d4, INTERVAL '15' MINUTE)) t GROUP BY a4, b4, c4")
  }

  @Test
  def testAggWithGroupingSets1(): Unit = {
    util.verifyPlan("SELECT a1, b1, c1, COUNT(d1) FROM T1 " +
      "GROUP BY GROUPING SETS ((a1, b1), (a1, c1))")
  }

  @Test
  def testAggWithGroupingSets2(): Unit = {
    util.verifyPlan("SELECT a1, SUM(b1) AS s FROM T1 GROUP BY GROUPING SETS((a1, c1), (a1), ())")
  }

  @Test
  def testAggWithGroupingSets3(): Unit = {
    util.verifyPlan("SELECT a1, b1, c1, COUNT(d1) FROM T1 " +
      "GROUP BY GROUPING SETS ((a1, b1, c1), (a1, b1, d1))")
  }

  @Test
  def testAggWithRollup(): Unit = {
    util.verifyPlan("SELECT a1, b1, c1, COUNT(d1) FROM T1 GROUP BY ROLLUP (a1, b1, c1)")
  }

  @Test
  def testAggWithCube(): Unit = {
    util.verifyPlan("SELECT a1, b1, c1, COUNT(d1) FROM T1 GROUP BY CUBE (a1, b1, c1)")
  }

  @Test
  def testSingleDistinctAgg1(): Unit = {
    util.verifyPlan("SELECT a1, COUNT(DISTINCT c1) FROM T1 GROUP BY a1")
  }

  @Test
  def testSingleDistinctAgg2(): Unit = {
    util.verifyPlan("SELECT a1, b1, COUNT(DISTINCT c1) FROM T1 GROUP BY a1, b1")
  }

  @Test
  def testSingleDistinctAgg_WithNonDistinctAgg1(): Unit = {
    util.verifyPlan("SELECT a1, COUNT(DISTINCT b1), SUM(b1) FROM T1 GROUP BY a1")
  }

  @Test
  def testSingleDistinctAgg_WithNonDistinctAgg2(): Unit = {
    util.verifyPlan("SELECT a1, c1, COUNT(DISTINCT b1), SUM(b1) FROM T1 GROUP BY a1, c1")
  }

  @Test
  def testSingleDistinctAgg_WithNonDistinctAgg3(): Unit = {
    util.verifyPlan("SELECT a1, COUNT(DISTINCT c1), SUM(b1) FROM T1 GROUP BY a1")
  }

  @Test
  def testSingleDistinctAgg_WithNonDistinctAgg4(): Unit = {
    util.verifyPlan("SELECT a1, d1, COUNT(DISTINCT c1), SUM(b1) FROM T1 GROUP BY a1, d1")
  }

  @Test
  def testMultiDistinctAggs1(): Unit = {
    util.verifyPlan("SELECT a1, COUNT(DISTINCT b1), SUM(DISTINCT b1) FROM T1 GROUP BY a1")
  }

  @Test
  def testMultiDistinctAggs2(): Unit = {
    util.verifyPlan("SELECT a1, d1, COUNT(DISTINCT c1), SUM(DISTINCT b1) FROM T1 GROUP BY a1, d1")
  }

  @Test
  def testMultiDistinctAggs3(): Unit = {
    util.verifyPlan(
      "SELECT a1, SUM(DISTINCT b1), MAX(DISTINCT b1), MIN(DISTINCT c1) FROM T1 GROUP BY a1")
  }

  @Test
  def testMultiDistinctAggs_WithNonDistinctAgg1(): Unit = {
    util.verifyPlan(
      "SELECT a1, d1, COUNT(DISTINCT c1), MAX(DISTINCT b1), SUM(b1) FROM T1 GROUP BY a1, d1")
  }

}

