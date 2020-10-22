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

package org.apache.flink.table.api.batch

import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.config.ExecutionConfigOptions
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge
import org.apache.flink.table.api.internal.{TableEnvironmentImpl, TableEnvironmentInternal}
import org.apache.flink.table.planner.delegation.BatchPlanner
import org.apache.flink.table.planner.plan.nodes.physical.batch.{BatchExecHashJoin, BatchExecMultipleInputNode}
import org.apache.flink.table.planner.plan.utils.FlinkRelOptUtil
import org.apache.flink.table.planner.utils.{ExecutorUtils, PlanUtil, TableTestBase, TableTestUtil}
import org.apache.flink.table.types.logical.{BigIntType, IntType, VarCharType}

import org.apache.calcite.rel.RelNode
import org.junit.Assert.assertEquals
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.{Before, Test}

import java.util.Collections

@RunWith(classOf[Parameterized])
class ExplainTest(extended: Boolean) extends TableTestBase {

  private val extraDetails = if (extended) {
    Array(ExplainDetail.CHANGELOG_MODE, ExplainDetail.ESTIMATED_COST)
  } else {
    Array.empty[ExplainDetail]
  }

  private val util = batchTestUtil()
  util.addTableSource[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
  util.addDataStream[(Int, Long, String)]("MyTable1", 'a, 'b, 'c)
  util.addDataStream[(Int, Long, String)]("MyTable2", 'd, 'e, 'f)

  val STRING = new VarCharType(VarCharType.MAX_LENGTH)
  val LONG = new BigIntType()
  val INT = new IntType()

  @Before
  def before(): Unit = {
    util.tableEnv.getConfig.getConfiguration.setInteger(
      ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 4)
  }

  @Test
  def testExplainWithTableSourceScan(): Unit = {
    util.verifyExplain("SELECT * FROM MyTable", extraDetails: _*)
  }

  @Test
  def testExplainWithDataStreamScan(): Unit = {
    util.verifyExplain("SELECT * FROM MyTable1", extraDetails: _*)
  }

  @Test
  def testExplainWithFilter(): Unit = {
    util.verifyExplain("SELECT * FROM MyTable1 WHERE mod(a, 2) = 0", extraDetails: _*)
  }

  @Test
  def testExplainWithAgg(): Unit = {
    util.verifyExplain("SELECT COUNT(*) FROM MyTable1 GROUP BY a", extraDetails: _*)
  }

  @Test
  def testExplainWithJoin(): Unit = {
    // TODO support other join operators when them are supported
    util.tableEnv.getConfig.getConfiguration.setString(
      ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "HashJoin, NestedLoopJoin")
    util.verifyExplain("SELECT a, b, c, e, f FROM MyTable1, MyTable2 WHERE a = d", extraDetails: _*)
  }

  @Test
  def testExplainWithUnion(): Unit = {
    util.verifyExplain("SELECT * FROM MyTable1 UNION ALL SELECT * FROM MyTable2", extraDetails: _*)
  }

  @Test
  def testExplainWithSort(): Unit = {
    util.verifyExplain("SELECT * FROM MyTable1 ORDER BY a LIMIT 5", extraDetails: _*)
  }

  @Test
  def testExplainWithSingleSink(): Unit = {
    val table = util.tableEnv.sqlQuery("SELECT * FROM MyTable1 WHERE a > 10")
    val sink = util.createCollectTableSink(Array("a", "b", "c"), Array(INT, LONG, STRING))
    util.verifyExplainInsert(table, sink, "sink", extraDetails: _*)
  }

  @Test
  def testExplainWithMultiSinks(): Unit = {
    val stmtSet = util.tableEnv.createStatementSet()
    val table = util.tableEnv.sqlQuery("SELECT a, COUNT(*) AS cnt FROM MyTable1 GROUP BY a")
    util.tableEnv.registerTable("TempTable", table)

    val table1 = util.tableEnv.sqlQuery("SELECT * FROM TempTable WHERE cnt > 10")
    val sink1 = util.createCollectTableSink(Array("a", "cnt"), Array(INT, LONG))
    util.tableEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal("sink1", sink1)
    stmtSet.addInsert("sink1", table1)

    val table2 = util.tableEnv.sqlQuery("SELECT * FROM TempTable WHERE cnt < 10")
    val sink2 = util.createCollectTableSink(Array("a", "cnt"), Array(INT, LONG))
    util.tableEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal("sink2", sink2)
    stmtSet.addInsert("sink2", table2)

    util.verifyExplain(stmtSet, extraDetails: _*)
  }

  @Test
  def testExplainMultipleInput(): Unit = {
    util.tableEnv.getConfig.getConfiguration.setString(
      ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "NestedLoopJoin,SortMergeJoin")
    val sql =
      """
        |select * from
        |   (select a, sum(b) from MyTable1 group by a) v1,
        |   (select d, sum(e) from MyTable2 group by d) v2
        |   where a = d
        |""".stripMargin
    // TODO use `util.verifyExplain` to verify the result
    //  after supporting converting rel nodes to multiple input node
    val relNode = TableTestUtil.toRelNode(util.tableEnv.sqlQuery(sql))
    val batchRelNode = util.getPlanner.optimize(relNode).asInstanceOf[BatchExecHashJoin]
    val firstBorder = batchRelNode.getInputNodes.get(0)
    val firstInput = firstBorder.getInputNodes.get(0)
    val firstEdge = firstBorder.getInputEdges.get(0)
    val secondBorder = batchRelNode.getInputNodes.get(1)
    val secondInput = secondBorder.getInputNodes.get(0)
    val secondEdge = secondBorder.getInputEdges.get(0)
    val multipleInputRel = new BatchExecMultipleInputNode(
      batchRelNode.getCluster,
      batchRelNode.getTraitSet,
      Array(firstInput.asInstanceOf[RelNode], secondInput.asInstanceOf[RelNode]),
      batchRelNode,
      Array(
        ExecEdge.builder()
          .requiredShuffle(firstEdge.getRequiredShuffle)
          .damBehavior(firstEdge.getDamBehavior)
          .priority(0)
          .build(),
        ExecEdge.builder()
          .requiredShuffle(secondEdge.getRequiredShuffle)
          .damBehavior(secondEdge.getDamBehavior)
          .priority(1)
          .build()))

    val ast = FlinkRelOptUtil.toString(multipleInputRel)

    val transform = multipleInputRel.translateToPlan(
      util.getTableEnv.asInstanceOf[TableEnvironmentImpl].getPlanner.asInstanceOf[BatchPlanner])
    val streamGraph = ExecutorUtils.generateStreamGraph(
      util.getStreamEnv, Collections.singletonList(transform))
    val executionPlan = PlanUtil.explainStreamGraph(streamGraph)

    val actual = ast + "\n" + executionPlan

    val expected = TableTestUtil.readFromResource("/explain/testExplainMultipleInput.out")
    assertEquals(TableTestUtil.replaceStageId(expected), TableTestUtil.replaceStageId(actual))
  }

}

object ExplainTest {
  @Parameterized.Parameters(name = "extended={0}")
  def parameters(): java.util.Collection[Boolean] = {
    java.util.Arrays.asList(true, false)
  }
}
