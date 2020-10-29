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

package org.apache.flink.table.api.stream

import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.config.{ExecutionConfigOptions, OptimizerConfigOptions}
import org.apache.flink.table.api.internal.{TableEnvironmentImpl, TableEnvironmentInternal}
import org.apache.flink.table.planner.delegation.StreamPlanner
import org.apache.flink.table.planner.plan.nodes.physical.stream.{StreamExecJoin, StreamExecMultipleInput}
import org.apache.flink.table.planner.plan.utils.FlinkRelOptUtil
import org.apache.flink.table.planner.utils.{ExecutorUtils, PlanUtil, TableTestBase, TableTestUtil}
import org.apache.flink.table.types.logical.{BigIntType, IntType, VarCharType}

import org.apache.calcite.rel.RelNode
import org.junit.Assert.assertEquals
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.{Before, Test}

import java.sql.Timestamp
import java.time.Duration
import java.util.Collections

@RunWith(classOf[Parameterized])
class ExplainTest(extended: Boolean) extends TableTestBase {

  private val extraDetails = if (extended) {
    Array(ExplainDetail.CHANGELOG_MODE, ExplainDetail.ESTIMATED_COST)
  } else {
    Array.empty[ExplainDetail]
  }

  private val util = streamTestUtil()
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
  def testExplainTableSourceScan(): Unit = {
    util.verifyExplain("SELECT * FROM MyTable", extraDetails:_*)
  }

  @Test
  def testExplainDataStreamScan(): Unit = {
    util.verifyExplain("SELECT * FROM MyTable1", extraDetails:_*)
  }

  @Test
  def testExplainWithFilter(): Unit = {
    util.verifyExplain("SELECT * FROM MyTable1 WHERE mod(a, 2) = 0", extraDetails:_*)
  }

  @Test
  def testExplainWithAgg(): Unit = {
    util.verifyExplain("SELECT COUNT(*) FROM MyTable1 GROUP BY a", extraDetails:_*)
  }

  @Test
  def testExplainWithJoin(): Unit = {
    util.verifyExplain("SELECT a, b, c, e, f FROM MyTable1, MyTable2 WHERE a = d", extraDetails:_*)
  }

  @Test
  def testExplainWithUnion(): Unit = {
    util.verifyExplain("SELECT * FROM MyTable1 UNION ALL SELECT * FROM MyTable2", extraDetails:_*)
  }

  @Test
  def testExplainWithSort(): Unit = {
    util.verifyExplain("SELECT * FROM MyTable1 ORDER BY a LIMIT 5", extraDetails:_*)
  }

  @Test
  def testExplainWithSingleSink(): Unit = {
    val table = util.tableEnv.sqlQuery("SELECT * FROM MyTable1 WHERE a > 10")
    val appendSink = util.createAppendTableSink(Array("a", "b", "c"), Array(INT, LONG, STRING))
    util.verifyExplainInsert(table, appendSink, "appendSink", extraDetails: _*)
  }

  @Test
  def testExplainWithMultiSinks(): Unit = {
    val stmtSet = util.tableEnv.createStatementSet()
    val table = util.tableEnv.sqlQuery("SELECT a, COUNT(*) AS cnt FROM MyTable1 GROUP BY a")
    util.tableEnv.registerTable("TempTable", table)

    val table1 = util.tableEnv.sqlQuery("SELECT * FROM TempTable WHERE cnt > 10")
    val upsertSink1 = util.createUpsertTableSink(Array(0), Array("a", "cnt"), Array(INT, LONG))
    util.tableEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal(
      "upsertSink1", upsertSink1)
    stmtSet.addInsert("upsertSink1", table1)

    val table2 = util.tableEnv.sqlQuery("SELECT * FROM TempTable WHERE cnt < 10")
    val upsertSink2 = util.createUpsertTableSink(Array(0), Array("a", "cnt"), Array(INT, LONG))
    util.tableEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal(
      "upsertSink2", upsertSink2)
    stmtSet.addInsert("upsertSink2", table2)

    util.verifyExplain(stmtSet, extraDetails: _*)
  }

  @Test
  def testMiniBatchIntervalInfer(): Unit = {
    val stmtSet = util.tableEnv.createStatementSet()
    // Test emit latency propagate among RelNodeBlocks
    util.addDataStream[(Int, String, Timestamp)]("T1", 'id1, 'text, 'rowtime.rowtime)
    util.addDataStream[(Int, String, Int, String, Long, Timestamp)](
      "T2", 'id2, 'cnt, 'name, 'goods, 'rowtime.rowtime)
    util.addTableWithWatermark("T3", util.tableEnv.from("T1"), "rowtime", 0)
    util.addTableWithWatermark("T4", util.tableEnv.from("T2"), "rowtime", 0)
    util.tableEnv.getConfig.getConfiguration.setBoolean(
      ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED, true)
    util.tableEnv.getConfig.getConfiguration.set(
      ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ALLOW_LATENCY, Duration.ofSeconds(3))
    val table = util.tableEnv.sqlQuery(
      """
        |SELECT id1, T3.rowtime AS ts, text
        |  FROM T3, T4
        |WHERE id1 = id2
        |      AND T3.rowtime > T4.rowtime - INTERVAL '5' MINUTE
        |      AND T3.rowtime < T4.rowtime + INTERVAL '3' MINUTE
      """.stripMargin)
    util.tableEnv.registerTable("TempTable", table)

    val table1 = util.tableEnv.sqlQuery(
      """
        |SELECT id1, LISTAGG(text, '#')
        |FROM TempTable
        |GROUP BY id1, TUMBLE(ts, INTERVAL '8' SECOND)
      """.stripMargin)
    val appendSink1 = util.createAppendTableSink(Array("a", "b"), Array(INT, STRING))
    util.tableEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal(
      "appendSink1", appendSink1)
    stmtSet.addInsert("appendSink1", table1)

    val table2 = util.tableEnv.sqlQuery(
      """
        |SELECT id1, LISTAGG(text, '*')
        |FROM TempTable
        |GROUP BY id1, HOP(ts, INTERVAL '12' SECOND, INTERVAL '6' SECOND)
      """.stripMargin)
    val appendSink2 = util.createAppendTableSink(Array("a", "b"), Array(INT, STRING))
    util.tableEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal(
      "appendSink2", appendSink2)
    stmtSet.addInsert("appendSink2", table2)

    util.verifyExplain(stmtSet, extraDetails: _*)
  }

  @Test
  def testExplainMultipleInput(): Unit = {
    val sql =
      """
        |select * from
        |   (select a, sum(b) from MyTable1 group by a) v1,
        |   (select d, sum(e) from MyTable2 group by d) v2
        |   where a = d
        |""".stripMargin

    // TODO use `util.verifyExplain` to verify the result once FLINK-19822 is finished
    val relNode = TableTestUtil.toRelNode(util.tableEnv.sqlQuery(sql))
    val streamRelNode = util.getPlanner.optimize(relNode).asInstanceOf[StreamExecJoin]
    val firstExchange = streamRelNode.getInput(0)
    val firstBorder = firstExchange.getInput(0)
    // remove exchange
    streamRelNode.replaceInput(0, firstBorder)
    val firstInput = firstBorder.getInput(0)

    val secondExchange = streamRelNode.getInput(1)
    val secondBorder = secondExchange.getInput(0)
    // remove exchange
    streamRelNode.replaceInput(1, secondBorder)
    val secondInput = secondBorder.getInput(0)

    val multipleInputRel = new StreamExecMultipleInput(
      streamRelNode.getCluster,
      streamRelNode.getTraitSet,
      Array(firstInput.asInstanceOf[RelNode], secondInput.asInstanceOf[RelNode]),
      streamRelNode)

    val ast = FlinkRelOptUtil.toString(multipleInputRel)

    val transform = multipleInputRel.translateToPlan(
      util.getTableEnv.asInstanceOf[TableEnvironmentImpl].getPlanner.asInstanceOf[StreamPlanner])
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
