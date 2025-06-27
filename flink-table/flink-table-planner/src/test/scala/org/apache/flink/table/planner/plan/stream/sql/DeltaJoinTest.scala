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
package org.apache.flink.table.planner.plan.stream.sql

import org.apache.flink.table.api.{DataTypes, ExplainDetail, Schema}
import org.apache.flink.table.api.config.{ExecutionConfigOptions, OptimizerConfigOptions}
import org.apache.flink.table.api.config.ExecutionConfigOptions.UpsertMaterialize
import org.apache.flink.table.api.config.OptimizerConfigOptions.DeltaJoinStrategy
import org.apache.flink.table.catalog.{CatalogTable, ObjectPath, ResolvedCatalogTable}
import org.apache.flink.table.planner.JMap
import org.apache.flink.table.planner.utils.{TableTestBase, TestingTableEnvironment}

import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.{BeforeEach, Test}

import java.util.{Collections, HashMap => JHashMap}

/** Test for delta join. */
class DeltaJoinTest extends TableTestBase {

  private val util = streamTestUtil()
  private val tEnv: TestingTableEnvironment = util.tableEnv.asInstanceOf[TestingTableEnvironment]

  private val testComment = "test comment"
  private val testValuesTableOptions: JMap[String, String] = {
    val options = new JHashMap[String, String]()
    options.put("connector", "values")
    options.put("bounded", "false")
    options.put("sink-insert-only", "false")
    options.put("sink-changelog-mode-enforced", "I,UA,UB,D")
    options.put("async", "true")
    options
  }

  @BeforeEach
  def setup(): Unit = {
    util.tableConfig.set(
      OptimizerConfigOptions.TABLE_OPTIMIZER_DELTA_JOIN_STRATEGY,
      OptimizerConfigOptions.DeltaJoinStrategy.AUTO)

    addTable(
      "src1",
      Schema
        .newBuilder()
        .column("a0", DataTypes.INT.notNull)
        .column("a1", DataTypes.DOUBLE.notNull)
        .column("a2", DataTypes.STRING)
        .column("a3", DataTypes.INT)
        .index("a1", "a2")
        .build()
    )

    addTable(
      "src2",
      Schema
        .newBuilder()
        .column("b0", DataTypes.INT.notNull)
        .column("b2", DataTypes.STRING)
        .column("b1", DataTypes.DOUBLE)
        .index("b2")
        .build()
    )

    addTable(
      "snk",
      Schema
        .newBuilder()
        .column("l0", DataTypes.INT.notNull)
        .column("l1", DataTypes.DOUBLE)
        .column("l2", DataTypes.STRING)
        .column("l3", DataTypes.INT)
        .column("r0", DataTypes.INT.notNull)
        .column("r2", DataTypes.STRING)
        .column("r1", DataTypes.DOUBLE)
        .primaryKey("l0", "r0")
        .build()
    )
  }

  @Test
  def testJoinKeysContainIndexOnBothSide(): Unit = {
    util.verifyRelPlanInsert(
      "insert into snk select * from src1 join src2 " +
        "on src1.a1 = src2.b1 " +
        "and src1.a2 = src2.b2")
  }

  @Test
  def testJoinKeysContainIndexOnBothSide2(): Unit = {
    util.verifyRelPlanInsert(
      "insert into snk select * from src1 join src2 " +
        "on src1.a1 = src2.b1 " +
        "and src1.a2 = src2.b2 " +
        "and src1.a3 = src2.b0")
  }

  @Test
  def testJoinKeysNotContainIndexOnOneSide(): Unit = {
    // could not optimize into delta join because join keys do not contain indexes strictly
    util.verifyRelPlanInsert(
      "insert into snk select * from src1 join src2 " +
        "on src1.a2 = src2.b2")
  }

  @Test
  def testWithNonEquiCondition1(): Unit = {
    util.verifyRelPlanInsert(
      "insert into snk select * from src1 join src2 " +
        "on src1.a1 = src2.b1 " +
        "and src1.a2 = src2.b2 " +
        "and src2.b0 > src1.a0")
  }

  @Test
  def testWithNonEquiCondition2(): Unit = {
    // could not optimize into delta join because there is a calc between join and source
    util.verifyRelPlanInsert(
      "insert into snk select * from src1 join src2 " +
        "on src1.a1 = src2.b1 " +
        "and src1.a2 = src2.b2 " +
        "and src2.b0 > src1.a0 " +
        "and src2.b2 <> 'Hello' " +
        "and src1.a0 > 99")
  }

  @Test
  def testJsonPlanWithTableHints(): Unit = {
    util.verifyJsonPlan(
      "insert into snk select * from src1 /*+ OPTIONS('failing-source'='true') */" +
        "join src2 /*+ OPTIONS('failing-source'='true') */" +
        "on src1.a1 = src2.b1 " +
        "and src1.a2 = src2.b2 " +
        "and src2.b0 > src1.a0")
  }

  @Test
  def testProjectFieldsBeforeJoin(): Unit = {
    // could not optimize into delta join because the source has ProjectPushDownSpec
    util.verifyRelPlanInsert(
      "insert into snk(l0, l1, l2, r0, r2, r1) " +
        "select * from ( " +
        "  select a0, a1, a2 from src1" +
        ") tmp join src2 " +
        "on tmp.a1 = src2.b1 " +
        "and tmp.a2 = src2.b2")
  }

  @Test
  def testProjectFieldsAfterJoin(): Unit = {
    util.verifyRelPlanInsert(
      "insert into snk select a0, a1 + 1.1, a2, a3, b0, b2, b1 from src1 join src2 " +
        "on src1.a1 = src2.b1 " +
        "and src1.a2 = src2.b2")
  }

  @Test
  def testFilterFieldsBeforeJoin(): Unit = {
    // could not optimize into delta join because there is a calc between source and join
    util.verifyRelPlanInsert(
      "insert into snk select * from (  " +
        "  select * from src1 where a1 > 1.1 " +
        ") tmp join src2 " +
        "on tmp.a1 = src2.b1 " +
        "and tmp.a2 = src2.b2")
  }

  @Test
  def testFilterFieldsAfterJoin(): Unit = {
    util.verifyRelPlanInsert(
      "insert into snk select * from src1 join src2 " +
        "on src1.a1 = src2.b1 " +
        "and src1.a2 = src2.b2 " +
        "where a3 > b0")
  }

  @Test
  def testMultiRootsWithReusingJoinView(): Unit = {
    util.tableConfig.set(
      OptimizerConfigOptions.TABLE_OPTIMIZER_REUSE_OPTIMIZE_BLOCK_WITH_DIGEST_ENABLED,
      Boolean.box(true))

    util.tableEnv.executeSql("create table snk2 like snk")

    util.tableEnv.executeSql(
      "create temporary view mv as " +
        "select * from src1 join src2 " +
        "on src1.a1 = src2.b1 " +
        "and src1.a2 = src2.b2")

    val stmt = tEnv.createStatementSet()
    stmt.addInsertSql("insert into snk select * from mv")
    stmt.addInsertSql("insert into snk2 select * from mv")

    util.verifyExecPlan(stmt)
  }

  @Test
  def testMultiRootsWithReusingJoinView2(): Unit = {
    util.tableConfig.set(
      OptimizerConfigOptions.TABLE_OPTIMIZER_REUSE_OPTIMIZE_BLOCK_WITH_DIGEST_ENABLED,
      Boolean.box(true))

    util.tableEnv.executeSql(
      "create table snk2 like snk(" +
        "  EXCLUDING CONSTRAINTS" +
        ")")

    util.tableEnv.executeSql(
      "create temporary view mv as " +
        "select * from src1 join src2 " +
        "on src1.a1 = src2.b1 " +
        "and src1.a2 = src2.b2")

    val stmt = tEnv.createStatementSet()
    stmt.addInsertSql("insert into snk select * from mv")
    stmt.addInsertSql("insert into snk2 select * from mv")

    // the join could not be optimized to delta join
    // because one of the sink doesn't satisfy the requirement
    util.verifyExecPlan(stmt)
  }

  @Test
  def testMultiRootsWithReusingDeltaJoin(): Unit = {
    util.tableEnv.executeSql("create table snk2 like snk")

    val stmt = tEnv.createStatementSet()
    stmt.addInsertSql(
      "insert into snk select * from src1 join src2 " +
        "on src1.a1 = src2.b1 " +
        "and src1.a2 = src2.b2")

    stmt.addInsertSql(
      "insert into snk2 select * from src1 join src2 " +
        "on src1.a1 = src2.b1 " +
        "and src1.a2 = src2.b2")

    util.verifyExecPlan(stmt)
  }

  @Test
  def testMultiRootsWithoutReusingDeltaJoin(): Unit = {
    util.tableEnv.executeSql(
      "create table snk2 like snk(" +
        "  EXCLUDING CONSTRAINTS" +
        ")")

    val stmt = tEnv.createStatementSet()
    stmt.addInsertSql(
      "insert into snk select * from src1 join src2 " +
        "on src1.a1 = src2.b1 " +
        "and src1.a2 = src2.b2")

    stmt.addInsertSql(
      "insert into snk2 select * from src1 join src2 " +
        "on src1.a1 = src2.b1 " +
        "and src1.a2 = src2.b2")

    util.verifyExecPlan(stmt)
  }

  @Test
  def testExplainPlanAdvice(): Unit = {
    util.verifyExplainInsert(
      "insert into snk select * from src1 join src2 " +
        "on src1.a1 = src2.b1 " +
        "and src1.a2 = src2.b2",
      ExplainDetail.PLAN_ADVICE)
  }

  @Test
  def testWithWatermarkAssigner(): Unit = {
    addTable(
      "wm_source",
      Schema
        .newBuilder()
        .column("a0", DataTypes.INT)
        .column("a1", DataTypes.STRING)
        .column("a2", DataTypes.TIMESTAMP(3))
        .watermark("a2", "a2 - INTERVAL '1' SECOND")
        .index("a1")
        .build()
    )
    addTable(
      "tmp_sink",
      Schema
        .newBuilder()
        .column("l0", DataTypes.INT)
        .column("l1", DataTypes.STRING)
        .column("l2", DataTypes.TIMESTAMP(3))
        .column("r0", DataTypes.INT.notNull)
        .column("r2", DataTypes.STRING)
        .column("r1", DataTypes.DOUBLE)
        .build()
    )

    // could not optimize into delta join because there is a calc between join and source
    util.verifyRelPlanInsert(
      "insert into tmp_sink select * from wm_source join src2 " +
        "on wm_source.a1 = src2.b2")
  }

  @Test
  def testWithoutLookupTable(): Unit = {
    util.tableEnv.executeSql(
      "create table non_lookup_src with ('disable-lookup' = 'true') " +
        "like src2 (OVERWRITING OPTIONS)")

    util.verifyRelPlanInsert(
      "insert into snk select * from src1 join non_lookup_src " +
        "on src1.a1 = non_lookup_src.b1 " +
        "and src1.a2 = non_lookup_src.b2")
  }

  @Test
  def testConstantConditionInIndex(): Unit = {
    // could not optimize into delta join because there is a calc between join and source
    util.verifyRelPlanInsert(
      "insert into snk select * from src1 join src2 " +
        "on src1.a1 = 1.1 " +
        "and src1.a2 = src2.b2")
  }

  @Test
  def testComputeIndexKeyOnJoinCondition(): Unit = {
    // could not optimize into delta join because there is a calc between join and source
    util.verifyRelPlanInsert(
      "insert into snk select * from src1 join src2 " +
        "on src1.a1 = src2.b1 " +
        "and src1.a2 = TRIM(src2.b2)")
  }

  @Test
  def testCdcSource(): Unit = {
    util.tableEnv.executeSql(
      "create table cdc_src with ('changelog-mode' = 'I,UA,UB,D') " +
        "like src2 (OVERWRITING OPTIONS)")

    util.verifyRelPlanInsert(
      "insert into snk select * from src1 join cdc_src " +
        "on src1.a1 = cdc_src.b1 " +
        "and src1.a2 = cdc_src.b2")
  }

  @Test
  def testSourceWithSourceAbilities(): Unit = {
    util.tableEnv.executeSql(
      "create table filterable_src with ('filterable-fields' = 'a3') " +
        "like src1 (OVERWRITING OPTIONS)")

    util.verifyRelPlanInsert(
      "insert into snk select * from filterable_src join src2 " +
        "on filterable_src.a1 = src2.b1 " +
        "and filterable_src.a2 = src2.b2 " +
        "and filterable_src.a3 = 1")
  }

  @Test
  def testWithAggregatingSourceTableBeforeJoin(): Unit = {
    util.verifyRelPlanInsert(
      "insert into snk select * from ( " +
        "  select distinct max(a0) as a0, a1, max(a2) as a2, max(a3) as a3 from src1 group by a1" +
        ") tmp join src2 " +
        "on tmp.a1 = src2.b1 " +
        "and tmp.a2 = src2.b2")
  }

  @Test
  def testWithAggregatingAfterJoin(): Unit = {
    util.verifyRelPlanInsert(
      "insert into snk " +
        "select a0, max(a1), max(a2), max(a3), max(b0), max(b2), b1 from src1 join src2 " +
        "on src1.a1 = src2.b1 " +
        "and src1.a2 = src2.b2 " +
        "group by a0, b1")
  }

  @Test
  def testWithCascadeJoin(): Unit = {
    util.tableEnv.executeSql("create table src3 like src2")

    addTable(
      "tmp_snk",
      Schema
        .newBuilder()
        .column("l0", DataTypes.INT.notNull)
        .column("l1", DataTypes.DOUBLE)
        .column("l2", DataTypes.STRING)
        .column("l3", DataTypes.INT)
        .column("r0", DataTypes.INT.notNull)
        .column("r2", DataTypes.STRING)
        .column("r1", DataTypes.DOUBLE)
        .column("r00", DataTypes.INT.notNull)
        .column("r22", DataTypes.STRING)
        .column("r11", DataTypes.DOUBLE)
        .primaryKey("l0", "r0", "r00")
        .build()
    )

    util.verifyRelPlanInsert(
      "insert into tmp_snk select * from src1 join src2 " +
        "on src1.a1 = src2.b1 " +
        "and src1.a2 = src2.b2 " +
        "join src3 " +
        "on src1.a1 = src3.b1 " +
        "and src1.a2 = src3.b2")
  }

  @Test
  def testWithUnsupportedJoinType(): Unit = {
    util.tableConfig.set(
      ExecutionConfigOptions.TABLE_EXEC_SINK_UPSERT_MATERIALIZE,
      UpsertMaterialize.NONE)

    util.verifyRelPlanInsert(
      "insert into snk select * from src1 left join src2 " +
        "on src1.a1 = src2.b1 " +
        "and src1.a2 = src2.b2")
  }

  @Test
  def testWithAlwaysTrueJoinCondition(): Unit = {
    util.verifyRelPlanInsert(
      "insert into snk select * from src1 join src2 " +
        "on 1 = 1")
  }

  @Test
  def testSourceWithoutIndexes(): Unit = {
    addTable(
      "non_index_src",
      Schema
        .newBuilder()
        .column("b0", DataTypes.INT.notNull)
        .column("b2", DataTypes.STRING)
        .column("b1", DataTypes.DOUBLE)
        .primaryKey("b0")
        .build()
    )

    util.verifyRelPlanInsert(
      "insert into snk select * from src1 join non_index_src " +
        "on src1.a1 = non_index_src.b1 " +
        "and src1.a2 = non_index_src.b2")
  }

  @Test
  def testDeltaJoinStrategyWithNone(): Unit = {
    util.tableConfig.set(
      OptimizerConfigOptions.TABLE_OPTIMIZER_DELTA_JOIN_STRATEGY,
      DeltaJoinStrategy.NONE)

    util.verifyRelPlanInsert(
      "insert into snk select * from src1 join src2 " +
        "on src1.a1 = src2.b1 " +
        "and src1.a2 = src2.b2")
  }

  @Test
  def testDeltaJoinStrategyWithForce(): Unit = {
    util.tableConfig.set(
      OptimizerConfigOptions.TABLE_OPTIMIZER_DELTA_JOIN_STRATEGY,
      DeltaJoinStrategy.FORCE)

    util.verifyRelPlanInsert(
      "insert into snk select * from src1 join src2 " +
        "on src1.a1 = src2.b1 " +
        "and src1.a2 = src2.b2")
  }

  @Test
  def testDeltaJoinStrategyWithForce2(): Unit = {
    util.tableConfig.set(
      OptimizerConfigOptions.TABLE_OPTIMIZER_DELTA_JOIN_STRATEGY,
      DeltaJoinStrategy.FORCE)

    // the join could not be converted into the delta join
    assertThatThrownBy(
      () =>
        util.verifyRelPlanInsert(
          "insert into snk select * from src1 join src2 " +
            "on src1.a1 = src2.b1"))
      .hasMessageContaining("The current sql doesn't support to do delta join optimization.")

  }

  @Test
  def testDeltaJoinStrategyWithForce3(): Unit = {
    util.tableConfig.set(
      OptimizerConfigOptions.TABLE_OPTIMIZER_DELTA_JOIN_STRATEGY,
      DeltaJoinStrategy.FORCE)

    util.tableEnv.executeSql(
      "create table snk2 like snk(" +
        "  EXCLUDING CONSTRAINTS" +
        ")")

    val stmt = tEnv.createStatementSet()
    stmt.addInsertSql(
      "insert into snk select * from src1 join src2 " +
        "on src1.a1 = src2.b1 " +
        "and src1.a2 = src2.b2")

    stmt.addInsertSql(
      "insert into snk2 select * from src1 join src2 " +
        "on src1.a1 = src2.b1 " +
        "and src1.a2 = src2.b2")

    // one of the joins can be converted into the delta join
    util.verifyExecPlan(stmt)
  }

  @Test
  def testDeltaJoinStrategyWithForce4(): Unit = {
    util.tableConfig.set(
      OptimizerConfigOptions.TABLE_OPTIMIZER_DELTA_JOIN_STRATEGY,
      DeltaJoinStrategy.FORCE)

    addTable(
      "tmp_snk",
      Schema
        .newBuilder()
        .column("l0", DataTypes.INT.notNull)
        .column("l1", DataTypes.DOUBLE)
        .primaryKey("l0")
        .build()
    )

    // no joins on this query
    util.verifyRelPlanInsert("insert into tmp_snk select a0, a1 from src1")
  }

  private def addTable(
      tableName: String,
      schema: Schema,
      extraOptions: JMap[String, String] = Collections.emptyMap()): Unit = {
    val currentCatalog = tEnv.getCurrentCatalog
    val currentDatabase = tEnv.getCurrentDatabase
    val tablePath = new ObjectPath(currentDatabase, tableName)
    val catalog = tEnv.getCatalog(currentCatalog).get()
    val schemaResolver = tEnv.getCatalogManager.getSchemaResolver

    val options = new JHashMap[String, String](testValuesTableOptions)
    options.putAll(extraOptions)

    val original = CatalogTable
      .newBuilder()
      .schema(schema)
      .comment(testComment)
      .partitionKeys(Collections.emptyList())
      .options(options)
      .build()
    val resolvedTable = new ResolvedCatalogTable(original, schemaResolver.resolve(schema))

    catalog.createTable(tablePath, resolvedTable, false)
  }

}
