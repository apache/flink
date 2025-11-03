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

import org.apache.flink.shaded.guava33.com.google.common.collect.Lists
import org.apache.flink.table.api.{DataTypes, ExplainDetail, Schema}
import org.apache.flink.table.api.config.{ExecutionConfigOptions, OptimizerConfigOptions}
import org.apache.flink.table.api.config.ExecutionConfigOptions.UpsertMaterialize
import org.apache.flink.table.api.config.OptimizerConfigOptions.DeltaJoinStrategy
import org.apache.flink.table.catalog.{CatalogTable, ObjectPath, ResolvedCatalogTable}
import org.apache.flink.table.planner.{JList, JMap}
import org.apache.flink.table.planner.utils.{TableTestBase, TestingTableEnvironment}

import org.assertj.core.api.Assertions.assertThatThrownBy
import org.assertj.core.util.Maps
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
    options.put("sink-changelog-mode-enforced", "I,UA,D")
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
      "no_delete_src1",
      Schema
        .newBuilder()
        .column("a0", DataTypes.INT.notNull)
        .column("a1", DataTypes.DOUBLE.notNull)
        .column("a2", DataTypes.STRING.notNull)
        .column("a3", DataTypes.INT)
        .primaryKey("a0", "a1", "a2")
        .index("a1", "a2")
        .build(),
      Maps.newHashMap("changelog-mode", "I,UA,UB")
    )

    addTable(
      "no_delete_src2",
      Schema
        .newBuilder()
        .column("b0", DataTypes.INT)
        .column("b2", DataTypes.STRING.notNull)
        .column("b1", DataTypes.DOUBLE.notNull)
        .primaryKey("b1", "b2")
        .index("b2")
        .build(),
      Maps.newHashMap("changelog-mode", "I,UA,UB")
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

    addTable(
      "snk_for_cdc_src",
      Schema
        .newBuilder()
        .column("l0", DataTypes.INT.notNull)
        .column("l1", DataTypes.DOUBLE.notNull)
        .column("l2", DataTypes.STRING.notNull)
        .column("l3", DataTypes.INT)
        .column("r0", DataTypes.INT)
        .column("r2", DataTypes.STRING.notNull)
        .column("r1", DataTypes.DOUBLE.notNull)
        .primaryKey("l0", "l1", "l2", "r1", "r2")
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
    util.verifyRelPlanInsert(
      "insert into snk select * from src1 join src2 " +
        "on src1.a1 = src2.b1 " +
        "and src1.a2 = src2.b2 " +
        "and src2.b0 > src1.a0 " +
        "and src2.b2 <> 'Hello' " +
        "and src1.a0 > 99")
  }

  @Test
  def testWithNonDeterministicInNonEquiCondition(): Unit = {
    util.verifyRelPlanInsert(
      "insert into snk select * from src1 join src2 " +
        "on src1.a1 = src2.b1 " +
        "and src1.a2 = src2.b2 " +
        "and src1.a0 + rand(10) < src2.b0")
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
  def testProjectFieldsBeforeJoinWhileModifyingOnIndex(): Unit = {
    // a2 is modified
    util.verifyRelPlanInsert(
      "insert into snk(l0, l1, l2, r0, r2, r1) " +
        "select * from ( " +
        "  select a0, a1, SUBSTRING(a2, 2) as a2 from src1" +
        ") tmp join src2 " +
        "on tmp.a1 = src2.b1 " +
        "and tmp.a2 = src2.b2")
  }

  @Test
  def testProjectFieldsBeforeJoinWhileModifyingOnOneIndexButRetainingAnother(): Unit = {
    addTable(
      "src1WithMultiIndexes",
      Schema
        .newBuilder()
        .column("a0", DataTypes.INT.notNull)
        .column("a1", DataTypes.DOUBLE.notNull)
        .column("a2", DataTypes.STRING)
        .column("a3", DataTypes.INT)
        .index("a1", "a2")
        .index("a1")
        .build()
    )

    // a2 is modified
    util.verifyRelPlanInsert(
      "insert into snk(l0, l1, l2, r0, r2, r1) " +
        "select * from ( " +
        "  select a0, a1, SUBSTRING(a2, 2) as a2 from src1WithMultiIndexes" +
        ") tmp join src2 " +
        "on tmp.a1 = src2.b1 " +
        "and tmp.a2 = src2.b2")
  }

  @Test
  def testProjectFieldsBeforeJoinWhileAliasAndReorderOnIndex(): Unit = {
    util.verifyRelPlanInsert(
      "insert into snk(l1, l2, l0, r0, r2, r1) " +
        "select * from ( " +
        "  select a1 as a0, a2 as a1, a0 as a2 from src1" +
        ") tmp join src2 " +
        "on tmp.a0 = src2.b1 " +
        "and tmp.a1 = src2.b2")
  }

  @Test
  def testSourceDDLContainsComputingCol(): Unit = {
    addTable(
      "src1WithComputingCol",
      Schema
        .newBuilder()
        .column("a0", DataTypes.INT.notNull)
        .column("a1", DataTypes.DOUBLE.notNull)
        .column("a2", DataTypes.STRING)
        .columnByExpression("new_a1", "a1 + 1")
        .index("a1", "a2")
        .build()
    )
    util.verifyRelPlanInsert(
      "insert into snk(l0, l1, r0) select a0, new_a1, b0 " +
        "from src1WithComputingCol join src2 " +
        "on a1 = b1 " +
        "and a2 = b2")
  }

  @Test
  def testSourceDDLContainsNonDeterministicComputingCol(): Unit = {
    addTable(
      "src1WithComputingCol",
      Schema
        .newBuilder()
        .column("a0", DataTypes.INT.notNull)
        .column("a1", DataTypes.DOUBLE.notNull)
        .column("a2", DataTypes.STRING)
        .columnByExpression("new_a1", "a1 + rand(10)")
        .index("a1", "a2")
        .build()
    )
    util.verifyRelPlanInsert(
      "insert into snk(l0, l1, r0) select a0, new_a1, b0 " +
        "from src1WithComputingCol join src2 " +
        "on a1 = b1 " +
        "and a2 = b2")
  }

  @Test
  def testFilterFieldsBeforeJoin(): Unit = {
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
  def testFilterFieldsBeforeJoinWithFilterPushDown(): Unit = {
    replaceTable("src1", "src1", Maps.newHashMap("filterable-fields", "a0"))
    replaceTable("src2", "src2", Maps.newHashMap("filterable-fields", "b0"))

    util.verifyRelPlanInsert("""
                               |insert into snk(l0, l1, r0, r2, r1)
                               |  select a0, a1, b0, b2, b1 from (
                               |    select a0, a2, a1 from src1 where a0 > 1
                               |  ) join (
                               |    select b1, b2, b0 from src2 where b1 < 10
                               |  )
                               |  on a1 = b1
                               |  and a2 = b2
                               |  and b0 <> 0
                               |""".stripMargin)
  }

  @Test
  def testNonDeterministicFilterFieldsBeforeJoin1(): Unit = {
    util.verifyRelPlanInsert("""
                               |insert into snk(l0, l1, r0, r2, r1)
                               |  select a0, a1, b0, b2, b1 from (
                               |    select a0, a2, a1 from src1 where a0 > rand(10)
                               |  ) join src2
                               |  on a1 = b1
                               |  and a2 = b2
                               |""".stripMargin)
  }

  @Test
  def testNonDeterministicFilterFieldsBeforeJoin2(): Unit = {
    util.verifyRelPlanInsert("""
                               |insert into snk
                               |  select * from src1
                               |  join (
                               |    select * from src2 where b0 > rand(10)
                               |  )
                               |  on a1 = b1
                               |  and a2 = b2
                               |""".stripMargin)
  }

  @Test
  def testNonDeterministicFilterFieldsBeforeJoinWithFilterPushDown(): Unit = {
    replaceTable("src1", "src1", Maps.newHashMap("filterable-fields", "a0"))

    // actually, 'values' source will not push down filter 'a0 > rand(10)' into source
    util.verifyRelPlanInsert("""
                               |insert into snk(l0, l1, r0, r2, r1)
                               |  select a0, a1, b0, b2, b1 from (
                               |    select a0, a2, a1 from src1 where a0 > rand(10)
                               |  ) join src2
                               |  on a1 = b1
                               |  and a2 = b2
                               |""".stripMargin)
  }

  @Test
  def testPartitionPushDown(): Unit = {
    addTable(
      "src1WithPartition",
      Schema
        .newBuilder()
        .column("a0", DataTypes.INT.notNull)
        .column("a1", DataTypes.DOUBLE.notNull)
        .column("a2", DataTypes.STRING)
        .column("pt", DataTypes.INT)
        .index("a1", "a2")
        .build(),
      Maps.newHashMap("partition-list", "pt:1;pt:2"),
      Lists.newArrayList("pt")
    )

    util.verifyRelPlanInsert("""
                               |insert into snk(l0, r0, r2)
                               |  select a0, b0, b2 from (
                               |    select a0, a2, a1 from src1WithPartition where pt = 1
                               |  ) join src2
                               |  on a1 = b1
                               |  and a2 = b2
                               |""".stripMargin)
  }

  @Test
  def testReadingMetadata(): Unit = {
    addTable(
      "src1WithMetadata",
      Schema
        .newBuilder()
        .columnByMetadata("a0", DataTypes.INT.notNull)
        .column("a1", DataTypes.DOUBLE.notNull)
        .column("a2", DataTypes.STRING)
        .column("a3", DataTypes.INT)
        .index("a1", "a2")
        .build(),
      Maps.newHashMap("readable-metadata", "a0:int")
    )

    util.verifyRelPlanInsert("""
                               |insert into snk(l0, r0, r2)
                               |  select a0, b0, b2 from (
                               |    select a0, a2, a1 from src1WithMetadata
                               |  ) join src2
                               |  on a1 = b1
                               |  and a2 = b2
                               |""".stripMargin)
  }

  @Test
  def testFilterOnNonUpsertKeysBeforeJoinWithCdcSourceWithoutDelete(): Unit = {
    testInnerFilterOnNonUpsertKeysBeforeJoinWithCdcSourceWithoutDelete()
  }

  @Test
  def testFilterOnNonUpsertKeysBeforeJoinWithCdcSourceWithoutDeleteAndFilterPushDown(): Unit = {
    replaceTable("no_delete_src1", "no_delete_src1", Maps.newHashMap("filterable-fields", "a3"))
    testInnerFilterOnNonUpsertKeysBeforeJoinWithCdcSourceWithoutDelete()
  }

  private def testInnerFilterOnNonUpsertKeysBeforeJoinWithCdcSourceWithoutDelete(): Unit = {
    util.verifyRelPlanInsert(
      "insert into snk_for_cdc_src select * from no_delete_src1 " +
        "join no_delete_src2 " +
        "on a1 = b1 " +
        "and a2 = b2 " +
        "where a3 > 1",
      ExplainDetail.CHANGELOG_MODE
    )
  }

  @Test
  def testFilterOnNonUpsertKeysAfterJoinWithCdcSourceWithoutDelete(): Unit = {
    util.verifyRelPlanInsert(
      "insert into snk_for_cdc_src select * from no_delete_src1 " +
        "join no_delete_src2 " +
        "on a1 = b1 " +
        "and a2 = b2 " +
        "where a3 > b0",
      ExplainDetail.CHANGELOG_MODE
    )
  }

  @Test
  def testFilterOnUpsertKeysBeforeJoinWithCdcSourceWithoutDelete(): Unit = {
    testFilterOnUpsertKeysBeforeJoinWithCdcSourceWithoutDeleteInner()
  }

  @Test
  def testFilterOnUpsertKeysBeforeJoinWithCdcSourceWithoutDeleteAndFilterPushDown(): Unit = {
    replaceTable("no_delete_src1", "no_delete_src1", Maps.newHashMap("filterable-fields", "a0"))
    testFilterOnUpsertKeysBeforeJoinWithCdcSourceWithoutDeleteInner()
  }

  private def testFilterOnUpsertKeysBeforeJoinWithCdcSourceWithoutDeleteInner(): Unit = {
    util.verifyRelPlanInsert(
      "insert into snk_for_cdc_src select * from no_delete_src1 " +
        "join no_delete_src2 " +
        "on a1 = b1 " +
        "and a2 = b2 " +
        "where a0 > 1",
      ExplainDetail.CHANGELOG_MODE
    )
  }

  @Test
  def testFilterOnUpsertKeysAfterJoinWithCdcSourceWithoutDelete(): Unit = {
    util.verifyRelPlanInsert(
      "insert into snk_for_cdc_src select * from no_delete_src1 " +
        "join no_delete_src2 " +
        "on a1 = b1 " +
        "and a2 = b2 " +
        "where a0 > b1",
      ExplainDetail.CHANGELOG_MODE
    )
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
  def testMultiRootsWithoutReusingDeltaJoin1(): Unit = {
    // one sink has pk but another doesn't
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
  def testMultiRootsWithoutReusingDeltaJoin2(): Unit = {
    // one sink is an upsert sink but another is a retract sink
    util.tableEnv
      .executeSql(
        "CREATE TABLE snk2 WITH (\n"
          + "  'sink-changelog-mode-enforced' = 'I,UA,UB,D'"
          + ") LIKE snk (\n"
          + "  OVERWRITING OPTIONS\n"
          + ")")

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
  def testLookupTableWithCache(): Unit = {
    val lookupOptions = new JHashMap[String, String]()
    lookupOptions.put("lookup.cache", "partial")
    lookupOptions.put("lookup.partial-cache.max-rows", "1000")

    replaceTable("src1", "src1", lookupOptions)

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
    replaceTable("src2", "non_lookup_src", Maps.newHashMap("disable-lookup", "true"))

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
  def testSourceWithAllRowKinds(): Unit = {
    util.tableConfig.set(
      ExecutionConfigOptions.TABLE_EXEC_SINK_UPSERT_MATERIALIZE,
      UpsertMaterialize.NONE)

    replaceTable("src2", "cdc_src", Maps.newHashMap("changelog-mode", "I,UA,UB,D"))

    util.verifyRelPlanInsert(
      "insert into snk select * from src1 join cdc_src " +
        "on src1.a1 = cdc_src.b1 " +
        "and src1.a2 = cdc_src.b2")
  }

  @Test
  def testPKContainsJoinKeyAndTwoSourcesNoDelete(): Unit = {
    util.verifyRelPlanInsert(
      "insert into snk_for_cdc_src " +
        "select * from no_delete_src1 join no_delete_src2 " +
        "on a1 = b1 " +
        "and a2 = b2",
      ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testPKNotContainJoinKeyAndTwoSourcesNoDelete(): Unit = {
    util.tableConfig.set(
      ExecutionConfigOptions.TABLE_EXEC_SINK_UPSERT_MATERIALIZE,
      UpsertMaterialize.NONE)

    util.verifyRelPlanInsert(
      "insert into snk_for_cdc_src " +
        "select * from no_delete_src1 join no_delete_src2 " +
        "on a0 = b0 " +
        "and a1 = b1 " +
        "and a2 = b2",
      ExplainDetail.CHANGELOG_MODE
    )
  }

  @Test
  def testPKContainJoinKeyAndOnlyOneSourceNoDelete(): Unit = {
    replaceTable(
      "no_delete_src1",
      "all_changelog_src",
      Maps.newHashMap("changelog-mode", "I,UA,UB,D"))

    util.verifyRelPlanInsert(
      "insert into snk_for_cdc_src " +
        "select * from all_changelog_src join no_delete_src2 " +
        "on a1 = b1 " +
        "and a2 = b2",
      ExplainDetail.CHANGELOG_MODE
    )
  }

  @Test
  def testPKContainsJoinKeyAndSourceNoUBAndD(): Unit = {
    // FLINK-38489 Currently, ChangelogNormalize will always generate changelog mode with D,
    // and Join with D cannot be optimized into Delta Join
    replaceTable(
      "no_delete_src1",
      "no_delete_and_update_before_src1",
      Maps.newHashMap("changelog-mode", "I,UA"))

    replaceTable(
      "no_delete_src2",
      "no_delete_and_update_before_src2",
      Maps.newHashMap("changelog-mode", "I,UA"))

    util.verifyRelPlanInsert(
      "insert into snk_for_cdc_src " +
        "select * from no_delete_and_update_before_src1 " +
        "join no_delete_and_update_before_src2 " +
        "on a1 = b1 " +
        "and a2 = b2",
      ExplainDetail.CHANGELOG_MODE
    )
  }

  @Test
  def testJoinAppendOnlySourceAndSourceWithoutDelete(): Unit = {
    util.tableConfig.set(
      ExecutionConfigOptions.TABLE_EXEC_SINK_UPSERT_MATERIALIZE,
      UpsertMaterialize.NONE)

    util.tableEnv.executeSql("""
                               |create table tmp_snk (
                               | primary key (r1, r2) not enforced
                               |) like snk_for_cdc_src (
                               |  EXCLUDING CONSTRAINTS
                               |)
                               |""".stripMargin)

    // the join could not be converted into the delta join
    // because the upsert key of the join is `null`
    util.verifyRelPlanInsert(
      "insert into tmp_snk " +
        "select * from src1 join no_delete_src2 " +
        "on a1 = b1 " +
        "and a2 = b2",
      ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testWithAggregatingSourceTableBeforeJoin(): Unit = {
    util.tableConfig.set(
      ExecutionConfigOptions.TABLE_EXEC_SINK_UPSERT_MATERIALIZE,
      UpsertMaterialize.NONE)

    util.verifyRelPlanInsert(
      "insert into snk select * from ( " +
        "  select distinct max(a0) as a0, a1, max(a2) as a2, max(a3) as a3 from src1 group by a1" +
        ") tmp join src2 " +
        "on tmp.a1 = src2.b1 " +
        "and tmp.a2 = src2.b2")
  }

  @Test
  def testWithAggregatingAfterJoin(): Unit = {
    util.tableConfig.set(
      ExecutionConfigOptions.TABLE_EXEC_SINK_UPSERT_MATERIALIZE,
      UpsertMaterialize.NONE)

    util.verifyRelPlanInsert(
      "insert into snk " +
        "select a0, max(a1), max(a2), max(a3), max(b0), max(b2), b1 from src1 join src2 " +
        "on src1.a1 = src2.b1 " +
        "and src1.a2 = src2.b2 " +
        "group by a0, b1")
  }

  @Test
  def testWithCascadeJoin(): Unit = {
    replaceTable("src2", "src3", Collections.emptyMap(), dropOldTable = false)

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

  @Test
  def testRetractSink(): Unit = {
    util.tableEnv
      .executeSql(
        "CREATE TABLE retract_snk WITH (\n"
          + "  'sink-changelog-mode-enforced' = 'I,UA,UB,D'"
          + ") LIKE snk (\n"
          + "  OVERWRITING OPTIONS\n"
          + ")")

    util.verifyRelPlanInsert(
      "insert into retract_snk select * from src1 join src2 " +
        "on src1.a1 = src2.b1 " +
        "and src1.a2 = src2.b2")
  }

  @Test
  def testAppendSink(): Unit = {
    util.tableEnv
      .executeSql(
        "CREATE TABLE append_snk WITH (\n"
          + "  'sink-insert-only' = 'true',\n"
          + "  'sink-changelog-mode-enforced' = 'I'\n"
          + ") LIKE snk (\n"
          + "  OVERWRITING OPTIONS\n"
          + ")")

    util.verifyRelPlanInsert(
      "insert into append_snk select * from src1 join src2 " +
        "on src1.a1 = src2.b1 " +
        "and src1.a2 = src2.b2")
  }

  @Test
  def testMultiIndexesInSourceWhileJoinKeyContainsOneOfThem(): Unit = {
    addTable(
      "tmp_src11",
      Schema
        .newBuilder()
        .column("a0", DataTypes.INT.notNull)
        .column("a1", DataTypes.DOUBLE.notNull)
        .column("a2", DataTypes.STRING)
        .column("a3", DataTypes.INT)
        .index("a1", "a2")
        .index("a2")
        .build()
    )

    addTable(
      "tmp_src12",
      Schema
        .newBuilder()
        .column("a0", DataTypes.INT.notNull)
        .column("a1", DataTypes.DOUBLE.notNull)
        .column("a2", DataTypes.STRING)
        .column("a3", DataTypes.INT)
        .index("a2")
        .index("a1", "a2")
        .build()
    )

    val stmt = tEnv.createStatementSet()
    stmt.addInsertSql(
      "insert into snk select * from tmp_src11 join src2 " +
        "on tmp_src11.a2 = src2.b2")
    stmt.addInsertSql(
      "insert into snk select * from tmp_src12 join src2 " +
        "on tmp_src12.a2 = src2.b2")
    util.verifyRelPlan(stmt)
  }

  private def addTable(
      tableName: String,
      schema: Schema,
      extraOptions: JMap[String, String] = Collections.emptyMap(),
      partitionKeys: JList[String] = Collections.emptyList()): Unit = {
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
      .partitionKeys(partitionKeys)
      .options(options)
      .build()
    val resolvedTable = new ResolvedCatalogTable(original, schemaResolver.resolve(schema))

    catalog.createTable(tablePath, resolvedTable, false)
  }

  /** TODO remove this after fix FLINK-38571. */
  private def replaceTable(
      oldTableName: String,
      newTableName: String,
      overridesOptions: JMap[String, String],
      dropOldTable: Boolean = true): Unit = {
    val currentCatalog = tEnv.getCurrentCatalog
    val currentDatabase = tEnv.getCurrentDatabase
    val oldTablePath = new ObjectPath(currentDatabase, oldTableName)
    val newTablePath = new ObjectPath(currentDatabase, newTableName)
    val catalog = tEnv.getCatalog(currentCatalog).get()
    val schemaResolver = tEnv.getCatalogManager.getSchemaResolver

    val originalTable = catalog.getTable(oldTablePath).asInstanceOf[CatalogTable]
    if (dropOldTable) {
      catalog.dropTable(oldTablePath, false)
    }

    val originalOptions = originalTable.getOptions
    val newOptions = new JHashMap[String, String]()
    newOptions.putAll(originalOptions)
    newOptions.putAll(overridesOptions)

    val newTable = CatalogTable
      .newBuilder()
      .schema(originalTable.getUnresolvedSchema)
      .comment(originalTable.getComment)
      .partitionKeys(originalTable.getPartitionKeys)
      .options(newOptions)
      .build()

    val newResolvedTable =
      new ResolvedCatalogTable(newTable, schemaResolver.resolve(originalTable.getUnresolvedSchema))

    catalog.createTable(newTablePath, newResolvedTable, false)
  }

}
