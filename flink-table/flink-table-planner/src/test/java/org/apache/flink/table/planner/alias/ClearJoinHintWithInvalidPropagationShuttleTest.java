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

package org.apache.flink.table.planner.alias;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.planner.calcite.FlinkRelBuilder;
import org.apache.flink.table.planner.hint.FlinkHints;
import org.apache.flink.table.planner.hint.JoinStrategy;
import org.apache.flink.table.planner.plan.utils.FlinkRelOptUtil;
import org.apache.flink.table.planner.utils.BatchTableTestUtil;
import org.apache.flink.table.planner.utils.PlannerMocks;
import org.apache.flink.table.planner.utils.TableTestBase;
import org.apache.flink.table.utils.CatalogManagerMocks;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.sql.SqlExplainLevel;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;

/** A test class for {@link ClearJoinHintWithInvalidPropagationShuttle}. */
public class ClearJoinHintWithInvalidPropagationShuttleTest extends TableTestBase {

    private final BatchTableTestUtil util = batchTestUtil(TableConfig.getDefault());
    private final Catalog catalog = new GenericInMemoryCatalog("MockCatalog", "default");
    private final CatalogManager catalogManager =
            CatalogManagerMocks.preparedCatalogManager()
                    .defaultCatalog("builtin", catalog)
                    .config(
                            Configuration.fromMap(
                                    Collections.singletonMap(
                                            ExecutionOptions.RUNTIME_MODE.key(),
                                            RuntimeExecutionMode.BATCH.name())))
                    .build();
    private final PlannerMocks plannerMocks =
            PlannerMocks.newBuilder()
                    .withBatchMode(true)
                    .withCatalogManager(catalogManager)
                    .build();
    private final FlinkRelBuilder builder = plannerMocks.getPlannerContext().createRelBuilder();

    @Before
    public void before() throws Exception {
        util.tableEnv().registerCatalog("testCatalog", catalog);
        util.tableEnv().executeSql("use catalog testCatalog");

        util.tableEnv()
                .executeSql(
                        "CREATE TABLE t1 (\n"
                                + "  a BIGINT\n"
                                + ") WITH (\n"
                                + " 'connector' = 'values',\n"
                                + " 'bounded' = 'true'\n"
                                + ")");

        util.tableEnv()
                .executeSql(
                        "CREATE TABLE t2 (\n"
                                + "  a BIGINT\n"
                                + ") WITH (\n"
                                + " 'connector' = 'values',\n"
                                + " 'bounded' = 'true'\n"
                                + ")");

        util.tableEnv()
                .executeSql(
                        "CREATE TABLE t3 (\n"
                                + "  a BIGINT\n"
                                + ") WITH (\n"
                                + " 'connector' = 'values',\n"
                                + " 'bounded' = 'true'\n"
                                + ")");
    }

    @Test
    public void testNoNeedToClearJoinHint() {
        // SELECT /*+ BROADCAST(t1)*/t1.a FROM t1 JOIN t2 ON t1.a = t2.a
        RelHint joinHintInView =
                RelHint.builder(JoinStrategy.BROADCAST.getJoinHintName()).hintOption("t1").build();

        RelNode root =
                builder.scan("t1")
                        .scan("t2")
                        .join(
                                JoinRelType.INNER,
                                builder.equals(builder.field(2, 0, "a"), builder.field(2, 1, "a")))
                        .project(builder.field(1, 0, "a"))
                        .hints(joinHintInView)
                        .build();
        verifyRelPlan(root);
    }

    @Test
    public void testClearJoinHintWithInvalidPropagationToViewWhileViewHasJoinHints() {
        //  SELECT /*+ BROADCAST(t3)*/t4.a FROM (
        //      SELECT /*+ BROADCAST(t1)*/t1.a FROM t1 JOIN t2 ON t1.a = t2.a
        //  ) t4 JOIN t3 ON t4.a = t3.a
        RelHint joinHintInView =
                RelHint.builder(JoinStrategy.BROADCAST.getJoinHintName()).hintOption("t1").build();

        RelHint joinHintRoot =
                RelHint.builder(JoinStrategy.BROADCAST.getJoinHintName()).hintOption("t3").build();

        RelHint aliasHint = RelHint.builder(FlinkHints.HINT_ALIAS).hintOption("t4").build();

        RelNode root =
                builder.scan("t1")
                        .scan("t2")
                        .join(
                                JoinRelType.INNER,
                                builder.equals(builder.field(2, 0, "a"), builder.field(2, 1, "a")))
                        .project(builder.field(1, 0, "a"))
                        .hints(joinHintInView, aliasHint)
                        .scan("t3")
                        .join(
                                JoinRelType.INNER,
                                builder.equals(builder.field(2, 0, "a"), builder.field(2, 1, "a")))
                        .project(builder.field(1, 0, "a"))
                        .hints(joinHintRoot)
                        .build();
        verifyRelPlan(root);
    }

    @Test
    public void testClearJoinHintWithInvalidPropagationToViewWhileViewHasNoJoinHints() {
        //  SELECT /*+ BROADCAST(t3)*/t4.a FROM (
        //      SELECT t1.a FROM t1 JOIN t2 ON t1.a = t2.a
        //  ) t4 JOIN t3 ON t4.a = t3.a
        RelHint joinHintRoot =
                RelHint.builder(JoinStrategy.BROADCAST.getJoinHintName()).hintOption("t3").build();

        RelHint aliasHint = RelHint.builder(FlinkHints.HINT_ALIAS).hintOption("t4").build();

        RelNode root =
                builder.scan("t1")
                        .scan("t2")
                        .join(
                                JoinRelType.INNER,
                                builder.equals(builder.field(2, 0, "a"), builder.field(2, 1, "a")))
                        .project(builder.field(1, 0, "a"))
                        .hints(aliasHint)
                        .scan("t3")
                        .join(
                                JoinRelType.INNER,
                                builder.equals(builder.field(2, 0, "a"), builder.field(2, 1, "a")))
                        .project(builder.field(1, 0, "a"))
                        .hints(joinHintRoot)
                        .build();
        verifyRelPlan(root);
    }

    private String buildRelPlanWithQueryBlockAlias(RelNode node) {
        return System.lineSeparator()
                + FlinkRelOptUtil.toString(
                        node, SqlExplainLevel.EXPPLAN_ATTRIBUTES, false, false, true, false, true);
    }

    private void verifyRelPlan(RelNode node) {
        String plan = buildRelPlanWithQueryBlockAlias(node);
        util.assertEqualsOrExpand("beforePropagatingHint", plan, true);

        RelNode rootAfterHintPropagation = RelOptUtil.propagateRelHints(node, false);
        plan = buildRelPlanWithQueryBlockAlias(rootAfterHintPropagation);
        util.assertEqualsOrExpand("afterPropagatingHints", plan, true);

        RelNode rootAfterClearingJoinHintWithInvalidPropagation =
                rootAfterHintPropagation.accept(new ClearJoinHintWithInvalidPropagationShuttle());
        plan = buildRelPlanWithQueryBlockAlias(rootAfterClearingJoinHintWithInvalidPropagation);
        util.assertEqualsOrExpand("afterClearingJoinHints", plan, false);
    }
}
