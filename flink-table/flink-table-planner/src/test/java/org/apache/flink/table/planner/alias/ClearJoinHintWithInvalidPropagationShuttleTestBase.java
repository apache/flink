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
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.planner.calcite.FlinkRelBuilder;
import org.apache.flink.table.planner.plan.utils.FlinkRelOptUtil;
import org.apache.flink.table.planner.utils.PlannerMocks;
import org.apache.flink.table.planner.utils.TableTestBase;
import org.apache.flink.table.planner.utils.TableTestUtil;
import org.apache.flink.table.utils.CatalogManagerMocks;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlExplainLevel;
import org.junit.Before;

import java.util.Collections;

/** A base class for testing clearing join hint with invalid propagation. */
public abstract class ClearJoinHintWithInvalidPropagationShuttleTestBase extends TableTestBase {

    protected final TableTestUtil util = getTableTestUtil();

    abstract TableTestUtil getTableTestUtil();

    abstract boolean isBatchMode();

    private final Catalog catalog = new GenericInMemoryCatalog("MockCatalog", "default");
    private final CatalogManager catalogManager =
            CatalogManagerMocks.preparedCatalogManager()
                    .defaultCatalog("builtin", catalog)
                    .config(
                            Configuration.fromMap(
                                    Collections.singletonMap(
                                            ExecutionOptions.RUNTIME_MODE.key(),
                                            isBatchMode()
                                                    ? RuntimeExecutionMode.BATCH.name()
                                                    : RuntimeExecutionMode.STREAMING.name())))
                    .build();

    private final PlannerMocks plannerMocks =
            PlannerMocks.newBuilder()
                    .withBatchMode(isBatchMode())
                    .withCatalogManager(catalogManager)
                    .build();
    protected final FlinkRelBuilder builder = plannerMocks.getPlannerContext().createRelBuilder();

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
                                + " 'bounded' = '"
                                + isBatchMode()
                                + "'\n"
                                + ")");

        util.tableEnv()
                .executeSql(
                        "CREATE TABLE t2 (\n"
                                + "  a BIGINT\n"
                                + ") WITH (\n"
                                + " 'connector' = 'values',\n"
                                + " 'bounded' = '"
                                + isBatchMode()
                                + "'\n"
                                + ")");

        util.tableEnv()
                .executeSql(
                        "CREATE TABLE t3 (\n"
                                + "  a BIGINT\n"
                                + ") WITH (\n"
                                + " 'connector' = 'values',\n"
                                + " 'bounded' = '"
                                + isBatchMode()
                                + "'\n"
                                + ")");
    }

    protected String buildRelPlanWithQueryBlockAlias(RelNode node) {
        return System.lineSeparator()
                + FlinkRelOptUtil.toString(
                        node, SqlExplainLevel.EXPPLAN_ATTRIBUTES, false, false, true, false, true);
    }

    protected void verifyRelPlan(RelNode node) {
        String plan = buildRelPlanWithQueryBlockAlias(node);
        util.assertEqualsOrExpand("beforePropagatingHints", plan, true);

        RelNode rootAfterHintPropagation = RelOptUtil.propagateRelHints(node, false);
        plan = buildRelPlanWithQueryBlockAlias(rootAfterHintPropagation);
        util.assertEqualsOrExpand("afterPropagatingHints", plan, true);

        RelNode rootAfterClearingJoinHintWithInvalidPropagation =
                rootAfterHintPropagation.accept(new ClearJoinHintWithInvalidPropagationShuttle());
        plan = buildRelPlanWithQueryBlockAlias(rootAfterClearingJoinHintWithInvalidPropagation);
        util.assertEqualsOrExpand("afterClearingJoinHints", plan, false);
    }
}
