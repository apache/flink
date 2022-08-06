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
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.catalog.ObjectPath;
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
import java.util.HashMap;
import java.util.Map;

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
        final ObjectPath path1 = new ObjectPath(catalogManager.getCurrentDatabase(), "t1");
        final ObjectPath path2 = new ObjectPath(catalogManager.getCurrentDatabase(), "t2");
        final ObjectPath path3 = new ObjectPath(catalogManager.getCurrentDatabase(), "t3");
        final TableSchema tableSchema =
                TableSchema.builder()
                        .field("a", DataTypes.BIGINT())
                        .field("b", DataTypes.VARCHAR(Integer.MAX_VALUE))
                        .field("c", DataTypes.INT())
                        .field("d", DataTypes.VARCHAR(Integer.MAX_VALUE))
                        .build();
        Map<String, String> options = new HashMap<>();
        options.put("connector", "COLLECTION");
        final CatalogTable catalogTable = new CatalogTableImpl(tableSchema, options, "");
        catalog.createTable(path1, catalogTable, true);
        catalog.createTable(path2, catalogTable, true);
        catalog.createTable(path3, catalogTable, true);
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
    public void testClearJoinHintWithInvalidPropagationToView() {
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

    private String buildRelPlanWithQueryBlockAlias(RelNode node) {
        StringBuilder astBuilder = new StringBuilder();
        astBuilder
                .append(System.lineSeparator())
                .append(
                        FlinkRelOptUtil.toString(
                                node,
                                SqlExplainLevel.EXPPLAN_ATTRIBUTES,
                                false,
                                false,
                                true,
                                false,
                                true));
        return astBuilder.toString();
    }

    private void verifyRelPlan(RelNode node) {
        String plan = buildRelPlanWithQueryBlockAlias(node);
        util.assertEqualsOrExpand("PlanBeforePropagatingHints", plan, true);

        RelNode rootAfterHintPropagation = RelOptUtil.propagateRelHints(node, false);
        plan = buildRelPlanWithQueryBlockAlias(rootAfterHintPropagation);
        util.assertEqualsOrExpand("PlanAfterPropagatingHints", plan, true);

        RelNode rootAfterClearingJoinHintWithInvalidPropagation =
                rootAfterHintPropagation.accept(new ClearJoinHintWithInvalidPropagationShuttle());
        plan = buildRelPlanWithQueryBlockAlias(rootAfterClearingJoinHintWithInvalidPropagation);
        util.assertEqualsOrExpand("rootAfterClearingJoinHintWithInvalidPropagation", plan, false);
    }
}
