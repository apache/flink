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

import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable;
import org.apache.flink.table.planner.hint.FlinkHints;
import org.apache.flink.table.planner.plan.nodes.exec.spec.LookupJoinHintTestUtil;
import org.apache.flink.table.planner.utils.TableTestUtil;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;

/** Tests clearing lookup join hint case-insensitive in stream. */
public class ClearJoinHintWithCapitalizeJoinHintShuttleTest
        extends ClearJoinHintWithInvalidPropagationShuttleTestBase {

    @Override
    TableTestUtil getTableTestUtil() {
        return streamTestUtil(TableConfig.getDefault());
    }

    @Override
    boolean isBatchMode() {
        return false;
    }

    @Before
    public void before() throws Exception {
        super.before();

        util.tableEnv()
                .executeSql(
                        "CREATE TABLE src (\n"
                                + "  a BIGINT,"
                                + "  pts AS PROCTIME()\n"
                                + ") WITH (\n"
                                + " 'connector' = 'values'\n"
                                + ")");
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE lookup (\n"
                                + "  a BIGINT\n"
                                + ") WITH (\n"
                                + " 'connector' = 'values'\n"
                                + ")");
    }

    @Test
    public void testClearCaseInsensitiveLookupHint() {
        // SELECT /*+ lookUp('table'='lookup', 'retry-predicate'='lookup_miss',
        // 'retry-strategy'='fixed_delay', 'fixed-delay'='155 ms', 'max-attempts'='10',
        // 'async'='true', 'output-mode'='allow_unordered','capacity'='1000', 'time-out'='300 s')
        // */ s.a
        // FROM src s
        // JOIN lookup FOR SYSTEM_TIME AS OF s.pts AS d
        // ON s.a=d.a
        CorrelationId cid = builder.getCluster().createCorrel();
        RelDataType aType =
                builder.getTypeFactory()
                        .createStructType(
                                Collections.singletonList(
                                        builder.getTypeFactory().createSqlType(SqlTypeName.BIGINT)),
                                Collections.singletonList("a"));
        RelDataType ptsType =
                builder.getTypeFactory()
                        .createStructType(
                                Collections.singletonList(
                                        builder.getTypeFactory()
                                                .createProctimeIndicatorType(false)),
                                Collections.singletonList("pts"));
        RelNode root =
                FlinkHints.capitalizeJoinHints(
                        builder.scan("src")
                                .scan("lookup")
                                .snapshot(
                                        builder.getRexBuilder()
                                                .makeCall(FlinkSqlOperatorTable.PROCTIME))
                                .filter(
                                        builder.equals(
                                                builder.field(
                                                        builder.getRexBuilder()
                                                                .makeCorrel(aType, cid),
                                                        "a"),
                                                builder.getRexBuilder().makeInputRef(aType, 0)))
                                .correlate(
                                        JoinRelType.INNER,
                                        cid,
                                        builder.getRexBuilder().makeInputRef(aType, 0),
                                        builder.getRexBuilder().makeInputRef(ptsType, 1))
                                .project(builder.field(1, 0, "a"))
                                .hints(
                                        RelHint.builder(FlinkHints.HINT_ALIAS)
                                                .hintOption("t1")
                                                .build())
                                .hints(
                                        RelHint.builder("lookUp")
                                                .hintOptions(
                                                        LookupJoinHintTestUtil
                                                                .getLookupJoinHintOptions(
                                                                        "d", true, false))
                                                .build())
                                .build());
        verifyRelPlan(root);
    }

    @Override
    public void verifyRelPlan(RelNode node) {
        String plan = buildRelPlanWithQueryBlockAlias(node);
        util.assertEqualsOrExpand("beforePropagatingHints", plan, true);

        RelNode rootAfterHintPropagation = RelOptUtil.propagateRelHints(node, false);
        plan = buildRelPlanWithQueryBlockAlias(rootAfterHintPropagation);
        util.assertEqualsOrExpand("afterPropagatingHints", plan, true);

        RelNode rootAfterHintCapitalize = FlinkHints.capitalizeJoinHints(rootAfterHintPropagation);
        plan = buildRelPlanWithQueryBlockAlias(rootAfterHintCapitalize);
        util.assertEqualsOrExpand("afterCapitalizeJoinHints", plan, true);

        RelNode rootAfterClearingJoinHintWithInvalidPropagation =
                rootAfterHintCapitalize.accept(new ClearJoinHintWithInvalidPropagationShuttle());
        plan = buildRelPlanWithQueryBlockAlias(rootAfterClearingJoinHintWithInvalidPropagation);
        util.assertEqualsOrExpand("afterClearingJoinHints", plan, false);
    }
}
