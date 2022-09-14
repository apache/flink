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

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.hint.RelHint;
import org.junit.Before;
import org.junit.Test;

/** Tests clearing lookup join hint with invalid propagation in stream. */
public class ClearLookupJoinHintWithInvalidPropagationShuttleTest
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
                                + "  a BIGINT\n"
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
    public void testNoNeedToClearLookupHint() {
        // SELECT /*+ LOOKUP('table'='lookup', 'retry-predicate'='lookup_miss',
        // 'retry-strategy'='fixed_delay', 'fixed-delay'='155 ms', 'max-attempts'='10') ) */ *
        //  FROM src
        //  JOIN lookup FOR SYSTEM_TIME AS OF T.proctime AS D
        //      ON T.a = D.a
        RelNode root =
                builder.scan("src")
                        .scan("lookup")
                        .snapshot(builder.getRexBuilder().makeCall(FlinkSqlOperatorTable.PROCTIME))
                        .join(
                                JoinRelType.INNER,
                                builder.equals(builder.field(2, 0, "a"), builder.field(2, 1, "a")))
                        .project(builder.field(1, 0, "a"))
                        .hints(LookupJoinHintTestUtil.getLookupJoinHint("lookup", false, true))
                        .build();
        verifyRelPlan(root);
    }

    @Test
    public void testClearLookupHintWithInvalidPropagationToViewWhileViewHasLookupHints() {
        // SELECT /*+ LOOKUP('table'='lookup', 'retry-predicate'='lookup_miss',
        // 'retry-strategy'='fixed_delay', 'fixed-delay'='155 ms', 'max-attempts'='10') ) */ *
        //   FROM (
        //     SELECT /*+ LOOKUP('table'='lookup', 'async'='true', 'output-mode'='allow_unordered',
        // 'capacity'='1000', 'time-out'='300 s'
        //       src.a, src.proctime
        //     FROM src
        //       JOIN lookup FOR SYSTEM_TIME AS OF T.proctime AS D
        //         ON T.a = D.id
        //     ) t1 JOIN lookup FOR SYSTEM_TIME AS OF t1.proctime AS t2 ON t1.a = t2.a
        RelNode root =
                builder.scan("src")
                        .scan("lookup")
                        .snapshot(builder.getRexBuilder().makeCall(FlinkSqlOperatorTable.PROCTIME))
                        .join(
                                JoinRelType.INNER,
                                builder.equals(builder.field(2, 0, "a"), builder.field(2, 1, "a")))
                        .project(builder.field(1, 0, "a"))
                        .hints(LookupJoinHintTestUtil.getLookupJoinHint("lookup", false, true))
                        .hints(RelHint.builder(FlinkHints.HINT_ALIAS).hintOption("t1").build())
                        .scan("src")
                        .snapshot(builder.getRexBuilder().makeCall(FlinkSqlOperatorTable.PROCTIME))
                        .join(
                                JoinRelType.INNER,
                                builder.equals(builder.field(2, 0, "a"), builder.field(2, 1, "a")))
                        .project(builder.field(1, 0, "a"))
                        .hints(LookupJoinHintTestUtil.getLookupJoinHint("lookup", true, false))
                        .build();
        verifyRelPlan(root);
    }

    @Test
    public void testClearLookupHintWithInvalidPropagationToSubQuery() {
        // SELECT /*+ LOOKUP('table'='lookup', 'retry-predicate'='lookup_miss',
        // 'retry-strategy'='fixed_delay', 'fixed-delay'='155 ms', 'max-attempts'='10',
        // 'async'='true', 'output-mode'='allow_unordered','capacity'='1000', 'time-out'='300 s' */*
        //   FROM (
        //     SELECT src.a
        //     FROM src
        //     JOIN lookup FOR SYSTEM_TIME AS OF T.proctime AS D
        //       ON T.a = D.id
        //   ) t1 JOIN src t2 ON t1.a = t2.a
        RelNode root =
                builder.scan("src")
                        .scan("lookup")
                        .snapshot(builder.getRexBuilder().makeCall(FlinkSqlOperatorTable.PROCTIME))
                        .join(
                                JoinRelType.INNER,
                                builder.equals(builder.field(2, 0, "a"), builder.field(2, 1, "a")))
                        .project(builder.field(1, 0, "a"))
                        .hints(RelHint.builder(FlinkHints.HINT_ALIAS).hintOption("t1").build())
                        .scan("src")
                        .hints(RelHint.builder(FlinkHints.HINT_ALIAS).hintOption("t2").build())
                        .join(
                                JoinRelType.INNER,
                                builder.equals(builder.field(2, 0, "a"), builder.field(2, 1, "a")))
                        .project(builder.field(1, 0, "a"))
                        .hints(LookupJoinHintTestUtil.getLookupJoinHint("lookup", true, true))
                        .build();
        verifyRelPlan(root);
    }
}
