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

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable;
import org.apache.flink.table.planner.hint.FlinkHints;
import org.apache.flink.table.planner.plan.nodes.exec.spec.LookupJoinHintTestUtil;
import org.apache.flink.table.planner.utils.TableTestUtil;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.fun.SqlCollectionTableOperator;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlModality;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;

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
                                + "  a BIGINT,"
                                + "  ds ARRAY<BIGINT>,"
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
        util.tableEnv()
                .createTemporarySystemFunction(
                        "MockOffset",
                        new ClearLookupJoinHintWithInvalidPropagationShuttleTest
                                .MockOffsetTableFunction());
    }

    @Test
    public void testNoNeedToClearLookupHint() {
        // SELECT /*+ LOOKUP('table'='lookup', 'retry-predicate'='lookup_miss',
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
                builder.scan("src")
                        .scan("lookup")
                        .snapshot(builder.getRexBuilder().makeCall(FlinkSqlOperatorTable.PROCTIME))
                        .filter(
                                builder.equals(
                                        builder.field(
                                                builder.getRexBuilder().makeCorrel(aType, cid),
                                                "a"),
                                        builder.getRexBuilder().makeInputRef(aType, 0)))
                        .correlate(
                                JoinRelType.INNER,
                                cid,
                                builder.getRexBuilder().makeInputRef(aType, 0),
                                builder.getRexBuilder().makeInputRef(ptsType, 1))
                        .project(builder.field(1, 0, "a"))
                        .hints(RelHint.builder(FlinkHints.HINT_ALIAS).hintOption("t1").build())
                        .hints(LookupJoinHintTestUtil.getLookupJoinHint("d", true, false))
                        .build();
        verifyRelPlan(root);
    }

    @Test
    public void testClearLookupHintWithInvalidPropagationToSubQuery() {
        // SELECT /*+ LOOKUP('table'='src', 'retry-predicate'='lookup_miss',
        // 'retry-strategy'='fixed_delay', 'fixed-delay'='155 ms', 'max-attempts'='10',
        // 'async'='true', 'output-mode'='allow_unordered','capacity'='1000', 'time-out'='300 s')
        // */ t1.a
        //  FROM (
        //      SELECT s.a
        //      FROM src s
        //      JOIN lookup FOR SYSTEM_TIME AS OF s.pts AS d
        //      ON s.a=d.a
        //  ) t1
        //  JOIN src t2
        //  ON t1.a=t2.a

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
                builder.scan("src")
                        .scan("lookup")
                        .snapshot(builder.getRexBuilder().makeCall(FlinkSqlOperatorTable.PROCTIME))
                        .filter(
                                builder.equals(
                                        builder.field(
                                                builder.getRexBuilder().makeCorrel(aType, cid),
                                                "a"),
                                        builder.getRexBuilder().makeInputRef(aType, 0)))
                        .correlate(
                                JoinRelType.INNER,
                                cid,
                                builder.getRexBuilder().makeInputRef(aType, 0),
                                builder.getRexBuilder().makeInputRef(ptsType, 1))
                        .project(builder.field(1, 0, "a"))
                        .hints(RelHint.builder(FlinkHints.HINT_ALIAS).hintOption("t1").build())
                        .hints(LookupJoinHintTestUtil.getLookupJoinHint("d", true, false))
                        .scan("src")
                        .hints(RelHint.builder(FlinkHints.HINT_ALIAS).hintOption("t2").build())
                        .join(
                                JoinRelType.INNER,
                                builder.equals(builder.field(2, 0, "a"), builder.field(2, 1, "a")))
                        .project(builder.field(1, 0, "a"))
                        .hints(LookupJoinHintTestUtil.getLookupJoinHint("src", true, true))
                        .build();
        verifyRelPlan(root);
    }

    @Test
    public void testNoNeedToClearLookupHintWhileJoinWithUnnest() {
        //  SELECT /*+ LOOKUP('table'='d', 'retry-predicate'='lookup_miss',
        //  'retry-strategy'='fixed_delay', 'fixed-delay'='155 ms', 'max-attempts'='10',
        //  'async'='true', 'output-mode'='allow_unordered','capacity'='1000', 'time-out'='300 s')
        //  */ s.a
        //  FROM src s
        //  CROSS JOIN UNNEST(s.ds) AS d(a)

        CorrelationId cid = builder.getCluster().createCorrel();
        RelDataType dsType =
                builder.getTypeFactory()
                        .createStructType(
                                Collections.singletonList(
                                        builder.getTypeFactory()
                                                .createArrayType(
                                                        builder.getTypeFactory()
                                                                .createSqlType(SqlTypeName.BIGINT),
                                                        -1L)),
                                Collections.singletonList("ds"));
        RelOptCluster cluster = util.getPlanner().plannerContext().getCluster();
        RelNode root =
                builder.scan("src")
                        .project(builder.field(1, 0, "a"))
                        .push(LogicalValues.createOneRow(cluster))
                        .project(
                                builder.field(
                                        builder.getRexBuilder().makeCorrel(dsType, cid), "ds"))
                        .uncollect(Collections.singletonList("a"), false)
                        .project(builder.field(1, 0, "a"))
                        .correlate(JoinRelType.INNER, cid)
                        .project(builder.field(1, 0, "a"))
                        .hints(LookupJoinHintTestUtil.getLookupJoinHint("d", true, false))
                        .build();
        verifyRelPlan(root);
    }

    @Test
    public void testNoNeedToClearLookupHintWhileJoinWithUDTF() {
        //  SELECT /*+ LOOKUP('table'='d', 'retry-predicate'='lookup_miss',
        //  'retry-strategy'='fixed_delay', 'fixed-delay'='155 ms', 'max-attempts'='10',
        //  'async'='true', 'output-mode'='allow_unordered','capacity'='1000', 'time-out'='300 s')
        //  */ s.a
        //  FROM src s
        //  CROSS JOIN LATERAL TABLE(MockOffset(a)) AS d(b)

        CorrelationId cid = builder.getCluster().createCorrel();
        RelDataType bType =
                builder.getTypeFactory()
                        .createStructType(
                                Collections.singletonList(
                                        builder.getTypeFactory().createSqlType(SqlTypeName.BIGINT)),
                                Collections.singletonList("b"));
        RelNode root =
                builder.scan("src")
                        .project(builder.field(1, 0, "a"))
                        .functionScan(
                                new SqlCollectionTableOperator("TABLE", SqlModality.RELATION) {
                                    @Override
                                    public RelDataType inferReturnType(
                                            SqlOperatorBinding opBinding) {
                                        return bType;
                                    }
                                },
                                0,
                                builder.getRexBuilder()
                                        .makeFieldAccess(
                                                builder.getRexBuilder().makeCorrel(bType, cid), 0))
                        .correlate(JoinRelType.INNER, cid)
                        .project(builder.field(1, 0, "a"))
                        .hints(LookupJoinHintTestUtil.getLookupJoinHint("d", true, false))
                        .build();
        verifyRelPlan(root);
    }

    /** Mock UDTF, used for JOIN LATERAL TABLE test. */
    @FunctionHint(output = @DataTypeHint("ROW< b BIGINT >"))
    public static class MockOffsetTableFunction extends TableFunction<Long> {
        public void eval(Long arg) {
            collect(arg + 10L);
        }
    }
}
