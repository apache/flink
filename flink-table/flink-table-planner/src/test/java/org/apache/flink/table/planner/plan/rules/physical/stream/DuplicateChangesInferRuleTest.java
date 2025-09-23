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

package org.apache.flink.table.planner.plan.rules.physical.stream;

import org.apache.flink.table.api.ExplainDetail;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.config.AggregatePhaseStrategy;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.planner.utils.PlanKind;
import org.apache.flink.table.planner.utils.StreamTableTestUtil;
import org.apache.flink.table.planner.utils.TableTestBase;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;

import scala.Enumeration;

import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static scala.runtime.BoxedUnit.UNIT;

/** Tests for {@link DuplicateChangesInferRule}. */
@ExtendWith(ParameterizedTestExtension.class)
public class DuplicateChangesInferRuleTest extends TableTestBase {

    private final StreamTableTestUtil util = streamTestUtil(TableConfig.getDefault());

    private final boolean testSinkWithPk;

    public DuplicateChangesInferRuleTest(boolean testSinkWithPk) {
        this.testSinkWithPk = testSinkWithPk;
    }

    @Parameters(name = "testSinkWithPk = {0}")
    private static Collection<Boolean[]> params() {
        return Arrays.asList(new Boolean[] {false}, new Boolean[] {true});
    }

    @BeforeEach
    void setup() {
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE append_src1 (\n"
                                + "   a int not null,\n"
                                + "   b string not null,\n"
                                + "   c bigint not null,\n"
                                + "   rt timestamp(3),\n"
                                + "   watermark for rt as rt - INTERVAL '1' SECOND)\n"
                                + " with (\n"
                                + "   'connector' = 'values',\n"
                                + "   'changelog-mode' = 'I'\n"
                                + ")");

        util.tableEnv().executeSql("CREATE TABLE append_src2 LIKE append_src1");

        util.tableEnv()
                .executeSql(
                        "CREATE TABLE retract_src (\n"
                                + "   primary key (a) not enforced\n"
                                + ") WITH (\n"
                                + "   'changelog-mode' = 'I,UA,UB,D'\n"
                                + ") LIKE append_src1 (\n"
                                + "   OVERWRITING OPTIONS\n"
                                + ")");

        util.tableEnv()
                .executeSql(
                        "CREATE TABLE upsert_src (\n"
                                + "   primary key (a) not enforced\n"
                                + ") WITH (\n"
                                + "   'changelog-mode' = 'I,UA,D'\n"
                                + ") LIKE append_src1 (\n"
                                + "   OVERWRITING OPTIONS\n"
                                + ")");

        util.tableEnv()
                .executeSql(
                        "CREATE TABLE no_pk_snk (\n"
                                + "   a int not null,\n"
                                + "   b string not null,\n"
                                + "   c bigint not null)\n"
                                + " with (\n"
                                + "   'connector' = 'values',\n"
                                + "   'sink-insert-only' = 'true'\n"
                                + ")");

        util.tableEnv()
                .executeSql(
                        "CREATE TABLE pk_upsert_snk (\n"
                                + "   a int not null,\n"
                                + "   b string not null,\n"
                                + "   c bigint not null,\n"
                                + "   primary key (a) not enforced\n"
                                + ")\n"
                                + " with (\n"
                                + "   'connector' = 'values',\n"
                                + "   'sink-insert-only' = 'false',\n"
                                + "   'sink-changelog-mode-enforced' = 'I,UA,D'\n"
                                + ")");

        util.tableEnv()
                .executeSql(
                        "CREATE TABLE pk_retract_snk WITH (\n"
                                + "  'sink-changelog-mode-enforced' = 'I,UA,UB,D'"
                                + ") LIKE pk_upsert_snk (\n"
                                + "  OVERWRITING OPTIONS\n"
                                + ")");
    }

    @TestTemplate
    void testCalc() {
        String sql =
                String.format("insert into %s select a, b, c from append_src1", getSinkTableName());
        verifyRelPlanInsert(sql);
    }

    @TestTemplate
    void testCalcWithNonDeterministicFilter1() {
        String sql =
                String.format(
                        "insert into %s select a, b, c from append_src1 where c < cast(now() as bigint)",
                        getSinkTableName());
        verifyRelPlanInsert(sql);
    }

    @TestTemplate
    void testCalcWithNonDeterministicFilter2() {
        String sql =
                String.format(
                        "insert into %s select a, b, c from append_src1 where a <> 1 and c < cast(now() as bigint)",
                        getSinkTableName());
        verifyRelPlanInsert(sql);
    }

    @TestTemplate
    void testCalcWithNestedNonDeterministicFilter() {
        String sql =
                String.format(
                        "insert into %s select a, b, c from append_src1 where c < cast(cast(now() as int) as bigint)",
                        getSinkTableName());
        verifyRelPlanInsert(sql);
    }

    @TestTemplate
    void testCalcWithNonDeterministicProjection() {
        String sql =
                String.format(
                        "insert into %s select a, b, cast(now() as bigint) from append_src1",
                        getSinkTableName());
        verifyRelPlanInsert(sql);
    }

    @TestTemplate
    void testCalcWithNestedNonDeterministicProjection() {
        String sql =
                String.format(
                        "insert into %s select a, b, cast(cast(now() as int) as bigint) from append_src1",
                        getSinkTableName());
        verifyRelPlanInsert(sql);
    }

    @TestTemplate
    void testAggregate() {
        assumeTrue(testSinkWithPk);
        String sql = "insert into pk_upsert_snk select a,max(b),sum(c) from append_src1 group by a";
        verifyRelPlanInsert(sql);
    }

    @TestTemplate
    void testOneStageWindowAggregate() {
        util.tableConfig()
                .set(
                        OptimizerConfigOptions.TABLE_OPTIMIZER_AGG_PHASE_STRATEGY,
                        AggregatePhaseStrategy.ONE_PHASE);
        testWindowAggregate();
    }

    @TestTemplate
    void testTwoStageWindowAggregate() {
        util.tableConfig()
                .set(
                        OptimizerConfigOptions.TABLE_OPTIMIZER_AGG_PHASE_STRATEGY,
                        AggregatePhaseStrategy.TWO_PHASE);
        testWindowAggregate();
    }

    private void testWindowAggregate() {
        String sql =
                String.format(
                        "insert into %s select a, max(b), max(c) "
                                + "from TABLE(TUMBLE(TABLE append_src1, DESCRIPTOR(rt), INTERVAL '1' MINUTE)) "
                                + "group by a, window_start, window_end",
                        getSinkTableName());
        verifyRelPlanInsert(sql);
    }

    @TestTemplate
    void testWindowTVF() {
        assumeTrue(testSinkWithPk);

        util.tableEnv()
                .executeSql(
                        "CREATE TABLE pk_upsert_snk_with_time_col (\n"
                                + "  w_start timestamp(3),\n"
                                + "  w_end timestamp(3)\n"
                                + ") LIKE pk_upsert_snk");
        String sql =
                "insert into pk_upsert_snk_with_time_col select a, b, c, window_start, window_end "
                        + "from TABLE(TUMBLE(TABLE append_src1, DESCRIPTOR(rt), INTERVAL '1' MINUTE))";
        verifyRelPlanInsert(sql);
    }

    @TestTemplate
    void testRank() {
        assumeTrue(testSinkWithPk);

        String sql =
                "insert into pk_upsert_snk select a, b, c "
                        + "from ( "
                        + "  select a, b, c, "
                        + "    ROW_NUMBER() OVER (PARTITION BY a ORDER BY c DESC) as rank_num "
                        + "  from append_src1) "
                        + "where rank_num <= 1";
        verifyRelPlanInsert(sql);
    }

    @TestTemplate
    void testWindowRank() {
        assumeTrue(testSinkWithPk);

        util.tableEnv()
                .executeSql(
                        "CREATE TABLE pk_upsert_snk_with_time_col (\n"
                                + "  w_start timestamp(3),\n"
                                + "  w_end timestamp(3)\n"
                                + ") LIKE pk_upsert_snk");
        String sql =
                "insert into pk_upsert_snk_with_time_col select a, b, c, window_start, window_end "
                        + "from ( "
                        + "  select a, b, c, window_start, window_end, "
                        + "    ROW_NUMBER() OVER (PARTITION BY a, window_start, window_end ORDER BY c DESC) as rank_num "
                        + "  from TABLE(TUMBLE(TABLE append_src1, DESCRIPTOR(rt), INTERVAL '1' MINUTE))) "
                        + "where rank_num <= 1";
        verifyRelPlanInsert(sql);
    }

    @TestTemplate
    void testMiniBatchTwoStageAggregate() {
        assumeTrue(testSinkWithPk);

        enableMiniBatch();

        String sql = "insert into pk_upsert_snk select a,max(b),sum(c) from append_src1 group by a";
        verifyRelPlanInsert(sql);
    }

    @TestTemplate
    void testExpandAndIncrementalAggregate() {
        assumeTrue(testSinkWithPk);

        enableMiniBatch();
        util.tableConfig()
                .set(
                        OptimizerConfigOptions.TABLE_OPTIMIZER_AGG_PHASE_STRATEGY,
                        AggregatePhaseStrategy.TWO_PHASE);
        util.tableConfig()
                .set(OptimizerConfigOptions.TABLE_OPTIMIZER_DISTINCT_AGG_SPLIT_ENABLED, true);

        String sql =
                "insert into pk_upsert_snk select a, max(b), count(distinct c) from append_src1 group by a";
        verifyRelPlanInsert(sql);
    }

    @TestTemplate
    void testDropUpdateBefore() {
        assumeTrue(testSinkWithPk);

        String sql = "insert into pk_upsert_snk select a, b, c from retract_src";
        verifyRelPlanInsert(sql);
    }

    @TestTemplate
    void testSinkWithMaterialize() {
        assumeTrue(testSinkWithPk);

        util.tableEnv()
                .executeSql(
                        "CREATE TABLE another_pk_upsert_snk (\n"
                                + "  primary key (b) not enforced\n"
                                + ") LIKE pk_upsert_snk (\n"
                                + "  EXCLUDING CONSTRAINTS\n"
                                + ")");

        String sql = "insert into another_pk_upsert_snk select a, b, c from retract_src";
        verifyRelPlanInsert(sql);
    }

    @TestTemplate
    void testRetractSink() {
        assumeTrue(testSinkWithPk);

        String sql = "insert into pk_retract_snk select a, b, c from retract_src";
        verifyRelPlanInsert(sql);
    }

    @TestTemplate
    void testChangelogNormalize() {
        assumeTrue(testSinkWithPk);

        String sql = "insert into pk_retract_snk select a, b, c from upsert_src";
        verifyRelPlanInsert(sql);
    }

    @TestTemplate
    void testJoin() {
        String sql =
                String.format(
                        "insert into %s select T1.a, T2.b, T1.c "
                                + "from append_src1 T1 join append_src2 T2 "
                                + "on T1.a = T2.a",
                        getSinkTableName());
        verifyRelPlanInsert(sql);
    }

    @TestTemplate
    void testWindowJoin() {
        assumeTrue(testSinkWithPk);

        String sql =
                String.format(
                        "insert into %s select T1.a, T2.b, T1.c "
                                + "from ("
                                + "   select * from TABLE(TUMBLE(TABLE append_src1, DESCRIPTOR(rt), INTERVAL '1' MINUTES))"
                                + ") T1 join ("
                                + "   select * from TABLE(TUMBLE(TABLE append_src2, DESCRIPTOR(rt), INTERVAL '1' MINUTES))"
                                + ") T2 "
                                + "on T1.a = T2.a and T1.window_start = T2.window_start and T1.window_end = T2.window_end",
                        getSinkTableName());
        verifyRelPlanInsert(sql);
    }

    @TestTemplate
    void testIntervalJoin() {
        assumeTrue(testSinkWithPk);

        String sql =
                String.format(
                        "insert into %s select T1.a, T2.b, T1.c "
                                + "from append_src1 T1 join append_src2 T2 "
                                + "on T1.a = T2.a and "
                                + "  T1.rt > T2.rt - INTERVAL '1' MINUTES and "
                                + "  T1.rt < T2.rt + INTERVAL '1' MINUTES",
                        getSinkTableName());
        verifyRelPlanInsert(sql);
    }

    @TestTemplate
    void testLimit() {
        String sql =
                String.format(
                        "insert into %s select a, b, c from append_src1 limit 10",
                        getSinkTableName());
        verifyRelPlanInsert(sql);
    }

    @TestTemplate
    void testTemporalJoin() {
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE dim_src (\n"
                                + "   primary key (a) not enforced\n"
                                + ") WITH (\n"
                                + "   'disable-lookup' = 'true'\n"
                                + ") LIKE append_src1 (\n"
                                + "   OVERWRITING OPTIONS\n"
                                + ")");

        String sql =
                String.format(
                        "insert into %s select T1.a, T1.b, T1.c from ( "
                                + "   select *, proctime() as pt from append_src1 "
                                + ") T1 "
                                + "join dim_src FOR SYSTEM_TIME AS OF T1.pt AS T2 "
                                + "on T1.a = T2.a",
                        getSinkTableName());
        verifyRelPlanInsert(sql);
    }

    @TestTemplate
    void testLookupJoin() {
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE dim_src (\n"
                                + "   primary key (a) not enforced\n"
                                + ") LIKE append_src1 (\n"
                                + "   OVERWRITING OPTIONS\n"
                                + "   EXCLUDING WATERMARKS\n"
                                + ")");

        String sql =
                String.format(
                        "insert into %s select T1.a, T1.b, T1.c from ( "
                                + "   select *, proctime() as pt from append_src1 "
                                + ") T1 "
                                + "join dim_src FOR SYSTEM_TIME AS OF T1.pt AS T2 "
                                + "on T1.a = T2.a",
                        getSinkTableName());
        verifyRelPlanInsert(sql);
    }

    @TestTemplate
    void testUnion() {
        String sql =
                String.format(
                        "insert into %s select a, b, c from append_src1 union all select a, b, c from append_src2",
                        getSinkTableName());
        verifyRelPlanInsert(sql);
    }

    @TestTemplate
    void testMultiSink1() {
        assumeTrue(testSinkWithPk);

        util.tableConfig()
                .set(
                        OptimizerConfigOptions
                                .TABLE_OPTIMIZER_REUSE_OPTIMIZE_BLOCK_WITH_DIGEST_ENABLED,
                        true);

        util.tableEnv().executeSql("CREATE VIEW my_view as select a, b, c+1 as c from append_src1");
        util.tableEnv().executeSql("CREATE TABLE pk_upsert_snk2 LIKE pk_upsert_snk");
        // left: allow
        // right: disallow
        // merged: disallow
        StatementSet stmtSet = util.tableEnv().createStatementSet();
        stmtSet.addInsertSql("insert into pk_upsert_snk select a, b, c/2 from my_view");
        stmtSet.addInsertSql(
                "insert into pk_upsert_snk2 select a, max(b), sum(c) from my_view group by a");
        verifyRelPlanInsert(stmtSet);
    }

    @TestTemplate
    void testMultiSink2() {
        assumeTrue(testSinkWithPk);

        util.tableConfig()
                .set(
                        OptimizerConfigOptions
                                .TABLE_OPTIMIZER_REUSE_OPTIMIZE_BLOCK_WITH_DIGEST_ENABLED,
                        true);

        util.tableEnv().executeSql("CREATE VIEW my_view as select a, b, c+1 as c from append_src1");
        util.tableEnv().executeSql("CREATE TABLE pk_upsert_snk2 LIKE pk_upsert_snk");

        // left: disallow
        // right: allow
        // merged: disallow
        StatementSet stmtSet = util.tableEnv().createStatementSet();
        stmtSet.addInsertSql(
                "insert into pk_upsert_snk select a, max(b), sum(c) from my_view group by a");
        stmtSet.addInsertSql("insert into pk_upsert_snk2 select a, b, c/2 from my_view");
        verifyRelPlanInsert(stmtSet);
    }

    @TestTemplate
    void testMultiSink3() {
        assumeTrue(testSinkWithPk);

        util.tableConfig()
                .set(
                        OptimizerConfigOptions
                                .TABLE_OPTIMIZER_REUSE_OPTIMIZE_BLOCK_WITH_DIGEST_ENABLED,
                        true);

        util.tableEnv().executeSql("CREATE VIEW my_view as select a, b, c+1 as c from append_src1");
        util.tableEnv().executeSql("CREATE TABLE pk_upsert_snk2 LIKE pk_upsert_snk");

        // left: allow
        // right: allow
        // merged: allow
        StatementSet stmtSet = util.tableEnv().createStatementSet();
        stmtSet.addInsertSql("insert into pk_upsert_snk select a, b, c/3 from my_view");
        stmtSet.addInsertSql("insert into pk_upsert_snk2 select a, b, c/2 from my_view");
        verifyRelPlanInsert(stmtSet);
    }

    @TestTemplate
    void testMultiSink4() {
        assumeTrue(testSinkWithPk);

        util.tableConfig()
                .set(
                        OptimizerConfigOptions
                                .TABLE_OPTIMIZER_REUSE_OPTIMIZE_BLOCK_WITH_DIGEST_ENABLED,
                        true);

        util.tableEnv().executeSql("CREATE VIEW my_view as select a, b, c+1 as c from append_src1");
        util.tableEnv().executeSql("CREATE TABLE pk_upsert_snk2 LIKE pk_upsert_snk");

        // left: disallow
        // right: disallow
        // merged: disallow
        StatementSet stmtSet = util.tableEnv().createStatementSet();
        stmtSet.addInsertSql(
                "insert into pk_upsert_snk select a, max(b), sum(c) from my_view group by a");
        stmtSet.addInsertSql(
                "insert into pk_upsert_snk2 select a, min(b), max(c) from my_view group by a");
        verifyRelPlanInsert(stmtSet);
    }

    @TestTemplate
    void testAppendOnlySinkWithPk() {
        assumeFalse(testSinkWithPk);

        util.tableEnv()
                .executeSql(
                        "CREATE TABLE pk_append_sink (\n"
                                + "   primary key (a) not enforced\n"
                                + ") LIKE no_pk_snk (\n"
                                + "   OVERWRITING OPTIONS\n"
                                + ")");

        String sql = "insert into pk_append_sink select a, b, c from append_src1";
        verifyRelPlanInsert(sql);
    }

    private void verifyRelPlanInsert(String insert) {
        StatementSet stmtSet = util.tableEnv().createStatementSet();
        stmtSet.addInsertSql(insert);
        verifyRelPlanInsert(stmtSet);
    }

    private void verifyRelPlanInsert(StatementSet stmtSet) {
        util.doVerifyPlan(
                stmtSet,
                new ExplainDetail[] {},
                false,
                new Enumeration.Value[] {PlanKind.AST(), PlanKind.OPT_REL()},
                () -> UNIT,
                false,
                true);
    }

    private String getSinkTableName() {
        return testSinkWithPk ? "pk_upsert_snk" : "no_pk_snk";
    }

    private void enableMiniBatch() {
        util.tableConfig().set(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED, true);
        util.tableConfig()
                .set(
                        ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ALLOW_LATENCY,
                        Duration.ofSeconds(1));
        util.tableEnv().getConfig().set(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_SIZE, 10L);
    }
}
