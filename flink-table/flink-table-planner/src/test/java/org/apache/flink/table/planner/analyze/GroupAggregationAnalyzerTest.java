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

package org.apache.flink.table.planner.analyze;

import org.apache.flink.table.api.ExplainDetail;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.planner.plan.utils.JavaUserDefinedAggFunctions;
import org.apache.flink.table.planner.utils.AggregatePhaseStrategy;
import org.apache.flink.table.planner.utils.PlanKind;
import org.apache.flink.table.planner.utils.StreamTableTestUtil;
import org.apache.flink.table.planner.utils.TableTestBase;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.time.Duration;

import scala.Enumeration;

import static scala.runtime.BoxedUnit.UNIT;

/** Test for {@link GroupAggregationAnalyzer}. */
@RunWith(Parameterized.class)
public class GroupAggregationAnalyzerTest extends TableTestBase {

    private final StreamTableTestUtil util = streamTestUtil(TableConfig.getDefault());

    @Parameterized.Parameter public boolean isMiniBatchEnabled;

    @Parameterized.Parameter(1)
    public AggregatePhaseStrategy strategy;

    @Parameterized.Parameter(2)
    public long miniBatchLatency;

    @Parameterized.Parameter(3)
    public long miniBatchSize;

    private final String query =
            "SELECT\n"
                    + "  AVG(a) AS avg_a,\n"
                    + "  COUNT(*) AS cnt,\n"
                    + "  count(b) AS cnt_b,\n"
                    + "  min(b) AS min_b,\n"
                    + "  MAX(c) FILTER (WHERE a > 1) AS max_c\n"
                    + "FROM MyTable";

    @Before
    public void before() {
        util.getTableEnv()
                .getConfig()
                .set(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED, isMiniBatchEnabled)
                .set(OptimizerConfigOptions.TABLE_OPTIMIZER_AGG_PHASE_STRATEGY, strategy.toString())
                .set(
                        ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ALLOW_LATENCY,
                        Duration.ofSeconds(miniBatchLatency))
                .set(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_SIZE, miniBatchSize);
        util.getTableEnv()
                .executeSql(
                        "CREATE TABLE MyTable (\n"
                                + "  a BIGINT,\n"
                                + "  b INT NOT NULL,\n"
                                + "  c VARCHAR,\n"
                                + "  d BIGINT\n"
                                + ") WITH (\n"
                                + "  'connector' = 'values',\n"
                                + "  'bounded' = 'false')");
        util.getTableEnv()
                .executeSql(
                        "CREATE TABLE MySink (\n"
                                + "  avg_a DOUBLE,\n"
                                + "  cnt BIGINT,\n"
                                + "  cnt_b BIGINT,\n"
                                + "  min_b BIGINT,\n"
                                + "  max_c VARCHAR\n"
                                + ") WITH (\n"
                                + "  'connector' = 'values',\n"
                                + "  'sink-insert-only' = 'false')");
    }

    @Test
    public void testSelect() {
        util.doVerifyPlan(
                query,
                new ExplainDetail[] {ExplainDetail.PLAN_ADVICE},
                false,
                new Enumeration.Value[] {PlanKind.OPT_REL_WITH_ADVICE()},
                false);
    }

    @Test
    public void testInsertInto() {
        util.doVerifyPlanInsert(
                String.format("INSERT INTO MySink\n%s", query),
                new ExplainDetail[] {ExplainDetail.PLAN_ADVICE},
                false,
                new Enumeration.Value[] {PlanKind.OPT_REL_WITH_ADVICE()});
    }

    @Test
    public void testStatementSet() {
        StatementSet stmtSet = util.getTableEnv().createStatementSet();
        util.getTableEnv().executeSql("CREATE TABLE MySink2 LIKE MySink");
        util.getTableEnv()
                .executeSql(
                        "CREATE TABLE MySink3 (\n"
                                + "  b INT NOT NULL,\n"
                                + "  sum_a BIGINT,\n"
                                + "  cnt_c BIGINT\n"
                                + "  ) WITH (\n"
                                + "  'connector' = 'values',\n"
                                + "  'sink-insert-only' = 'false')");

        stmtSet.addInsertSql(String.format("INSERT INTO MySink\n%s", query));
        stmtSet.addInsertSql(String.format("INSERT INTO MySink2\n%s", query));
        stmtSet.addInsertSql(
                "INSERT INTO MySink3\n"
                        + "SELECT \n"
                        + "  b, \n"
                        + "  SUM(a) AS sum_a,\n"
                        + "  COUNT(c) AS cnt_c\n"
                        + "FROM MyTable\n"
                        + "GROUP BY b");
        util.doVerifyPlan(
                stmtSet,
                new ExplainDetail[] {ExplainDetail.PLAN_ADVICE},
                false,
                new Enumeration.Value[] {PlanKind.OPT_REL_WITH_ADVICE()},
                () -> UNIT,
                false);
    }

    @Test
    public void testSubplanReuse() {
        util.doVerifyPlan(
                "WITH r AS (SELECT c, SUM(a) a, SUM(b) b FROM MyTable GROUP BY c)\n"
                        + "SELECT * FROM r r1, r r2 WHERE r1.a = CAST(r2.b AS BIGINT) AND r2.a > 1",
                new ExplainDetail[] {ExplainDetail.PLAN_ADVICE},
                false,
                new Enumeration.Value[] {PlanKind.OPT_REL_WITH_ADVICE()},
                false);
    }

    @Test
    public void testUserDefinedAggCalls() {
        StatementSet stmtSet = util.getTableEnv().createStatementSet();
        util.addTemporarySystemFunction(
                "weightedAvg", JavaUserDefinedAggFunctions.WeightedAvgWithMerge.class);
        util.addTemporarySystemFunction(
                "weightedAvgWithoutMerge", JavaUserDefinedAggFunctions.WeightedAvg.class);

        util.getTableEnv()
                .executeSql(
                        "CREATE TABLE MySink1 (\n"
                                + "  avg_a_1 DOUBLE\n"
                                + ") WITH (\n"
                                + "  'connector' = 'values',\n"
                                + "  'sink-insert-only' = 'false')");
        util.getTableEnv().executeSql("CREATE TABLE MySink2 (avg_a_2 DOUBLE) LIKE MySink1");

        stmtSet.addInsertSql(
                "INSERT INTO MySink1\n"
                        + "SELECT \n"
                        + "  weightedAvg(a, d) AS avg_a_1\n"
                        + "FROM MyTable");
        stmtSet.addInsertSql(
                "INSERT INTO MySink2\n"
                        + "SELECT \n"
                        + "  weightedAvg(a, d) AS avg_a_1,\n"
                        + "  weightedAvgWithoutMerge(a, d) AS avg_a_2\n"
                        + "FROM MyTable");
        util.doVerifyPlan(
                stmtSet,
                new ExplainDetail[] {ExplainDetail.PLAN_ADVICE},
                false,
                new Enumeration.Value[] {PlanKind.OPT_REL_WITH_ADVICE()},
                () -> UNIT,
                false);
    }

    @Parameterized.Parameters(
            name = "isMiniBatchEnabled={0}, strategy={1}, miniBatchLatency={2}, miniBatchSize={3}")
    public static Object[][] data() {
        return new Object[][] {
            new Object[] {true, AggregatePhaseStrategy.ONE_PHASE, 10L, 5L},
            new Object[] {true, AggregatePhaseStrategy.AUTO, 10L, 5L},
            new Object[] {false, AggregatePhaseStrategy.ONE_PHASE, 0L, -1L},
            new Object[] {false, AggregatePhaseStrategy.AUTO, 10L, 5L}
        };
    }
}
