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

package org.apache.flink.table.planner.plan.rules.logical;

import org.apache.flink.table.api.SqlParserException;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.planner.calcite.CalciteConfig;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalSort;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalTableSourceScan;
import org.apache.flink.table.planner.plan.optimize.program.BatchOptimizeContext;
import org.apache.flink.table.planner.plan.optimize.program.FlinkBatchProgram;
import org.apache.flink.table.planner.plan.optimize.program.FlinkHepRuleSetProgramBuilder;
import org.apache.flink.table.planner.plan.optimize.program.HEP_RULES_EXECUTION_TYPE;
import org.apache.flink.table.planner.utils.BatchTableTestUtil;
import org.apache.flink.table.planner.utils.TableConfigUtils;
import org.apache.flink.table.planner.utils.TableTestBase;

import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.tools.RuleSets;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

/** Test for {@link PushLimitIntoTableSourceScanRule}. */
class PushLimitIntoTableSourceScanRuleTest extends TableTestBase {

    private final BatchTableTestUtil util = batchTestUtil(TableConfig.getDefault());

    @BeforeEach
    public void setup() {
        util.buildBatchProgram(FlinkBatchProgram.DEFAULT_REWRITE());
        CalciteConfig calciteConfig =
                TableConfigUtils.getCalciteConfig(util.tableEnv().getConfig());
        calciteConfig
                .getBatchProgram()
                .get()
                .addLast(
                        "rules",
                        FlinkHepRuleSetProgramBuilder.<BatchOptimizeContext>newBuilder()
                                .setHepRulesExecutionType(
                                        HEP_RULES_EXECUTION_TYPE.RULE_COLLECTION())
                                .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
                                .add(
                                        RuleSets.ofList(
                                                PushLimitIntoTableSourceScanRule.INSTANCE,
                                                CoreRules.SORT_PROJECT_TRANSPOSE,
                                                // converts calcite rel(RelNode) to flink
                                                // rel(FlinkRelNode)
                                                FlinkLogicalSort.BATCH_CONVERTER(),
                                                FlinkLogicalTableSourceScan.CONVERTER()))
                                .build());

        String ddl =
                "CREATE TABLE LimitTable (\n"
                        + "  a int,\n"
                        + "  b bigint,\n"
                        + "  c string\n"
                        + ") WITH (\n"
                        + " 'connector' = 'values',\n"
                        + " 'bounded' = 'true'\n"
                        + ")";
        util.tableEnv().executeSql(ddl);
    }

    @Test
    void testLimitWithNegativeOffset() {
        assertThatExceptionOfType(SqlParserException.class)
                .isThrownBy(
                        () -> util.verifyRelPlan("SELECT a, c FROM LimitTable LIMIT 10 OFFSET -1"));
    }

    @Test
    void testNegativeLimitWithoutOffset() {
        assertThatExceptionOfType(SqlParserException.class)
                .isThrownBy(() -> util.verifyRelPlan("SELECT a, c FROM LimitTable LIMIT -1"));
    }

    @Test
    void testMysqlLimit() {
        assertThatExceptionOfType(SqlParserException.class)
                .isThrownBy(() -> util.verifyRelPlan("SELECT a, c FROM LimitTable LIMIT 1, 10"));
    }

    @Test
    void testCanPushdownLimitWithoutOffset() {
        util.verifyRelPlan("SELECT a, c FROM LimitTable LIMIT 5");
    }

    @Test
    void testCanPushdownLimitWithOffset() {
        util.verifyRelPlan("SELECT a, c FROM LimitTable LIMIT 10 OFFSET 1");
    }

    @Test
    void testCanPushdownFetchWithOffset() {
        util.verifyRelPlan("SELECT a, c FROM LimitTable OFFSET 10 ROWS FETCH NEXT 10 ROWS ONLY");
    }

    @Test
    void testCanPushdownFetchWithoutOffset() {
        util.verifyRelPlan("SELECT a, c FROM LimitTable FETCH FIRST 10 ROWS ONLY");
    }

    @Test
    void testCannotPushDownWithoutLimit() {
        util.verifyRelPlan("SELECT a, c FROM LimitTable OFFSET 10");
    }

    @Test
    void testCannotPushDownWithoutFetch() {
        util.verifyRelPlan("SELECT a, c FROM LimitTable OFFSET 10 ROWS");
    }

    @Test
    void testCannotPushDownWithOrderBy() {
        util.verifyRelPlan("SELECT a, c FROM LimitTable ORDER BY c LIMIT 10");
    }
}
