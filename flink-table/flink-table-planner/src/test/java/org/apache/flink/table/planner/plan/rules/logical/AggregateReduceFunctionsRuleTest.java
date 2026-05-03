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

import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.planner.calcite.CalciteConfig;
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

/**
 * Test for {@link CoreRules#AGGREGATE_REDUCE_FUNCTIONS}.
 *
 * <p>For now COVAR_POP/COVAR_SAMP/REGR_SXX/REGR_SYY are unsupported yet.
 */
public class AggregateReduceFunctionsRuleTest extends TableTestBase {

    private BatchTableTestUtil util;

    @BeforeEach
    void setup() {
        util = batchTestUtil(TableConfig.getDefault());
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
                                .add(RuleSets.ofList(CoreRules.AGGREGATE_REDUCE_FUNCTIONS))
                                .build());
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE src (\n"
                                + "  a VARCHAR,\n"
                                + "  b BIGINT\n"
                                + ") WITH (\n"
                                + " 'connector' = 'values',\n"
                                + " 'bounded' = 'true'\n"
                                + ")");

        util.tableEnv()
                .executeSql(
                        "CREATE TABLE src2 (\n"
                                + "  id INT,\n"
                                + "  category VARCHAR,\n"
                                + "  x DOUBLE,\n"
                                + "  y DOUBLE,\n"
                                + "  z DECIMAL(10,2)\n"
                                + ") WITH (\n"
                                + " 'connector' = 'values',\n"
                                + " 'bounded' = 'true'\n"
                                + ")");
    }

    @Test
    void testVarianceStddevWithFilter() {
        util.verifyRelPlan(
                "SELECT a, \n"
                        + "STDDEV_POP(b) FILTER (WHERE b > 10), \n"
                        + "STDDEV_SAMP(b) FILTER (WHERE b > 20), \n"
                        + "VAR_POP(b) FILTER (WHERE b > 30), \n"
                        + "VAR_SAMP(b) FILTER (WHERE b > 40), \n"
                        + "AVG(b) FILTER (WHERE b > 50)\n"
                        + "FROM src GROUP BY a");
    }

    @Test
    void testVarianceAndStandardDeviation() {
        // Test variance and standard deviation reductions without filters
        util.verifyRelPlan(
                "SELECT category, \n"
                        + "VAR_POP(x), \n"
                        + "VAR_SAMP(x), \n"
                        + "STDDEV_POP(x), \n"
                        + "STDDEV_SAMP(x) \n"
                        + "FROM src2 GROUP BY category");
    }

    @Test
    void testMixedAggregatesWithDistinct() {
        // Test combinations of different aggregate functions with DISTINCT
        util.verifyRelPlan(
                "SELECT category, \n"
                        + "AVG(DISTINCT x), \n"
                        + "VAR_POP(DISTINCT y), \n"
                        + "STDDEV_SAMP(DISTINCT z) \n"
                        + "FROM src2 GROUP BY category");
    }

    @Test
    void testComplexExpressionsInAggregates() {
        // Test aggregates with complex expressions as arguments
        util.verifyRelPlan(
                "SELECT category, \n"
                        + "AVG(x + y), \n"
                        + "VAR_POP(x * 2), \n"
                        + "STDDEV_SAMP(CASE WHEN x > 0 THEN x ELSE 0 END) \n"
                        + "FROM src2 GROUP BY category");
    }

    @Test
    void testMultipleGroupingColumns() {
        // Test aggregate reductions with multiple grouping columns
        util.verifyRelPlan(
                "SELECT category, id, \n"
                        + "AVG(x), \n"
                        + "VAR_SAMP(y), \n"
                        + "VAR_POP(z) \n"
                        + "FROM src2 GROUP BY category, id");
    }

    @Test
    void testAggregatesWithOrderBy() {
        // Test aggregate reductions with ORDER BY clause
        util.verifyRelPlan(
                "SELECT category, \n"
                        + "AVG(x) as avg_x, \n"
                        + "STDDEV_POP(y) as stddev_y \n"
                        + "FROM src2 \n"
                        + "GROUP BY category \n"
                        + "ORDER BY avg_x DESC");
    }

    @Test
    void testAggregatesWithHaving() {
        // Test aggregate reductions with HAVING clause
        util.verifyRelPlan(
                "SELECT category, \n"
                        + "AVG(x) as avg_x, \n"
                        + "VAR_POP(y) as var_y \n"
                        + "FROM src2 \n"
                        + "GROUP BY category \n"
                        + "HAVING AVG(x) > 50 AND VAR_POP(y) < 100");
    }

    @Test
    void testDifferentDataTypes() {
        // Test aggregate reductions with different data types
        util.verifyRelPlan(
                "SELECT category, \n"
                        + "AVG(CAST(id AS DOUBLE)), \n"
                        + "VAR_POP(CAST(x AS DECIMAL(20,4))), \n"
                        + "STDDEV_SAMP(z) \n"
                        + "FROM src2 GROUP BY category");
    }

    @Test
    void testEmptyGroupBy() {
        // Test aggregate reductions without GROUP BY (global aggregates)
        util.verifyRelPlan(
                "SELECT \n" + "AVG(x), \n" + "VAR_POP(y), \n" + "STDDEV_SAMP(z) \n" + "FROM src2");
    }

    @Test
    void testComplexFilterConditions() {
        // Test aggregates with complex filter conditions
        util.verifyRelPlan(
                "SELECT category, \n"
                        + "AVG(x) FILTER (WHERE x > y AND category LIKE 'A%'), \n"
                        + "STDDEV_POP(x) FILTER (WHERE x + y > 100), \n"
                        + "VAR_SAMP(y) FILTER (WHERE MOD(id, 2) = 0) \n"
                        + "FROM src2 GROUP BY category");
    }

    @Test
    void testAllSupportedFunctionsInSingleQuery() {
        // Comprehensive test with all supported aggregate functions
        util.verifyRelPlan(
                "SELECT category, \n"
                        + "SUM(x), \n"
                        + "AVG(x), \n"
                        + "VAR_POP(x), \n"
                        + "VAR_SAMP(x), \n"
                        + "STDDEV_POP(x), \n"
                        + "STDDEV_SAMP(x) \n"
                        + "FROM src2 GROUP BY category");
    }
}
