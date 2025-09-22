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

/** Test for {@link AggregateReduceFunctionsRule}. */
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
}
