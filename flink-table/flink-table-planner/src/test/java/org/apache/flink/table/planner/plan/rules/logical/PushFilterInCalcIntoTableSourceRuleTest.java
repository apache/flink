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
import org.apache.flink.table.planner.plan.nodes.FlinkConventions;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalCalc;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalTableSourceScan;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalWatermarkAssigner;
import org.apache.flink.table.planner.plan.optimize.program.FlinkChainedProgram;
import org.apache.flink.table.planner.plan.optimize.program.FlinkHepRuleSetProgramBuilder;
import org.apache.flink.table.planner.plan.optimize.program.FlinkVolcanoProgramBuilder;
import org.apache.flink.table.planner.plan.optimize.program.HEP_RULES_EXECUTION_TYPE;
import org.apache.flink.table.planner.plan.optimize.program.StreamOptimizeContext;
import org.apache.flink.table.planner.utils.StreamTableTestUtil;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.tools.RuleSets;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.ZoneId;

/** Test for {@link PushFilterInCalcIntoTableSourceRuleTest}. */
class PushFilterInCalcIntoTableSourceRuleTest extends PushFilterIntoTableSourceScanRuleTestBase {

    @BeforeEach
    void setup() {
        util = streamTestUtil(TableConfig.getDefault());

        FlinkChainedProgram<StreamOptimizeContext> program = new FlinkChainedProgram<>();
        program.addLast(
                "Converters",
                FlinkVolcanoProgramBuilder.<StreamOptimizeContext>newBuilder()
                        .add(
                                RuleSets.ofList(
                                        CoreRules.PROJECT_TO_CALC,
                                        CoreRules.FILTER_TO_CALC,
                                        FlinkCalcMergeRule.INSTANCE,
                                        FlinkLogicalCalc.CONVERTER(),
                                        FlinkLogicalTableSourceScan.CONVERTER(),
                                        FlinkLogicalWatermarkAssigner.CONVERTER()))
                        .setRequiredOutputTraits(new Convention[] {FlinkConventions.LOGICAL()})
                        .build());
        program.addLast(
                "Filter push in calc down",
                FlinkHepRuleSetProgramBuilder.<StreamOptimizeContext>newBuilder()
                        .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE())
                        .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
                        .add(RuleSets.ofList(PushFilterInCalcIntoTableSourceScanRule.INSTANCE))
                        .build());
        ((StreamTableTestUtil) util).replaceStreamProgram(program);

        String ddl1 =
                "CREATE TABLE MyTable (\n"
                        + "  name STRING,\n"
                        + "  id bigint,\n"
                        + "  amount int,\n"
                        + "  price double\n"
                        + ") WITH (\n"
                        + " 'connector' = 'values',\n"
                        + " 'filterable-fields' = 'amount',\n"
                        + " 'bounded' = 'true'\n"
                        + ")";
        util.tableEnv().executeSql(ddl1);

        String ddl2 =
                "CREATE TABLE VirtualTable (\n"
                        + "  name STRING,\n"
                        + "  id bigint,\n"
                        + "  amount int,\n"
                        + "  virtualField as amount + 1,\n"
                        + "  price double\n"
                        + ") WITH (\n"
                        + " 'connector' = 'values',\n"
                        + " 'filterable-fields' = 'amount',\n"
                        + " 'bounded' = 'true'\n"
                        + ")";

        util.tableEnv().executeSql(ddl2);

        String ddl3 =
                "CREATE TABLE WithWatermark ("
                        + "  name STRING,\n"
                        + "  event_time TIMESTAMP(3),\n"
                        + "  WATERMARK FOR event_time as event_time - INTERVAL '5' SECOND"
                        + ") WITH (\n"
                        + " 'connector' = 'values',\n"
                        + " 'bounded' = 'false',\n"
                        + " 'filterable-fields' = 'name',\n"
                        + " 'disable-lookup' = 'true'"
                        + ")";

        util.tableEnv().executeSql(ddl3);
    }

    @Test
    void testFailureToPushFilterIntoSourceWithoutWatermarkPushdown() {
        util.verifyRelPlan("SELECT * FROM WithWatermark WHERE LOWER(name) = 'foo'");
    }

    @Test
    void testLowerUpperPushdown() {
        String ddl =
                "CREATE TABLE MTable (\n"
                        + "  a STRING,\n"
                        + "  b STRING\n"
                        + ") WITH (\n"
                        + " 'connector' = 'values',\n"
                        + " 'filterable-fields' = 'a;b',\n"
                        + " 'bounded' = 'true'\n"
                        + ")";
        util.tableEnv().executeSql(ddl);
        super.testLowerUpperPushdown();
    }

    @Test
    void testWithInterval() {
        String ddl =
                "CREATE TABLE MTable (\n"
                        + "a TIMESTAMP(3),\n"
                        + "b TIMESTAMP(3)\n"
                        + ") WITH (\n"
                        + " 'connector' = 'values',\n"
                        + " 'bounded' = 'false',\n"
                        + " 'filterable-fields' = 'a;b',\n"
                        + " 'disable-lookup' = 'true'"
                        + ")";

        util.tableEnv().executeSql(ddl);
        super.testWithInterval();
    }

    @Test
    public void testWithTimestampWithTimeZone() {
        String ddl =
                "CREATE TABLE MTable (\n"
                        + "a TIMESTAMP_LTZ(3),\n"
                        + "b TIMESTAMP(3)\n"
                        + ") WITH (\n"
                        + " 'connector' = 'values',\n"
                        + " 'bounded' = 'false',\n"
                        + " 'filterable-fields' = 'a',\n"
                        + " 'disable-lookup' = 'true'"
                        + ")";

        util.tableEnv().executeSql(ddl);
        ZoneId preZoneId = util.tableEnv().getConfig().getLocalTimeZone();
        util.tableEnv().getConfig().setLocalTimeZone(ZoneId.of("Asia/Shanghai"));
        try {
            super.testWithTimestampWithTimeZone();
        } finally {
            util.tableEnv().getConfig().setLocalTimeZone(preZoneId);
        }
    }
}
