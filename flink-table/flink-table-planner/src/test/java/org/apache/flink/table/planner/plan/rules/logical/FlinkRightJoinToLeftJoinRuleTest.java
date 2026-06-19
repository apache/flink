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

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.planner.plan.optimize.program.FlinkChainedProgram;
import org.apache.flink.table.planner.plan.optimize.program.FlinkGroupProgramBuilder;
import org.apache.flink.table.planner.plan.optimize.program.FlinkHepRuleSetProgramBuilder;
import org.apache.flink.table.planner.plan.optimize.program.FlinkStreamProgram;
import org.apache.flink.table.planner.plan.optimize.program.HEP_RULES_EXECUTION_TYPE;
import org.apache.flink.table.planner.plan.optimize.program.StreamOptimizeContext;
import org.apache.flink.table.planner.utils.StreamTableTestUtil;
import org.apache.flink.table.planner.utils.TableConfigUtils;
import org.apache.flink.table.planner.utils.TableTestBase;

import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.tools.RuleSets;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests for {@link FlinkRightJoinToLeftJoinRule}. */
public class FlinkRightJoinToLeftJoinRuleTest extends TableTestBase {

    private StreamTableTestUtil util;

    @BeforeEach
    public void setup() {
        util = streamTestUtil(TableConfig.getDefault());

        util.buildStreamProgram(FlinkStreamProgram.DEFAULT_REWRITE());

        TableConfig config = util.tableEnv().getConfig();

        FlinkChainedProgram<StreamOptimizeContext> streamProgram =
                TableConfigUtils.getCalciteConfig(config).getStreamProgram().get();

        streamProgram.addLast(
                "right-to-left-rules",
                FlinkGroupProgramBuilder.<StreamOptimizeContext>newBuilder()
                        .addProgram(
                                FlinkHepRuleSetProgramBuilder.<StreamOptimizeContext>newBuilder()
                                        .setHepRulesExecutionType(
                                                HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE())
                                        .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
                                        .add(RuleSets.ofList(FlinkRightJoinToLeftJoinRule.INSTANCE))
                                        .build(),
                                "right-join-to-left-join")
                        .build());

        util.addTableSource(
                "T1",
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .column("b", DataTypes.BIGINT())
                        .build());

        util.addTableSource(
                "T2",
                Schema.newBuilder()
                        .column("c", DataTypes.INT())
                        .column("d", DataTypes.BIGINT())
                        .column("e", DataTypes.BIGINT())
                        .build());

        util.addTableSource(
                "T3",
                Schema.newBuilder()
                        .column("f", DataTypes.INT())
                        .column("g", DataTypes.BIGINT())
                        .build());
    }

    @Test
    public void testRightJoin() {
        util.verifyRelPlan("SELECT * FROM T1 RIGHT JOIN T2 ON a = c");
    }

    @Test
    public void testNestedProject() {
        util.verifyRelPlan(
                "SELECT * FROM (SELECT * FROM T1 JOIN T2 ON a = c) RIGHT JOIN T3 ON a = f");
    }

    @Test
    public void testRightInnerJoinChain() {
        util.verifyRelPlan("SELECT * FROM T2 RIGHT JOIN T3 on T2.c = T3.f JOIN T1 ON T1.a = T2.c");
    }

    @Test
    public void testRightRightJoinChain() {
        util.verifyRelPlan(
                "SELECT * FROM T2 RIGHT JOIN T3 on T2.c = T3.f RIGHT JOIN T1 ON T1.a = T2.c");
    }

    @Test
    public void testRightLeftJoinChain() {
        util.verifyRelPlan(
                "SELECT * FROM T2 RIGHT JOIN T3 on T2.c = T3.f LEFT JOIN T1 ON T1.a = T2.c");
    }

    @Test
    public void testLeftJoinRemainsUnchanged() {
        util.verifyRelPlan("SELECT * FROM T1 LEFT JOIN T2 ON a = c");
    }

    @Test
    public void testRightJoinWithExpressionCondition() {
        util.verifyRelPlan("SELECT * FROM T1 RIGHT JOIN T2 ON a + 1 = c - 1");
    }

    @Test
    public void testRightJoinWithProjection() {
        util.verifyRelPlan("SELECT b, d FROM T1 RIGHT JOIN T2 ON a = c");
    }
}
