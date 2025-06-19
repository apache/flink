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
import org.apache.flink.table.planner.plan.rules.FlinkStreamRuleSets;
import org.apache.flink.table.planner.utils.StreamTableTestUtil;
import org.apache.flink.table.planner.utils.TableConfigUtils;
import org.apache.flink.table.planner.utils.TableTestBase;

import org.apache.calcite.plan.hep.HepMatchOrder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests for [[FlinkStreamJoinToMultiJoinRule]]. */
class FlinkStreamJoinToMultiJoinRuleTest extends TableTestBase {

    private StreamTableTestUtil util;

    @BeforeEach
    public void setup() {
        util = streamTestUtil(TableConfig.getDefault());

        util.buildStreamProgram(FlinkStreamProgram.DEFAULT_REWRITE());

        TableConfig config = util.tableEnv().getConfig();

        FlinkChainedProgram<StreamOptimizeContext> streamProgram =
                TableConfigUtils.getCalciteConfig(config).getStreamProgram().get();

        streamProgram.addLast(
                "multi-join-rules",
                FlinkGroupProgramBuilder.<StreamOptimizeContext>newBuilder()
                        .addProgram(
                                FlinkHepRuleSetProgramBuilder.<StreamOptimizeContext>newBuilder()
                                        .setHepRulesExecutionType(
                                                HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE())
                                        .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
                                        .add(FlinkStreamRuleSets.MULTI_JOIN())
                                        .build(),
                                "multi-join-rule-set")
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

        util.addTableSource(
                "T4",
                Schema.newBuilder()
                        .column("h", DataTypes.INT())
                        .column("i", DataTypes.BIGINT())
                        .build());
    }

    @Test
    public void testInnerJoinChain() {
        util.verifyRelPlan("SELECT * FROM T2 JOIN T3 on T2.c = T3.f JOIN T1 ON T1.a = T2.c");
    }

    @Test
    public void testInnerJoinChainComplexCondition() {
        util.verifyRelPlan(
                "SELECT * FROM T2 JOIN T3 on T2.c = T3.f JOIN T1 ON T1.a = T2.c AND T1.b = T2.d");
    }

    @Test
    public void testInnerJoinChainNoCommonJoinKey() {
        util.verifyRelPlan("SELECT * FROM T2 JOIN T3 on T2.c = T3.f JOIN T1 ON T1.b = T2.d");
    }

    @Test
    public void testLeftInnerJoinChain() {
        util.verifyRelPlan("SELECT * FROM T2 LEFT JOIN T3 on T2.c = T3.f JOIN T1 ON T1.a = T2.c");
    }

    @Test
    public void testLeftLeftJoinChain() {
        util.verifyRelPlan(
                "SELECT * FROM T2 LEFT JOIN T3 on T2.c = T3.f LEFT JOIN T1 ON T1.a = T2.c");
    }

    @Test
    public void testRightInnerJoinChain() {
        util.verifyRelPlan(
                "SELECT * FROM T2 RIGHT JOIN T3 on T2.c = T3.f INNER JOIN T1 ON T1.a = T2.c");
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
    public void testInnerRightJoinChain() {
        util.verifyRelPlan(
                "SELECT * FROM T2 INNER JOIN T3 on T2.c = T3.f RIGHT JOIN T1 ON T1.a = T2.c");
    }

    @Test
    public void testInnerInnerInnerJoinChain() {
        util.verifyRelPlan(
                "SELECT * FROM T2 INNER JOIN T3 on T2.c = T3.f INNER JOIN T1 ON T1.a = T2.c INNER JOIN T4 ON T4.h = T2.c");
    }
}
