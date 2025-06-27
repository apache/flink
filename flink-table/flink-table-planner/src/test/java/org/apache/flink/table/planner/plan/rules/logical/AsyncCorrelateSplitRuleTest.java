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
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.planner.plan.optimize.program.FlinkChainedProgram;
import org.apache.flink.table.planner.plan.optimize.program.FlinkHepRuleSetProgramBuilder;
import org.apache.flink.table.planner.plan.optimize.program.HEP_RULES_EXECUTION_TYPE;
import org.apache.flink.table.planner.plan.optimize.program.StreamOptimizeContext;
import org.apache.flink.table.planner.plan.rules.FlinkStreamRuleSets;
import org.apache.flink.table.planner.plan.rules.logical.AsyncCalcSplitRuleTest.Func1;
import org.apache.flink.table.planner.plan.rules.logical.AsyncCalcSplitRuleTest.RandomTableFunction;
import org.apache.flink.table.planner.utils.TableTestBase;
import org.apache.flink.table.planner.utils.TableTestUtil;

import org.apache.calcite.plan.hep.HepMatchOrder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Test for {@link AsyncCorrelateSplitRule}. */
public class AsyncCorrelateSplitRuleTest extends TableTestBase {

    private final TableTestUtil util = streamTestUtil(TableConfig.getDefault());

    @BeforeEach
    public void setup() {
        FlinkChainedProgram<StreamOptimizeContext> programs = new FlinkChainedProgram<>();
        programs.addLast(
                "logical_rewrite",
                FlinkHepRuleSetProgramBuilder.<StreamOptimizeContext>newBuilder()
                        .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE())
                        .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
                        .add(FlinkStreamRuleSets.LOGICAL_REWRITE())
                        .build());

        TableEnvironment tEnv = util.getTableEnv();
        tEnv.executeSql(
                "CREATE TABLE MyTable (\n"
                        + "  a int,\n"
                        + "  b bigint,\n"
                        + "  c string,\n"
                        + "  d ARRAY<INT NOT NULL>\n"
                        + ") WITH (\n"
                        + "  'connector' = 'test-simple-table-source'\n"
                        + ") ;");

        util.addTemporarySystemFunction("func1", new Func1());
        util.addTemporarySystemFunction("tableFunc", new RandomTableFunction());
    }

    @Test
    public void testCorrelateImmediate() {
        String sqlQuery = "select * FROM MyTable, LATERAL TABLE(tableFunc(func1(a)))";
        util.verifyRelPlan(sqlQuery);
    }

    @Test
    public void testCorrelateIndirect() {
        String sqlQuery = "select * FROM MyTable, LATERAL TABLE(tableFunc(ABS(func1(a))))";
        util.verifyRelPlan(sqlQuery);
    }

    @Test
    public void testCorrelateIndirectOtherWay() {
        String sqlQuery = "select * FROM MyTable, LATERAL TABLE(tableFunc(func1(ABS(a))))";
        util.verifyRelPlan(sqlQuery);
    }
}
