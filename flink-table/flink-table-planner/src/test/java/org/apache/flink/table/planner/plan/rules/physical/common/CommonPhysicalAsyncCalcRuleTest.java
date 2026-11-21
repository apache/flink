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

package org.apache.flink.table.planner.plan.rules.physical.common;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalCalc;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalAsyncCalc;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalAsyncCalc;
import org.apache.flink.table.planner.plan.rules.physical.batch.BatchPhysicalAsyncCalcRule;
import org.apache.flink.table.planner.plan.rules.physical.stream.StreamPhysicalAsyncCalcRule;
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedScalarFunctions.AsyncPythonScalarFunction;
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedTableFunctions.AsyncSumScalarFunction;
import org.apache.flink.table.planner.utils.JavaTableTestUtil;
import org.apache.flink.table.planner.utils.TableTestBase;
import org.apache.flink.table.planner.utils.TableTestUtil;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Calc;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests that JVM async conversion only accepts Java async functions. */
class CommonPhysicalAsyncCalcRuleTest extends TableTestBase {

    @ParameterizedTest
    @CsvSource({
        "true, python_async, false",
        "true, java_async, true",
        "false, python_async, false",
        "false, java_async, true"
    })
    void testAsyncFunctionConversion(boolean isStreaming, String function, boolean converts) {
        final JavaTableTestUtil util = isStreaming ? javaStreamTestUtil() : javaBatchTestUtil();
        final RelOptRule rule =
                isStreaming
                        ? StreamPhysicalAsyncCalcRule.INSTANCE
                        : BatchPhysicalAsyncCalcRule.INSTANCE;
        final Class<?> asyncCalcClass =
                isStreaming ? StreamPhysicalAsyncCalc.class : BatchPhysicalAsyncCalc.class;
        util.addTableSource(
                "src",
                Schema.newBuilder()
                        .column("a", DataTypes.INT().notNull())
                        .column("b", DataTypes.INT().notNull())
                        .build());
        util.tableEnv()
                .createTemporarySystemFunction(
                        "python_async", new AsyncPythonScalarFunction("python_async"));
        util.tableEnv().createTemporarySystemFunction("java_async", new AsyncSumScalarFunction());
        final Calc calc =
                (Calc)
                        util.getPlanner()
                                .optimize(
                                        TableTestUtil.toRelNode(
                                                util.tableEnv()
                                                        .sqlQuery(
                                                                "SELECT "
                                                                        + function
                                                                        + "(a, b) FROM src")));
        final HepPlanner planner =
                new HepPlanner(new HepProgramBuilder().addRuleInstance(rule).build());
        planner.setRoot(FlinkLogicalCalc.create(calc.getInput(), calc.getProgram()));
        final RelNode result = planner.findBestExp();
        assertThat(result).isInstanceOf(converts ? asyncCalcClass : FlinkLogicalCalc.class);
    }
}
