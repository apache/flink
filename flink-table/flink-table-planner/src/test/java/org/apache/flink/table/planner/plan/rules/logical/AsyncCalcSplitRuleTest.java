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

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.functions.AsyncScalarFunction;
import org.apache.flink.table.planner.plan.optimize.program.BatchOptimizeContext;
import org.apache.flink.table.planner.plan.optimize.program.FlinkChainedProgram;
import org.apache.flink.table.planner.plan.optimize.program.FlinkHepRuleSetProgramBuilder;
import org.apache.flink.table.planner.plan.optimize.program.HEP_RULES_EXECUTION_TYPE;
import org.apache.flink.table.planner.plan.rules.FlinkStreamRuleSets;
import org.apache.flink.table.planner.utils.TableTestBase;
import org.apache.flink.table.planner.utils.TableTestUtil;
import org.apache.flink.types.Row;

import org.apache.calcite.plan.hep.HepMatchOrder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;

/** Test for {@link AsyncCalcSplitRule}. */
public class AsyncCalcSplitRuleTest extends TableTestBase {

    private TableTestUtil util = streamTestUtil(TableConfig.getDefault());

    @BeforeEach
    public void setup() {
        FlinkChainedProgram programs = new FlinkChainedProgram<BatchOptimizeContext>();
        programs.addLast(
                "logical_rewrite",
                FlinkHepRuleSetProgramBuilder.newBuilder()
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
                        + ") ;");

        tEnv.executeSql(
                "CREATE TABLE MyTable2 (\n"
                        + "  a2 int,\n"
                        + "  b2 bigint,\n"
                        + "  c2 string,\n"
                        + "  d2 ARRAY<INT NOT NULL>\n"
                        + ") ;");

        util.addTemporarySystemFunction("func1", new Func1());
        util.addTemporarySystemFunction("func2", new Func2());
        util.addTemporarySystemFunction("func3", new Func3());
        util.addTemporarySystemFunction("func4", new Func4());
        util.addTemporarySystemFunction("func5", new Func5());
    }

    @Test
    public void testSingleCall() {
        String sqlQuery = "SELECT func1(a) FROM MyTable";
        util.verifyRelPlan(sqlQuery);
    }

    @Test
    public void testLiteralPlusTableSelect() {
        String sqlQuery = "SELECT 'foo', func1(a) FROM MyTable";
        util.verifyRelPlan(sqlQuery);
    }

    @Test
    public void testFieldPlusTableSelect() {
        String sqlQuery = "SELECT a, func1(a) from MyTable";
        util.verifyRelPlan(sqlQuery);
    }

    @Test
    public void testTwoCalls() {
        String sqlQuery = "SELECT func1(a), func1(a) from MyTable";
        util.verifyRelPlan(sqlQuery);
    }

    @Test
    public void testFourCalls() {
        String sqlQuery = "SELECT func1(a), func2(a), func1(a), func2(a) from MyTable";
        util.verifyRelPlan(sqlQuery);
    }

    @Test
    public void testNestedCalls() {
        String sqlQuery = "SELECT func1(func1(func1(a))) from MyTable";
        util.verifyRelPlan(sqlQuery);
    }

    @Test
    public void testThreeNestedCalls() {
        String sqlQuery = "SELECT func1(func1(a)), func1(func1(func1(a))), func1(a) from MyTable";
        util.verifyRelPlan(sqlQuery);
    }

    @Test
    public void testPassedToOtherUDF() {
        String sqlQuery = "SELECT Concat(func2(a), 'foo') from MyTable";
        util.verifyRelPlan(sqlQuery);
    }

    @Test
    public void testJustCall() {
        String sqlQuery = "SELECT func1(1)";
        util.verifyRelPlan(sqlQuery);
    }

    @Test
    public void testWhereCondition() {
        String sqlQuery = "SELECT a from MyTable where REGEXP(func2(a), 'string (2|3)')";
        util.verifyRelPlan(sqlQuery);
    }

    @Test
    public void testWhereConditionAndProjection() {
        String sqlQuery = "SELECT func2(a) from MyTable where REGEXP(func2(a), 'val (2|3)')";
        util.verifyRelPlan(sqlQuery);
    }

    @Test
    public void testWhereConditionWithInts() {
        String sqlQuery = "SELECT a from MyTable where func1(a) >= 12";
        util.verifyRelPlan(sqlQuery);
    }

    @Test
    public void testAggregate() {
        String sqlQuery = "SELECT a, func3(count(*)) from MyTable group by a";
        util.verifyRelPlan(sqlQuery);
    }

    @Test
    public void testSelectCallWithIntArray() {
        String sqlQuery = "SELECT func4(d) from MyTable";
        util.verifyRelPlan(sqlQuery);
    }

    @Test
    public void testFieldAccessAfter() {
        String sqlQuery = "SELECT func5(a).f0 from MyTable";
        util.verifyRelPlan(sqlQuery);
    }

    @Test
    public void testFieldOperand() {
        String sqlQuery = "SELECT func1(func5(a).f0) from MyTable";
        util.verifyRelPlan(sqlQuery);
    }

    @Test
    public void testInnerJoinWithFuncInOn() {
        String sqlQuery =
                "SELECT a from MyTable INNER JOIN MyTable2 ON func2(a) = func2(a2) AND "
                        + "REGEXP(func2(a), 'string (2|4)')";
        util.verifyRelPlan(sqlQuery);
    }

    @Test
    public void testInnerJoinWithFuncProjection() {
        String sqlQuery = "SELECT func1(a) from MyTable INNER JOIN MyTable2 ON a = a2";
        util.verifyRelPlan(sqlQuery);
    }

    @Test
    public void testInnerJoinWithFuncInWhere() {
        String sqlQuery =
                "SELECT a from MyTable INNER JOIN MyTable2 ON a = a2 "
                        + "WHERE REGEXP(func2(a), 'val (2|3)')";
        util.verifyRelPlan(sqlQuery);
    }

    @Test
    public void testLeftJoinWithFuncInOn() {
        String sqlQuery = "SELECT a, a2 from MyTable LEFT JOIN MyTable2 ON func1(a) = func1(a2)";
        util.verifyRelPlan(sqlQuery);
    }

    @Test
    public void testLeftJoinWithFuncInWhere() {
        String sqlQuery =
                "SELECT a, a2 from MyTable LEFT JOIN MyTable2 ON a = a2 "
                        + "WHERE REGEXP(func2(a), 'string (2|3)')";
        util.verifyRelPlan(sqlQuery);
    }

    @Test
    public void testRightJoinWithFuncInOn() {
        String sqlQuery = "SELECT a, a2 from MyTable RIGHT JOIN MyTable2 ON func1(a) = func1(a2)";
        util.verifyRelPlan(sqlQuery);
    }

    @Test
    public void testProjectCallInSubquery() {
        String sqlQuery =
                "SELECT blah FROM (SELECT func2(a) as blah from MyTable) "
                        + "WHERE REGEXP(blah, 'string (2|3)')";
        util.verifyRelPlan(sqlQuery);
    }

    @Test
    public void testWhereConditionCallInSubquery() {
        String sqlQuery =
                "SELECT blah FROM (select a as blah from MyTable "
                        + "WHERE REGEXP(func2(a), 'string (2|3)'))";
        util.verifyRelPlan(sqlQuery);
    }

    @Test
    public void testWhereNotInSubquery() {
        String sqlQuery = "SELECT func1(a) FROM MyTable where a not in (select a2 from MyTable2)";
        util.verifyRelPlan(sqlQuery);
    }

    /** Test function. */
    public static class Func1 extends AsyncScalarFunction {
        public void eval(CompletableFuture<Integer> future, Integer param) {
            future.complete(param + 10);
        }
    }

    /** Test function. */
    public static class Func2 extends AsyncScalarFunction {
        public void eval(CompletableFuture<String> future, Integer param) {
            future.complete("string " + param + 10);
        }
    }

    /** Test function. */
    public static class Func3 extends AsyncScalarFunction {
        public void eval(CompletableFuture<Long> future, Long param) {
            future.complete(param + 10);
        }
    }

    /** Test function. */
    public static class Func4 extends AsyncScalarFunction {
        public void eval(CompletableFuture<int[]> future, int[] param) {
            future.complete(param);
        }
    }

    /** Test function. */
    public static class Func5 extends AsyncScalarFunction {

        @DataTypeHint("ROW<f0 INT, f1 String>")
        public void eval(CompletableFuture<Row> future, Integer a) {
            future.complete(Row.of(a + 1, "" + (a * a)));
        }
    }
}
