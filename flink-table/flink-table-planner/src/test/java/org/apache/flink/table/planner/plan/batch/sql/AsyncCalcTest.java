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

package org.apache.flink.table.planner.plan.batch.sql;

import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.planner.plan.rules.logical.AsyncCalcSplitRuleTest;
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedScalarFunctions.AsyncPythonScalarFunction;
import org.apache.flink.table.planner.utils.BatchTableTestUtil;
import org.apache.flink.table.planner.utils.TableTestBase;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Plan tests for async scalar functions in a batch calc ({@code BatchPhysicalAsyncCalc}). */
class AsyncCalcTest extends TableTestBase {

    private BatchTableTestUtil util;

    @BeforeEach
    void setup() {
        util = batchTestUtil(TableConfig.getDefault());
        for (String table : new String[] {"MyTable", "MyTable2"}) {
            util.getTableEnv()
                    .executeSql(
                            "CREATE TABLE "
                                    + table
                                    + " (\n"
                                    + "  a INT,\n"
                                    + "  b BIGINT,\n"
                                    + "  c STRING\n"
                                    + ") WITH (\n"
                                    + "  'connector' = 'test-simple-table-source'\n"
                                    + ")");
        }
        util.addTemporarySystemFunction("func1", new AsyncCalcSplitRuleTest.Func1());
        util.addTemporarySystemFunction("func2", new AsyncCalcSplitRuleTest.Func2());
        util.addTemporarySystemFunction(
                "tableFunc", new AsyncCalcSplitRuleTest.RandomTableFunction());
        util.addTemporarySystemFunction("pya", new AsyncPythonScalarFunction("pya"));
    }

    @Test
    void testAsyncCalc() {
        util.verifyExecPlan("SELECT func1(a) FROM MyTable");
    }

    @Test
    void testAsyncCalcInFilterIsSplit() {
        util.verifyExecPlan("SELECT a FROM MyTable WHERE func1(a) > 0");
    }

    @Test
    void testTwoAsyncCallsTwoCalcs() {
        util.verifyExecPlan("SELECT func1(a), func2(a) FROM MyTable");
    }

    @Test
    void testNestedAsyncCalls() {
        util.verifyExecPlan("SELECT func1(func1(a)) FROM MyTable");
    }

    @Test
    void testAsyncCalcNextToJoin() {
        util.verifyExecPlan(
                "SELECT func1(t1.a), t2.b FROM MyTable t1 JOIN MyTable2 t2 ON t1.a = t2.a");
    }

    @Test
    void testAsyncCalcIsNotMergedIntoMultipleInput() {
        util.verifyExecPlan(
                "SELECT t1.a, t1.f, t2.b FROM "
                        + "(SELECT a, func1(a) AS f FROM (SELECT a FROM MyTable GROUP BY a)) t1 "
                        + "JOIN MyTable2 t2 ON t1.a = t2.a");
    }

    @Test
    void testAsyncCallInCorrelateArgument() {
        util.verifyExecPlan("SELECT * FROM MyTable, LATERAL TABLE(tableFunc(func1(a)))");
    }

    @Test
    void testNestedAsyncCallInCorrelateArgument() {
        util.verifyExecPlan("SELECT * FROM MyTable, LATERAL TABLE(tableFunc(ABS(func1(a))))");
    }

    @Test
    void testPythonAsyncAndJavaAsyncInProjection() {
        util.verifyExecPlan("SELECT pya(a, a), func1(a) FROM MyTable");
    }

    @Test
    void testJavaAsyncOnPythonAsync() {
        util.verifyExecPlan("SELECT func1(pya(a, a)) FROM MyTable");
    }

    @Test
    void testPythonAsyncOnJavaAsync() {
        util.verifyExecPlan("SELECT pya(func1(a), a) FROM MyTable");
    }

    @Test
    void testPythonAsyncInCorrelateArgument() {
        util.verifyExecPlan("SELECT * FROM MyTable, LATERAL TABLE(tableFunc(pya(a, a)))");
    }
}
