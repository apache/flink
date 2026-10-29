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
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedScalarFunctions.AsyncPythonScalarFunction;
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedScalarFunctions.PythonScalarFunction;
import org.apache.flink.table.planner.utils.JavaTableTestUtil;
import org.apache.flink.table.planner.utils.TableTestBase;

import org.junit.jupiter.api.Test;

/** Tests projection ordering after splitting Python async calls. */
class PythonAsyncCalcProjectionTest extends TableTestBase {

    @Test
    void testStreamingSyncPythonFunctionBeforeAsyncPythonFunction() {
        verifySyncPythonFunctionBeforeAsyncPythonFunction(javaStreamTestUtil());
    }

    @Test
    void testBatchSyncPythonFunctionBeforeAsyncPythonFunction() {
        verifySyncPythonFunctionBeforeAsyncPythonFunction(javaBatchTestUtil());
    }

    @Test
    void testStreamingNestedAsyncPythonFunctionWithAnotherAsyncPythonFunction() {
        verifyNestedAsyncPythonFunctionWithAnotherAsyncPythonFunction(javaStreamTestUtil());
    }

    @Test
    void testBatchNestedAsyncPythonFunctionWithAnotherAsyncPythonFunction() {
        verifyNestedAsyncPythonFunctionWithAnotherAsyncPythonFunction(javaBatchTestUtil());
    }

    @Test
    void testStreamingRepeatedNestedPythonFunctionsWithLiteral() {
        verifyRepeatedNestedPythonFunctionsWithLiteral(javaStreamTestUtil());
    }

    @Test
    void testBatchRepeatedNestedPythonFunctionsWithLiteral() {
        verifyRepeatedNestedPythonFunctionsWithLiteral(javaBatchTestUtil());
    }

    private void verifySyncPythonFunctionBeforeAsyncPythonFunction(JavaTableTestUtil util) {
        registerTableAndFunctions(util);
        util.verifyExecPlan("SELECT pyFunc1(b, c), asyncFunc1(a, b) FROM MyTable");
    }

    private void verifyNestedAsyncPythonFunctionWithAnotherAsyncPythonFunction(
            JavaTableTestUtil util) {
        registerTableAndFunctions(util);
        util.verifyExecPlan(
                "SELECT asyncFunc2(asyncFunc1(a, b), c), asyncFunc1(a + 1, b) FROM MyTable");
    }

    private void verifyRepeatedNestedPythonFunctionsWithLiteral(JavaTableTestUtil util) {
        registerTableAndFunctions(util);
        util.verifyExecPlan(
                "SELECT pyFunc1(asyncFunc1(a, b), c), pyFunc1(asyncFunc1(a, b), c), 42 FROM MyTable");
    }

    private void registerTableAndFunctions(JavaTableTestUtil util) {
        util.addTableSource(
                "MyTable",
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .column("b", DataTypes.INT())
                        .column("c", DataTypes.INT())
                        .build());
        util.tableEnv()
                .createTemporarySystemFunction(
                        "asyncFunc1", new AsyncPythonScalarFunction("asyncFunc1"));
        util.tableEnv()
                .createTemporarySystemFunction(
                        "asyncFunc2", new AsyncPythonScalarFunction("asyncFunc2"));
        util.tableEnv()
                .createTemporarySystemFunction("pyFunc1", new PythonScalarFunction("pyFunc1"));
    }
}
