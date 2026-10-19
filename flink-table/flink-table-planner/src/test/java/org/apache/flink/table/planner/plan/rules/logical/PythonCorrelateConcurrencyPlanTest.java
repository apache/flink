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
import org.apache.flink.table.functions.python.PythonFunctionKind;
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedScalarFunctions.AsyncPythonScalarFunction;
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedScalarFunctions.ParallelPythonScalarFunction;
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedScalarFunctions.PythonScalarFunction;
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedScalarFunctions.ResourcePythonScalarFunction;
import org.apache.flink.table.planner.utils.JavaTableTestUtil;
import org.apache.flink.table.planner.utils.MockPythonTableFunction;
import org.apache.flink.table.planner.utils.TableTestBase;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Verifies join predicates and field mappings when scalar calls move out of a correlate. */
class PythonCorrelateConcurrencyPlanTest extends TableTestBase {
    private JavaTableTestUtil util;

    @BeforeEach
    void setUp() {
        util = javaStreamTestUtil();
        util.addTableSource(
                "src",
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .column("b", DataTypes.INT())
                        .build());
        util.tableEnv().createTemporarySystemFunction("tf", new MockPythonTableFunction());
        util.tableEnv()
                .createTemporarySystemFunction("f3", new ParallelPythonScalarFunction("f3", 3));
        util.tableEnv()
                .createTemporarySystemFunction("f5", new ParallelPythonScalarFunction("f5", 5));
        util.tableEnv()
                .createTemporarySystemFunction(
                        "async_func", new AsyncPythonScalarFunction("async_func"));
        util.tableEnv()
                .createTemporarySystemFunction(
                        "vector5",
                        new ResourcePythonScalarFunction(
                                "vector5", 5, PythonFunctionKind.PANDAS, -1));
    }

    @Test
    void testInnerJoinPreservesFilterAndFields() {
        util.verifyRelPlan(
                "SELECT a, b, x, y FROM src JOIN "
                        + "LATERAL TABLE(tf(a, f3(f5(a, b), b))) AS T(x, y) ON y > 0");
    }

    @Test
    void testLeftJoinPreservesFieldsAcrossFunctionKinds() {
        util.verifyRelPlan(
                "SELECT a, b, x, y FROM src LEFT JOIN "
                        + "LATERAL TABLE(tf(a, f3(vector5(a, b), b))) AS T(x, y) ON TRUE");
    }

    @Test
    void testSplitsConcurrencyAfterAsyncExtraction() {
        util.verifyRelPlan(
                "SELECT x, y FROM src, "
                        + "LATERAL TABLE(tf(f3(async_func(a, b), b), f5(a, b))) AS T(x, y)");
    }

    @Test
    void testLeftJoinPreservesFieldsAfterAsyncExtraction() {
        util.verifyRelPlan(
                "SELECT a, b, x, y FROM src LEFT JOIN "
                        + "LATERAL TABLE(tf(f3(async_func(a, b), b), f5(a, b))) AS T(x, y) ON TRUE");
    }

    @Test
    void testPreservesSharedPythonResultAcrossAsyncExtraction() {
        util.tableEnv().createTemporarySystemFunction("plain", new PythonScalarFunction("plain"));
        final String query =
                "SELECT p, x, y FROM (SELECT a, b, plain(a, b) AS p FROM src), "
                        + "LATERAL TABLE(tf(async_func(p, b), p)) AS T(x, y)";
        util.verifyRelPlan(query);
    }
}
