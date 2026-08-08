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

package org.apache.flink.table.planner.plan.stream.sql;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedScalarFunctions.BooleanPythonScalarFunction;
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedScalarFunctions.PythonScalarFunction;
import org.apache.flink.table.planner.utils.JavaStreamTableTestUtil;
import org.apache.flink.table.planner.utils.TableTestBase;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link
 * org.apache.flink.table.planner.plan.rules.logical.RemoteCalcConditionProjectionCseRule}.
 *
 * <p>Verifies that Python UDF calls shared between condition and projection are deduplicated.
 */
class PythonCalcConditionCseTest extends TableTestBase {

    private final JavaStreamTableTestUtil util = javaStreamTestUtil();

    @BeforeEach
    void setup() {
        util.addTableSource(
                "MyTable",
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .column("b", DataTypes.INT())
                        .column("c", DataTypes.INT())
                        .build());
        util.tableEnv()
                .createTemporarySystemFunction("pyFunc1", new PythonScalarFunction("pyFunc1"));
        util.tableEnv()
                .createTemporarySystemFunction("pyFunc2", new PythonScalarFunction("pyFunc2"));
        util.tableEnv()
                .createTemporarySystemFunction("pyFunc3", new PythonScalarFunction("pyFunc3"));
        util.tableEnv()
                .createTemporarySystemFunction(
                        "pyFunc4", new BooleanPythonScalarFunction("pyFunc4"));
    }

    @Test
    void testSameUdfInConditionAndProjection() {
        util.verifyExecPlan(
                "SELECT pyFunc1(a, b) + 1, pyFunc1(a, b) + 2 FROM MyTable WHERE pyFunc1(a, b) > 0");
    }

    @Test
    void testDifferentUdfInConditionAndProjection() {
        util.verifyExecPlan("SELECT pyFunc1(a, b) FROM MyTable WHERE pyFunc2(a, c) > 0");
    }

    @Test
    void testNestedUdfInProjectionWithSameInCondition() {
        util.verifyExecPlan(
                "SELECT pyFunc2(pyFunc1(a, b), c), pyFunc1(a, b) FROM MyTable WHERE pyFunc1(a, b) > 0");
    }

    @Test
    void testRepeatedUdfCallsInProjectionWithCondition() {
        util.verifyExecPlan(
                "SELECT pyFunc1(a, b) + 1, pyFunc1(a, b) + 2, pyFunc1(a, b) + 3 "
                        + "FROM MyTable WHERE pyFunc1(a, b) > 0");
    }

    @Test
    void testConditionOnlyUdf() {
        util.verifyExecPlan("SELECT a, b FROM MyTable WHERE pyFunc4(a, c)");
    }

    @Test
    void testMultipleUdfsSharedBetweenConditionAndProjection() {
        util.verifyExecPlan(
                "SELECT pyFunc1(a, b) + pyFunc2(b, c) "
                        + "FROM MyTable WHERE pyFunc1(a, b) > 0 AND pyFunc2(b, c) > 0");
    }

    @Test
    void testNestedUdfWithCseAnnotation() {
        util.verifyExecPlan(
                "SELECT pyFunc1(a, b), pyFunc1(pyFunc1(a, b), c), "
                        + "pyFunc1(pyFunc1(pyFunc1(a, b), c), a) "
                        + "FROM MyTable WHERE pyFunc1(a, b) > 0");
    }
}
