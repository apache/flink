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
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedScalarFunctions.NonDeterministicPythonScalarFunction;
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedScalarFunctions.PythonScalarFunction;
import org.apache.flink.table.planner.utils.JavaStreamTableTestUtil;
import org.apache.flink.table.planner.utils.TableTestBase;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for Python UDF common sub-expression elimination in the planner, covering {@link
 * org.apache.flink.table.planner.plan.rules.logical.RemoteCalcProjectionCseRule} (duplicates within
 * a projection) and {@link
 * org.apache.flink.table.planner.plan.rules.logical.RemoteCalcConditionProjectionCseRule}
 * (duplicates shared between a condition and a projection).
 */
class PythonCalcCseTest extends TableTestBase {

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
                .createTemporarySystemFunction(
                        "pyFuncNonDet", new NonDeterministicPythonScalarFunction("pyFuncNonDet"));
    }

    // -------------------------------------------------------------------------
    //  Duplicates within a projection
    // -------------------------------------------------------------------------

    @Test
    void testDuplicatedCallsInProjection() {
        util.verifyExecPlan("SELECT pyFunc1(a, b), pyFunc1(a, b), pyFunc1(a, b) FROM MyTable");
    }

    @Test
    void testDuplicatedCallsWithForwardedField() {
        util.verifyExecPlan("SELECT a, pyFunc1(a, b), pyFunc1(a, b) FROM MyTable");
    }

    @Test
    void testDistinctCallsAreNotDeduplicated() {
        util.verifyExecPlan("SELECT pyFunc1(a, b), pyFunc1(b, a), pyFunc2(a, b) FROM MyTable");
    }

    @Test
    void testNonDeterministicCallsAreNotDeduplicated() {
        util.verifyExecPlan("SELECT pyFuncNonDet(a, b), pyFuncNonDet(a, b) FROM MyTable");
    }

    // -------------------------------------------------------------------------
    //  Duplicates shared between a condition and a projection
    // -------------------------------------------------------------------------

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
    void testDuplicatedCallsInProjectionWithCondition() {
        util.verifyExecPlan(
                "SELECT pyFunc1(a, b), pyFunc1(a, b) FROM MyTable WHERE pyFunc1(a, b) > 0");
    }

    /**
     * The same function with different arguments must not be shared. After the condition split both
     * calls can be printed as {@code pyFunc1($0, $1)} even though they read different columns, so
     * comparing them without accounting for the frame of reference would silently return the
     * condition's result.
     */
    @Test
    void testSameUdfWithDifferentArgsInConditionAndProjection() {
        util.verifyExecPlan("SELECT pyFunc1(b, c) FROM MyTable WHERE pyFunc1(a, b) > 0");
    }

    @Test
    void testSameUdfWithSwappedArgsInConditionAndProjection() {
        util.verifyExecPlan("SELECT pyFunc1(b, a) FROM MyTable WHERE pyFunc1(a, b) > 0");
    }
}
