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

package org.apache.flink.table.planner.plan.nodes.exec.utils;

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.functions.python.PythonFunctionInfo;
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedScalarFunctions.ParallelPythonScalarFunction;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests the concurrency invariant expected after Python function splitting. */
class CommonPythonUtilTest {

    @Test
    void testRejectConflictingNestedParallelism() {
        final PythonFunctionInfo nested = functionInfo(2, functionInfo(-1, functionInfo(3)));

        assertThatThrownBy(
                        () ->
                                CommonPythonUtil.deriveExplicitPythonFunctionParallelism(
                                        new PythonFunctionInfo[] {nested},
                                        "Python functions",
                                        false))
                .isInstanceOf(TableException.class)
                .hasMessageContaining("different concurrency values");
    }

    private static PythonFunctionInfo functionInfo(int parallelism, PythonFunctionInfo... inputs) {
        return new PythonFunctionInfo(new ParallelPythonScalarFunction("f", parallelism), inputs);
    }
}
