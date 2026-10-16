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
import org.apache.flink.table.functions.python.PythonEnv;
import org.apache.flink.table.functions.python.PythonFunction;
import org.apache.flink.table.functions.python.PythonFunctionInfo;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for deriving execution resources from Python function trees. */
class CommonPythonUtilTest {

    @Test
    void testDeriveParallelismFromNestedFunctions() {
        final PythonFunctionInfo nested =
                functionInfo(-1, -1, functionInfo(3, -1), functionInfo(-1, -1));

        assertThat(
                        CommonPythonUtil.deriveExplicitPythonFunctionParallelism(
                                new PythonFunctionInfo[] {nested}, "Python functions"))
                .hasValue(3);
        assertThat(
                        CommonPythonUtil.deriveExplicitPythonFunctionParallelism(
                                new PythonFunctionInfo[] {functionInfo(-1, -1)},
                                "Python functions"))
                .isEmpty();
    }

    @Test
    void testRejectConflictingNestedParallelism() {
        final PythonFunctionInfo nested =
                functionInfo(2, -1, functionInfo(-1, -1, functionInfo(3, -1)));

        assertThatThrownBy(
                        () ->
                                CommonPythonUtil.deriveExplicitPythonFunctionParallelism(
                                        new PythonFunctionInfo[] {nested}, "Python functions"))
                .isInstanceOf(TableException.class)
                .hasMessageContaining("different concurrency values");
    }

    @Test
    void testDeriveMinimumBatchSizeFromNestedFunctions() {
        final PythonFunctionInfo nested =
                functionInfo(2, 64, functionInfo(-1, 32), functionInfo(-1, -1));

        assertThat(CommonPythonUtil.deriveArrowBatchSize(new PythonFunctionInfo[] {nested}))
                .isEqualTo(32);
        assertThat(
                        CommonPythonUtil.deriveArrowBatchSize(
                                new PythonFunctionInfo[] {functionInfo(-1, -1)}))
                .isEqualTo(-1);
    }

    private static PythonFunctionInfo functionInfo(
            int parallelism, int maxArrowBatchSize, PythonFunctionInfo... inputs) {
        return new PythonFunctionInfo(
                new TestPythonFunction(parallelism, maxArrowBatchSize), inputs);
    }

    private static final class TestPythonFunction implements PythonFunction {
        private final int parallelism;
        private final int maxArrowBatchSize;

        private TestPythonFunction(int parallelism, int maxArrowBatchSize) {
            this.parallelism = parallelism;
            this.maxArrowBatchSize = maxArrowBatchSize;
        }

        @Override
        public byte[] getSerializedPythonFunction() {
            return new byte[0];
        }

        @Override
        public PythonEnv getPythonEnv() {
            return null;
        }

        @Override
        public int getParallelism() {
            return parallelism;
        }

        @Override
        public int getMaxArrowBatchSize() {
            return maxArrowBatchSize;
        }
    }
}
