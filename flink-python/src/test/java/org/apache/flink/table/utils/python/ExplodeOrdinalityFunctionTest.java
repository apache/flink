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

package org.apache.flink.table.utils.python;

import org.apache.flink.util.Collector;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ExplodeOrdinalityFunctionTest {

    private final List<Integer> output = new ArrayList<>();
    private final ExplodeOrdinalityFunction function = new ExplodeOrdinalityFunction();

    ExplodeOrdinalityFunctionTest() {
        function.setCollector(
                new Collector<Integer>() {
                    @Override
                    public void collect(Integer record) {
                        output.add(record);
                    }

                    @Override
                    public void close() {}
                });
    }

    @Test
    void testEqualLengths() {
        function.eval(1);
        assertThat(output).containsExactly(1);

        output.clear();
        function.eval(3, 3);
        assertThat(output).containsExactly(1, 2, 3);

        output.clear();
        function.eval(4, 4, 4, 4);
        assertThat(output).containsExactly(1, 2, 3, 4);
    }

    @Test
    void testEmptyLengths() {
        function.eval();
        function.eval(0, 0, 0);
        function.eval(null, null, null);
        function.eval(null, 0, null, 0);
        function.eval(0, null, 0, null);

        assertThat(output).isEmpty();
    }

    @Test
    void testMismatchedLengthsEmitNothing() {
        final Integer[][] mismatches = {
            {0, 1},
            {null, 1},
            {1, 0},
            {1, null},
            {2, 3},
            {2, 2, 3},
            {0, null, 1},
            {3, 3, 3, 0}
        };
        for (final Integer[] lengths : mismatches) {
            assertThatThrownBy(() -> function.eval(lengths))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("same length")
                    .hasMessageContaining(Arrays.toString(lengths));
            assertThat(output).isEmpty();
        }
    }

    @Test
    void testRepeatedCalls() {
        function.eval(2, 2, 2);
        function.eval(0, null);
        function.eval(1, 1);

        assertThat(output).containsExactly(1, 2, 1);
    }
}
