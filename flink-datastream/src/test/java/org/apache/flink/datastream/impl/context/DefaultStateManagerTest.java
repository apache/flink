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

package org.apache.flink.datastream.impl.context;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.datastream.impl.operators.KeyedProcessOperator;
import org.apache.flink.datastream.impl.operators.MockFreqCountProcessFunction;
import org.apache.flink.datastream.impl.operators.MockGlobalDecuplicateCountProcessFunction;
import org.apache.flink.datastream.impl.operators.MockGlobalListAppenderProcessFunction;
import org.apache.flink.datastream.impl.operators.MockListAppenderProcessFunction;
import org.apache.flink.datastream.impl.operators.MockMultiplierProcessFunction;
import org.apache.flink.datastream.impl.operators.MockRecudingMultiplierProcessFunction;
import org.apache.flink.datastream.impl.operators.MockSumAggregateProcessFunction;
import org.apache.flink.streaming.api.operators.collect.utils.MockOperatorStateStore;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.MockStreamingRuntimeContext;

import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link DefaultStateManager}. */
class DefaultStateManagerTest {
    @Test
    void testGetCurrentKey() {
        final String key = "key";
        DefaultStateManager stateManager =
                new DefaultStateManager(
                        () -> key,
                        ignore -> {},
                        new MockStreamingRuntimeContext(false, 1, 0),
                        new MockOperatorStateStore());
        assertThat((String) stateManager.getCurrentKey()).isEqualTo(key);
    }

    @Test
    void testErrorInGetCurrentKey() {
        DefaultStateManager stateManager =
                new DefaultStateManager(
                        () -> {
                            throw new RuntimeException("Expected Error");
                        },
                        ignore -> {},
                        new MockStreamingRuntimeContext(false, 1, 0),
                        new MockOperatorStateStore());
        assertThatThrownBy(stateManager::getCurrentKey)
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Expected Error");
    }

    @Test
    void testExecuteInKeyContext() {
        final int oldKey = 1;
        final int newKey = 2;
        // -1 as unset value
        AtomicInteger setKey = new AtomicInteger(-1);
        DefaultStateManager stateManager =
                new DefaultStateManager(
                        () -> oldKey,
                        k -> setKey.set((Integer) k),
                        new MockStreamingRuntimeContext(false, 1, 0),
                        new MockOperatorStateStore());
        stateManager.executeInKeyContext(() -> assertThat(setKey).hasValue(newKey), newKey);
        assertThat(setKey).hasValue(oldKey);
    }

    @Test
    void testListState() throws Exception {
        MockListAppenderProcessFunction function = new MockListAppenderProcessFunction();
        KeyedProcessOperator<Integer, Integer, Integer> processOperator =
                new KeyedProcessOperator<>(
                        function, (KeySelector<Integer, Integer>) value -> value);

        try (KeyedOneInputStreamOperatorTestHarness<Integer, Integer, Integer> testHarness =
                new KeyedOneInputStreamOperatorTestHarness<>(
                        processOperator,
                        (KeySelector<Integer, Integer>) value -> value,
                        Types.INT)) {
            testHarness.open();
            testHarness.processElement(new StreamRecord<>(1));
            testHarness.processElement(new StreamRecord<>(1));
            testHarness.processElement(new StreamRecord<>(1));
            testHarness.processElement(new StreamRecord<>(1));

            List<Integer> listState = function.getResultingState();
            assertThat(listState).containsExactly(1, 1, 1, 1);
        }
    }

    @Test
    void testAggState() throws Exception {
        MockSumAggregateProcessFunction function = new MockSumAggregateProcessFunction();
        KeyedProcessOperator<Integer, Integer, Integer> processOperator =
                new KeyedProcessOperator<>(function);

        try (KeyedOneInputStreamOperatorTestHarness<Integer, Integer, Integer> testHarness =
                new KeyedOneInputStreamOperatorTestHarness<>(
                        processOperator,
                        (KeySelector<Integer, Integer>) value -> value,
                        Types.INT)) {
            testHarness.open();
            testHarness.processElement(new StreamRecord<>(1));
            testHarness.processElement(new StreamRecord<>(1));

            testHarness.processElement(new StreamRecord<>(2));
            testHarness.processElement(new StreamRecord<>(2));

            Collection<StreamRecord<Integer>> recordOutput = testHarness.getRecordOutput();
            assertThat(recordOutput)
                    .containsExactly(
                            new StreamRecord<>(1),
                            new StreamRecord<>(2),
                            new StreamRecord<>(2),
                            new StreamRecord<>(4));
        }
    }

    @Test
    void testValueState() throws Exception {
        MockMultiplierProcessFunction function = new MockMultiplierProcessFunction();
        KeyedProcessOperator<Integer, Integer, Integer> processOperator =
                new KeyedProcessOperator<>(function);

        try (KeyedOneInputStreamOperatorTestHarness<Integer, Integer, Integer> testHarness =
                new KeyedOneInputStreamOperatorTestHarness<>(
                        processOperator,
                        (KeySelector<Integer, Integer>) value -> value,
                        Types.INT)) {
            testHarness.open();
            testHarness.processElement(new StreamRecord<>(3));
            testHarness.processElement(new StreamRecord<>(3));
            testHarness.processElement(new StreamRecord<>(4));
            testHarness.processElement(new StreamRecord<>(4));

            Collection<StreamRecord<Integer>> recordOutput = testHarness.getRecordOutput();
            assertThat(recordOutput)
                    .containsExactly(
                            new StreamRecord<>(3),
                            new StreamRecord<>(9),
                            new StreamRecord<>(4),
                            new StreamRecord<>(16));
        }
    }

    @Test
    void testMapState() throws Exception {
        MockFreqCountProcessFunction function = new MockFreqCountProcessFunction();
        KeyedProcessOperator<Integer, Integer, Integer> processOperator =
                new KeyedProcessOperator<>(function);

        try (KeyedOneInputStreamOperatorTestHarness<Integer, Integer, Integer> testHarness =
                new KeyedOneInputStreamOperatorTestHarness<>(
                        processOperator,
                        (KeySelector<Integer, Integer>) value -> value,
                        Types.INT)) {
            testHarness.open();
            testHarness.processElement(new StreamRecord<>(1));
            testHarness.processElement(new StreamRecord<>(1));
            testHarness.processElement(new StreamRecord<>(1));

            testHarness.processElement(new StreamRecord<>(3));
            Collection<StreamRecord<Integer>> recordOutput = testHarness.getRecordOutput();
            assertThat(recordOutput)
                    .containsExactly(
                            new StreamRecord<>(1),
                            new StreamRecord<>(2),
                            new StreamRecord<>(3),
                            new StreamRecord<>(1));
        }
    }

    @Test
    void testReducingState() throws Exception {
        MockRecudingMultiplierProcessFunction function =
                new MockRecudingMultiplierProcessFunction();
        KeyedProcessOperator<Integer, Integer, Integer> processOperator =
                new KeyedProcessOperator<>(function);

        try (KeyedOneInputStreamOperatorTestHarness<Integer, Integer, Integer> testHarness =
                new KeyedOneInputStreamOperatorTestHarness<>(
                        processOperator,
                        (KeySelector<Integer, Integer>) value -> value,
                        Types.INT)) {
            testHarness.open();
            testHarness.processElement(new StreamRecord<>(2));
            testHarness.processElement(new StreamRecord<>(2));
            testHarness.processElement(new StreamRecord<>(3));
            testHarness.processElement(new StreamRecord<>(2));

            Collection<StreamRecord<Integer>> recordOutput = testHarness.getRecordOutput();
            assertThat(recordOutput)
                    .containsExactly(
                            new StreamRecord<>(2),
                            new StreamRecord<>(4),
                            new StreamRecord<>(3),
                            new StreamRecord<>(8));
        }
    }

    @Test
    void testBroadcastMapState() throws Exception {
        MockGlobalDecuplicateCountProcessFunction function =
                new MockGlobalDecuplicateCountProcessFunction();
        KeyedProcessOperator<Integer, Integer, Integer> processOperator =
                new KeyedProcessOperator<>(function);

        try (KeyedOneInputStreamOperatorTestHarness<Integer, Integer, Integer> testHarness =
                new KeyedOneInputStreamOperatorTestHarness<>(
                        processOperator,
                        (KeySelector<Integer, Integer>) value -> value,
                        Types.INT)) {
            testHarness.open();
            testHarness.processElement(new StreamRecord<>(1));
            testHarness.processElement(new StreamRecord<>(2));
            testHarness.processElement(new StreamRecord<>(3));
            testHarness.processElement(new StreamRecord<>(4));

            Collection<StreamRecord<Integer>> recordOutput = testHarness.getRecordOutput();
            assertThat(recordOutput)
                    .containsExactly(
                            new StreamRecord<>(1),
                            new StreamRecord<>(2),
                            new StreamRecord<>(3),
                            new StreamRecord<>(4));
        }
    }

    @Test
    void testBroadcastListState() throws Exception {
        MockGlobalListAppenderProcessFunction function =
                new MockGlobalListAppenderProcessFunction();
        KeyedProcessOperator<Integer, Integer, Integer> processOperator =
                new KeyedProcessOperator<>(
                        function, (KeySelector<Integer, Integer>) value -> value);

        try (KeyedOneInputStreamOperatorTestHarness<Integer, Integer, Integer> testHarness =
                new KeyedOneInputStreamOperatorTestHarness<>(
                        processOperator,
                        (KeySelector<Integer, Integer>) value -> value,
                        Types.INT)) {
            testHarness.open();
            testHarness.processElement(new StreamRecord<>(1));
            testHarness.processElement(new StreamRecord<>(2));
            testHarness.processElement(new StreamRecord<>(3));
            testHarness.processElement(new StreamRecord<>(4));

            List<Integer> listState = function.getResultingState();
            assertThat(listState).containsExactly(1, 2, 3, 4);
        }
    }
}
