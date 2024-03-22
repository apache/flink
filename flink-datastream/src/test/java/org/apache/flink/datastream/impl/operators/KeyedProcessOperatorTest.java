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

package org.apache.flink.datastream.impl.operators;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.datastream.api.common.Collector;
import org.apache.flink.datastream.api.context.NonPartitionedContext;
import org.apache.flink.datastream.api.context.RuntimeContext;
import org.apache.flink.datastream.api.function.OneInputStreamProcessFunction;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;

import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link KeyedProcessOperator}. */
class KeyedProcessOperatorTest {
    @Test
    void testProcessRecord() throws Exception {
        KeyedProcessOperator<Integer, Integer, Integer> processOperator =
                new KeyedProcessOperator<>(
                        new OneInputStreamProcessFunction<Integer, Integer>() {
                            @Override
                            public void processRecord(
                                    Integer record, Collector<Integer> output, RuntimeContext ctx) {
                                output.collect(record + 1);
                            }
                        });

        try (KeyedOneInputStreamOperatorTestHarness<Integer, Integer, Integer> testHarness =
                new KeyedOneInputStreamOperatorTestHarness<>(
                        processOperator,
                        (KeySelector<Integer, Integer>) value -> value,
                        Types.INT)) {
            testHarness.open();
            testHarness.processElement(new StreamRecord<>(1));
            testHarness.processElement(new StreamRecord<>(2));
            testHarness.processElement(new StreamRecord<>(3));

            Collection<StreamRecord<Integer>> recordOutput = testHarness.getRecordOutput();
            assertThat(recordOutput)
                    .containsExactly(
                            new StreamRecord<>(2), new StreamRecord<>(3), new StreamRecord<>(4));
        }
    }

    @Test
    void testEndInput() throws Exception {
        CompletableFuture<Void> future = new CompletableFuture<>();
        KeyedProcessOperator<Integer, Integer, Integer> processOperator =
                new KeyedProcessOperator<>(
                        new OneInputStreamProcessFunction<Integer, Integer>() {
                            @Override
                            public void processRecord(
                                    Integer record, Collector<Integer> output, RuntimeContext ctx) {
                                // do nothing.
                            }

                            @Override
                            public void endInput(NonPartitionedContext<Integer> ctx) {
                                future.complete(null);
                            }
                        });

        try (KeyedOneInputStreamOperatorTestHarness<Integer, Integer, Integer> testHarness =
                new KeyedOneInputStreamOperatorTestHarness<>(
                        processOperator,
                        (KeySelector<Integer, Integer>) value -> value,
                        Types.INT)) {
            testHarness.open();
            testHarness.endInput();
            assertThat(future).isCompleted();
        }
    }

    @Test
    void testCheckKey() throws Exception {
        KeyedProcessOperator<Integer, Integer, Integer> processOperator =
                new KeyedProcessOperator<>(
                        new OneInputStreamProcessFunction<Integer, Integer>() {
                            @Override
                            public void processRecord(
                                    Integer record, Collector<Integer> output, RuntimeContext ctx) {
                                // forward the record to check input key.
                                output.collect(record);
                            }
                        },
                        // -1 is an invalid key in this suite.
                        (ignore) -> -1);

        try (KeyedOneInputStreamOperatorTestHarness<Integer, Integer, Integer> testHarness =
                new KeyedOneInputStreamOperatorTestHarness<>(
                        processOperator,
                        (KeySelector<Integer, Integer>) value -> value,
                        Types.INT)) {
            testHarness.open();
            assertThatThrownBy(() -> testHarness.processElement(new StreamRecord<>(2)))
                    .isInstanceOf(IllegalStateException.class);
        }
    }
}
