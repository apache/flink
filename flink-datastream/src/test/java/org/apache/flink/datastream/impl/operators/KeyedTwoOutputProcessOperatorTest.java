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
import org.apache.flink.datastream.api.context.TwoOutputNonPartitionedContext;
import org.apache.flink.datastream.api.context.TwoOutputPartitionedContext;
import org.apache.flink.datastream.api.function.TwoOutputStreamProcessFunction;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.util.OutputTag;

import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link KeyedTwoOutputProcessOperator}. */
class KeyedTwoOutputProcessOperatorTest {
    @Test
    void testProcessRecord() throws Exception {
        OutputTag<Long> sideOutputTag = new OutputTag<Long>("side-output") {};

        KeyedTwoOutputProcessOperator<Integer, Integer, Integer, Long> processOperator =
                new KeyedTwoOutputProcessOperator<>(
                        new TwoOutputStreamProcessFunction<Integer, Integer, Long>() {
                            @Override
                            public void processRecord(
                                    Integer record,
                                    Collector<Integer> output1,
                                    Collector<Long> output2,
                                    TwoOutputPartitionedContext ctx) {
                                output1.collect(record);
                                output2.collect((long) (record * 2));
                            }
                        },
                        sideOutputTag);

        try (KeyedOneInputStreamOperatorTestHarness<Integer, Integer, Integer> testHarness =
                new KeyedOneInputStreamOperatorTestHarness<>(
                        processOperator,
                        (KeySelector<Integer, Integer>) value -> value,
                        Types.INT)) {
            testHarness.open();
            testHarness.processElement(new StreamRecord<>(1));
            testHarness.processElement(new StreamRecord<>(2));
            testHarness.processElement(new StreamRecord<>(3));
            Collection<StreamRecord<Integer>> firstOutput = testHarness.getRecordOutput();
            ConcurrentLinkedQueue<StreamRecord<Long>> secondOutput =
                    testHarness.getSideOutput(sideOutputTag);
            assertThat(firstOutput)
                    .containsExactly(
                            new StreamRecord<>(1), new StreamRecord<>(2), new StreamRecord<>(3));
            assertThat(secondOutput)
                    .containsExactly(
                            new StreamRecord<>(2L), new StreamRecord<>(4L), new StreamRecord<>(6L));
        }
    }

    @Test
    void testEndInput() throws Exception {
        AtomicInteger counter = new AtomicInteger();
        OutputTag<Long> sideOutputTag = new OutputTag<Long>("side-output") {};

        KeyedTwoOutputProcessOperator<Integer, Integer, Integer, Long> processOperator =
                new KeyedTwoOutputProcessOperator<>(
                        new TwoOutputStreamProcessFunction<Integer, Integer, Long>() {
                            @Override
                            public void processRecord(
                                    Integer record,
                                    Collector<Integer> output1,
                                    Collector<Long> output2,
                                    TwoOutputPartitionedContext ctx) {
                                // do nothing.
                            }

                            @Override
                            public void endInput(
                                    TwoOutputNonPartitionedContext<Integer, Long> ctx) {
                                try {
                                    ctx.applyToAllPartitions(
                                            (firstOutput, secondOutput, context) -> {
                                                counter.incrementAndGet();
                                                Integer currentKey =
                                                        context.getStateManager().getCurrentKey();
                                                firstOutput.collect(currentKey);
                                                secondOutput.collect(Long.valueOf(currentKey));
                                            });
                                } catch (Exception e) {
                                    throw new RuntimeException(e);
                                }
                            }
                        },
                        sideOutputTag);

        try (KeyedOneInputStreamOperatorTestHarness<Integer, Integer, Integer> testHarness =
                new KeyedOneInputStreamOperatorTestHarness<>(
                        processOperator,
                        (KeySelector<Integer, Integer>) value -> value,
                        Types.INT)) {
            testHarness.open();
            testHarness.processElement(new StreamRecord<>(1)); // key is 1
            testHarness.processElement(new StreamRecord<>(2)); //  key is 2
            testHarness.endInput();
            assertThat(counter).hasValue(2);
            Collection<StreamRecord<Integer>> firstOutput = testHarness.getRecordOutput();
            ConcurrentLinkedQueue<StreamRecord<Long>> secondOutput =
                    testHarness.getSideOutput(sideOutputTag);
            assertThat(firstOutput).containsExactly(new StreamRecord<>(1), new StreamRecord<>(2));
            assertThat(secondOutput)
                    .containsExactly(new StreamRecord<>(1L), new StreamRecord<>(2L));
        }
    }

    @Test
    void testKeyCheck() throws Exception {
        OutputTag<Long> sideOutputTag = new OutputTag<Long>("side-output") {};
        AtomicBoolean emitToFirstOutput = new AtomicBoolean(true);
        KeyedTwoOutputProcessOperator<Integer, Integer, Integer, Long> processOperator =
                new KeyedTwoOutputProcessOperator<>(
                        new TwoOutputStreamProcessFunction<Integer, Integer, Long>() {
                            @Override
                            public void processRecord(
                                    Integer record,
                                    Collector<Integer> output1,
                                    Collector<Long> output2,
                                    TwoOutputPartitionedContext ctx) {
                                if (emitToFirstOutput.get()) {
                                    output1.collect(record);
                                } else {
                                    output2.collect((long) (record));
                                }
                            }
                        },
                        sideOutputTag,
                        // -1 is an invalid key in this suite.
                        (KeySelector<Integer, Integer>) value -> -1,
                        // -1 is an invalid key in this suite.
                        (KeySelector<Long, Integer>) value -> -1);

        try (KeyedOneInputStreamOperatorTestHarness<Integer, Integer, Integer> testHarness =
                new KeyedOneInputStreamOperatorTestHarness<>(
                        processOperator,
                        (KeySelector<Integer, Integer>) value -> value,
                        Types.INT)) {
            testHarness.open();
            assertThatThrownBy(() -> testHarness.processElement(new StreamRecord<>(1)))
                    .isInstanceOf(IllegalStateException.class);
            emitToFirstOutput.set(false);
            assertThatThrownBy(() -> testHarness.processElement(new StreamRecord<>(1)))
                    .isInstanceOf(IllegalStateException.class);
        }
    }
}
