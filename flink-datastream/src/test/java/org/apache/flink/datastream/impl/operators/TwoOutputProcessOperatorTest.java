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

import org.apache.flink.datastream.api.common.Collector;
import org.apache.flink.datastream.api.context.TwoOutputNonPartitionedContext;
import org.apache.flink.datastream.api.context.TwoOutputPartitionedContext;
import org.apache.flink.datastream.api.function.TwoOutputStreamProcessFunction;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.util.OutputTag;

import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link TwoOutputProcessOperator}. */
class TwoOutputProcessOperatorTest {
    @Test
    void testProcessRecord() throws Exception {
        OutputTag<Long> sideOutputTag = new OutputTag<Long>("side-output") {};

        TwoOutputProcessOperator<Integer, Integer, Long> processOperator =
                new TwoOutputProcessOperator<>(
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

        try (OneInputStreamOperatorTestHarness<Integer, Integer> testHarness =
                new OneInputStreamOperatorTestHarness<>(processOperator)) {
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

        TwoOutputProcessOperator<Integer, Integer, Long> processOperator =
                new TwoOutputProcessOperator<>(
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
                                                firstOutput.collect(1);
                                                secondOutput.collect(2L);
                                            });
                                } catch (Exception e) {
                                    throw new RuntimeException(e);
                                }
                            }
                        },
                        sideOutputTag);

        try (OneInputStreamOperatorTestHarness<Integer, Integer> testHarness =
                new OneInputStreamOperatorTestHarness<>(processOperator)) {
            testHarness.open();
            testHarness.endInput();
            assertThat(counter).hasValue(1);
            Collection<StreamRecord<Integer>> firstOutput = testHarness.getRecordOutput();
            ConcurrentLinkedQueue<StreamRecord<Long>> secondOutput =
                    testHarness.getSideOutput(sideOutputTag);
            assertThat(firstOutput).containsExactly(new StreamRecord<>(1));
            assertThat(secondOutput).containsExactly(new StreamRecord<>(2L));
        }
    }
}
