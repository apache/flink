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
import org.apache.flink.datastream.api.context.NonPartitionedContext;
import org.apache.flink.datastream.api.context.PartitionedContext;
import org.apache.flink.datastream.api.function.TwoInputNonBroadcastStreamProcessFunction;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.TwoInputStreamOperatorTestHarness;

import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link TwoInputNonBroadcastProcessOperator}. */
class TwoInputNonBroadcastProcessOperatorTest {
    @Test
    void testProcessRecord() throws Exception {
        TwoInputNonBroadcastProcessOperator<Integer, Long, Long> processOperator =
                new TwoInputNonBroadcastProcessOperator<>(
                        new TwoInputNonBroadcastStreamProcessFunction<Integer, Long, Long>() {
                            @Override
                            public void processRecordFromFirstInput(
                                    Integer record,
                                    Collector<Long> output,
                                    PartitionedContext ctx) {
                                output.collect(Long.valueOf(record));
                            }

                            @Override
                            public void processRecordFromSecondInput(
                                    Long record, Collector<Long> output, PartitionedContext ctx) {
                                output.collect(record);
                            }
                        });

        try (TwoInputStreamOperatorTestHarness<Integer, Long, Long> testHarness =
                new TwoInputStreamOperatorTestHarness<>(processOperator)) {
            testHarness.open();
            testHarness.processElement1(new StreamRecord<>(1));
            testHarness.processElement2(new StreamRecord<>(2L));
            testHarness.processElement1(new StreamRecord<>(3));
            testHarness.processElement1(new StreamRecord<>(5));
            testHarness.processElement2(new StreamRecord<>(4L));
            Collection<StreamRecord<Long>> recordOutput = testHarness.getRecordOutput();
            assertThat(recordOutput)
                    .containsExactly(
                            new StreamRecord<>(1L),
                            new StreamRecord<>(2L),
                            new StreamRecord<>(3L),
                            new StreamRecord<>(5L),
                            new StreamRecord<>(4L));
        }
    }

    @Test
    void testEndInput() throws Exception {
        AtomicInteger firstInputCounter = new AtomicInteger();
        AtomicInteger secondInputCounter = new AtomicInteger();
        TwoInputNonBroadcastProcessOperator<Integer, Long, Long> processOperator =
                new TwoInputNonBroadcastProcessOperator<>(
                        new TwoInputNonBroadcastStreamProcessFunction<Integer, Long, Long>() {
                            @Override
                            public void processRecordFromFirstInput(
                                    Integer record, Collector<Long> output, PartitionedContext ctx)
                                    throws Exception {
                                // do nothing.
                            }

                            @Override
                            public void processRecordFromSecondInput(
                                    Long record, Collector<Long> output, PartitionedContext ctx)
                                    throws Exception {
                                // do nothing.
                            }

                            @Override
                            public void endFirstInput(NonPartitionedContext<Long> ctx) {
                                try {
                                    ctx.applyToAllPartitions(
                                            (out, context) -> {
                                                firstInputCounter.incrementAndGet();
                                                out.collect(1L);
                                            });
                                } catch (Exception e) {
                                    throw new RuntimeException(e);
                                }
                            }

                            @Override
                            public void endSecondInput(NonPartitionedContext<Long> ctx) {
                                try {
                                    ctx.applyToAllPartitions(
                                            (out, context) -> {
                                                secondInputCounter.incrementAndGet();
                                                out.collect(2L);
                                            });
                                } catch (Exception e) {
                                    throw new RuntimeException(e);
                                }
                            }
                        });

        try (TwoInputStreamOperatorTestHarness<Integer, Long, Long> testHarness =
                new TwoInputStreamOperatorTestHarness<>(processOperator)) {
            testHarness.open();
            testHarness.endInput1();
            assertThat(firstInputCounter).hasValue(1);
            testHarness.endInput2();
            assertThat(secondInputCounter).hasValue(1);
            Collection<StreamRecord<Long>> recordOutput = testHarness.getRecordOutput();
            assertThat(recordOutput)
                    .containsExactly(new StreamRecord<>(1L), new StreamRecord<>(2L));
        }
    }
}
