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
import org.apache.flink.datastream.api.context.PartitionedContext;
import org.apache.flink.datastream.api.function.TwoInputBroadcastStreamProcessFunction;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link KeyedTwoInputBroadcastProcessOperator}. */
class KeyedTwoInputBroadcastProcessOperatorTest {
    @Test
    void testProcessRecord() throws Exception {
        List<Integer> fromNonBroadcastInput = new ArrayList<>();
        List<Long> fromBroadcastInput = new ArrayList<>();
        KeyedTwoInputBroadcastProcessOperator<Long, Integer, Long, Long> processOperator =
                new KeyedTwoInputBroadcastProcessOperator<>(
                        new TwoInputBroadcastStreamProcessFunction<Integer, Long, Long>() {
                            @Override
                            public void processRecordFromNonBroadcastInput(
                                    Integer record,
                                    Collector<Long> output,
                                    PartitionedContext ctx) {
                                fromNonBroadcastInput.add(record);
                            }

                            @Override
                            public void processRecordFromBroadcastInput(
                                    Long record, NonPartitionedContext<Long> ctx) {
                                fromBroadcastInput.add(record);
                            }
                        });

        try (KeyedTwoInputStreamOperatorTestHarness<Long, Integer, Long, Long> testHarness =
                new KeyedTwoInputStreamOperatorTestHarness<>(
                        processOperator,
                        (KeySelector<Integer, Long>) (data) -> (long) (data + 1),
                        (KeySelector<Long, Long>) value -> value + 1,
                        Types.LONG)) {
            testHarness.open();
            testHarness.processElement1(new StreamRecord<>(1));
            testHarness.processElement2(new StreamRecord<>(2L));
            testHarness.processElement2(new StreamRecord<>(4L));
            testHarness.processElement1(new StreamRecord<>(3));
        }

        assertThat(fromNonBroadcastInput).containsExactly(1, 3);
        assertThat(fromBroadcastInput).containsExactly(2L, 4L);
    }

    @Test
    void testEndInput() throws Exception {
        AtomicInteger nonBroadcastInputCounter = new AtomicInteger();
        AtomicInteger broadcastInputCounter = new AtomicInteger();
        KeyedTwoInputBroadcastProcessOperator<Long, Integer, Long, Long> processOperator =
                new KeyedTwoInputBroadcastProcessOperator<>(
                        new TwoInputBroadcastStreamProcessFunction<Integer, Long, Long>() {
                            @Override
                            public void processRecordFromNonBroadcastInput(
                                    Integer record,
                                    Collector<Long> output,
                                    PartitionedContext ctx) {
                                //  do nothing.
                            }

                            @Override
                            public void processRecordFromBroadcastInput(
                                    Long record, NonPartitionedContext<Long> ctx) {
                                // do nothing.
                            }

                            @Override
                            public void endNonBroadcastInput(NonPartitionedContext<Long> ctx) {
                                try {
                                    ctx.applyToAllPartitions(
                                            (out, context) -> {
                                                nonBroadcastInputCounter.incrementAndGet();
                                                Long currentKey =
                                                        context.getStateManager().getCurrentKey();
                                                out.collect(currentKey);
                                            });
                                } catch (Exception e) {
                                    throw new RuntimeException(e);
                                }
                            }

                            @Override
                            public void endBroadcastInput(NonPartitionedContext<Long> ctx) {
                                try {
                                    ctx.applyToAllPartitions(
                                            (out, context) -> {
                                                broadcastInputCounter.incrementAndGet();
                                                Long currentKey =
                                                        context.getStateManager().getCurrentKey();
                                                out.collect(currentKey);
                                            });
                                } catch (Exception e) {
                                    throw new RuntimeException(e);
                                }
                            }
                        });

        try (KeyedTwoInputStreamOperatorTestHarness<Long, Integer, Long, Long> testHarness =
                new KeyedTwoInputStreamOperatorTestHarness<>(
                        processOperator,
                        (KeySelector<Integer, Long>) Long::valueOf,
                        (KeySelector<Long, Long>) value -> value,
                        Types.LONG)) {
            testHarness.open();
            testHarness.processElement1(new StreamRecord<>(1)); // key is 1L
            testHarness.processElement2(new StreamRecord<>(2L)); // broadcast input is not keyed
            testHarness.endInput1();
            assertThat(nonBroadcastInputCounter).hasValue(1);
            Collection<StreamRecord<Long>> recordOutput = testHarness.getRecordOutput();
            assertThat(recordOutput).containsExactly(new StreamRecord<>(1L));
            testHarness.processElement2(new StreamRecord<>(3L)); // broadcast input is not keyed
            testHarness.endInput2();
            assertThat(broadcastInputCounter).hasValue(1);
            recordOutput = testHarness.getRecordOutput();
            assertThat(recordOutput)
                    .containsExactly(new StreamRecord<>(1L), new StreamRecord<>(1L));
        }
    }

    @Test
    void testCheckKey() throws Exception {
        KeyedTwoInputBroadcastProcessOperator<Long, Integer, Long, Long> processOperator =
                new KeyedTwoInputBroadcastProcessOperator<>(
                        new TwoInputBroadcastStreamProcessFunction<Integer, Long, Long>() {
                            @Override
                            public void processRecordFromNonBroadcastInput(
                                    Integer record,
                                    Collector<Long> output,
                                    PartitionedContext ctx) {
                                output.collect(Long.valueOf(record));
                            }

                            @Override
                            public void processRecordFromBroadcastInput(
                                    Long record, NonPartitionedContext<Long> ctx) throws Exception {
                                ctx.applyToAllPartitions(
                                        (collect, context) -> collect.collect(record));
                            }
                        },
                        // -1 is an invalid key in this suite.
                        (out) -> -1L);

        try (KeyedTwoInputStreamOperatorTestHarness<Long, Integer, Long, Long> testHarness =
                new KeyedTwoInputStreamOperatorTestHarness<>(
                        processOperator,
                        (KeySelector<Integer, Long>) Long::valueOf,
                        (KeySelector<Long, Long>) value -> value,
                        Types.LONG)) {
            testHarness.open();
            assertThatThrownBy(() -> testHarness.processElement1(new StreamRecord<>(1)))
                    .isInstanceOf(IllegalStateException.class);
            // TODO also test processElement2 after non-partitioned context fully implemented.
        }
    }
}
