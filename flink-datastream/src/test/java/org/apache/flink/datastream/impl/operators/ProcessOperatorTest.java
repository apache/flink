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
import org.apache.flink.datastream.api.context.RuntimeContext;
import org.apache.flink.datastream.api.function.OneInputStreamProcessFunction;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;

import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ProcessOperator}. */
class ProcessOperatorTest {
    @Test
    void testProcessRecord() throws Exception {
        ProcessOperator<Integer, String> processOperator =
                new ProcessOperator<>((record, output, ctx) -> output.collect(record + "-"));

        try (OneInputStreamOperatorTestHarness<Integer, String> testHarness =
                new OneInputStreamOperatorTestHarness<>(processOperator)) {
            testHarness.open();
            testHarness.processElement(new StreamRecord<>(1));
            testHarness.processElement(new StreamRecord<>(2));
            testHarness.processElement(new StreamRecord<>(3));

            Collection<StreamRecord<String>> recordOutput = testHarness.getRecordOutput();
            assertThat(recordOutput)
                    .containsExactly(
                            new StreamRecord<>("1-"),
                            new StreamRecord<>("2-"),
                            new StreamRecord<>("3-"));
        }
    }

    @Test
    void testEndInput() throws Exception {
        CompletableFuture<Void> future = new CompletableFuture<>();
        ProcessOperator<Integer, String> processOperator =
                new ProcessOperator<>(
                        new OneInputStreamProcessFunction<Integer, String>() {
                            @Override
                            public void processRecord(
                                    Integer record, Collector<String> output, RuntimeContext ctx) {
                                //  do nothing.
                            }

                            @Override
                            public void endInput(NonPartitionedContext<String> ctx) {
                                future.complete(null);
                            }
                        });

        try (OneInputStreamOperatorTestHarness<Integer, String> testHarness =
                new OneInputStreamOperatorTestHarness<>(processOperator)) {
            testHarness.open();
            testHarness.endInput();
            assertThat(future).isCompleted();
        }
    }
}
