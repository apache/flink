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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichReduceFunction;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.TestHarnessUtil;

import org.junit.jupiter.api.Test;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit test for {@link PartitionReduceOperator}. */
class PartitionReduceOperatorTest {

    private static final int RECORD = 10;

    @Test
    void testReduce() throws Exception {
        PartitionReduceOperator<Integer> partitionReduceOperator =
                new PartitionReduceOperator<>(
                        new Reduce(new CompletableFuture<>(), new CompletableFuture<>()));
        OneInputStreamOperatorTestHarness<Integer, Integer> testHarness =
                new OneInputStreamOperatorTestHarness<>(partitionReduceOperator);
        Queue<Object> expectedOutput = new LinkedList<>();
        testHarness.open();
        testHarness.processElement(new StreamRecord<>(RECORD));
        testHarness.processElement(new StreamRecord<>(RECORD));
        testHarness.processElement(new StreamRecord<>(RECORD));
        testHarness.endInput();
        expectedOutput.add(new StreamRecord<>(RECORD * 3));
        TestHarnessUtil.assertOutputEquals(
                "The reduce result is not correct.", expectedOutput, testHarness.getOutput());
        testHarness.close();
    }

    @Test
    void testOpenClose() throws Exception {
        CompletableFuture<Object> openIdentifier = new CompletableFuture<>();
        CompletableFuture<Object> closeIdentifier = new CompletableFuture<>();
        PartitionReduceOperator<Integer> partitionReduceOperator =
                new PartitionReduceOperator<>(new Reduce(openIdentifier, closeIdentifier));
        OneInputStreamOperatorTestHarness<Integer, Integer> testHarness =
                new OneInputStreamOperatorTestHarness<>(partitionReduceOperator);
        testHarness.open();
        testHarness.processElement(new StreamRecord<>(RECORD));
        testHarness.endInput();
        testHarness.close();
        assertThat(openIdentifier).isCompleted();
        assertThat(closeIdentifier).isCompleted();
        assertThat(testHarness.getOutput()).isNotEmpty();
    }

    /** The test user implementation of {@link ReduceFunction}. */
    private static class Reduce extends RichReduceFunction<Integer> {

        private final CompletableFuture<Object> openIdentifier;

        private final CompletableFuture<Object> closeIdentifier;

        public Reduce(
                CompletableFuture<Object> openIdentifier,
                CompletableFuture<Object> closeIdentifier) {
            this.openIdentifier = openIdentifier;
            this.closeIdentifier = closeIdentifier;
        }

        @Override
        public void open(OpenContext openContext) throws Exception {
            super.open(openContext);
            openIdentifier.complete(null);
        }

        @Override
        public Integer reduce(Integer value1, Integer value2) {
            return value1 + value2;
        }

        @Override
        public void close() throws Exception {
            super.close();
            closeIdentifier.complete(null);
        }
    }
}
