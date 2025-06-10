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

package org.apache.flink.table.runtime.operators.join.lookup.keyordered;

import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.functions.async.CollectionSupplier;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.api.operators.async.queue.StreamElementQueueEntry;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTaskActionExecutor;
import org.apache.flink.streaming.runtime.tasks.mailbox.MailboxExecutorImpl;
import org.apache.flink.streaming.runtime.tasks.mailbox.MailboxProcessor;
import org.apache.flink.streaming.runtime.tasks.mailbox.TaskMailbox;
import org.apache.flink.streaming.runtime.tasks.mailbox.TaskMailboxImpl;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import static org.apache.flink.streaming.api.operators.async.AsyncWaitOperatorTest.LazyAsyncFunction;
import static org.apache.flink.table.runtime.operators.join.lookup.utils.AsyncLookupTestUtils.assertKeyOrdered;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Test for the component {@link AsyncExecutionController} used in table runtime for async look up
 * join.
 */
public class AsyncExecutionControllerTest {

    private static final KeySelector<Integer, Integer> keySelector = input -> input;

    private static final AtomicInteger outputAccount = new AtomicInteger(0);

    private static final Queue<Integer> outputQueue = new LinkedList<>();

    private static final Queue<Watermark> outputWatermark = new LinkedList<>();

    private static final Queue<StreamRecord<Integer>> outputProcessed = new LinkedList<>();

    private static TestAsyncExecutionController asyncExecutionController;

    private static TestLazyAsyncFunction asyncFunction;

    private static MailboxExecutor mailboxExecutor;

    @BeforeEach
    public void before() throws Exception {
        asyncFunction = new TestLazyAsyncFunction();
        outputAccount.set(0);
        outputQueue.clear();
        outputProcessed.clear();
        outputWatermark.clear();
        Consumer<StreamElementQueueEntry<Integer>> emitResult = entry -> entry.emitResult(null);
        asyncExecutionController =
                new TestAsyncExecutionController(
                        keySelector,
                        element -> {
                            // check the value is Integer
                            try {
                                asyncFunction.asyncInvoke(
                                        element.getRecord().getValue(),
                                        new Handler(
                                                element,
                                                new TestStreamElementQueueEntry(
                                                        element.getRecord())));
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        },
                        outputWatermark::add,
                        emitResult);
    }

    @BeforeEach
    void beforeEach() {
        TaskMailbox mailbox = new TaskMailboxImpl();
        MailboxProcessor mailboxProcessor =
                new MailboxProcessor(controller -> {}, mailbox, StreamTaskActionExecutor.IMMEDIATE);
        mailboxExecutor =
                new MailboxExecutorImpl(
                        mailbox, 0, StreamTaskActionExecutor.IMMEDIATE, mailboxProcessor);
    }

    @AfterEach
    public void after() throws Exception {
        asyncFunction.close();
        asyncExecutionController.close();
    }

    @Test
    public void testPendingRecords() throws Exception {
        asyncExecutionController.submitRecord(new StreamRecord<>(1, 1), null);
        asyncExecutionController.submitRecord(new StreamRecord<>(2, 2), null);
        asyncExecutionController.submitRecord(new StreamRecord<>(2, 3), null);
        asyncExecutionController.submitRecord(new StreamRecord<>(3, 4), null);
        asyncExecutionController.submitRecord(new StreamRecord<>(3, 5), null);
        asyncExecutionController.submitRecord(new StreamRecord<>(4, 6), null);
        Map<Integer, Deque<AecRecord<Integer, Integer>>> actualPending =
                asyncExecutionController.pendingElements();
        Epoch<Integer> epoch = new Epoch<>(new Watermark(Long.MIN_VALUE));
        IntStream.range(0, 6).forEach(i -> epoch.incrementCount());
        assertThat(actualPending.get(1))
                .containsExactlyInAnyOrder(new AecRecord<>(new StreamRecord<>(1, 1), epoch));
        assertThat(actualPending.get(2))
                .containsExactly(
                        new AecRecord<>(new StreamRecord<>(2, 2), epoch),
                        new AecRecord<>(new StreamRecord<>(2, 3), epoch));
        assertThat(actualPending.get(3))
                .containsExactly(
                        new AecRecord<>(new StreamRecord<>(3, 4), epoch),
                        new AecRecord<>(new StreamRecord<>(3, 5), epoch));
        assertThat(actualPending.get(4))
                .containsExactlyInAnyOrder(new AecRecord<>(new StreamRecord<>(4, 6), epoch));
    }

    @Test
    public void testDifferentKeyWithoutWatermark() throws Exception {
        asyncExecutionController.submitRecord(new StreamRecord<>(1, 1), null);
        asyncExecutionController.submitRecord(new StreamRecord<>(2, 2), null);
        asyncExecutionController.submitRecord(new StreamRecord<>(3, 3), null);
        asyncExecutionController.submitRecord(new StreamRecord<>(4, 4), null);
        asyncExecutionController.submitRecord(new StreamRecord<>(5, 5), null);
        asyncExecutionController.submitRecord(new StreamRecord<>(6, 6), null);
        assertThat(asyncExecutionController.processedSize()).isEqualTo(6);
        waitComplete();
        Queue<Integer> expectedOutput = new LinkedList<>(Arrays.asList(1, 2, 3, 4, 5, 6));
        assertThat(outputQueue.stream().sorted()).isEqualTo(expectedOutput);
        Epoch<Integer> expectedEpoch = new Epoch<>(new Watermark(Long.MIN_VALUE));
        assertThat(asyncExecutionController.getActiveEpoch()).isEqualTo(expectedEpoch);
    }

    @Test
    public void testDifferentKeyWithWatermark() throws Exception {
        asyncExecutionController.submitRecord(new StreamRecord<>(1, 1), null);
        asyncExecutionController.submitRecord(new StreamRecord<>(2, 2), null);
        asyncExecutionController.submitWatermark(new Watermark(3));
        asyncExecutionController.submitRecord(new StreamRecord<>(3, 4), null);
        asyncExecutionController.submitRecord(new StreamRecord<>(4, 5), null);
        asyncExecutionController.submitWatermark(new Watermark(6));
        asyncExecutionController.submitRecord(new StreamRecord<>(5, 7), null);
        asyncExecutionController.submitRecord(new StreamRecord<>(6, 8), null);
        assertThat(asyncExecutionController.processedSize()).isEqualTo(6);
        waitComplete();
        Queue<Integer> expectedOutput = new LinkedList<>(Arrays.asList(1, 2, 3, 4, 5, 6));
        assertThat(outputQueue.stream().sorted()).isEqualTo(expectedOutput);
        Queue<Watermark> expectedWatermark =
                new LinkedList<>(Arrays.asList(new Watermark(3), new Watermark(6)));
        assertThat(outputWatermark).isEqualTo(expectedWatermark);
        Epoch<Integer> expectedEpoch = new Epoch<>(new Watermark(6));
        assertThat(asyncExecutionController.getActiveEpoch()).isEqualTo(expectedEpoch);
    }

    @Test
    public void testSameKeyWithWatermark() throws Exception {
        asyncExecutionController.submitRecord(new StreamRecord<>(1, 1), null);
        asyncExecutionController.submitRecord(new StreamRecord<>(1, 2), null);
        asyncExecutionController.submitRecord(new StreamRecord<>(1, 3), null);
        asyncExecutionController.submitRecord(new StreamRecord<>(1, 4), null);
        asyncExecutionController.submitWatermark(new Watermark(5));
        asyncExecutionController.submitWatermark(new Watermark(6));
        asyncExecutionController.submitRecord(new StreamRecord<>(1, 7), null);
        asyncExecutionController.submitRecord(new StreamRecord<>(1, 8), null);
        asyncExecutionController.submitWatermark(new Watermark(9));

        assertThat(asyncExecutionController.processedSize()).isEqualTo(6);
        waitComplete();
        Queue<Integer> expectedOutput = new LinkedList<>(Arrays.asList(1, 1, 1, 1, 1, 1));
        assertThat(outputQueue).isEqualTo(expectedOutput);
        Queue<StreamRecord<Integer>> expectedProcessed =
                new LinkedList<>(
                        Arrays.asList(
                                new StreamRecord<>(1, 1),
                                new StreamRecord<>(1, 2),
                                new StreamRecord<>(1, 3),
                                new StreamRecord<>(1, 4),
                                new StreamRecord<>(1, 7),
                                new StreamRecord<>(1, 8)));
        assertThat(outputProcessed).isEqualTo(expectedProcessed);
        Queue<Watermark> expectedWatermark =
                new LinkedList<>(
                        Arrays.asList(new Watermark(5), new Watermark(6), new Watermark(9)));
        assertThat(outputWatermark).isEqualTo(expectedWatermark);
        Epoch<Integer> expectedEpoch = new Epoch<>(new Watermark(9));
        assertThat(asyncExecutionController.getActiveEpoch()).isEqualTo(expectedEpoch);
    }

    @Test
    public void testMixKeyWithWatermark() throws Exception {
        asyncExecutionController.submitRecord(new StreamRecord<>(1, 1), null);
        asyncExecutionController.submitRecord(new StreamRecord<>(3, 2), null);
        asyncExecutionController.submitRecord(new StreamRecord<>(4, 3), null);
        asyncExecutionController.submitRecord(new StreamRecord<>(3, 4), null);
        asyncExecutionController.submitWatermark(new Watermark(5));
        asyncExecutionController.submitWatermark(new Watermark(6));
        asyncExecutionController.submitRecord(new StreamRecord<>(4, 7), null);
        asyncExecutionController.submitRecord(new StreamRecord<>(1, 8), null);
        asyncExecutionController.submitWatermark(new Watermark(9));

        assertThat(asyncExecutionController.processedSize()).isEqualTo(6);
        waitComplete();
        Queue<Integer> expectedOutput = new LinkedList<>(Arrays.asList(1, 1, 3, 3, 4, 4));
        assertThat(outputQueue.stream().sorted()).isEqualTo(expectedOutput);

        Queue<StreamRecord<Integer>> expectedProcessed =
                new LinkedList<>(Arrays.asList(new StreamRecord<>(1, 1), new StreamRecord<>(1, 8)));
        assertKeyOrdered(outputProcessed, expectedProcessed);
        expectedProcessed =
                new LinkedList<>(Arrays.asList(new StreamRecord<>(3, 2), new StreamRecord<>(3, 4)));
        assertKeyOrdered(outputProcessed, expectedProcessed);
        expectedProcessed =
                new LinkedList<>(Arrays.asList(new StreamRecord<>(4, 3), new StreamRecord<>(4, 7)));
        assertKeyOrdered(outputProcessed, expectedProcessed);

        Queue<Watermark> expectedWatermark =
                new LinkedList<>(
                        Arrays.asList(new Watermark(5), new Watermark(6), new Watermark(9)));
        assertThat(outputWatermark).isEqualTo(expectedWatermark);
        Epoch<Integer> expectedEpoch = new Epoch<>(new Watermark(9));
        assertThat(asyncExecutionController.getActiveEpoch()).isEqualTo(expectedEpoch);
    }

    private void waitComplete() {
        long now = System.currentTimeMillis();
        while (mailboxExecutor.tryYield()) {
            if (System.currentTimeMillis() - now > 3000) {
                fail("Execution timeout");
            }
        }
    }

    private static class Handler implements ResultFuture<Integer> {

        private final AecRecord<Integer, Integer> inputRecord;
        private final StreamElementQueueEntry<Integer> resultFuture;

        public Handler(
                AecRecord<Integer, Integer> inputRecord,
                StreamElementQueueEntry<Integer> resultFuture) {
            this.inputRecord = inputRecord;
            this.resultFuture = resultFuture;
        }

        @Override
        public void complete(java.util.Collection<Integer> results) {
            mailboxExecutor.execute(
                    () -> {
                        try {
                            resultFuture.complete(results);
                            asyncExecutionController.completeRecord(resultFuture, inputRecord);
                        } catch (Exception e) {
                            // Note: exception is thrown in different threads here.
                            throw new RuntimeException(e);
                        }
                    },
                    "handler complete results");
        }

        @Override
        public void completeExceptionally(Throwable error) {}

        @Override
        public void complete(CollectionSupplier<Integer> supplier) {}
    }

    private static class TestStreamElementQueueEntry implements StreamElementQueueEntry<Integer> {

        @Nonnull private final StreamRecord<Integer> inputRecord;
        private java.util.Collection<Integer> results;

        public TestStreamElementQueueEntry(@Nonnull StreamRecord<Integer> inputRecord) {
            this.inputRecord = inputRecord;
        }

        @Override
        public void complete(Collection<Integer> result) {
            this.results = result;
        }

        @Override
        public boolean isDone() {
            return false;
        }

        @Override
        public void emitResult(TimestampedCollector<Integer> output) {
            outputQueue.addAll(results);
            outputProcessed.add(inputRecord);
        }

        @Override
        public StreamElement getInputElement() {
            return inputRecord;
        }
    }

    private static class TestAsyncExecutionController
            extends AsyncExecutionController<Integer, Integer, Integer> {

        private final AtomicLong processedAccount = new AtomicLong(0);

        public TestAsyncExecutionController(
                KeySelector<Integer, Integer> keySelector,
                Consumer<AecRecord<Integer, Integer>> asyncInvoke,
                Consumer<Watermark> emitWatermark,
                Consumer<StreamElementQueueEntry<Integer>> emitResult) {
            super(keySelector, asyncInvoke, emitWatermark, emitResult);
        }

        @Override
        public void submitRecord(StreamRecord<Integer> record, @Nullable Epoch<Integer> epoch)
                throws Exception {
            processedAccount.incrementAndGet();
            super.submitRecord(record, epoch);
        }

        public long processedSize() {
            return processedAccount.get();
        }
    }

    private static class TestLazyAsyncFunction extends LazyAsyncFunction {

        public TestLazyAsyncFunction() {}

        @Override
        public void asyncInvoke(final Integer input, final ResultFuture<Integer> resultFuture) {
            resultFuture.complete(Collections.singletonList(input));
        }
    }
}
