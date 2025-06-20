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

import org.apache.flink.api.common.functions.DefaultOpenContext;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.functions.async.CollectionSupplier;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.api.operators.async.AsyncWaitOperatorTest;
import org.apache.flink.streaming.api.operators.async.queue.StreamElementQueueEntry;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTaskActionExecutor;
import org.apache.flink.streaming.runtime.tasks.mailbox.MailboxExecutorImpl;
import org.apache.flink.streaming.runtime.tasks.mailbox.MailboxProcessor;
import org.apache.flink.streaming.runtime.tasks.mailbox.TaskMailbox;
import org.apache.flink.streaming.runtime.tasks.mailbox.TaskMailboxImpl;
import org.apache.flink.util.function.ThrowingConsumer;

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
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;

import static org.apache.flink.table.runtime.util.AsyncKeyOrderedTestUtils.assertKeyOrdered;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

/** Test for the component {@link TableAsyncExecutionController} used in table runtime. */
public class TableAsyncExecutionControllerTest {

    private static final KeySelector<Integer, Integer> keySelector = input -> input;

    private final Queue<Integer> outputQueue = new LinkedList<>();

    private final Queue<Watermark> outputWatermark = new LinkedList<>();

    private final Queue<StreamRecord<Integer>> outputProcessedRecords = new LinkedList<>();

    private final Queue<Integer> outputProcessedInputIndexes = new LinkedList<>();

    private TestAsyncExecutionController asyncExecutionController;

    private MailboxExecutor mailboxExecutor;

    private TestLazyAsyncFunction asyncFunction;

    @BeforeEach
    public void before() throws Exception {
        TaskMailbox mailbox = new TaskMailboxImpl();
        MailboxProcessor mailboxProcessor =
                new MailboxProcessor(controller -> {}, mailbox, StreamTaskActionExecutor.IMMEDIATE);
        mailboxExecutor =
                new MailboxExecutorImpl(
                        mailbox, 0, StreamTaskActionExecutor.IMMEDIATE, mailboxProcessor);
        asyncFunction = new TestLazyAsyncFunction();
        asyncFunction.open(DefaultOpenContext.INSTANCE);
        asyncExecutionController =
                new TestAsyncExecutionController(
                        element ->
                                asyncFunction.asyncInvoke(
                                        element.getRecord().getValue(),
                                        new Handler(
                                                element,
                                                new TestStreamElementQueueEntry(
                                                        element.getRecord(),
                                                        element.getInputIndex(),
                                                        outputProcessedRecords,
                                                        outputProcessedInputIndexes,
                                                        outputQueue),
                                                mailboxExecutor,
                                                asyncExecutionController)),
                        outputWatermark::add);
    }

    @AfterEach
    public void after() throws Exception {
        asyncFunction.close();
        asyncExecutionController.close();
        outputQueue.clear();
        outputProcessedRecords.clear();
        outputProcessedInputIndexes.clear();
        outputWatermark.clear();
    }

    @Test
    public void testPendingRecords() throws Exception {
        asyncExecutionController.submitRecord(new StreamRecord<>(1, 1), null, 0);
        asyncExecutionController.submitRecord(new StreamRecord<>(2, 2), null, 0);
        asyncExecutionController.submitRecord(new StreamRecord<>(2, 3), null, 0);
        asyncExecutionController.submitRecord(new StreamRecord<>(3, 4), null, 0);
        asyncExecutionController.submitRecord(new StreamRecord<>(3, 5), null, 0);
        asyncExecutionController.submitRecord(new StreamRecord<>(4, 6), null, 0);
        Map<Integer, Deque<AecRecord<Integer, Integer>>> actualPending =
                asyncExecutionController.pendingElements();
        Epoch<Integer> epoch = new Epoch<>(new Watermark(Long.MIN_VALUE));
        IntStream.range(0, 6).forEach(i -> epoch.incrementCount());
        assertThat(actualPending.get(1))
                .containsExactlyInAnyOrder(new AecRecord<>(new StreamRecord<>(1, 1), epoch, 0));
        assertThat(actualPending.get(2))
                .containsExactly(
                        new AecRecord<>(new StreamRecord<>(2, 2), epoch, 0),
                        new AecRecord<>(new StreamRecord<>(2, 3), epoch, 0));
        assertThat(actualPending.get(3))
                .containsExactly(
                        new AecRecord<>(new StreamRecord<>(3, 4), epoch, 0),
                        new AecRecord<>(new StreamRecord<>(3, 5), epoch, 0));
        assertThat(actualPending.get(4))
                .containsExactlyInAnyOrder(new AecRecord<>(new StreamRecord<>(4, 6), epoch, 0));
    }

    @Test
    public void testDifferentKeyWithoutWatermark() throws Exception {
        asyncExecutionController.submitRecord(new StreamRecord<>(1, 1), null, 0);
        asyncExecutionController.submitRecord(new StreamRecord<>(2, 2), null, 0);
        asyncExecutionController.submitRecord(new StreamRecord<>(3, 3), null, 0);
        asyncExecutionController.submitRecord(new StreamRecord<>(4, 4), null, 0);
        asyncExecutionController.submitRecord(new StreamRecord<>(5, 5), null, 0);
        asyncExecutionController.submitRecord(new StreamRecord<>(6, 6), null, 0);
        assertThat(asyncExecutionController.processedSize()).isEqualTo(6);
        waitComplete();
        Queue<Integer> expectedOutput = new LinkedList<>(Arrays.asList(1, 2, 3, 4, 5, 6));
        assertThat(outputQueue.stream().sorted()).isEqualTo(expectedOutput);
        Epoch<Integer> expectedEpoch = new Epoch<>(new Watermark(Long.MIN_VALUE));
        assertThat(asyncExecutionController.getActiveEpoch()).isEqualTo(expectedEpoch);
    }

    @Test
    public void testDifferentKeyWithWatermark() throws Exception {
        asyncExecutionController.submitRecord(new StreamRecord<>(1, 1), null, 0);
        asyncExecutionController.submitRecord(new StreamRecord<>(2, 2), null, 0);
        asyncExecutionController.submitWatermark(new Watermark(3));
        asyncExecutionController.submitRecord(new StreamRecord<>(3, 4), null, 0);
        asyncExecutionController.submitRecord(new StreamRecord<>(4, 5), null, 0);
        asyncExecutionController.submitWatermark(new Watermark(6));
        asyncExecutionController.submitRecord(new StreamRecord<>(5, 7), null, 0);
        asyncExecutionController.submitRecord(new StreamRecord<>(6, 8), null, 0);
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
        asyncExecutionController.submitRecord(new StreamRecord<>(1, 1), null, 0);
        asyncExecutionController.submitRecord(new StreamRecord<>(1, 2), null, 0);
        asyncExecutionController.submitRecord(new StreamRecord<>(1, 3), null, 0);
        asyncExecutionController.submitRecord(new StreamRecord<>(1, 4), null, 0);
        asyncExecutionController.submitWatermark(new Watermark(5));
        asyncExecutionController.submitWatermark(new Watermark(6));
        asyncExecutionController.submitRecord(new StreamRecord<>(1, 7), null, 0);
        asyncExecutionController.submitRecord(new StreamRecord<>(1, 8), null, 0);
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
        assertThat(outputProcessedRecords).isEqualTo(expectedProcessed);
        Queue<Watermark> expectedWatermark =
                new LinkedList<>(
                        Arrays.asList(new Watermark(5), new Watermark(6), new Watermark(9)));
        assertThat(outputWatermark).isEqualTo(expectedWatermark);
        Epoch<Integer> expectedEpoch = new Epoch<>(new Watermark(9));
        assertThat(asyncExecutionController.getActiveEpoch()).isEqualTo(expectedEpoch);
    }

    @Test
    public void testMixKeyWithWatermark() throws Exception {
        asyncExecutionController.submitRecord(new StreamRecord<>(1, 1), null, 0);
        asyncExecutionController.submitRecord(new StreamRecord<>(3, 2), null, 0);
        asyncExecutionController.submitRecord(new StreamRecord<>(4, 3), null, 0);
        asyncExecutionController.submitRecord(new StreamRecord<>(3, 4), null, 0);
        asyncExecutionController.submitWatermark(new Watermark(5));
        asyncExecutionController.submitWatermark(new Watermark(6));
        asyncExecutionController.submitRecord(new StreamRecord<>(4, 7), null, 0);
        asyncExecutionController.submitRecord(new StreamRecord<>(1, 8), null, 0);
        asyncExecutionController.submitWatermark(new Watermark(9));

        assertThat(asyncExecutionController.processedSize()).isEqualTo(6);
        waitComplete();
        Queue<Integer> expectedOutput = new LinkedList<>(Arrays.asList(1, 1, 3, 3, 4, 4));
        assertThat(outputQueue.stream().sorted()).isEqualTo(expectedOutput);

        Queue<StreamRecord<Integer>> expectedProcessed =
                new LinkedList<>(Arrays.asList(new StreamRecord<>(1, 1), new StreamRecord<>(1, 8)));
        assertKeyOrdered(outputProcessedRecords, expectedProcessed);
        expectedProcessed =
                new LinkedList<>(Arrays.asList(new StreamRecord<>(3, 2), new StreamRecord<>(3, 4)));
        assertKeyOrdered(outputProcessedRecords, expectedProcessed);
        expectedProcessed =
                new LinkedList<>(Arrays.asList(new StreamRecord<>(4, 3), new StreamRecord<>(4, 7)));
        assertKeyOrdered(outputProcessedRecords, expectedProcessed);

        Queue<Watermark> expectedWatermark =
                new LinkedList<>(
                        Arrays.asList(new Watermark(5), new Watermark(6), new Watermark(9)));
        assertThat(outputWatermark).isEqualTo(expectedWatermark);
        Epoch<Integer> expectedEpoch = new Epoch<>(new Watermark(9));
        assertThat(asyncExecutionController.getActiveEpoch()).isEqualTo(expectedEpoch);
    }

    @Test
    public void testProcessWithMultiInputs() throws Exception {
        asyncExecutionController.submitRecord(new StreamRecord<>(1, 1), null, 1);
        asyncExecutionController.submitRecord(new StreamRecord<>(1, 2), null, 2);
        asyncExecutionController.submitRecord(new StreamRecord<>(1, 3), null, 1);
        asyncExecutionController.submitRecord(new StreamRecord<>(1, 4), null, 3);
        asyncExecutionController.submitWatermark(new Watermark(5));
        asyncExecutionController.submitRecord(new StreamRecord<>(1, 6), null, 4);

        assertThat(asyncExecutionController.processedSize()).isEqualTo(5);
        waitComplete();
        Queue<Integer> expectedOutput = new LinkedList<>(Arrays.asList(1, 1, 1, 1, 1));
        assertThat(outputQueue).isEqualTo(expectedOutput);
        Queue<StreamRecord<Integer>> expectedProcessed =
                new LinkedList<>(
                        Arrays.asList(
                                new StreamRecord<>(1, 1),
                                new StreamRecord<>(1, 2),
                                new StreamRecord<>(1, 3),
                                new StreamRecord<>(1, 4),
                                new StreamRecord<>(1, 6)));
        assertThat(outputProcessedRecords).isEqualTo(expectedProcessed);
        Queue<Watermark> expectedWatermark =
                new LinkedList<>(Collections.singletonList(new Watermark(5)));
        assertThat(outputWatermark).isEqualTo(expectedWatermark);
        Epoch<Integer> expectedEpoch = new Epoch<>(new Watermark(5));
        assertThat(asyncExecutionController.getActiveEpoch()).isEqualTo(expectedEpoch);
        Queue<Integer> expectedProcessedInputIndexes =
                new LinkedList<>(Arrays.asList(1, 2, 1, 3, 4));
        assertThat(outputProcessedInputIndexes).isEqualTo(expectedProcessedInputIndexes);
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
        private final MailboxExecutor mailboxExecutor;
        private final TestAsyncExecutionController asyncExecutionController;

        public Handler(
                AecRecord<Integer, Integer> inputRecord,
                StreamElementQueueEntry<Integer> resultFuture,
                MailboxExecutor mailboxExecutor,
                TestAsyncExecutionController asyncExecutionController) {
            this.inputRecord = inputRecord;
            this.resultFuture = resultFuture;
            this.mailboxExecutor = mailboxExecutor;
            this.asyncExecutionController = asyncExecutionController;
        }

        @Override
        public void complete(Collection<Integer> results) {
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
        private Collection<Integer> results;

        private final int inputIndex;

        private final Queue<StreamRecord<Integer>> outputProcessedRecords;
        private final Queue<Integer> outputProcessedInputIndexes;
        private final Queue<Integer> outputQueue;

        public TestStreamElementQueueEntry(
                @Nonnull StreamRecord<Integer> inputRecord,
                int inputIndex,
                Queue<StreamRecord<Integer>> outputProcessedRecords,
                Queue<Integer> outputProcessedInputIndexes,
                Queue<Integer> outputQueue) {
            this.inputRecord = inputRecord;
            this.inputIndex = inputIndex;
            this.outputProcessedRecords = outputProcessedRecords;
            this.outputProcessedInputIndexes = outputProcessedInputIndexes;
            this.outputQueue = outputQueue;
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
            outputProcessedRecords.add(inputRecord);
            outputProcessedInputIndexes.add(inputIndex);
        }

        @Nonnull
        @Override
        public StreamElement getInputElement() {
            return inputRecord;
        }

        public int getInputIndex() {
            return inputIndex;
        }
    }

    private static class TestAsyncExecutionController
            extends TableAsyncExecutionController<Integer, Integer, Integer> {

        private final AtomicLong processedAccount = new AtomicLong(0);

        public TestAsyncExecutionController(
                ThrowingConsumer<AecRecord<Integer, Integer>, Exception> asyncInvoke,
                ThrowingConsumer<Watermark, Exception> emitWatermark) {
            super(
                    asyncInvoke,
                    emitWatermark,
                    entry -> entry.emitResult(null),
                    entry -> ((TestStreamElementQueueEntry) entry).getInputIndex(),
                    (record, inputIndex) -> keySelector.getKey(record.getValue()));
        }

        @Override
        public void submitRecord(
                StreamRecord<Integer> record, @Nullable Epoch<Integer> epoch, int inputIndex)
                throws Exception {
            processedAccount.incrementAndGet();
            super.submitRecord(record, epoch, inputIndex);
        }

        public long processedSize() {
            return processedAccount.get();
        }
    }

    private static class TestLazyAsyncFunction extends AsyncWaitOperatorTest.LazyAsyncFunction {

        public TestLazyAsyncFunction() {}

        @Override
        public void asyncInvoke(final Integer input, final ResultFuture<Integer> resultFuture) {
            resultFuture.complete(Collections.singletonList(input));
        }
    }
}
