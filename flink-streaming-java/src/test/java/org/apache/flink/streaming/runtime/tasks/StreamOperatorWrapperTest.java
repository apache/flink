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

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.MockStreamTaskBuilder;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** Tests for {@link StreamOperatorWrapper}. */
public class StreamOperatorWrapperTest extends TestLogger {

    private static SystemProcessingTimeService timerService;

    private static final int numOperators = 3;

    private List<StreamOperatorWrapper<?, ?>> operatorWrappers;

    private ConcurrentLinkedQueue<Object> output;

    private volatile StreamTask<?, ?> containingTask;

    @BeforeClass
    public static void startTimeService() {
        CompletableFuture<Throwable> errorFuture = new CompletableFuture<>();
        timerService = new SystemProcessingTimeService(errorFuture::complete);
    }

    @AfterClass
    public static void shutdownTimeService() {
        timerService.shutdownService();
    }

    @Before
    public void setup() throws Exception {
        this.operatorWrappers = new ArrayList<>();
        this.output = new ConcurrentLinkedQueue<>();

        try (MockEnvironment env = MockEnvironment.builder().build()) {
            this.containingTask = new MockStreamTaskBuilder(env).build();

            // initialize operator wrappers
            for (int i = 0; i < numOperators; i++) {
                MailboxExecutor mailboxExecutor =
                        containingTask.getMailboxExecutorFactory().createExecutor(i);

                TimerMailController timerMailController =
                        new TimerMailController(containingTask, mailboxExecutor);
                ProcessingTimeServiceImpl processingTimeService =
                        new ProcessingTimeServiceImpl(
                                timerService, timerMailController::wrapCallback);

                TestOneInputStreamOperator streamOperator =
                        new TestOneInputStreamOperator(
                                "Operator" + i,
                                output,
                                processingTimeService,
                                mailboxExecutor,
                                timerMailController);
                streamOperator.setProcessingTimeService(processingTimeService);

                StreamOperatorWrapper<?, ?> operatorWrapper =
                        new StreamOperatorWrapper<>(
                                streamOperator,
                                Optional.ofNullable(streamOperator.getProcessingTimeService()),
                                mailboxExecutor,
                                i == 0);
                operatorWrappers.add(operatorWrapper);
            }

            StreamOperatorWrapper<?, ?> previous = null;
            for (StreamOperatorWrapper<?, ?> current : operatorWrappers) {
                if (previous != null) {
                    previous.setNext(current);
                }
                current.setPrevious(previous);
                previous = current;
            }
        }
    }

    @After
    public void teardown() throws Exception {
        containingTask.cleanUpInternal();
    }

    @Test
    public void testFinish() throws Exception {
        output.clear();
        operatorWrappers.get(0).finish(containingTask.getActionExecutor());

        List<Object> expected = new ArrayList<>();
        for (int i = 0; i < operatorWrappers.size(); i++) {
            String prefix = "[" + "Operator" + i + "]";
            Collections.addAll(
                    expected,
                    prefix + ": End of input",
                    prefix + ": Timer that was in mailbox before closing operator",
                    prefix + ": Bye",
                    prefix + ": Mail to put in mailbox when finishing operator");
        }

        assertArrayEquals(
                "Output was not correct.",
                expected.subList(2, expected.size()).toArray(),
                output.toArray());
    }

    @Test
    public void testFinishingOperatorWithException() {
        AbstractStreamOperator<Void> streamOperator =
                new AbstractStreamOperator<Void>() {
                    @Override
                    public void finish() throws Exception {
                        throw new Exception("test exception at finishing");
                    }
                };

        StreamOperatorWrapper<?, ?> operatorWrapper =
                new StreamOperatorWrapper<>(
                        streamOperator,
                        Optional.ofNullable(streamOperator.getProcessingTimeService()),
                        containingTask
                                .getMailboxExecutorFactory()
                                .createExecutor(Integer.MAX_VALUE - 1),
                        true);

        try {
            operatorWrapper.finish(containingTask.getActionExecutor());
            fail("should throw an exception");
        } catch (Throwable t) {
            Optional<Throwable> optional =
                    ExceptionUtils.findThrowableWithMessage(t, "test exception at finishing");
            assertTrue(optional.isPresent());
        }
    }

    @Test
    public void testReadIterator() {
        // traverse operators in forward order
        Iterator<StreamOperatorWrapper<?, ?>> it =
                new StreamOperatorWrapper.ReadIterator(operatorWrappers.get(0), false);
        for (int i = 0; i < operatorWrappers.size(); i++) {
            assertTrue(it.hasNext());

            StreamOperatorWrapper<?, ?> next = it.next();
            assertNotNull(next);

            TestOneInputStreamOperator operator = getStreamOperatorFromWrapper(next);
            assertEquals("Operator" + i, operator.getName());
        }
        assertFalse(it.hasNext());

        // traverse operators in reverse order
        it =
                new StreamOperatorWrapper.ReadIterator(
                        operatorWrappers.get(operatorWrappers.size() - 1), true);
        for (int i = operatorWrappers.size() - 1; i >= 0; i--) {
            assertTrue(it.hasNext());

            StreamOperatorWrapper<?, ?> next = it.next();
            assertNotNull(next);

            TestOneInputStreamOperator operator = getStreamOperatorFromWrapper(next);
            assertEquals("Operator" + i, operator.getName());
        }
        assertFalse(it.hasNext());
    }

    private TestOneInputStreamOperator getStreamOperatorFromWrapper(
            StreamOperatorWrapper<?, ?> operatorWrapper) {
        return (TestOneInputStreamOperator)
                Objects.requireNonNull(operatorWrapper.getStreamOperator());
    }

    private static class TimerMailController {

        private final StreamTask<?, ?> containingTask;

        private final MailboxExecutor mailboxExecutor;

        private final ConcurrentHashMap<ProcessingTimeCallback, OneShotLatch> puttingLatches;

        private final ConcurrentHashMap<ProcessingTimeCallback, OneShotLatch> inMailboxLatches;

        TimerMailController(StreamTask<?, ?> containingTask, MailboxExecutor mailboxExecutor) {
            this.containingTask = containingTask;
            this.mailboxExecutor = mailboxExecutor;

            this.puttingLatches = new ConcurrentHashMap<>();
            this.inMailboxLatches = new ConcurrentHashMap<>();
        }

        OneShotLatch getPuttingLatch(ProcessingTimeCallback callback) {
            return puttingLatches.get(callback);
        }

        OneShotLatch getInMailboxLatch(ProcessingTimeCallback callback) {
            return inMailboxLatches.get(callback);
        }

        ProcessingTimeCallback wrapCallback(ProcessingTimeCallback callback) {
            puttingLatches.put(callback, new OneShotLatch());
            inMailboxLatches.put(callback, new OneShotLatch());

            return timestamp -> {
                puttingLatches.get(callback).trigger();
                containingTask
                        .deferCallbackToMailbox(mailboxExecutor, callback)
                        .onProcessingTime(timestamp);
                inMailboxLatches.get(callback).trigger();
            };
        }
    }

    private static class TestOneInputStreamOperator extends AbstractStreamOperator<String>
            implements OneInputStreamOperator<String, String>, BoundedOneInput {

        private static final long serialVersionUID = 1L;

        private final String name;

        private final ConcurrentLinkedQueue<Object> output;

        private final ProcessingTimeService processingTimeService;

        private final MailboxExecutor mailboxExecutor;

        private final TimerMailController timerMailController;

        TestOneInputStreamOperator(
                String name,
                ConcurrentLinkedQueue<Object> output,
                ProcessingTimeService processingTimeService,
                MailboxExecutor mailboxExecutor,
                TimerMailController timerMailController) {

            this.name = name;
            this.output = output;
            this.processingTimeService = processingTimeService;
            this.mailboxExecutor = mailboxExecutor;
            this.timerMailController = timerMailController;

            processingTimeService.registerTimer(
                    Long.MAX_VALUE, t2 -> output.add("[" + name + "]: Timer not triggered"));
        }

        public String getName() {
            return name;
        }

        @Override
        public void processElement(StreamRecord<String> element) {}

        @Override
        public void endInput() throws InterruptedException {
            output.add("[" + name + "]: End of input");

            ProcessingTimeCallback callback =
                    t1 ->
                            output.add(
                                    "["
                                            + name
                                            + "]: Timer that was in mailbox before closing operator");
            processingTimeService.registerTimer(0, callback);
            timerMailController.getInMailboxLatch(callback).await();
        }

        @Override
        public void finish() throws Exception {
            ProcessingTimeCallback callback =
                    t1 ->
                            output.add(
                                    "["
                                            + name
                                            + "]: Timer to put in mailbox when finishing operator");
            assertNotNull(processingTimeService.registerTimer(0, callback));
            assertNull(timerMailController.getPuttingLatch(callback));

            mailboxExecutor.submit(
                    () ->
                            output.add(
                                    "["
                                            + name
                                            + "]: Mail to put in mailbox when finishing operator"),
                    "");

            output.add("[" + name + "]: Bye");
        }
    }
}
