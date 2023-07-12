/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.checkpoint.channel;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.runtime.state.storage.JobManagerCheckpointStorage;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.function.BiConsumerWithException;

import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.runtime.checkpoint.channel.ChannelStateWriteRequestDispatcher.NO_OP;
import static org.apache.flink.util.ExceptionUtils.findThrowable;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;

/** {@link ChannelStateWriteRequestExecutorImpl} test. */
class ChannelStateWriteRequestExecutorImplTest {

    private static final JobID JOB_ID = new JobID();
    private static final JobVertexID JOB_VERTEX_ID = new JobVertexID();
    private static final int SUBTASK_INDEX = 0;

    @Test
    void testCloseAfterSubmit() {
        assertThatThrownBy(() -> testCloseAfterSubmit(ChannelStateWriteRequestExecutor::submit))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void testCloseAfterSubmitPriority() {
        assertThatThrownBy(
                        () ->
                                testCloseAfterSubmit(
                                        ChannelStateWriteRequestExecutor::submitPriority))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void testSubmitFailure() throws Exception {
        testSubmitFailure(ChannelStateWriteRequestExecutor::submit);
    }

    @Test
    void testSubmitPriorityFailure() throws Exception {
        testSubmitFailure(ChannelStateWriteRequestExecutor::submitPriority);
    }

    private void testCloseAfterSubmit(
            BiConsumerWithException<
                            ChannelStateWriteRequestExecutor, ChannelStateWriteRequest, Exception>
                    requestFun)
            throws Exception {
        WorkerClosingDeque closingDeque = new WorkerClosingDeque();
        Object registerLock = new Object();
        ChannelStateWriteRequestExecutorImpl worker =
                new ChannelStateWriteRequestExecutorImpl(
                        NO_OP, closingDeque, 5, registerLock, e -> {});
        closingDeque.setWorker(worker);
        synchronized (registerLock) {
            worker.registerSubtask(JOB_VERTEX_ID, SUBTASK_INDEX);
        }
        TestWriteRequest request = new TestWriteRequest(JOB_VERTEX_ID, SUBTASK_INDEX);
        requestFun.accept(worker, request);
        assertThat(closingDeque).isEmpty();
        assertThat(request.isCancelled()).isFalse();
    }

    private void testSubmitFailure(
            BiConsumerWithException<
                            ChannelStateWriteRequestExecutor, ChannelStateWriteRequest, Exception>
                    submitAction)
            throws Exception {
        TestWriteRequest request = new TestWriteRequest(JOB_VERTEX_ID, SUBTASK_INDEX);
        Deque<ChannelStateWriteRequest> deque = new ArrayDeque<>();
        try {
            Object registerLock = new Object();
            ChannelStateWriteRequestExecutorImpl executor =
                    new ChannelStateWriteRequestExecutorImpl(
                            NO_OP, deque, 5, registerLock, e -> {});
            synchronized (registerLock) {
                executor.registerSubtask(JOB_VERTEX_ID, SUBTASK_INDEX);
            }
            submitAction.accept(executor, request);
        } catch (IllegalStateException e) {
            // expected: executor not started;
            return;
        } finally {
            assertThat(request.cancelled).isTrue();
            assertThat(deque).isEmpty();
        }
        throw new RuntimeException("expected exception not thrown");
    }

    @Test
    @SuppressWarnings("CallToThreadRun")
    void testCleanup() throws IOException {
        TestWriteRequest request = new TestWriteRequest(JOB_VERTEX_ID, SUBTASK_INDEX);
        Deque<ChannelStateWriteRequest> deque = new ArrayDeque<>();
        deque.add(request);
        TestRequestDispatcher requestProcessor = new TestRequestDispatcher();
        Object registerLock = new Object();
        ChannelStateWriteRequestExecutorImpl worker =
                new ChannelStateWriteRequestExecutorImpl(
                        requestProcessor, deque, 5, registerLock, e -> {});
        synchronized (registerLock) {
            worker.registerSubtask(JOB_VERTEX_ID, SUBTASK_INDEX);
        }
        worker.releaseSubtask(JOB_VERTEX_ID, SUBTASK_INDEX);
        worker.run();

        assertThat(requestProcessor.isStopped()).isTrue();
        assertThat(deque).isEmpty();
        assertThat(request.isCancelled()).isTrue();
    }

    @Test
    void testIgnoresInterruptsWhileRunning() throws Exception {
        TestRequestDispatcher requestProcessor = new TestRequestDispatcher();
        Deque<ChannelStateWriteRequest> deque = new ArrayDeque<>();
        Object registerLock = new Object();
        ChannelStateWriteRequestExecutorImpl worker =
                new ChannelStateWriteRequestExecutorImpl(
                        requestProcessor, deque, 5, registerLock, e -> {});
        synchronized (registerLock) {
            worker.registerSubtask(JOB_VERTEX_ID, SUBTASK_INDEX);
        }
        try {
            worker.start();
            worker.getThread().interrupt();
            worker.submit(new TestWriteRequest(JOB_VERTEX_ID, SUBTASK_INDEX));
            worker.getThread().interrupt();
            while (!deque.isEmpty()) {
                Thread.sleep(100);
            }
        } finally {
            worker.releaseSubtask(JOB_VERTEX_ID, SUBTASK_INDEX);
        }
    }

    @Test
    void testCanBeClosed() throws Exception {
        long checkpointId = 1L;
        ChannelStateWriteRequestDispatcher processor =
                new ChannelStateWriteRequestDispatcherImpl(
                        new JobManagerCheckpointStorage(),
                        JOB_ID,
                        new ChannelStateSerializerImpl());
        Object registerLock = new Object();
        ChannelStateWriteRequestExecutorImpl worker =
                new ChannelStateWriteRequestExecutorImpl(processor, 5, e -> {}, registerLock);
        synchronized (registerLock) {
            worker.registerSubtask(JOB_VERTEX_ID, SUBTASK_INDEX);
        }
        try {
            worker.start();
            worker.submit(
                    new CheckpointStartRequest(
                            JOB_VERTEX_ID,
                            SUBTASK_INDEX,
                            checkpointId,
                            new ChannelStateWriter.ChannelStateWriteResult(),
                            CheckpointStorageLocationReference.getDefault()));
            worker.submit(
                    ChannelStateWriteRequest.write(
                            JOB_VERTEX_ID,
                            SUBTASK_INDEX,
                            checkpointId,
                            new ResultSubpartitionInfo(0, 0),
                            new CompletableFuture<>()));
            worker.submit(
                    ChannelStateWriteRequest.write(
                            JOB_VERTEX_ID,
                            SUBTASK_INDEX,
                            checkpointId,
                            new ResultSubpartitionInfo(0, 0),
                            new CompletableFuture<>()));
        } finally {
            worker.releaseSubtask(JOB_VERTEX_ID, SUBTASK_INDEX);
        }
    }

    @Test
    void testSkipUnreadyDataFuture() throws Exception {
        int subtaskIndex0 = 0;
        int subtaskIndex1 = 1;

        Queue<ChannelStateWriteRequest> firstBatchRequests = new LinkedList<>();
        Queue<ChannelStateWriteRequest> secondBatchRequests = new LinkedList<>();
        CompletableFuture<List<Buffer>> dataFuture = new CompletableFuture<>();
        int firstBatchSubtask1Count = 3;
        int subtask0Count = 4;
        int subtask1Count = 4;

        {
            // Generate the first batch requests.
            firstBatchRequests.add(new TestWriteRequest(JOB_VERTEX_ID, subtaskIndex1));
            firstBatchRequests.add(new TestWriteRequest(JOB_VERTEX_ID, subtaskIndex0));
            // Add a data future request, all subsequent requests of subtaskIndex0 should be blocked
            // before this future is completed.
            firstBatchRequests.add(new TestWriteRequest(JOB_VERTEX_ID, subtaskIndex0, dataFuture));
            firstBatchRequests.add(new TestWriteRequest(JOB_VERTEX_ID, subtaskIndex0));
            firstBatchRequests.add(new TestWriteRequest(JOB_VERTEX_ID, subtaskIndex1));
            firstBatchRequests.add(new TestWriteRequest(JOB_VERTEX_ID, subtaskIndex1));

            // Generate the second batch requests.
            secondBatchRequests.add(new TestWriteRequest(JOB_VERTEX_ID, subtaskIndex0));
            secondBatchRequests.add(new TestWriteRequest(JOB_VERTEX_ID, subtaskIndex1));
        }

        CompletableFuture<Void> firstBatchFuture = new CompletableFuture<>();
        CompletableFuture<Void> allReceivedFuture = new CompletableFuture<>();

        // The subtask register request cannot be count.
        AtomicInteger subtask0ReceivedCounter = new AtomicInteger(-1);
        AtomicInteger subtask1ReceivedCounter = new AtomicInteger(-1);

        TestRequestDispatcher throwingRequestProcessor =
                new TestRequestDispatcher() {
                    @Override
                    public void dispatch(ChannelStateWriteRequest request) {
                        if (request.getSubtaskIndex() == subtaskIndex0) {
                            subtask0ReceivedCounter.incrementAndGet();
                        } else if (request.getSubtaskIndex() == subtaskIndex1) {
                            if (subtask1ReceivedCounter.incrementAndGet()
                                    == firstBatchSubtask1Count) {
                                firstBatchFuture.complete(null);
                            }
                        } else {
                            throw new IllegalStateException(
                                    String.format(
                                            "Unknown subtask index %s.",
                                            request.getSubtaskIndex()));
                        }
                        if (subtask0ReceivedCounter.get() == subtask0Count
                                && subtask1ReceivedCounter.get() == subtask1Count) {
                            allReceivedFuture.complete(null);
                        }
                    }
                };
        Object registerLock = new Object();
        ChannelStateWriteRequestExecutorImpl worker =
                new ChannelStateWriteRequestExecutorImpl(
                        throwingRequestProcessor, 5, e -> {}, registerLock);
        synchronized (registerLock) {
            worker.registerSubtask(JOB_VERTEX_ID, subtaskIndex0);
            worker.registerSubtask(JOB_VERTEX_ID, subtaskIndex1);
        }
        try {
            worker.start();
            // start the first batch
            for (ChannelStateWriteRequest request : firstBatchRequests) {
                worker.submit(request);
            }
            firstBatchFuture.get();
            assertThat(subtask0ReceivedCounter.get()).isOne();
            assertThat(subtask1ReceivedCounter.get()).isEqualTo(firstBatchSubtask1Count);

            // start the second batch
            for (ChannelStateWriteRequest request : secondBatchRequests) {
                worker.submit(request);
            }
            dataFuture.complete(Collections.emptyList());
            allReceivedFuture.get();
            assertThat(subtask0ReceivedCounter.get()).isEqualTo(subtask0Count);
            assertThat(subtask1ReceivedCounter.get()).isEqualTo(subtask1Count);
        } finally {
            worker.releaseSubtask(JOB_VERTEX_ID, subtaskIndex0);
            worker.releaseSubtask(JOB_VERTEX_ID, subtaskIndex1);
        }
    }

    @Test
    void testRecordsException() throws IOException {
        TestException testException = new TestException();
        TestRequestDispatcher throwingRequestProcessor =
                new TestRequestDispatcher() {
                    @Override
                    public void dispatch(ChannelStateWriteRequest request) {
                        throw testException;
                    }
                };
        Deque<ChannelStateWriteRequest> deque =
                new ArrayDeque<>(
                        Collections.singletonList(
                                new TestWriteRequest(JOB_VERTEX_ID, SUBTASK_INDEX)));
        Object registerLock = new Object();
        ChannelStateWriteRequestExecutorImpl worker =
                new ChannelStateWriteRequestExecutorImpl(
                        throwingRequestProcessor, deque, 5, registerLock, e -> {});
        synchronized (registerLock) {
            worker.registerSubtask(JOB_VERTEX_ID, SUBTASK_INDEX);
        }
        worker.run();
        try {
            worker.releaseSubtask(JOB_VERTEX_ID, SUBTASK_INDEX);
        } catch (IOException e) {
            if (findThrowable(e, TestException.class)
                    .filter(found -> found == testException)
                    .isPresent()) {
                return;
            } else {
                throw e;
            }
        }
        fail("exception not thrown");
    }

    @Test
    void testSubmitRequestOfUnregisteredSubtask() throws Exception {
        Object registerLock = new Object();
        ChannelStateWriteRequestExecutorImpl worker =
                new ChannelStateWriteRequestExecutorImpl(NO_OP, 5, e -> {}, registerLock);
        synchronized (registerLock) {
            worker.registerSubtask(JOB_VERTEX_ID, SUBTASK_INDEX);
        }
        worker.start();
        worker.submit(new TestWriteRequest(JOB_VERTEX_ID, SUBTASK_INDEX));

        assertThatThrownBy(
                        () -> worker.submit(new TestWriteRequest(new JobVertexID(), SUBTASK_INDEX)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("is not yet registered.");

        assertThatThrownBy(
                        () ->
                                worker.submitPriority(
                                        new TestWriteRequest(new JobVertexID(), SUBTASK_INDEX)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("is not yet registered.");
    }

    @Test
    void testSubmitPriorityUnreadyRequest() throws Exception {
        Object registerLock = new Object();
        ChannelStateWriteRequestExecutorImpl worker =
                new ChannelStateWriteRequestExecutorImpl(NO_OP, 5, e -> {}, registerLock);
        synchronized (registerLock) {
            worker.registerSubtask(JOB_VERTEX_ID, SUBTASK_INDEX);
        }
        worker.start();
        worker.submitPriority(new TestWriteRequest(JOB_VERTEX_ID, SUBTASK_INDEX));

        assertThatThrownBy(
                        () ->
                                worker.submitPriority(
                                        new TestWriteRequest(
                                                JOB_VERTEX_ID,
                                                SUBTASK_INDEX,
                                                new CompletableFuture<>())))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("The priority request must be ready.");
    }

    @Test
    void testRegisterSubtaskAfterRegisterCompleted() throws Exception {
        int maxSubtasksPerChannelStateFile = 5;
        Object registerLock = new Object();
        ChannelStateWriteRequestExecutorImpl worker =
                new ChannelStateWriteRequestExecutorImpl(
                        NO_OP, maxSubtasksPerChannelStateFile, e -> {}, registerLock);
        synchronized (registerLock) {
            for (int i = 0; i < maxSubtasksPerChannelStateFile; i++) {
                assertThat(worker.isRegistering()).isTrue();
                worker.registerSubtask(new JobVertexID(), SUBTASK_INDEX);
            }
        }
        assertThat(worker.isRegistering()).isFalse();
        assertThatThrownBy(
                        () -> {
                            synchronized (registerLock) {
                                worker.registerSubtask(new JobVertexID(), SUBTASK_INDEX);
                            }
                        })
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("This executor has been registered.");
    }

    @Test
    void testSubmitStartRequestBeforeRegisterCompleted() throws Exception {
        CompletableFuture<Void> dispatcherFuture = new CompletableFuture<>();
        TestRequestDispatcher dispatcher =
                new TestRequestDispatcher() {
                    @Override
                    public void dispatch(ChannelStateWriteRequest request) {
                        if (request instanceof CheckpointStartRequest) {
                            dispatcherFuture.complete(null);
                        }
                    }
                };
        int maxSubtasksPerChannelStateFile = 5;
        CompletableFuture<ChannelStateWriteRequestExecutor> workerFuture =
                new CompletableFuture<>();
        Object registerLock = new Object();
        ChannelStateWriteRequestExecutorImpl worker =
                new ChannelStateWriteRequestExecutorImpl(
                        dispatcher,
                        maxSubtasksPerChannelStateFile,
                        workerFuture::complete,
                        registerLock);
        worker.start();
        synchronized (registerLock) {
            worker.registerSubtask(JOB_VERTEX_ID, SUBTASK_INDEX);
        }
        assertThat(worker.isRegistering()).isTrue();

        worker.submit(
                ChannelStateWriteRequest.start(
                        JOB_VERTEX_ID,
                        SUBTASK_INDEX,
                        1,
                        new ChannelStateWriter.ChannelStateWriteResult(),
                        CheckpointStorageLocationReference.getDefault()));
        dispatcherFuture.get();
        assertThat(worker.isRegistering()).isFalse();
        assertThat(workerFuture).isCompletedWithValue(worker);
        assertThatThrownBy(
                        () -> {
                            synchronized (registerLock) {
                                worker.registerSubtask(new JobVertexID(), SUBTASK_INDEX);
                            }
                        })
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("This executor has been registered.");
    }

    @Test
    void testReleaseSubtaskBeforeRegisterCompleted() throws Exception {
        int maxSubtasksPerChannelStateFile = 5;
        CompletableFuture<ChannelStateWriteRequestExecutor> workerFuture =
                new CompletableFuture<>();
        Object registerLock = new Object();
        ChannelStateWriteRequestExecutorImpl worker =
                new ChannelStateWriteRequestExecutorImpl(
                        new TestRequestDispatcher(),
                        maxSubtasksPerChannelStateFile,
                        workerFuture::complete,
                        registerLock);
        worker.start();
        synchronized (registerLock) {
            worker.registerSubtask(JOB_VERTEX_ID, SUBTASK_INDEX);
        }
        assertThat(worker.isRegistering()).isTrue();

        worker.releaseSubtask(JOB_VERTEX_ID, SUBTASK_INDEX);
        assertThat(worker.isRegistering()).isFalse();
        assertThat(workerFuture).isCompletedWithValue(worker);
        assertThatThrownBy(
                        () -> {
                            synchronized (registerLock) {
                                worker.registerSubtask(new JobVertexID(), SUBTASK_INDEX);
                            }
                        })
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("This executor has been registered.");
    }

    private static class TestWriteRequest extends ChannelStateWriteRequest {
        private boolean cancelled = false;

        @Nullable private final CompletableFuture<?> readyFuture;

        public TestWriteRequest(JobVertexID jobVertexID, int subtaskIndex) {
            this(jobVertexID, subtaskIndex, null);
        }

        public TestWriteRequest(
                JobVertexID jobVertexID,
                int subtaskIndex,
                @Nullable CompletableFuture<?> readyFuture) {
            super(jobVertexID, subtaskIndex, 0, "Test");
            this.readyFuture = readyFuture;
        }

        @Override
        public void cancel(Throwable cause) {
            cancelled = true;
        }

        public boolean isCancelled() {
            return cancelled;
        }

        @Override
        public CompletableFuture<?> getReadyFuture() {
            if (readyFuture != null) {
                return readyFuture;
            }
            return super.getReadyFuture();
        }
    }

    private static class WorkerClosingDeque extends ArrayDeque<ChannelStateWriteRequest> {
        private ChannelStateWriteRequestExecutor worker;

        @Override
        public boolean add(@Nonnull ChannelStateWriteRequest request) {
            boolean add = super.add(request);
            try {
                worker.releaseSubtask(JOB_VERTEX_ID, SUBTASK_INDEX);
            } catch (IOException e) {
                ExceptionUtils.rethrow(e);
            }
            return add;
        }

        @Override
        public void addFirst(@Nonnull ChannelStateWriteRequest request) {
            super.addFirst(request);
            try {
                worker.releaseSubtask(JOB_VERTEX_ID, SUBTASK_INDEX);
            } catch (IOException e) {
                ExceptionUtils.rethrow(e);
            }
        }

        public void setWorker(ChannelStateWriteRequestExecutor worker) {
            this.worker = worker;
        }
    }

    private static class TestRequestDispatcher implements ChannelStateWriteRequestDispatcher {
        private boolean isStopped;

        @Override
        public void dispatch(ChannelStateWriteRequest request) {}

        @Override
        public void fail(Throwable cause) {
            isStopped = true;
        }

        public boolean isStopped() {
            return isStopped;
        }
    }
}
