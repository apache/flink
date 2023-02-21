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
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter.ChannelStateWriteResult;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.state.storage.JobManagerCheckpointStorage;
import org.apache.flink.util.function.BiConsumerWithException;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import static org.apache.flink.util.CloseableIterator.ofElements;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** {@link ChannelStateWriterImpl} lifecycle tests. */
class ChannelStateWriterImplTest {
    private static final long CHECKPOINT_ID = 42L;
    private static final String TASK_NAME = "test";
    private static final JobID JOB_ID = new JobID();
    private static final JobVertexID JOB_VERTEX_ID = new JobVertexID();
    private static final int SUBTASK_INDEX = 0;

    @Test
    void testAddEventBuffer() throws Exception {

        NetworkBuffer dataBuf = getBuffer();
        NetworkBuffer eventBuf = getBuffer();
        eventBuf.setDataType(Buffer.DataType.EVENT_BUFFER);

        executeCallbackWithSyncWorker(
                (writer, worker) -> {
                    callStart(writer);
                    callAddInputData(writer, eventBuf, dataBuf);
                    assertThatThrownBy(worker::processAllRequests)
                            .isInstanceOf(IllegalArgumentException.class);
                });
        assertThat(dataBuf.isRecycled()).isTrue();
    }

    @Test
    void testResultCompletion() throws IOException {
        ChannelStateWriteResult result;
        try (ChannelStateWriterImpl writer = openWriter()) {
            callStart(writer);
            result = writer.getAndRemoveWriteResult(CHECKPOINT_ID);
            assertThat(result.resultSubpartitionStateHandles).isNotDone();
            assertThat(result.inputChannelStateHandles).isNotDone();
        }
        assertThat(result.inputChannelStateHandles).isDone();
        assertThat(result.resultSubpartitionStateHandles).isDone();
    }

    @Test
    void testAbort() throws Exception {
        NetworkBuffer buffer = getBuffer();
        executeCallbackWithSyncWorker(
                (writer, worker) -> {
                    callStart(writer);
                    ChannelStateWriteResult result = writer.getAndRemoveWriteResult(CHECKPOINT_ID);
                    callAddInputData(writer, buffer);
                    callAbort(writer);
                    worker.processAllRequests();
                    assertThat(result.isDone()).isTrue();
                    assertThat(buffer.isRecycled()).isTrue();
                });
    }

    @Test
    void testAbortClearsResults() throws Exception {
        executeCallbackWithSyncWorker(
                (writer, worker) -> {
                    callStart(writer);
                    writer.abort(CHECKPOINT_ID, new TestException(), true);

                    assertThatThrownBy(() -> writer.getAndRemoveWriteResult(CHECKPOINT_ID))
                            .isInstanceOf(IllegalArgumentException.class);
                });
    }

    @Test
    void testAbortDoesNotClearsResults() throws Exception {
        executeCallbackWithSyncWorker(
                (writer, worker) -> {
                    callStart(writer);
                    callAbort(writer);
                    worker.processAllRequests();
                    writer.getAndRemoveWriteResult(CHECKPOINT_ID);
                });
    }

    @Test
    void testAbortIgnoresMissing() throws Exception {
        executeCallbackAndProcessWithSyncWorker(this::callAbort);
    }

    @Test
    void testAbortOldAndStartNewCheckpoint() throws Exception {
        executeCallbackWithSyncWorker(
                (writer, worker) -> {
                    int checkpoint42 = 42;
                    int checkpoint43 = 43;
                    writer.start(
                            checkpoint42, CheckpointOptions.forCheckpointWithDefaultLocation());
                    writer.abort(checkpoint42, new TestException(), false);
                    writer.start(
                            checkpoint43, CheckpointOptions.forCheckpointWithDefaultLocation());
                    worker.processAllRequests();

                    ChannelStateWriteResult result42 = writer.getAndRemoveWriteResult(checkpoint42);
                    assertThat(result42.isDone()).isTrue();
                    assertThatThrownBy(() -> result42.getInputChannelStateHandles().get())
                            .as("The result should have failed.")
                            .hasCauseInstanceOf(TestException.class);

                    ChannelStateWriteResult result43 = writer.getAndRemoveWriteResult(checkpoint43);
                    assertThat(result43.isDone()).isFalse();
                });
    }

    @Test
    void testBuffersRecycledOnError() {
        NetworkBuffer buffer = getBuffer();
        ChannelStateWriterImpl writer =
                new ChannelStateWriterImpl(
                        JOB_VERTEX_ID,
                        TASK_NAME,
                        SUBTASK_INDEX,
                        new ConcurrentHashMap<>(),
                        failingWorker(),
                        5);
        assertThatThrownBy(() -> callAddInputData(writer, buffer))
                .isInstanceOf(RuntimeException.class)
                .hasCauseInstanceOf(TestException.class);
        assertThat(buffer.isRecycled()).isTrue();
    }

    @Test
    void testBuffersRecycledOnClose() throws Exception {
        NetworkBuffer buffer = getBuffer();
        executeCallbackAndProcessWithSyncWorker(
                writer -> {
                    callStart(writer);
                    callAddInputData(writer, buffer);
                    assertThat(buffer.isRecycled()).isFalse();
                });
        assertThat(buffer.isRecycled()).isTrue();
    }

    @Test
    void testNoAddDataAfterFinished() throws Exception {
        executeCallbackWithSyncWorker(
                (writer, worker) -> {
                    callStart(writer);
                    callFinish(writer);
                    worker.processAllRequests();

                    callAddInputData(writer);
                    assertThatThrownBy(worker::processAllRequests)
                            .isInstanceOf(IllegalArgumentException.class);
                });
    }

    @Test
    void testAddDataNotStarted() {
        assertThatThrownBy(() -> executeCallbackAndProcessWithSyncWorker(this::callAddInputData))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testFinishNotStarted() {
        assertThatThrownBy(() -> executeCallbackAndProcessWithSyncWorker(this::callFinish))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testRethrowOnClose() {
        assertThatThrownBy(
                        () ->
                                executeCallbackAndProcessWithSyncWorker(
                                        writer -> {
                                            try {
                                                callFinish(writer);
                                            } catch (IllegalArgumentException e) {
                                                // ignore here - should rethrow in
                                                // close
                                            }
                                        }))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testRethrowOnNextCall() {
        SyncChannelStateWriteRequestExecutor worker =
                new SyncChannelStateWriteRequestExecutor(JOB_ID);
        ChannelStateWriterImpl writer =
                new ChannelStateWriterImpl(
                        JOB_VERTEX_ID,
                        TASK_NAME,
                        SUBTASK_INDEX,
                        new ConcurrentHashMap<>(),
                        worker,
                        5);
        worker.registerSubtask(JOB_VERTEX_ID, SUBTASK_INDEX);
        worker.setThrown(new TestException());
        assertThatThrownBy(() -> callStart(writer)).hasCauseInstanceOf(TestException.class);
    }

    @Test
    void testLimit() throws IOException {
        int maxCheckpoints = 3;
        try (ChannelStateWriterImpl writer =
                new ChannelStateWriterImpl(
                        JOB_VERTEX_ID,
                        TASK_NAME,
                        SUBTASK_INDEX,
                        new JobManagerCheckpointStorage(),
                        maxCheckpoints,
                        new ChannelStateWriteRequestExecutorFactory(JOB_ID),
                        5)) {
            for (int i = 0; i < maxCheckpoints; i++) {
                writer.start(i, CheckpointOptions.forCheckpointWithDefaultLocation());
            }
            assertThatThrownBy(
                            () ->
                                    writer.start(
                                            maxCheckpoints,
                                            CheckpointOptions.forCheckpointWithDefaultLocation()))
                    .isInstanceOf(IllegalStateException.class);
        }
    }

    @Test
    void testNoStartAfterClose() throws IOException {
        ChannelStateWriterImpl writer = openWriter();
        writer.close();
        assertThatThrownBy(
                        () ->
                                writer.start(
                                        42, CheckpointOptions.forCheckpointWithDefaultLocation()))
                .hasCauseInstanceOf(IllegalStateException.class);
    }

    @Test
    void testNoAddDataAfterClose() throws IOException {
        ChannelStateWriterImpl writer = openWriter();
        callStart(writer);
        writer.close();
        assertThatThrownBy(() -> callAddInputData(writer))
                .hasCauseInstanceOf(IllegalStateException.class);
    }

    private NetworkBuffer getBuffer() {
        return new NetworkBuffer(
                MemorySegmentFactory.allocateUnpooledSegment(123, null),
                FreeingBufferRecycler.INSTANCE);
    }

    private ChannelStateWriteRequestExecutor failingWorker() {
        return new ChannelStateWriteRequestExecutor() {

            @Override
            public void submit(ChannelStateWriteRequest e) {
                throw new TestException();
            }

            @Override
            public void submitPriority(ChannelStateWriteRequest e) {
                throw new TestException();
            }

            @Override
            public void start() throws IllegalStateException {}

            @Override
            public void registerSubtask(JobVertexID jobVertexID, int subtaskIndex) {}

            @Override
            public void releaseSubtask(JobVertexID jobVertexID, int subtaskIndex) {}
        };
    }

    private void executeCallbackAndProcessWithSyncWorker(
            Consumer<ChannelStateWriter> writerConsumer) throws Exception {
        executeCallbackWithSyncWorker(
                (channelStateWriter, syncChannelStateWriterWorker) -> {
                    writerConsumer.accept(channelStateWriter);
                    syncChannelStateWriterWorker.processAllRequests();
                });
    }

    private void executeCallbackWithSyncWorker(
            BiConsumerWithException<
                            ChannelStateWriter, SyncChannelStateWriteRequestExecutor, Exception>
                    testFn)
            throws Exception {
        SyncChannelStateWriteRequestExecutor worker =
                new SyncChannelStateWriteRequestExecutor(JOB_ID);
        try (ChannelStateWriterImpl writer =
                new ChannelStateWriterImpl(
                        JOB_VERTEX_ID,
                        TASK_NAME,
                        SUBTASK_INDEX,
                        new ConcurrentHashMap<>(),
                        worker,
                        5)) {
            worker.registerSubtask(JOB_VERTEX_ID, SUBTASK_INDEX);
            testFn.accept(writer, worker);
        } finally {
            worker.releaseSubtask(JOB_VERTEX_ID, SUBTASK_INDEX);
        }
    }

    private ChannelStateWriterImpl openWriter() {
        return new ChannelStateWriterImpl(
                JOB_VERTEX_ID,
                TASK_NAME,
                SUBTASK_INDEX,
                new JobManagerCheckpointStorage(),
                new ChannelStateWriteRequestExecutorFactory(JOB_ID),
                5);
    }

    private void callStart(ChannelStateWriter writer) {
        writer.start(CHECKPOINT_ID, CheckpointOptions.forCheckpointWithDefaultLocation());
    }

    private void callAddInputData(ChannelStateWriter writer, NetworkBuffer... buffer) {
        writer.addInputData(
                CHECKPOINT_ID,
                new InputChannelInfo(1, 1),
                1,
                ofElements(Buffer::recycleBuffer, buffer));
    }

    private void callAbort(ChannelStateWriter writer) {
        writer.abort(CHECKPOINT_ID, new TestException(), false);
    }

    private void callFinish(ChannelStateWriter writer) {
        writer.finishInput(CHECKPOINT_ID);
        writer.finishOutput(CHECKPOINT_ID);
    }
}

class TestException extends RuntimeException {}

class SyncChannelStateWriteRequestExecutor implements ChannelStateWriteRequestExecutor {
    private final ChannelStateWriteRequestDispatcher requestProcessor;
    private final Deque<ChannelStateWriteRequest> deque;
    private Exception thrown;

    SyncChannelStateWriteRequestExecutor(JobID jobID) {
        deque = new ArrayDeque<>();
        requestProcessor =
                new ChannelStateWriteRequestDispatcherImpl(
                        new JobManagerCheckpointStorage(), jobID, new ChannelStateSerializerImpl());
    }

    @Override
    public void submit(ChannelStateWriteRequest e) throws Exception {
        deque.offer(e);
        if (thrown != null) {
            throw thrown;
        }
    }

    @Override
    public void submitPriority(ChannelStateWriteRequest e) throws Exception {
        deque.offerFirst(e);
        if (thrown != null) {
            throw thrown;
        }
    }

    @Override
    public void start() throws IllegalStateException {}

    @Override
    public void registerSubtask(JobVertexID jobVertexID, int subtaskIndex) {
        deque.add(ChannelStateWriteRequest.registerSubtask(jobVertexID, subtaskIndex));
    }

    @Override
    public void releaseSubtask(JobVertexID jobVertexID, int subtaskIndex) {
        deque.add(ChannelStateWriteRequest.releaseSubtask(jobVertexID, subtaskIndex));
    }

    void processAllRequests() throws Exception {
        while (!deque.isEmpty()) {
            requestProcessor.dispatch(deque.poll());
        }
    }

    public void setThrown(Exception thrown) {
        this.thrown = thrown;
    }
}
