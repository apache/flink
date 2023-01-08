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

import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter.ChannelStateWriteResult;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.util.function.BiConsumerWithException;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import static org.apache.flink.runtime.state.ChannelPersistenceITCase.getStreamFactoryFactory;
import static org.apache.flink.util.CloseableIterator.ofElements;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** {@link ChannelStateWriterImpl} lifecycle tests. */
class ChannelStateWriterImplTest {
    private static final long CHECKPOINT_ID = 42L;
    private static final String TASK_NAME = "test";

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
    void testBuffersRecycledOnError() throws IOException {
        NetworkBuffer buffer = getBuffer();
        try (ChannelStateWriterImpl writer =
                new ChannelStateWriterImpl(
                        TASK_NAME, new ConcurrentHashMap<>(), failingWorker(), 5)) {
            writer.open();
            assertThatThrownBy(() -> callAddInputData(writer, buffer))
                    .isInstanceOf(RuntimeException.class)
                    .hasCauseInstanceOf(TestException.class);
            assertThat(buffer.isRecycled()).isTrue();
        }
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
        SyncChannelStateWriteRequestExecutor worker = new SyncChannelStateWriteRequestExecutor();
        ChannelStateWriterImpl writer =
                new ChannelStateWriterImpl(TASK_NAME, new ConcurrentHashMap<>(), worker, 5);
        writer.open();
        worker.setThrown(new TestException());
        assertThatThrownBy(() -> callStart(writer)).hasCauseInstanceOf(TestException.class);
    }

    @Test
    void testLimit() throws IOException {
        int maxCheckpoints = 3;
        try (ChannelStateWriterImpl writer =
                new ChannelStateWriterImpl(
                        TASK_NAME, 0, getStreamFactoryFactory(), maxCheckpoints)) {
            writer.open();
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
    void testStartNotOpened() throws IOException {
        try (ChannelStateWriterImpl writer =
                new ChannelStateWriterImpl(TASK_NAME, 0, getStreamFactoryFactory())) {
            assertThatThrownBy(() -> callStart(writer))
                    .hasCauseInstanceOf(IllegalStateException.class);
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
            public void close() {}

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
        try (SyncChannelStateWriteRequestExecutor worker =
                        new SyncChannelStateWriteRequestExecutor();
                ChannelStateWriterImpl writer =
                        new ChannelStateWriterImpl(
                                TASK_NAME, new ConcurrentHashMap<>(), worker, 5)) {
            writer.open();
            testFn.accept(writer, worker);
        }
    }

    private ChannelStateWriterImpl openWriter() {
        ChannelStateWriterImpl writer =
                new ChannelStateWriterImpl(TASK_NAME, 0, getStreamFactoryFactory());
        writer.open();
        return writer;
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

    SyncChannelStateWriteRequestExecutor() {
        deque = new ArrayDeque<>();
        requestProcessor =
                new ChannelStateWriteRequestDispatcherImpl(
                        "dummy task",
                        0,
                        getStreamFactoryFactory(),
                        new ChannelStateSerializerImpl());
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
    public void close() {}

    void processAllRequests() throws Exception {
        while (!deque.isEmpty()) {
            requestProcessor.dispatch(deque.poll());
        }
    }

    public void setThrown(Exception thrown) {
        this.thrown = thrown;
    }
}
