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

package org.apache.flink.core.fs;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link ObjectStorageOutputStream}. */
@Timeout(value = 10, unit = TimeUnit.MINUTES)
class ObjectStorageOutputStreamTest {

    private static final String FILE_PATH = "test/output.txt";

    private ObjectStorageOutputStream stream;

    @AfterEach
    void tearDown() throws Exception {
        if (stream != null) {
            try {
                stream.close();
            } catch (final Exception ignored) {
                // stream may already be closed or in a failed state
            }
        }
    }

    /** Test double that records writes and captures the written content. */
    private static final class RecordingOutputStream extends OutputStream {
        private final ByteArrayOutputStream delegate = new ByteArrayOutputStream();
        private final List<String> operations = new ArrayList<>();
        private boolean flushed;
        private boolean closed;

        @Override
        public void write(final int b) throws IOException {
            delegate.write(b);
        }

        @Override
        public void write(final byte[] b, final int off, final int len) throws IOException {
            delegate.write(b, off, len);
        }

        @Override
        public void flush() throws IOException {
            flushed = true;
            operations.add("flush");
            delegate.flush();
        }

        @Override
        public void close() throws IOException {
            closed = true;
            operations.add("close");
            delegate.close();
        }

        byte[] getWrittenData() {
            return delegate.toByteArray();
        }
    }

    private ObjectStorageOutputStream createStream() {
        return createStream(new ByteArrayOutputStream());
    }

    private ObjectStorageOutputStream createStream(final OutputStream outputStream) {
        stream = new ObjectStorageOutputStream(outputStream, FILE_PATH);
        return stream;
    }

    // --- Constructor validation ---

    @Test
    void shouldThrowOnNullDelegate() {
        assertThatThrownBy(() -> new ObjectStorageOutputStream(null, FILE_PATH))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("delegate");
    }

    @Test
    void shouldThrowOnNullFilePath() {
        assertThatThrownBy(() -> new ObjectStorageOutputStream(new ByteArrayOutputStream(), null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("filePath");
    }

    // --- Write operations ---

    @Test
    void shouldWriteSingleByte() throws Exception {
        createStream();
        stream.write(42);
        assertThat(stream.getPos()).isEqualTo(1);
    }

    @Test
    void shouldWriteByteArray() throws Exception {
        final byte[] data = {1, 2, 3, 4, 5};
        createStream();
        stream.write(data, 0, data.length);
        assertThat(stream.getPos()).isEqualTo(5);
    }

    @Test
    void shouldWriteByteArrayWithOffset() throws Exception {
        final byte[] data = {1, 2, 3, 4, 5};
        createStream();
        stream.write(data, 2, 3);
        assertThat(stream.getPos()).isEqualTo(3);
    }

    @Test
    void shouldTrackPositionAcrossMultipleWrites() throws Exception {
        createStream();
        stream.write(1);
        stream.write(new byte[] {2, 3, 4}, 0, 3);
        assertThat(stream.getPos()).isEqualTo(4);
    }

    @Test
    void shouldDelegateSingleArgWriteToDelegateStream() throws Exception {
        // tests the inherited write(byte[]) -> write(byte[], 0, len) delegation
        createStream();
        final byte[] data = {1, 2, 3};
        stream.write(data);
        assertThat(stream.getPos()).isEqualTo(3);
    }

    @Test
    void shouldHandleZeroLengthWrite() throws Exception {
        createStream();
        stream.write(new byte[5], 0, 0);
        assertThat(stream.getPos()).isEqualTo(0);
    }

    @Test
    void shouldThrowOnNullArrayWrite() throws Exception {
        createStream();
        assertThatThrownBy(() -> stream.write(null, 0, 1))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("buffer");
    }

    @Test
    void shouldThrowOnBoundsViolation() throws Exception {
        createStream();
        final byte[] buf = new byte[5];
        assertThatThrownBy(() -> stream.write(buf, 3, 5))
                .isInstanceOf(IndexOutOfBoundsException.class);
    }

    @ParameterizedTest
    @MethodSource("closedWriteOperations")
    void shouldThrowOnWriteAfterClose(final WriteOperation operation) throws Exception {
        createStream();
        stream.close();
        assertThatThrownBy(() -> operation.execute(stream)).isInstanceOf(IOException.class);
    }

    @FunctionalInterface
    interface WriteOperation {
        void execute(ObjectStorageOutputStream stream) throws Exception;
    }

    private static Stream<Arguments> closedWriteOperations() {
        return Stream.of(
                Arguments.of((WriteOperation) s -> s.write(42)),
                Arguments.of((WriteOperation) s -> s.write(new byte[] {1}, 0, 1)),
                Arguments.of((WriteOperation) ObjectStorageOutputStream::flush),
                Arguments.of((WriteOperation) ObjectStorageOutputStream::getPos));
    }

    // --- Write content verification ---

    @Test
    void shouldDelegateWritesToDelegateStream() throws Exception {
        final RecordingOutputStream recordingOut = new RecordingOutputStream();
        createStream(recordingOut);

        final byte[] data = {1, 2, 3};
        stream.write(data, 0, data.length);
        stream.close();

        assertThat(recordingOut.getWrittenData()).isEqualTo(data);
    }

    @Test
    void shouldDelegateSingleByteWritesToDelegateStream() throws Exception {
        final RecordingOutputStream recordingOut = new RecordingOutputStream();
        createStream(recordingOut);

        stream.write(42);
        stream.write(99);
        stream.close();

        assertThat(recordingOut.getWrittenData()).isEqualTo(new byte[] {42, 99});
    }

    // --- Flush and sync ---

    @Test
    void shouldFlushWithoutError() throws Exception {
        createStream();
        stream.write(1);
        stream.flush();
    }

    @Test
    void shouldFlushDelegateToDelegateStream() throws Exception {
        final RecordingOutputStream recordingOut = new RecordingOutputStream();
        createStream(recordingOut);

        stream.write(1);
        stream.flush();

        assertThat(recordingOut.flushed).isTrue();
    }

    @Test
    void shouldSyncFlushAndCloseDelegateStream() throws Exception {
        final RecordingOutputStream recordingOut = new RecordingOutputStream();
        createStream(recordingOut);

        stream.write(1);
        stream.sync();

        assertThat(recordingOut.flushed).isTrue();
        assertThat(recordingOut.closed).isTrue();
        assertThat(recordingOut.operations).containsExactly("flush", "close");
    }

    @Test
    void shouldSyncBeIdempotent() throws Exception {
        final RecordingOutputStream recordingOut = new RecordingOutputStream();
        createStream(recordingOut);

        stream.write(1);
        stream.sync();
        stream.sync(); // second call is a no-op

        assertThat(recordingOut.operations).containsExactly("flush", "close");
    }

    @Test
    void shouldThrowOnWriteAfterSync() throws Exception {
        createStream();
        stream.write(1);
        stream.sync();

        assertThatThrownBy(() -> stream.write(42)).isInstanceOf(IOException.class);
        assertThatThrownBy(() -> stream.flush()).isInstanceOf(IOException.class);
        assertThatThrownBy(() -> stream.getPos()).isInstanceOf(IOException.class);
    }

    @Test
    void shouldCloseBeNoOpAfterSync() throws Exception {
        final RecordingOutputStream recordingOut = new RecordingOutputStream();
        createStream(recordingOut);

        stream.write(1);
        stream.sync();
        stream.close(); // should be no-op — already closed by sync

        assertThat(recordingOut.operations).containsExactly("flush", "close");
    }

    // --- Close ---

    @Test
    void shouldCloseDelegateStreamOnClose() throws Exception {
        final RecordingOutputStream recordingOut = new RecordingOutputStream();
        createStream(recordingOut);

        stream.write(1);
        stream.close();

        assertThat(recordingOut.closed).isTrue();
    }

    @Test
    void shouldFlushBeforeClosingDelegateStream() throws Exception {
        final RecordingOutputStream recordingOut = new RecordingOutputStream();
        createStream(recordingOut);

        stream.write(1);
        stream.close();

        assertThat(recordingOut.operations).containsExactly("flush", "close");
    }

    @Test
    void shouldAllowCloseFromDifferentThread() throws Exception {
        final RecordingOutputStream recordingOut = new RecordingOutputStream();
        createStream(recordingOut);
        stream.write(1);

        final Thread closer =
                new Thread(
                        () -> {
                            try {
                                stream.close();
                            } catch (final Exception e) {
                                throw new RuntimeException(e);
                            }
                        });
        closer.start();
        closer.join();
        assertThat(recordingOut.closed).isTrue();
    }

    @Test
    void shouldHandleDoubleCloseGracefully() throws Exception {
        final RecordingOutputStream recordingOut = new RecordingOutputStream();
        createStream(recordingOut);
        stream.write(1);
        stream.close();
        stream.close(); // should not throw or close the delegate again
        assertThat(recordingOut.closed).isTrue();
    }

    @Test
    void shouldCloseEmptyStreamWithoutError() throws Exception {
        final RecordingOutputStream recordingOut = new RecordingOutputStream();
        createStream(recordingOut);
        stream.close();
        assertThat(recordingOut.closed).isTrue();
        assertThat(recordingOut.getWrittenData()).isEmpty();
    }

    @Test
    void shouldCloseDelegateStreamEvenIfFlushThrows() throws Exception {
        final IOException flushFailure = new IOException("flush failed");
        final boolean[] closeCalled = {false};
        final OutputStream failingStream =
                new OutputStream() {
                    @Override
                    public void write(final int b) {}

                    @Override
                    public void flush() throws IOException {
                        throw flushFailure;
                    }

                    @Override
                    public void close() {
                        closeCalled[0] = true;
                    }
                };

        createStream(failingStream);
        assertThatThrownBy(stream::close).isInstanceOf(IOException.class).isSameAs(flushFailure);
        assertThat(closeCalled[0]).isTrue();
    }

    @Test
    void shouldPropagateCloseException() throws Exception {
        final IOException closeFailure = new IOException("close failed");
        final OutputStream failingStream =
                new OutputStream() {
                    @Override
                    public void write(final int b) {}

                    @Override
                    public void close() throws IOException {
                        throw closeFailure;
                    }
                };

        createStream(failingStream);
        assertThatThrownBy(stream::close).isInstanceOf(IOException.class).isSameAs(closeFailure);
    }

    @Test
    void shouldChainCloseExceptionAsSuppressedWhenFlushAlsoFails() throws Exception {
        final IOException flushFailure = new IOException("flush failed");
        final IOException closeFailure = new IOException("close failed");
        final OutputStream failingStream =
                new OutputStream() {
                    @Override
                    public void write(final int b) {}

                    @Override
                    public void flush() throws IOException {
                        throw flushFailure;
                    }

                    @Override
                    public void close() throws IOException {
                        throw closeFailure;
                    }
                };

        createStream(failingStream);
        assertThatThrownBy(stream::close)
                .isInstanceOf(IOException.class)
                .isSameAs(flushFailure)
                .satisfies(
                        ex ->
                                assertThat(ex.getSuppressed())
                                        .hasSize(1)
                                        .allSatisfy(s -> assertThat(s).isSameAs(closeFailure)));
    }

    // --- Thread-safety ---

    /**
     * Verifies that {@code close()} blocks until a concurrent write completes.
     *
     * <p>A blocking write holds the lock; close() must wait until the write releases it.
     */
    @Test
    void shouldBlockConcurrentWriteDuringClose() throws Exception {
        final CountDownLatch writeStarted = new CountDownLatch(1);
        final CountDownLatch writePermit = new CountDownLatch(1);
        final RecordingOutputStream recordingOut = new RecordingOutputStream();
        final OutputStream blockingDelegate =
                new OutputStream() {
                    @Override
                    public void write(final int b) throws IOException {
                        writeStarted.countDown();
                        try {
                            writePermit.await();
                        } catch (final InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new IOException(e);
                        }
                        recordingOut.write(b);
                    }

                    @Override
                    public void flush() throws IOException {
                        recordingOut.flush();
                    }

                    @Override
                    public void close() throws IOException {
                        recordingOut.close();
                    }
                };

        createStream(blockingDelegate);

        final AtomicReference<Throwable> writeError = new AtomicReference<>();
        final Thread writer =
                new Thread(
                        () -> {
                            try {
                                stream.write(1);
                            } catch (final Throwable t) {
                                writeError.set(t);
                            }
                        });
        writer.start();

        // Wait until the write has the lock and is blocked inside the delegate.
        writeStarted.await();

        // close() must wait for the write to finish.
        final CountDownLatch closeEntered = new CountDownLatch(1);
        final AtomicReference<Throwable> closeError = new AtomicReference<>();
        final Thread closer =
                new Thread(
                        () -> {
                            closeEntered.countDown();
                            try {
                                stream.close();
                            } catch (final Throwable t) {
                                closeError.set(t);
                            }
                        });
        closer.start();
        closeEntered.await();

        // Writer is still blocked inside the delegate — nothing written yet.
        assertThat(recordingOut.getWrittenData()).isEmpty();

        // Allow the write to complete, then close() should proceed.
        writePermit.countDown();

        writer.join();
        closer.join();

        assertThat(writeError.get()).isNull();
        assertThat(closeError.get()).isNull();
        assertThat(recordingOut.closed).isTrue();
        assertThat(recordingOut.getWrittenData()).isEqualTo(new byte[] {1});
    }

    /**
     * Verifies that setting the interrupt flag before calling {@link
     * ObjectStorageOutputStream#write(int)} causes an {@link IOException} wrapping {@link
     * InterruptedException}.
     */
    @Test
    void shouldInterruptWriteOnInterrupt() throws Exception {
        createStream();

        Thread.currentThread().interrupt();
        try {
            assertThatThrownBy(() -> stream.write(1))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("Interrupted")
                    .hasCauseInstanceOf(InterruptedException.class);
        } finally {
            // Clear interrupt flag to avoid contaminating subsequent tests.
            Thread.interrupted();
        }
    }
}
