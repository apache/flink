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

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link ObjectStorageInputStream}. */
@Timeout(value = 10, unit = TimeUnit.MINUTES)
class ObjectStorageInputStreamTest {

    private static final int BUFFER_SIZE = 1024;
    private static final long SKIP_THRESHOLD = 4 * 1024 * 1024;

    /** Poll interval for {@link #awaitBlocked}. */
    private static final long STATE_POLL_INTERVAL_MS = 100;

    private ObjectStorageInputStream stream;

    @AfterEach
    void tearDown() throws Exception {
        if (stream != null) {
            stream.close();
        }
    }

    /**
     * Creates an opener that returns a {@link ByteArrayInputStream} starting at the requested
     * position.
     */
    private static InputStreamOpener testOpener(final byte[] data) {
        return ctx -> {
            final int pos = (int) Math.min(ctx.getPos(), data.length);
            return new ByteArrayInputStream(data, pos, data.length - pos);
        };
    }

    private static InputStreamExtension buffering(final byte[] data) {
        return InputStreamExtension.buffering(testOpener(data), BUFFER_SIZE);
    }

    private static InputStreamExtension buffering(final InputStreamOpener opener) {
        return InputStreamExtension.buffering(opener, BUFFER_SIZE);
    }

    private ObjectStorageInputStream createStream(final byte[] data) {
        stream = new ObjectStorageInputStream(data.length, SKIP_THRESHOLD, buffering(data));
        return stream;
    }

    /** InputStream that tracks how many times {@link #close()} is called. */
    private static final class TrackingInputStream extends ByteArrayInputStream {
        private int closeCount;

        TrackingInputStream(final byte[] data) {
            super(data);
        }

        @Override
        public void close() throws IOException {
            closeCount++;
            super.close();
        }
    }

    /** Opener that tracks how many times it has been called. */
    private static final class TrackingOpener implements InputStreamOpener {
        private final byte[] data;
        int openCount;

        TrackingOpener(final byte[] data) {
            this.data = data;
        }

        @Override
        public InputStream open(final ReadContext ctx) {
            openCount++;
            final int pos = (int) Math.min(ctx.getPos(), data.length);
            return new ByteArrayInputStream(data, pos, data.length - pos);
        }
    }

    /** Extension for tests that opens streams via the provided opener. */
    private static final class TestingExtension implements InputStreamExtension {
        private final InputStreamOpener opener;

        TestingExtension(final InputStreamOpener opener) {
            this.opener = opener;
        }

        @Override
        public RawAndWrappedInputStreams openStream(final StreamContext ctx) throws IOException {
            final InputStream raw = opener.open(ReadContext.of(ctx.getPos()));
            return new RawAndWrappedInputStreams(raw, new BufferedInputStream(raw, BUFFER_SIZE));
        }
    }

    // --- Constructor validation ---

    @Test
    void shouldThrowOnNullExtension() {
        assertThatThrownBy(() -> new ObjectStorageInputStream(100L, SKIP_THRESHOLD, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("extension");
    }

    @Test
    void shouldThrowOnNegativeContentLength() {
        assertThatThrownBy(
                        () ->
                                new ObjectStorageInputStream(
                                        -1L, SKIP_THRESHOLD, buffering(new byte[0])))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("non-negative");
    }

    @Test
    void shouldThrowOnNegativeSkipThreshold() {
        assertThatThrownBy(() -> new ObjectStorageInputStream(100L, -1L, buffering(new byte[0])))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("non-negative");
    }

    @Test
    void shouldAcceptZeroSkipThresholdForEmptyFile() throws Exception {
        stream = new ObjectStorageInputStream(0L, 0L, buffering(new byte[0]));
        assertThat(stream.available()).isEqualTo(0);
        assertThat(stream.read()).isEqualTo(-1);
    }

    // --- Lazy initialization ---

    @Test
    void shouldNotOpenStreamOnConstruction() {
        final TrackingOpener opener = new TrackingOpener(new byte[0]);
        stream = new ObjectStorageInputStream(100L, SKIP_THRESHOLD, buffering(opener));
        assertThat(opener.openCount).isEqualTo(0);
    }

    @Test
    void shouldOpenStreamOnFirstRead() throws Exception {
        final byte[] data = {42};
        final TrackingOpener opener = new TrackingOpener(data);

        stream = new ObjectStorageInputStream(data.length, SKIP_THRESHOLD, buffering(opener));
        assertThat(opener.openCount).isEqualTo(0);
        stream.read();
        assertThat(opener.openCount).isEqualTo(1);
    }

    // --- Single byte read ---

    @Test
    void shouldReadSingleByte() throws Exception {
        createStream(new byte[] {42});
        assertThat(stream.read()).isEqualTo(42);
    }

    @ParameterizedTest
    @MethodSource("eofScenarios")
    void shouldReturnMinusOneAtEof(final byte[] data) throws Exception {
        createStream(data);
        for (int i = 0; i < data.length; i++) {
            stream.read();
        }
        assertThat(stream.read()).isEqualTo(-1);
    }

    private static Stream<Arguments> eofScenarios() {
        return Stream.of(Arguments.of((Object) new byte[] {1}), Arguments.of((Object) new byte[0]));
    }

    // --- Byte array read ---

    @Test
    void shouldReadByteArray() throws Exception {
        final byte[] data = {1, 2, 3, 4, 5};
        createStream(data);
        final byte[] buf = new byte[5];
        final int read = stream.read(buf, 0, 5);
        assertThat(read).isEqualTo(5);
        assertThat(buf).isEqualTo(data);
    }

    @Test
    void shouldReturnMinusOneForByteArrayReadAtEof() throws Exception {
        final byte[] data = {1};
        createStream(data);
        final byte[] buf = new byte[1];
        stream.read(buf, 0, 1);
        assertThat(stream.read(buf, 0, 1)).isEqualTo(-1);
    }

    @Test
    void shouldReturnZeroForZeroLengthRead() throws Exception {
        createStream(new byte[] {1, 2, 3});
        final byte[] buf = new byte[3];
        assertThat(stream.read(buf, 0, 0)).isEqualTo(0);
    }

    @Test
    void shouldThrowOnNullArrayRead() throws Exception {
        createStream(new byte[0]);
        assertThatThrownBy(() -> stream.read(null, 0, 1))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("buffer");
    }

    @ParameterizedTest
    @MethodSource("boundsViolations")
    void shouldThrowOnBoundsViolation(final int bufLen, final int off, final int len)
            throws Exception {
        createStream(new byte[0]);
        final byte[] buf = new byte[bufLen];
        assertThatThrownBy(() -> stream.read(buf, off, len))
                .isInstanceOf(IndexOutOfBoundsException.class);
    }

    private static Stream<Arguments> boundsViolations() {
        return Stream.of(
                Arguments.of(5, -1, 1),
                Arguments.of(5, 0, -1),
                Arguments.of(5, 3, 5),
                Arguments.of(5, 6, 1));
    }

    // --- Position tracking ---

    @Test
    void shouldTrackPositionAfterSingleByteReads() throws Exception {
        createStream(new byte[] {1, 2, 3});
        assertThat(stream.getPos()).isEqualTo(0);
        stream.read();
        assertThat(stream.getPos()).isEqualTo(1);
        stream.read();
        assertThat(stream.getPos()).isEqualTo(2);
    }

    @Test
    void shouldTrackPositionAfterArrayRead() throws Exception {
        createStream(new byte[] {1, 2, 3, 4, 5});
        final byte[] buf = new byte[3];
        stream.read(buf, 0, 3);
        assertThat(stream.getPos()).isEqualTo(3);
    }

    // --- Seek ---

    @Test
    void shouldSeekForward() throws Exception {
        final byte[] data = {1, 2, 3, 4, 5};
        createStream(data);
        stream.read(); // initialize
        stream.seek(3);
        assertThat(stream.getPos()).isEqualTo(3);
        assertThat(stream.read()).isEqualTo(4);
    }

    @Test
    void shouldSeekBackward() throws Exception {
        final byte[] data = {1, 2, 3, 4, 5};
        createStream(data);
        stream.read();
        stream.read();
        stream.read();
        assertThat(stream.getPos()).isEqualTo(3);

        stream.seek(1);
        assertThat(stream.getPos()).isEqualTo(1);
        assertThat(stream.read()).isEqualTo(2);
    }

    @Test
    void shouldNotReopenStreamOnSeekToSamePosition() throws Exception {
        final byte[] data = {1, 2, 3};
        final TrackingOpener opener = new TrackingOpener(data);

        stream = new ObjectStorageInputStream(data.length, SKIP_THRESHOLD, buffering(opener));
        stream.read(); // initialize, position=1
        stream.seek(1); // same position — should not reopen
        assertThat(opener.openCount).isEqualTo(1);
    }

    @Test
    void shouldThrowOnNegativeSeek() throws Exception {
        createStream(new byte[0]);
        assertThatThrownBy(() -> stream.seek(-1)).isInstanceOf(IOException.class);
    }

    @Test
    void shouldThrowOnSeekAfterClose() throws Exception {
        createStream(new byte[0]);
        stream.close();
        assertThatThrownBy(() -> stream.seek(0)).isInstanceOf(IOException.class);
    }

    // --- Skip ---

    @Test
    void shouldSkipForward() throws Exception {
        createStream(new byte[] {1, 2, 3, 4, 5});
        final long skipped = stream.skip(3);
        assertThat(skipped).isEqualTo(3);
        assertThat(stream.getPos()).isEqualTo(3);
    }

    @Test
    void shouldCapSkipAtContentLength() throws Exception {
        createStream(new byte[] {1, 2, 3});
        final long skipped = stream.skip(100);
        assertThat(skipped).isEqualTo(3);
        assertThat(stream.getPos()).isEqualTo(3);
    }

    @Test
    void shouldReturnZeroForNonPositiveSkip() throws Exception {
        stream = new ObjectStorageInputStream(100L, SKIP_THRESHOLD, buffering(new byte[0]));
        assertThat(stream.skip(0)).isEqualTo(0);
        assertThat(stream.skip(-5)).isEqualTo(0);
    }

    @Test
    void shouldSkipAfterSeek() throws Exception {
        final byte[] data = {1, 2, 3, 4, 5};
        createStream(data);
        stream.seek(2);
        final long skipped = stream.skip(2);
        assertThat(skipped).isEqualTo(2);
        assertThat(stream.getPos()).isEqualTo(4);
        assertThat(stream.read()).isEqualTo(5);
    }

    // --- Seek optimization ---

    @Test
    void shouldUseBufferForSmallForwardSeek() throws Exception {
        final byte[] data = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
        final TrackingOpener opener = new TrackingOpener(data);

        stream = new ObjectStorageInputStream(data.length, SKIP_THRESHOLD, buffering(opener));
        stream.read(); // trigger initialization, openCount=1
        assertThat(opener.openCount).isEqualTo(1);

        stream.seek(3); // small forward delta (2 bytes), should NOT reopen
        assertThat(opener.openCount).isEqualTo(1);
        assertThat(stream.getPos()).isEqualTo(3);
        assertThat(stream.read()).isEqualTo(4);
    }

    @Test
    void shouldReopenForLargeForwardSeek() throws Exception {
        final int fileSize = (int) (SKIP_THRESHOLD + 1024);
        final byte[] data = new byte[fileSize];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) (i & 0xFF);
        }
        final TrackingOpener opener = new TrackingOpener(data);

        stream = new ObjectStorageInputStream(data.length, SKIP_THRESHOLD, buffering(opener));
        stream.read(); // trigger initialization, openCount=1
        assertThat(opener.openCount).isEqualTo(1);

        // delta > SKIP_THRESHOLD, closes stream
        stream.seek(SKIP_THRESHOLD + 512);
        assertThat(stream.getPos()).isEqualTo(SKIP_THRESHOLD + 512);
        stream.read(); // lazy reopen on read
        assertThat(opener.openCount).isEqualTo(2);
    }

    @Test
    void shouldReopenForBackwardSeek() throws Exception {
        final byte[] data = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
        final TrackingOpener opener = new TrackingOpener(data);

        stream = new ObjectStorageInputStream(data.length, SKIP_THRESHOLD, buffering(opener));
        stream.read();
        stream.read();
        stream.read(); // position=3, openCount=1

        stream.seek(1); // backward seek, closes stream
        assertThat(stream.read()).isEqualTo(2); // lazy reopen on read
        assertThat(opener.openCount).isEqualTo(2);
    }

    @Test
    void shouldThrowWhenReadAndDiscardEncountersTruncatedStream() throws Exception {
        final byte[] fullData = {1, 2, 3, 4, 5, 6, 7, 8};
        final int bufferSize = 8;
        // Opener returns only 2 bytes regardless of position
        final InputStreamOpener opener =
                ctx ->
                        new ByteArrayInputStream(
                                fullData, (int) ctx.getPos(), 2 - (int) ctx.getPos());

        stream =
                new ObjectStorageInputStream(
                        fullData.length,
                        SKIP_THRESHOLD,
                        InputStreamExtension.buffering(opener, bufferSize));
        stream.read(); // position=1, buffer has 1 byte from truncated stream

        // Seek forward within threshold triggers read-and-discard, which hits EOF prematurely
        assertThatThrownBy(() -> stream.seek(3))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Unexpected end of stream during read-and-discard seek");
    }

    // --- Close ---

    @Test
    void shouldCloseUnderlyingStreams() throws Exception {
        final byte[] data = {1, 2, 3};
        final TrackingInputStream tracking = new TrackingInputStream(data);
        final InputStreamOpener opener = ctx -> tracking;

        stream = new ObjectStorageInputStream(data.length, SKIP_THRESHOLD, buffering(opener));
        stream.read(); // trigger initialization
        stream.close();
        // BufferedInputStream.close() closes the underlying stream once.
        assertThat(tracking.closeCount).isEqualTo(1);
    }

    @Test
    void shouldCloseSuccessfullyWhenThreadIsInterrupted() throws Exception {
        createStream(new byte[] {1, 2, 3});
        stream.read();

        Thread.currentThread().interrupt();
        stream.close();

        assertThat(Thread.interrupted()).isTrue();
        assertThatThrownBy(stream::read).isInstanceOf(IOException.class);
    }

    @Test
    void shouldHandleDoubleCloseGracefully() throws Exception {
        stream = new ObjectStorageInputStream(100L, SKIP_THRESHOLD, buffering(new byte[0]));
        stream.close();
        stream.close(); // should not throw
    }

    @ParameterizedTest
    @MethodSource("closedStreamOperations")
    void shouldThrowOnOperationAfterClose(final StreamOperation operation) throws Exception {
        stream = new ObjectStorageInputStream(100L, SKIP_THRESHOLD, buffering(new byte[0]));
        stream.close();
        assertThatThrownBy(() -> operation.execute(stream)).isInstanceOf(IOException.class);
    }

    @FunctionalInterface
    interface StreamOperation {
        void execute(ObjectStorageInputStream stream) throws Exception;
    }

    private static Stream<Arguments> closedStreamOperations() {
        return Stream.of(
                Arguments.of((StreamOperation) ObjectStorageInputStream::read),
                Arguments.of((StreamOperation) s -> s.read(new byte[1], 0, 1)),
                Arguments.of((StreamOperation) s -> s.seek(0)),
                Arguments.of((StreamOperation) s -> s.skip(1)),
                Arguments.of((StreamOperation) ObjectStorageInputStream::available));
    }

    // --- Concurrency ---

    @Test
    void shouldInterruptLockAcquisitionAndThrowIOException() throws Exception {
        final CountDownLatch openerBlocked = new CountDownLatch(1);
        final CountDownLatch openerRelease = new CountDownLatch(1);
        final InputStreamOpener blockingOpener =
                blockingOpener(new byte[] {1, 2, 3}, openerBlocked, openerRelease);
        stream = new ObjectStorageInputStream(3L, SKIP_THRESHOLD, buffering(blockingOpener));

        // First thread: holds the lock while blocked inside the opener.
        final CompletableFuture<Void> holder =
                CompletableFuture.runAsync(
                        () -> {
                            try {
                                stream.read();
                            } catch (final IOException ignored) {
                            }
                        });
        openerBlocked.await();

        // Second thread: blocks on lock acquisition — we need the raw Thread for interrupt().
        final AtomicReference<Throwable> readerError = new AtomicReference<>();
        final Thread readerThread =
                new Thread(
                        () -> {
                            try {
                                stream.read();
                            } catch (final IOException e) {
                                readerError.set(e);
                            }
                        });
        readerThread.start();
        awaitBlocked(readerThread);

        readerThread.interrupt();
        readerThread.join();

        openerRelease.countDown();
        holder.join();

        assertThat(readerThread.isAlive()).isFalse();
        assertThat(readerError.get())
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Interrupted");
    }

    /**
     * Creates an opener that blocks on {@code blocked}/{@code release} latches on first call,
     * allowing tests to hold the stream's lock from a background thread.
     */
    private static InputStreamOpener blockingOpener(
            final byte[] data, final CountDownLatch blocked, final CountDownLatch release) {
        return ctx -> {
            blocked.countDown();
            try {
                release.await();
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted in blocking opener", e);
            }
            final int pos = (int) Math.min(ctx.getPos(), data.length);
            return new ByteArrayInputStream(data, pos, data.length - pos);
        };
    }

    private static void awaitBlocked(final Thread thread) throws InterruptedException {
        while (thread.getState() != Thread.State.WAITING) {
            Thread.sleep(STATE_POLL_INTERVAL_MS);
        }
    }

    // --- Available ---

    @Test
    void shouldReturnRemainingBytesFromAvailable() throws Exception {
        final byte[] data = {1, 2, 3, 4, 5};
        createStream(data);
        assertThat(stream.available()).isEqualTo(5);
        stream.read();
        assertThat(stream.available()).isEqualTo(4);
    }

    @Test
    void shouldCapAvailableAtIntegerMaxValue() throws Exception {
        stream =
                new ObjectStorageInputStream(
                        Long.MAX_VALUE, SKIP_THRESHOLD, buffering(new byte[0]));
        assertThat(stream.available()).isEqualTo(Integer.MAX_VALUE);
    }

    @Test
    void shouldReturnZeroAvailableAfterSeekToEof() throws Exception {
        final byte[] data = {1, 2, 3};
        createStream(data);
        stream.seek(data.length);
        assertThat(stream.available()).isEqualTo(0);
    }

    @Test
    void shouldThrowOnSeekBeyondContentLength() throws Exception {
        final byte[] data = {1, 2, 3};
        createStream(data);
        assertThatThrownBy(() -> stream.seek(data.length + 1))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("file size");
    }

    @Test
    void shouldThrowOnAvailableAfterClose() throws Exception {
        stream = new ObjectStorageInputStream(100L, SKIP_THRESHOLD, buffering(new byte[0]));
        stream.close();
        assertThatThrownBy(stream::available).isInstanceOf(IOException.class);
    }

    // --- Opener failure ---

    @Test
    void shouldPropagateOpenerIOException() {
        final InputStreamOpener failingOpener =
                ctx -> {
                    throw new IOException("connection refused");
                };
        stream = new ObjectStorageInputStream(100L, SKIP_THRESHOLD, buffering(failingOpener));
        assertThatThrownBy(stream::read).isInstanceOf(IOException.class);
    }

    @Test
    void shouldPropagateOpenerRuntimeException() {
        final RuntimeException expectedException = new RuntimeException("unexpected");
        final InputStreamOpener failingOpener =
                ctx -> {
                    throw expectedException;
                };
        stream = new ObjectStorageInputStream(100L, SKIP_THRESHOLD, buffering(failingOpener));
        assertThatThrownBy(stream::read)
                .isInstanceOf(RuntimeException.class)
                .isSameAs(expectedException);
    }

    // --- Edge cases ---

    @Test
    void shouldHandleZeroContentLength() throws Exception {
        createStream(new byte[0]);
        assertThat(stream.available()).isEqualTo(0);
        assertThat(stream.read()).isEqualTo(-1);
        final byte[] buf = new byte[1];
        assertThat(stream.read(buf, 0, 1)).isEqualTo(-1);
    }

    @Test
    void shouldNotOpenStreamWhenAlreadyAtEof() throws Exception {
        final TrackingOpener opener = new TrackingOpener(new byte[0]);
        stream = new ObjectStorageInputStream(0L, SKIP_THRESHOLD, buffering(opener));
        stream.read();
        assertThat(opener.openCount).isEqualTo(0);
    }

    // --- Truncated stream (contentLength exceeds actual data) ---

    @Test
    void shouldReturnMinusOneFromSingleByteReadWhenStreamTruncated() throws Exception {
        final byte[] actualData = {1, 2};
        final long reportedLength = 10;
        stream =
                new ObjectStorageInputStream(reportedLength, SKIP_THRESHOLD, buffering(actualData));
        assertThat(stream.read()).isEqualTo(1);
        assertThat(stream.read()).isEqualTo(2);
        assertThat(stream.read()).isEqualTo(-1);
        assertThat(stream.getPos()).isEqualTo(2);
    }

    @Test
    void shouldReturnMinusOneFromArrayReadWhenStreamTruncated() throws Exception {
        final byte[] actualData = {1, 2};
        final long reportedLength = 10;
        stream =
                new ObjectStorageInputStream(reportedLength, SKIP_THRESHOLD, buffering(actualData));
        final byte[] buf = new byte[5];
        final int firstRead = stream.read(buf, 0, 5);
        assertThat(firstRead).isEqualTo(2);
        assertThat(stream.read(buf, 0, 5)).isEqualTo(-1);
        assertThat(stream.getPos()).isEqualTo(2);
    }

    // --- closeCurrentStream error handling ---

    @Test
    void shouldPropagateCloseExceptionFromUnderlyingStream() throws Exception {
        final IOException closeFailure = new IOException("close failed");
        final InputStreamOpener opener =
                ctx ->
                        new ByteArrayInputStream(new byte[] {1, 2, 3}) {
                            @Override
                            public void close() throws IOException {
                                throw closeFailure;
                            }
                        };

        stream = new ObjectStorageInputStream(3L, SKIP_THRESHOLD, buffering(opener));
        stream.read(); // trigger lazy init

        assertThatThrownBy(stream::close).isInstanceOf(IOException.class).isSameAs(closeFailure);
    }

    // --- InputStreamExtension: openStream ---

    @Test
    void shouldCallExtensionOpenStreamOnInit() throws Exception {
        final byte[] data = {1, 2, 3};
        final TrackingOpener opener = new TrackingOpener(data);
        final InputStreamExtension extension = new TestingExtension(opener);

        stream = new ObjectStorageInputStream(data.length, SKIP_THRESHOLD, extension);
        assertThat(opener.openCount).isEqualTo(0);
        final int ignore = stream.read();
        assertThat(opener.openCount).isEqualTo(1);
    }

    @Test
    void shouldCallExtensionOpenStreamOnReopen() throws Exception {
        final byte[] data = {1, 2, 3, 4, 5};
        final TrackingOpener opener = new TrackingOpener(data);
        final InputStreamExtension extension = new TestingExtension(opener);

        stream = new ObjectStorageInputStream(data.length, SKIP_THRESHOLD, extension);
        stream.read(); // triggers init, openCount=1
        stream.seek(0); // backward seek forces reopen on next read
        stream.read(); // triggers reopen, openCount=2
        assertThat(opener.openCount).isEqualTo(2);
    }

    @Test
    void shouldUseDefaultBufferingExtension() throws Exception {
        final byte[] data = {1, 2, 3, 4, 5};
        stream = new ObjectStorageInputStream(data.length, SKIP_THRESHOLD, buffering(data));
        final byte[] buf = new byte[5];
        final int read = stream.read(buf, 0, 5);
        assertThat(read).isEqualTo(5);
        assertThat(buf).isEqualTo(data);
        assertThat(stream.getPos()).isEqualTo(5);
    }

    @Test
    void shouldPropagateExtensionOpenStreamException() {
        final IOException openFailure = new IOException("open failed");
        final InputStreamExtension extension =
                new TestingExtension(
                        ctx -> {
                            throw openFailure;
                        });

        stream = new ObjectStorageInputStream(1L, SKIP_THRESHOLD, extension);
        assertThatThrownBy(stream::read).isSameAs(openFailure);
    }
}
