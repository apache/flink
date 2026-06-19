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

package org.apache.flink.fs.s3native;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.http.Abortable;
import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link NativeS3InputStream}. */
class NativeS3InputStreamTest {

    private static final String BUCKET = "test-bucket";
    private static final String KEY = "test-key";

    private static final byte[] DATA;

    static {
        DATA = new byte[256];
        for (int i = 0; i < DATA.length; i++) {
            DATA[i] = (byte) i;
        }
    }

    private static class TrackingInputStream extends InputStream implements Abortable {
        private final ByteArrayInputStream delegate;
        private final AtomicBoolean aborted = new AtomicBoolean();
        private final AtomicBoolean closed = new AtomicBoolean();
        private volatile boolean abortedBeforeClose;
        private final int maxAvailable;
        private final AtomicLong bytesRead = new AtomicLong();
        private final AtomicLong bytesSkipped = new AtomicLong();

        TrackingInputStream(byte[] data, int offset) {
            this(data, offset, Integer.MAX_VALUE);
        }

        TrackingInputStream(byte[] data, int offset, int maxAvailable) {
            this.delegate = new ByteArrayInputStream(data, offset, data.length - offset);
            this.maxAvailable = maxAvailable;
        }

        @Override
        public int read() {
            int b = delegate.read();
            if (b != -1) {
                bytesRead.incrementAndGet();
            }
            return b;
        }

        @Override
        public int read(byte[] b, int off, int len) {
            int n = delegate.read(b, off, len);
            if (n > 0) {
                bytesRead.addAndGet(n);
            }
            return n;
        }

        @Override
        public long skip(long n) {
            long skipped = delegate.skip(n);
            bytesSkipped.addAndGet(skipped);
            return skipped;
        }

        @Override
        public int available() {
            return Math.min(delegate.available(), maxAvailable);
        }

        @Override
        public void abort() {
            aborted.set(true);
        }

        @Override
        public void close() throws IOException {
            if (aborted.get()) {
                abortedBeforeClose = true;
            }
            closed.set(true);
            delegate.close();
        }

        boolean wasAborted() {
            return aborted.get();
        }

        boolean wasClosed() {
            return closed.get();
        }

        boolean wasAbortedBeforeClose() {
            return abortedBeforeClose;
        }

        long bytesReadFromUnderlying() {
            return bytesRead.get();
        }

        long bytesSkippedFromUnderlying() {
            return bytesSkipped.get();
        }
    }

    /**
     * A {@link TrackingInputStream} whose first {@link #skip} call returns {@code 0}, simulating a
     * stalled/closed underlying HTTP connection during in-buffer skipping.
     */
    private static final class FailFirstSkipTrackingInputStream extends TrackingInputStream {
        private final AtomicBoolean firstSkipFailed = new AtomicBoolean();

        FailFirstSkipTrackingInputStream(byte[] data, int offset, int maxAvailable) {
            super(data, offset, maxAvailable);
        }

        @Override
        public long skip(long n) {
            if (firstSkipFailed.compareAndSet(false, true)) {
                return 0;
            }
            return super.skip(n);
        }
    }

    @FunctionalInterface
    private interface StreamFactory {
        TrackingInputStream create(byte[] data, int offset, int maxAvailable);
    }

    /** {@link S3Client} stub. */
    private static final class StubS3Client implements S3Client {
        private final byte[] data;
        private final AtomicInteger getObjectCalls = new AtomicInteger();
        private volatile TrackingInputStream lastStream;
        private final int maxAvailable;
        private final StreamFactory factory;

        StubS3Client(byte[] data) {
            this(data, Integer.MAX_VALUE);
        }

        StubS3Client(byte[] data, int maxAvailable) {
            this(data, maxAvailable, TrackingInputStream::new);
        }

        StubS3Client(byte[] data, int maxAvailable, StreamFactory factory) {
            this.data = data;
            this.maxAvailable = maxAvailable;
            this.factory = factory;
        }

        @Override
        public ResponseInputStream<GetObjectResponse> getObject(GetObjectRequest request) {
            getObjectCalls.incrementAndGet();
            int offset = 0;
            String range = request.range();
            if (range != null && range.startsWith("bytes=")) {
                offset = Integer.parseInt(range.substring(6, range.indexOf('-')));
            }
            TrackingInputStream tracking = factory.create(data, offset, maxAvailable);
            lastStream = tracking;
            AbortableInputStream abortable = AbortableInputStream.create(tracking, tracking);
            return new ResponseInputStream<>(
                    GetObjectResponse.builder().build(), abortable, Duration.ZERO);
        }

        @Override
        public String serviceName() {
            return "s3";
        }

        @Override
        public void close() {}

        TrackingInputStream lastStream() {
            return lastStream;
        }

        int getObjectCalls() {
            return getObjectCalls.get();
        }
    }

    // --- close behavior ---

    @Test
    void closeAbortsUnderlyingStream() throws Exception {
        StubS3Client client = new StubS3Client(DATA);
        try (NativeS3InputStream in = new NativeS3InputStream(client, BUCKET, KEY, DATA.length)) {
            assertThat(in.read()).isEqualTo(0);
            assertThat(in.getPos()).isEqualTo(1);
        }
        assertThat(client.lastStream().wasAborted()).isTrue();
    }

    @Test
    void closeAbortsAndThenClosesUnderlyingStream() throws Exception {
        StubS3Client client = new StubS3Client(DATA);
        try (NativeS3InputStream in = new NativeS3InputStream(client, BUCKET, KEY, DATA.length)) {
            in.read();
        }
        TrackingInputStream stream = client.lastStream();
        assertThat(stream.wasAborted()).isTrue();
        assertThat(stream.wasClosed()).isTrue();
        assertThat(stream.wasAbortedBeforeClose()).isTrue();
    }

    @Test
    void closeWithoutReadNeverOpensStream() throws Exception {
        StubS3Client client = new StubS3Client(DATA);
        try (NativeS3InputStream in = new NativeS3InputStream(client, BUCKET, KEY, DATA.length)) {}
        assertThat(client.getObjectCalls()).isEqualTo(0);
    }

    @Test
    void doubleCloseIsIdempotent() throws Exception {
        StubS3Client client = new StubS3Client(DATA);
        NativeS3InputStream in = new NativeS3InputStream(client, BUCKET, KEY, DATA.length);
        in.read();

        in.close();
        assertThat(client.getObjectCalls()).isEqualTo(1);
        assertThat(client.lastStream().wasAborted()).isTrue();

        in.close();
        assertThat(client.getObjectCalls()).isEqualTo(1);
        assertThat(client.lastStream().wasAborted()).isTrue();
    }

    // --- lazy seek: seek() does no I/O ---

    @Test
    void seekBeforeFirstReadUpdatesPositionOnly() throws Exception {
        StubS3Client client = new StubS3Client(DATA);
        try (NativeS3InputStream in = new NativeS3InputStream(client, BUCKET, KEY, DATA.length)) {
            in.seek(50);
            assertThat(in.getPos()).isEqualTo(50);
            assertThat(client.getObjectCalls()).isEqualTo(0);
        }
    }

    @Test
    void seekDoesNotTriggerStreamOperations() throws Exception {
        StubS3Client client = new StubS3Client(DATA);
        try (NativeS3InputStream in = new NativeS3InputStream(client, BUCKET, KEY, DATA.length)) {
            in.read();
            TrackingInputStream first = client.lastStream();
            assertThat(client.getObjectCalls()).isEqualTo(1);

            in.seek(0);
            assertThat(first.wasAborted()).isFalse();
            assertThat(first.wasClosed()).isFalse();
            assertThat(client.getObjectCalls()).isEqualTo(1);
            assertThat(in.getPos()).isEqualTo(0);
        }
    }

    @Test
    void multipleSeeksWithoutReadCoalesce() throws Exception {
        StubS3Client client = new StubS3Client(DATA);
        try (NativeS3InputStream in = new NativeS3InputStream(client, BUCKET, KEY, DATA.length)) {
            in.read();
            assertThat(client.getObjectCalls()).isEqualTo(1);

            in.seek(200);
            in.seek(0);
            in.seek(100);
            in.seek(50);

            assertThat(client.getObjectCalls()).isEqualTo(1);

            assertThat(in.read()).isEqualTo(50);
            assertThat(in.getPos()).isEqualTo(51);
        }
    }

    // --- seek + read: forward within buffer ---

    @Test
    void seekForwardWithinBufferReusesStream() throws Exception {
        StubS3Client client = new StubS3Client(DATA);
        try (NativeS3InputStream in = new NativeS3InputStream(client, BUCKET, KEY, DATA.length)) {
            in.read();
            TrackingInputStream first = client.lastStream();

            in.seek(100);
            assertThat(in.read()).isEqualTo(100);

            assertThat(first.wasAborted()).isFalse();
            assertThat(first.wasClosed()).isFalse();
            assertThat(client.getObjectCalls()).isEqualTo(1);
            assertThat(in.getPos()).isEqualTo(101);

            in.seek(101);
            assertThat(in.read()).isEqualTo(101);
            assertThat(client.getObjectCalls()).isEqualTo(1);
        }
    }

    @Test
    void seekForwardWithinBufferReturnsCorrectData() throws Exception {
        StubS3Client client = new StubS3Client(DATA);
        try (NativeS3InputStream in = new NativeS3InputStream(client, BUCKET, KEY, DATA.length)) {
            assertThat(in.read()).isEqualTo(0);
            assertThat(in.read()).isEqualTo(1);

            in.seek(10);
            assertThat(in.read()).isEqualTo(10);

            in.seek(50);
            byte[] buf = new byte[5];
            assertThat(in.read(buf, 0, 5)).isEqualTo(5);
            for (int i = 0; i < 5; i++) {
                assertThat(buf[i]).isEqualTo(DATA[50 + i]);
            }

            in.seek(200);
            assertThat(in.read()).isEqualTo(200 & 0xFF);

            assertThat(client.getObjectCalls()).isEqualTo(1);
        }
    }

    @Test
    void sequentialSmallSeeksReuseStream() throws Exception {
        StubS3Client client = new StubS3Client(DATA);
        try (NativeS3InputStream in = new NativeS3InputStream(client, BUCKET, KEY, DATA.length)) {
            for (int i = 0; i < 100; i++) {
                in.seek(i * 2);
                assertThat(in.read()).isEqualTo((i * 2) & 0xFF);
            }
            assertThat(client.getObjectCalls()).isEqualTo(1);
        }
    }

    // --- seek + read: forward beyond buffer ---

    @Test
    void seekForwardBeyondBufferReopensStream() throws Exception {
        StubS3Client client = new StubS3Client(DATA, 0);
        int smallBuffer = 16;
        try (NativeS3InputStream in =
                new NativeS3InputStream(client, BUCKET, KEY, DATA.length, smallBuffer)) {
            in.read();
            TrackingInputStream first = client.lastStream();

            in.seek(1 + smallBuffer + 1);
            assertThat(first.wasAborted()).isFalse();

            assertThat(in.read()).isEqualTo(1 + smallBuffer + 1);
            assertThat(first.wasAborted()).isTrue();
            assertThat(first.wasClosed()).isTrue();
            assertThat(first.wasAbortedBeforeClose()).isTrue();
            assertThat(client.getObjectCalls()).isEqualTo(2);
        }
    }

    // --- seek + read: backward ---

    @Test
    void seekBackwardReopensStreamOnRead() throws Exception {
        StubS3Client client = new StubS3Client(DATA);
        try (NativeS3InputStream in = new NativeS3InputStream(client, BUCKET, KEY, DATA.length)) {
            byte[] buf = new byte[50];
            in.read(buf, 0, 50);
            TrackingInputStream first = client.lastStream();

            in.seek(10);
            assertThat(first.wasAborted()).isFalse();
            assertThat(client.getObjectCalls()).isEqualTo(1);

            assertThat(in.read()).isEqualTo(10);
            assertThat(first.wasAborted()).isTrue();
            assertThat(first.wasClosed()).isTrue();
            assertThat(first.wasAbortedBeforeClose()).isTrue();
            assertThat(client.getObjectCalls()).isEqualTo(2);
            assertThat(in.getPos()).isEqualTo(11);
        }
    }

    // --- seek to EOF ---

    @Test
    void seekToContentLengthReleasesStreamOnRead() throws Exception {
        StubS3Client client = new StubS3Client(DATA);
        try (NativeS3InputStream in = new NativeS3InputStream(client, BUCKET, KEY, DATA.length)) {
            in.read();
            TrackingInputStream first = client.lastStream();
            assertThat(client.getObjectCalls()).isEqualTo(1);

            in.seek(DATA.length);
            assertThat(first.wasAborted()).isFalse();

            assertThat(in.read()).isEqualTo(-1);
            assertThat(in.read(new byte[4], 0, 4)).isEqualTo(-1);
            assertThat(client.getObjectCalls()).isEqualTo(1);
        }
    }

    @Test
    void seekPastEofThrowsEofException() throws Exception {
        StubS3Client client = new StubS3Client(DATA);
        try (NativeS3InputStream in = new NativeS3InputStream(client, BUCKET, KEY, DATA.length)) {
            assertThatThrownBy(() -> in.seek(DATA.length + 1))
                    .isInstanceOf(EOFException.class)
                    .hasMessageContaining("past end of stream");
            in.seek(DATA.length);
            assertThat(in.getPos()).isEqualTo(DATA.length);
            assertThat(client.getObjectCalls()).isEqualTo(0);
        }
    }

    @Test
    void readAtEofReturnsMinusOne() throws Exception {
        StubS3Client client = new StubS3Client(DATA);
        try (NativeS3InputStream in = new NativeS3InputStream(client, BUCKET, KEY, DATA.length)) {
            in.seek(DATA.length);
            assertThat(in.read()).isEqualTo(-1);
            assertThat(in.read(new byte[8], 0, 8)).isEqualTo(-1);
            assertThat(in.getPos()).isEqualTo(DATA.length);
            assertThat(in.available()).isZero();
            assertThat(client.getObjectCalls()).isZero();
        }
    }

    // --- skip (also lazy) ---

    @Test
    void skipForwardWithinBufferReusesStream() throws Exception {
        StubS3Client client = new StubS3Client(DATA);
        try (NativeS3InputStream in = new NativeS3InputStream(client, BUCKET, KEY, DATA.length)) {
            in.read();
            TrackingInputStream first = client.lastStream();

            assertThat(in.skip(50)).isEqualTo(50);
            assertThat(in.getPos()).isEqualTo(51);

            assertThat(first.wasAborted()).isFalse();
            assertThat(client.getObjectCalls()).isEqualTo(1);

            assertThat(in.read()).isEqualTo(51);
            assertThat(first.wasAborted()).isFalse();
            assertThat(client.getObjectCalls()).isEqualTo(1);
        }
    }

    @Test
    void skipForwardBeyondBufferReopensStreamOnRead() throws Exception {
        StubS3Client client = new StubS3Client(DATA, 0);
        int smallBuffer = 16;
        try (NativeS3InputStream in =
                new NativeS3InputStream(client, BUCKET, KEY, DATA.length, smallBuffer)) {
            in.read();
            TrackingInputStream first = client.lastStream();

            assertThat(in.skip(smallBuffer + 1)).isEqualTo(smallBuffer + 1);
            assertThat(first.wasAborted()).isFalse();

            assertThat(in.read()).isEqualTo(1 + smallBuffer + 1);
            assertThat(first.wasAborted()).isTrue();
            assertThat(first.wasClosed()).isTrue();
            assertThat(first.wasAbortedBeforeClose()).isTrue();
            assertThat(client.getObjectCalls()).isEqualTo(2);
        }
    }

    @Test
    void skipToEofReleasesStreamOnRead() throws Exception {
        StubS3Client client = new StubS3Client(DATA);
        try (NativeS3InputStream in = new NativeS3InputStream(client, BUCKET, KEY, DATA.length)) {
            in.read();
            TrackingInputStream first = client.lastStream();
            assertThat(client.getObjectCalls()).isEqualTo(1);

            assertThat(in.skip(DATA.length)).isEqualTo(DATA.length - 1);
            assertThat(in.getPos()).isEqualTo(DATA.length);
            assertThat(first.wasAborted()).isFalse();

            assertThat(in.read()).isEqualTo(-1);
        }
    }

    @Test
    void skipZeroAndNegativeAreNoOps() throws Exception {
        StubS3Client client = new StubS3Client(DATA);
        try (NativeS3InputStream in = new NativeS3InputStream(client, BUCKET, KEY, DATA.length)) {
            in.read();
            assertThat(in.skip(0)).isZero();
            assertThat(in.skip(-5)).isZero();
            assertThat(client.getObjectCalls()).isEqualTo(1);
        }
    }

    // --- read + seek integration ---

    @Test
    void readAndSeekReturnCorrectData() throws Exception {
        StubS3Client client = new StubS3Client(DATA);
        try (NativeS3InputStream in = new NativeS3InputStream(client, BUCKET, KEY, DATA.length)) {
            assertThat(in.read()).isEqualTo(0);
            assertThat(in.getPos()).isEqualTo(1);
            byte[] buf = new byte[10];
            assertThat(in.read(buf, 0, 10)).isEqualTo(10);
            assertThat(in.getPos()).isEqualTo(11);
            for (int i = 0; i < 10; i++) {
                assertThat(buf[i]).isEqualTo(DATA[i + 1]);
            }
            assertThat(in.available()).isEqualTo(DATA.length - 11);
            in.seek(200);
            assertThat(in.read()).isEqualTo(200);
            assertThat(in.getPos()).isEqualTo(201);
            in.seek(250);
            byte[] tail = new byte[20];
            assertThat(in.read(tail, 0, 20)).isEqualTo(6);
            assertThat(in.getPos()).isEqualTo(256);
            assertThat(in.read()).isEqualTo(-1);
            assertThat(in.read(new byte[1], 0, 1)).isEqualTo(-1);
        }
    }

    // --- buffer-efficiency: skip stays in local buffer vs. underlying stream ---

    @Test
    void seekWithinBuffer_afterSmallRead_doesNotTouchUnderlyingStream() throws Exception {
        int smallBuffer = 32;
        StubS3Client client = new StubS3Client(DATA);
        try (NativeS3InputStream in =
                new NativeS3InputStream(client, BUCKET, KEY, DATA.length, smallBuffer)) {
            // single-byte read opens the S3 stream and advances position to 1
            assertThat(in.read()).isEqualTo(0);
            TrackingInputStream underlying = client.lastStream();
            long initialBytesRead = underlying.bytesReadFromUnderlying();

            // forward seek of 9 bytes — fits in the local buffer, no underlying access needed
            in.seek(10);
            assertThat(in.read()).isEqualTo(10);
            assertThat(in.getPos()).isEqualTo(11);

            assertThat(client.getObjectCalls()).isEqualTo(1);
            assertThat(underlying.bytesSkippedFromUnderlying()).isEqualTo(0);
            assertThat(underlying.bytesReadFromUnderlying()).isEqualTo(initialBytesRead);
        }
    }

    @Test
    void seekWithinBuffer_afterLargeRead_touchesUnderlyingStream() throws Exception {
        int smallBuffer = 16;
        StubS3Client client = new StubS3Client(DATA);
        try (NativeS3InputStream in =
                new NativeS3InputStream(client, BUCKET, KEY, DATA.length, smallBuffer)) {
            // bulk read >= bufferSize bypasses BufferedInputStream's local array entirely,
            // leaving the local buffer empty afterward
            byte[] buf = new byte[smallBuffer];
            in.read(buf, 0, smallBuffer);
            TrackingInputStream underlying = client.lastStream();

            // forward seek of 10 bytes — within readBufferSize but buffer is empty, so
            // BufferedInputStream.skip() delegates directly to the underlying stream
            in.seek((long) smallBuffer + 10);
            assertThat(in.read()).isEqualTo(DATA[smallBuffer + 10] & 0xFF);

            assertThat(client.getObjectCalls()).isEqualTo(1);
            assertThat(underlying.bytesSkippedFromUnderlying()).isEqualTo(10);
        }
    }

    @Test
    void seekBeyondReadBufferSize_inflatedByUnderlyingAvailable_reopensWithRangeRequest()
            throws Exception {
        int smallBuffer = 16;
        StubS3Client client = new StubS3Client(DATA);
        try (NativeS3InputStream in =
                new NativeS3InputStream(client, BUCKET, KEY, DATA.length, smallBuffer)) {
            in.read();
            TrackingInputStream first = client.lastStream();

            // seek 50 bytes forward — exceeds readBufferSize=16, must reopen with range request
            in.seek(51);
            assertThat(in.read()).isEqualTo(51);

            assertThat(client.getObjectCalls()).isEqualTo(2);
            assertThat(first.bytesSkippedFromUnderlying()).isEqualTo(0);
        }
    }

    @Test
    void skipBytesInBuffer_skipFailureTriggersRangeRequest() throws Exception {
        int smallBuffer = 32;
        // FailFirstSkipTrackingInputStream returns 0 on the first skip() call, simulating a
        // stalled/closed HTTP connection. BufferedInputStream propagates 0 when its internal
        // buffer is empty, so skipBytesInBuffer() falls back to openStreamAtCurrentPosition().
        StubS3Client client =
                new StubS3Client(DATA, Integer.MAX_VALUE, FailFirstSkipTrackingInputStream::new);
        try (NativeS3InputStream in =
                new NativeS3InputStream(client, BUCKET, KEY, DATA.length, smallBuffer)) {
            // Read exactly one buffer-worth: BufferedInputStream bypasses its internal array for
            // len >= bufSize reads, leaving the buffer empty after the call.
            byte[] buf = new byte[smallBuffer];
            in.read(buf, 0, smallBuffer);
            assertThat(client.getObjectCalls()).isEqualTo(1);

            // Seek 10 bytes forward — within readBufferSize, so skipBytesInBuffer() is chosen.
            // With the buffer empty, BufferedInputStream delegates to the underlying skip(),
            // which returns 0 on the first call, triggering openStreamAtCurrentPosition().
            int seekTarget = smallBuffer + 10;
            in.seek(seekTarget);

            assertThat(in.read()).isEqualTo(DATA[seekTarget] & 0xFF);
            assertThat(in.getPos()).isEqualTo(seekTarget + 1);
            assertThat(client.getObjectCalls()).isEqualTo(2);
        }
    }

    // --- argument validation ---

    @Test
    void rejectsInvalidArguments() throws Exception {
        StubS3Client client = new StubS3Client(DATA);
        try (NativeS3InputStream in = new NativeS3InputStream(client, BUCKET, KEY, DATA.length)) {
            assertThatThrownBy(() -> in.read(null, 0, 1)).isInstanceOf(NullPointerException.class);
            assertThatThrownBy(() -> in.read(new byte[5], -1, 1))
                    .isInstanceOf(IndexOutOfBoundsException.class);
            assertThatThrownBy(() -> in.read(new byte[5], 0, 6))
                    .isInstanceOf(IndexOutOfBoundsException.class);
            assertThatThrownBy(() -> in.seek(-1))
                    .isInstanceOf(EOFException.class)
                    .hasMessageContaining("negative");
            assertThat(in.read(new byte[5], 0, 0)).isZero();
        }
        NativeS3InputStream closed = new NativeS3InputStream(client, BUCKET, KEY, DATA.length);
        closed.close();
        assertThatThrownBy(closed::read).isInstanceOf(IOException.class);
        assertThatThrownBy(() -> closed.read(new byte[1], 0, 1)).isInstanceOf(IOException.class);
        assertThatThrownBy(() -> closed.seek(0)).isInstanceOf(IOException.class);
        assertThatThrownBy(closed::available).isInstanceOf(IOException.class);
    }
}
