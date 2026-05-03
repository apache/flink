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

        TrackingInputStream(byte[] data, int offset) {
            this.delegate = new ByteArrayInputStream(data, offset, data.length - offset);
        }

        TrackingInputStream(byte[] data) {
            this(data, 0);
        }

        @Override
        public int read() {
            return delegate.read();
        }

        @Override
        public int read(byte[] b, int off, int len) {
            return delegate.read(b, off, len);
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
    }

    /** {@link S3Client} stub. */
    private static final class StubS3Client implements S3Client {
        private final byte[] data;
        private final AtomicInteger getObjectCalls = new AtomicInteger();
        private volatile TrackingInputStream lastStream;

        StubS3Client(byte[] data) {
            this.data = data;
        }

        @Override
        public ResponseInputStream<GetObjectResponse> getObject(GetObjectRequest request) {
            getObjectCalls.incrementAndGet();
            int offset = 0;
            String range = request.range();
            if (range != null && range.startsWith("bytes=")) {
                offset = Integer.parseInt(range.substring(6, range.indexOf('-')));
            }
            TrackingInputStream tracking = new TrackingInputStream(data, offset);
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
        // abort() must be called to kill the HTTP connection (prevents drain)
        assertThat(stream.wasAborted()).isTrue();
        // close() must still be called for SDK resource cleanup (connection pool return, etc.)
        assertThat(stream.wasClosed()).isTrue();
        // abort() must happen BEFORE close() - otherwise close() drains remaining bytes
        assertThat(stream.wasAbortedBeforeClose()).isTrue();
    }

    @Test
    void seekAbortsAndClosesOldStreamBeforeOpeningNew() throws Exception {
        StubS3Client client = new StubS3Client(DATA);
        try (NativeS3InputStream in = new NativeS3InputStream(client, BUCKET, KEY, DATA.length)) {
            in.read();
            TrackingInputStream first = client.lastStream();

            in.seek(100);

            // old stream must be aborted, closed, and in the correct order
            assertThat(first.wasAborted()).isTrue();
            assertThat(first.wasClosed()).isTrue();
            assertThat(first.wasAbortedBeforeClose()).isTrue();
            assertThat(client.getObjectCalls()).isEqualTo(2);
            assertThat(in.getPos()).isEqualTo(100);
            in.seek(100);
            assertThat(client.getObjectCalls()).isEqualTo(2);
        }
    }

    @Test
    void skipAbortsOldStreamAndOpensNew() throws Exception {
        StubS3Client client = new StubS3Client(DATA);
        try (NativeS3InputStream in = new NativeS3InputStream(client, BUCKET, KEY, DATA.length)) {
            in.read();
            TrackingInputStream first = client.lastStream();
            assertThat(in.skip(100)).isEqualTo(100);
            assertThat(first.wasAborted()).isTrue();
            assertThat(first.wasClosed()).isTrue();
            assertThat(first.wasAbortedBeforeClose()).isTrue();
            assertThat(client.getObjectCalls()).isEqualTo(2);
            // skip(0) and skip(negative) are no-ops
            assertThat(in.skip(0)).isZero();
            assertThat(in.skip(-5)).isZero();
            assertThat(client.getObjectCalls()).isEqualTo(2);
        }
    }

    @Test
    void closeWithoutReadNeverOpensStream() throws Exception {
        StubS3Client client = new StubS3Client(DATA);
        try (NativeS3InputStream in = new NativeS3InputStream(client, BUCKET, KEY, DATA.length)) {
            // lazy init means no getObject call
        }
        assertThat(client.getObjectCalls()).isEqualTo(0);
    }

    @Test
    void doubleCloseIsIdempotent() throws Exception {
        StubS3Client client = new StubS3Client(DATA);
        NativeS3InputStream in = new NativeS3InputStream(client, BUCKET, KEY, DATA.length);
        in.read();

        // Verify state after first close
        in.close();
        assertThat(client.getObjectCalls()).isEqualTo(1);
        assertThat(client.lastStream().wasAborted()).isTrue();

        // Second close should be a no-op
        in.close();
        assertThat(client.getObjectCalls()).isEqualTo(1);
        assertThat(client.lastStream().wasAborted()).isTrue();
    }

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
    void readAndSeekReturnCorrectData() throws Exception {
        StubS3Client client = new StubS3Client(DATA);
        try (NativeS3InputStream in = new NativeS3InputStream(client, BUCKET, KEY, DATA.length)) {
            // single-byte read
            assertThat(in.read()).isEqualTo(0);
            assertThat(in.getPos()).isEqualTo(1);
            // bulk read returns correct bytes and advances position
            byte[] buf = new byte[10];
            assertThat(in.read(buf, 0, 10)).isEqualTo(10);
            assertThat(in.getPos()).isEqualTo(11);
            for (int i = 0; i < 10; i++) {
                assertThat(buf[i]).isEqualTo(DATA[i + 1]);
            }
            // available() reflects remaining bytes
            assertThat(in.available()).isEqualTo(DATA.length - 11);
            // seek then read returns data at the seeked position
            in.seek(200);
            assertThat(in.read()).isEqualTo(200);
            assertThat(in.getPos()).isEqualTo(201);
            // partial read at EOF returns only remaining bytes
            in.seek(250);
            byte[] tail = new byte[20];
            assertThat(in.read(tail, 0, 20)).isEqualTo(6);
            assertThat(in.getPos()).isEqualTo(256);
            // read past EOF
            assertThat(in.read()).isEqualTo(-1);
            assertThat(in.read(new byte[1], 0, 1)).isEqualTo(-1);
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
            // EOF short-circuits before lazyInitialize, so no range request is issued.
            assertThat(client.getObjectCalls()).isZero();
        }
    }

    @Test
    void seekToContentLengthWithOpenStreamReleasesStream() throws Exception {
        StubS3Client client = new StubS3Client(DATA);
        try (NativeS3InputStream in = new NativeS3InputStream(client, BUCKET, KEY, DATA.length)) {
            in.read();
            TrackingInputStream first = client.lastStream();
            assertThat(client.getObjectCalls()).isEqualTo(1);

            in.seek(DATA.length);

            assertThat(first.wasAborted()).isTrue();
            assertThat(first.wasClosed()).isTrue();
            assertThat(first.wasAbortedBeforeClose()).isTrue();
            // bytes=contentLength- is unsatisfiable, so we must not reopen.
            assertThat(client.getObjectCalls()).isEqualTo(1);
            assertThat(in.read()).isEqualTo(-1);
            assertThat(in.read(new byte[4], 0, 4)).isEqualTo(-1);
            assertThat(client.getObjectCalls()).isEqualTo(1);
        }
    }

    @Test
    void skipToEofWithOpenStreamReleasesStream() throws Exception {
        StubS3Client client = new StubS3Client(DATA);
        try (NativeS3InputStream in = new NativeS3InputStream(client, BUCKET, KEY, DATA.length)) {
            in.read();
            TrackingInputStream first = client.lastStream();
            assertThat(client.getObjectCalls()).isEqualTo(1);

            assertThat(in.skip(DATA.length)).isEqualTo(DATA.length - 1);
            assertThat(in.getPos()).isEqualTo(DATA.length);

            assertThat(first.wasAborted()).isTrue();
            assertThat(first.wasClosed()).isTrue();
            assertThat(first.wasAbortedBeforeClose()).isTrue();
            assertThat(client.getObjectCalls()).isEqualTo(1);
            assertThat(in.read()).isEqualTo(-1);
        }
    }

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
