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

package org.apache.flink.state.forst.fs.cache;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the stream life cycle of {@link CachedDataInputStream}. */
class CachedDataInputStreamTest {

    @TempDir java.nio.file.Path tempDir;

    private FileBasedCache newCache() {
        return new FileBasedCache(
                new Configuration(),
                new SizeBasedCacheLimitPolicy(1024 * 1024, 64 * 1024 * 1024),
                FileSystem.getLocalFileSystem(),
                new Path(tempDir.toString(), "cache"),
                null);
    }

    private FileCacheEntry newEntry(FileBasedCache cache) {
        return new FileCacheEntry(
                cache,
                new Path(tempDir.toString(), "remote/1.sst"),
                new Path(tempDir.toString(), "cache/1.sst"),
                16);
    }

    /**
     * The original (remote) stream is handed over to the cached stream by {@link
     * FileCacheEntry#open} and nothing else references it, so closing the cached stream must close
     * it — otherwise it leaks whatever the remote file system holds for it (for S3A: a leased
     * connection of the HTTP connection pool).
     */
    @Test
    void testCloseClosesOriginalStream() throws Exception {
        try (FileBasedCache cache = newCache()) {
            FileCacheEntry entry = newEntry(cache);
            TrackingStream original = new TrackingStream();

            // The entry is not loaded, so the stream is opened on the original stream only.
            CachedDataInputStream stream = entry.open(original);
            assertThat(original.closed).isFalse();

            stream.close();
            assertThat(original.closed).as("original stream must be closed").isTrue();

            // Idempotent.
            stream.close();
            assertThat(original.closeCalls).isEqualTo(1);
        }
    }

    @Test
    void testCloseClosesCachedAndOriginalStream() throws Exception {
        try (FileBasedCache cache = newCache()) {
            FileCacheEntry entry = newEntry(cache);
            TrackingStream cached = new TrackingStream();
            TrackingStream original = new TrackingStream();

            CachedDataInputStream stream =
                    new CachedDataInputStream(cache, entry, cached, original);
            stream.close();

            assertThat(cached.closed).isTrue();
            assertThat(original.closed).isTrue();
        }
    }

    @Test
    void testOriginalStreamIsClosedEvenIfCachedStreamFailsToClose() throws Exception {
        try (FileBasedCache cache = newCache()) {
            FileCacheEntry entry = newEntry(cache);
            TrackingStream cached = new TrackingStream();
            cached.failOnClose = new IOException("cached close failed");
            TrackingStream original = new TrackingStream();
            original.failOnClose = new IOException("original close failed");

            CachedDataInputStream stream =
                    new CachedDataInputStream(cache, entry, cached, original);

            assertThatThrownBy(stream::close)
                    .isInstanceOf(IOException.class)
                    .hasMessage("cached close failed")
                    .satisfies(
                            e ->
                                    assertThat(e.getSuppressed())
                                            .extracting(Throwable::getMessage)
                                            .containsExactly("original close failed"));
            assertThat(cached.closeCalls).isEqualTo(1);
            assertThat(original.closeCalls).as("original stream must still be closed").isEqualTo(1);
        }
    }

    /** An in-memory {@link FSDataInputStream} that records whether it has been closed. */
    static final class TrackingStream extends FSDataInputStream {
        private long pos;
        volatile boolean closed;
        int closeCalls;
        IOException failOnClose;

        @Override
        public void seek(long desired) {
            pos = desired;
        }

        @Override
        public long getPos() {
            return pos;
        }

        @Override
        public int read() {
            pos++;
            return 0;
        }

        @Override
        public void close() throws IOException {
            closeCalls++;
            closed = true;
            if (failOnClose != null) {
                throw failOnClose;
            }
        }
    }
}
