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

/** Tests for the bookkeeping of opened streams in {@link FileCacheEntry}. */
class FileCacheEntryTest {

    @TempDir java.nio.file.Path tempDir;

    /**
     * A closed stream must leave {@link FileCacheEntry#openedStreams} immediately. Before, closed
     * streams were only dropped by {@link FileCacheEntry#doRemoveFile()}, i.e. when the file was
     * evicted or deleted, so they accumulated on the heap for as long as the file stayed cached.
     */
    @Test
    void testClosedStreamIsUnregisteredFromEntry() throws Exception {
        FileBasedCache cache =
                new FileBasedCache(
                        new Configuration(),
                        new SizeBasedCacheLimitPolicy(1024 * 1024, 64 * 1024 * 1024),
                        FileSystem.getLocalFileSystem(),
                        new Path(tempDir.toString(), "cache"),
                        null);
        try {
            FileCacheEntry entry =
                    new FileCacheEntry(
                            cache,
                            new Path(tempDir.toString(), "remote/1.sst"),
                            new Path(tempDir.toString(), "cache/1.sst"),
                            16);

            CachedDataInputStream first = entry.open(new NoopStream());
            CachedDataInputStream second = entry.open(new NoopStream());
            assertThat(entry.openedStreams).containsExactly(first, second);

            first.close();
            assertThat(entry.openedStreams)
                    .as("closed stream must not stay registered")
                    .containsExactly(second);

            // Closing again must not fail or touch the registration of the other stream.
            first.close();
            assertThat(entry.openedStreams).containsExactly(second);

            second.close();
            assertThat(entry.openedStreams).isEmpty();
        } finally {
            cache.close();
        }
    }

    /**
     * The wrapper is unregistered before the cached stream is closed, so a failure while closing
     * the cached stream must not leave the (now unusable) wrapper behind in {@link
     * FileCacheEntry#openedStreams}. A real cached stream from {@link FileCacheEntry#open} never
     * fails to close, so the CACHED_OPEN stream is built directly with a stream that throws.
     */
    @Test
    void testClosedStreamIsUnregisteredWhenCachedCloseThrows() throws Exception {
        FileBasedCache cache =
                new FileBasedCache(
                        new Configuration(),
                        new SizeBasedCacheLimitPolicy(1024 * 1024, 64 * 1024 * 1024),
                        FileSystem.getLocalFileSystem(),
                        new Path(tempDir.toString(), "cache"),
                        null);
        try {
            FileCacheEntry entry =
                    new FileCacheEntry(
                            cache,
                            new Path(tempDir.toString(), "remote/1.sst"),
                            new Path(tempDir.toString(), "cache/1.sst"),
                            16);

            // CACHED_OPEN stream (constructor with a cache stream); its cached stream's close()
            // throws, so close() propagates from closeCachedStream() -> fsdis.close().
            CachedDataInputStream wrapper =
                    new CachedDataInputStream(
                            cache, entry, new ThrowingOnCloseStream(), new NoopStream());
            entry.openedStreams.add(wrapper);
            assertThat(entry.openedStreams).containsExactly(wrapper);

            assertThatThrownBy(wrapper::close).isInstanceOf(IOException.class);

            assertThat(entry.openedStreams)
                    .as("a failing cached-stream close must still unregister the wrapper")
                    .doesNotContain(wrapper);
        } finally {
            cache.close();
        }
    }

    private static final class NoopStream extends FSDataInputStream {
        private long pos;

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
    }

    /** A stream whose {@link #close()} always fails, to exercise the failing-close path. */
    private static final class ThrowingOnCloseStream extends FSDataInputStream {
        @Override
        public void seek(long desired) {}

        @Override
        public long getPos() {
            return 0;
        }

        @Override
        public int read() {
            return 0;
        }

        @Override
        public void close() throws IOException {
            throw new IOException("close failed");
        }
    }
}
