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

import static org.assertj.core.api.Assertions.assertThat;

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
}
