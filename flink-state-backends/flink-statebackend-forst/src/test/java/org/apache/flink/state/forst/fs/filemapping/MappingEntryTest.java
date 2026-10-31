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

package org.apache.flink.state.forst.fs.filemapping;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.checkpoint.filemerging.LogicalFile;
import org.apache.flink.runtime.state.CheckpointedStateScope;
import org.apache.flink.runtime.state.filemerging.SegmentFileStateHandle;
import org.apache.flink.runtime.state.filesystem.FileStateHandle;
import org.apache.flink.state.forst.fs.ForStFlinkFileSystem;
import org.apache.flink.state.forst.fs.cache.CachedDataInputStream;
import org.apache.flink.state.forst.fs.cache.FileBasedCache;
import org.apache.flink.state.forst.fs.cache.FileCacheEntry;
import org.apache.flink.state.forst.fs.cache.SizeBasedCacheLimitPolicy;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.io.IOException;
import java.time.Duration;

import static org.apache.flink.core.testutils.CommonTestUtils.waitUtil;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for releasing the source and cached copy of a {@link MappingEntry}. */
@Isolated
@Execution(ExecutionMode.SAME_THREAD)
class MappingEntryTest {

    private static final byte[] CONTENTS = {1, 2, 3, 4};

    @TempDir private java.nio.file.Path tempDir;

    private final FileSystem localFs = FileSystem.getLocalFileSystem();

    private FileBasedCache cache;
    private ForStFlinkFileSystem fileSystem;
    private Path dbPath;
    private Path cacheDirectory;
    private boolean wasFlinkThread;

    @BeforeEach
    void setUp() {
        wasFlinkThread = FileBasedCache.isFlinkThread();
        FileBasedCache.unsetFlinkThread();
        dbPath = new Path(tempDir.resolve("db").toUri());
        cacheDirectory = new Path(tempDir.resolve("cache").toUri());
        cache =
                new FileBasedCache(
                        new Configuration(),
                        new SizeBasedCacheLimitPolicy(1024, 1024),
                        localFs,
                        cacheDirectory,
                        null);
        fileSystem = new ForStFlinkFileSystem(localFs, dbPath.toString(), dbPath.toString(), cache);
    }

    @AfterEach
    void tearDown() throws IOException {
        try {
            fileSystem.close();
        } finally {
            if (wasFlinkThread) {
                FileBasedCache.setFlinkThread();
            } else {
                FileBasedCache.unsetFlinkThread();
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testFinalReleaseRemovesCachedFile(boolean giveUpOwnership) throws Exception {
        final Path file = new Path(dbPath, "state.sst");
        final MappingEntry entry = writeSst(file);
        final Path sourcePath = entry.getSourcePath();
        final Path cachePath = new Path(cacheDirectory, sourcePath.getName());
        final FileCacheEntry cachedEntry = cache.get(cachePath.toString(), false);
        assertThat(cachedEntry).isNotNull();
        assertThat(localFs.exists(cachePath)).isTrue();
        assertThat(entry.getFileOwnership()).isEqualTo(FileOwnership.SHAREABLE_OWNED_BY_DB);
        entry.retain();

        final FileStateHandle stateHandle = new FileStateHandle(sourcePath, CONTENTS.length);
        if (giveUpOwnership) {
            fileSystem.giveUpOwnership(file, stateHandle);
            assertThat(entry.getSource()).isInstanceOf(HandleBackedMappingEntrySource.class);
            assertThat(entry.getSource().toStateHandle()).isSameAs(stateHandle);
            assertThat(entry.getSourcePath()).isEqualTo(sourcePath);
            assertThat(entry.getSource().cacheable()).isTrue();
            assertThat(entry.getFileOwnership()).isEqualTo(FileOwnership.NOT_OWNED);
        }

        assertThat(fileSystem.delete(file, false)).isTrue();
        assertThat(entry.getReferenceCount()).isOne();
        assertThat(cache.get(cachePath.toString(), false)).isSameAs(cachedEntry);
        assertThat(localFs.exists(cachePath)).isTrue();
        try (FSDataInputStream input = stateHandle.openInputStream()) {
            assertThat(input.readAllBytes()).isEqualTo(CONTENTS);
        }

        entry.release();

        assertThat(entry.getReferenceCount()).isZero();
        assertThat(cache.get(cachePath.toString(), false)).isNull();
        awaitCacheRemoval(cachePath);
        if (giveUpOwnership) {
            try (FSDataInputStream input = stateHandle.openInputStream()) {
                assertThat(input.readAllBytes()).isEqualTo(CONTENTS);
            }
        } else {
            assertThat(localFs.exists(sourcePath)).isFalse();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    @Timeout(30)
    void testOpenReaderFallsBackToCheckpointAfterCacheRemoval(boolean flinkThread)
            throws Exception {
        if (flinkThread) {
            FileBasedCache.setFlinkThread();
        }
        final Path file = new Path(dbPath, "state.sst");
        final MappingEntry entry = writeSst(file);
        final Path sourcePath = entry.getSourcePath();
        final Path cachePath = new Path(cacheDirectory, sourcePath.getName());
        final FileCacheEntry cachedEntry = cache.get(cachePath.toString(), false);
        final FileStateHandle stateHandle = new FileStateHandle(sourcePath, CONTENTS.length);
        fileSystem.giveUpOwnership(file, stateHandle);

        try (FSDataInputStream original = stateHandle.openInputStream();
                CachedDataInputStream reader = cache.open(sourcePath, original)) {
            assertThat(reader).isNotNull();
            assertThat(reader.read()).isEqualTo(CONTENTS[0]);
            assertThat(original.getPos()).isZero();
            // An open reader retains the cache entry only for the duration of each operation.
            assertThat(cachedEntry.getReferenceCount()).isOne();

            // Hold the removal worker before it can capture the cached stream's position.
            synchronized (cachedEntry) {
                assertThat(fileSystem.delete(file, false)).isTrue();
                assertThat(cache.get(cachePath.toString(), false)).isNull();
                assertThat(cachedEntry.getReferenceCount()).isZero();
                assertThat(localFs.exists(cachePath)).isTrue();
                assertThat(original.getPos()).isZero();

                assertThat(reader.read()).isEqualTo(CONTENTS[1]);
                assertThat(original.getPos()).isEqualTo(2);
                assertThat(localFs.exists(cachePath)).isTrue();
            }
            awaitCacheRemoval(cachePath);

            assertThat(reader.isClosed()).isFalse();
            assertThat(reader.readAllBytes()).containsExactly((byte) 3, (byte) 4);
            assertThat(original.getPos()).isEqualTo(CONTENTS.length);
        }
        try (FSDataInputStream input = stateHandle.openInputStream()) {
            assertThat(input.readAllBytes()).isEqualTo(CONTENTS);
        }
    }

    @Test
    void testDirectoryReleaseSkipsCache() throws Exception {
        final Path directory = new Path(dbPath, "directory");
        final Path child = new Path(directory, "child");
        writeSource(child);
        final MappingEntrySource source =
                new FileBackedMappingEntrySource(directory) {
                    @Override
                    public boolean cacheable() {
                        throw new AssertionError("Directories must not be considered for caching");
                    }
                };
        final MappingEntry entry =
                new MappingEntry(1, source, FileOwnership.PRIVATE_OWNED_BY_DB, cache, true, false);

        entry.release();

        assertThat(localFs.exists(directory)).isFalse();
        assertThat(localFs.exists(child)).isFalse();
        assertThat(cache).isEmpty();
    }

    @Test
    void testSegmentHandleReleaseSkipsCache() throws Exception {
        final Path sourcePath = new Path(dbPath, "merged-file");
        writeSource(sourcePath);
        final SegmentFileStateHandle stateHandle =
                new SegmentFileStateHandle(
                        sourcePath,
                        1,
                        2,
                        CheckpointedStateScope.SHARED,
                        new LogicalFile.LogicalFileId("segment"));
        final MappingEntrySource source =
                new HandleBackedMappingEntrySource(stateHandle) {
                    @Override
                    public Path getFilePath() {
                        throw new AssertionError("Segments must not be cached by physical path");
                    }
                };
        final MappingEntry entry =
                new MappingEntry(1, source, FileOwnership.NOT_OWNED, cache, false, false);

        entry.release();

        assertThat(entry.getReferenceCount()).isZero();
        assertThat(cache).isEmpty();
        try (FSDataInputStream input = stateHandle.openInputStream()) {
            assertThat(input.readAllBytes()).containsExactly((byte) 2, (byte) 3);
        }
    }

    private MappingEntry writeSst(Path file) throws IOException {
        try (FSDataOutputStream output = fileSystem.create(file)) {
            output.write(CONTENTS);
        }
        return fileSystem.getMappingEntry(file);
    }

    private void writeSource(Path file) throws IOException {
        try (FSDataOutputStream output = localFs.create(file, FileSystem.WriteMode.NO_OVERWRITE)) {
            output.write(CONTENTS);
        }
    }

    private static void awaitCacheRemoval(Path cachePath) throws Exception {
        waitUtil(
                () -> !new File(cachePath.toUri()).exists(),
                Duration.ofSeconds(10),
                "Cached file was not removed: " + cachePath);
    }
}
