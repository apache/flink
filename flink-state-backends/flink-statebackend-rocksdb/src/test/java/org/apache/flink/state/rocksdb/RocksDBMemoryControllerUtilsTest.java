/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.state.rocksdb;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.Cache;
import org.rocksdb.NativeLibraryLoader;
import org.rocksdb.WriteBufferManager;

import java.io.IOException;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests to guard {@link RocksDBMemoryControllerUtils}. */
class RocksDBMemoryControllerUtilsTest {

    @BeforeEach
    void ensureRocksDbNativeLibraryLoaded(@TempDir Path temporaryFolder) throws IOException {
        NativeLibraryLoader.getInstance().loadLibrary(temporaryFolder.toFile().getAbsolutePath());
    }

    @Test
    void testCreateSharedResourcesWithExpectedCapacity() {
        long totalMemorySize = 2048L;
        double writeBufferRatio = 0.5;
        double highPriPoolRatio = 0.1;
        TestingRocksDBMemoryFactory factory = new TestingRocksDBMemoryFactory();
        RocksDBSharedResources rocksDBSharedResources =
                RocksDBMemoryControllerUtils.allocateRocksDBSharedResources(
                        totalMemorySize, writeBufferRatio, highPriPoolRatio, false, factory);
        long expectedCacheCapacity =
                RocksDBMemoryControllerUtils.calculateActualCacheCapacity(
                        totalMemorySize, writeBufferRatio);
        long expectedWbmCapacity =
                RocksDBMemoryControllerUtils.calculateWriteBufferManagerCapacity(
                        totalMemorySize, writeBufferRatio);

        assertThat(factory.actualCacheCapacity).isEqualTo(expectedCacheCapacity);
        assertThat(factory.actualWbmCapacity).isEqualTo(expectedWbmCapacity);
        assertThat(rocksDBSharedResources.getWriteBufferManagerCapacity())
                .isEqualTo(expectedWbmCapacity);
    }

    @Test
    void testCalculateRocksDBDefaultArenaBlockSize() {
        final long align = 4 * 1024;
        final long writeBufferSize = 64 * 1024 * 1024;
        final long expectArenaBlockSize = writeBufferSize / 8;

        // Normal case test
        assertThat(
                        RocksDBMemoryControllerUtils.calculateRocksDBDefaultArenaBlockSize(
                                writeBufferSize))
                .as("Arena block size calculation error for normal case")
                .isEqualTo(expectArenaBlockSize);

        // Alignment tests
        assertThat(
                        RocksDBMemoryControllerUtils.calculateRocksDBDefaultArenaBlockSize(
                                writeBufferSize - 1))
                .as("Arena block size calculation error for alignment case")
                .isEqualTo(expectArenaBlockSize);
        assertThat(
                        RocksDBMemoryControllerUtils.calculateRocksDBDefaultArenaBlockSize(
                                writeBufferSize + 8))
                .as("Arena block size calculation error for alignment case2")
                .isEqualTo(expectArenaBlockSize + align);
    }

    @Test
    void testCalculateRocksDBMutableLimit() {
        long bufferSize = 64 * 1024 * 1024;
        long limit = bufferSize * 7 / 8;
        assertThat(RocksDBMemoryControllerUtils.calculateRocksDBMutableLimit(bufferSize))
                .isEqualTo(limit);
    }

    @Test
    void testValidateArenaBlockSize() {
        long arenaBlockSize = 8 * 1024 * 1024;
        assertThat(
                        RocksDBMemoryControllerUtils.validateArenaBlockSize(
                                arenaBlockSize, (long) (arenaBlockSize * 0.5)))
                .isFalse();
        assertThat(
                        RocksDBMemoryControllerUtils.validateArenaBlockSize(
                                arenaBlockSize, (long) (arenaBlockSize * 1.5)))
                .isTrue();
    }

    private static final class TestingRocksDBMemoryFactory
            implements RocksDBMemoryControllerUtils.RocksDBMemoryFactory {
        private Long actualCacheCapacity = null;
        private Long actualWbmCapacity = null;

        @Override
        public Cache createCache(long cacheCapacity, double highPriorityPoolRatio) {
            actualCacheCapacity = cacheCapacity;
            return RocksDBMemoryControllerUtils.RocksDBMemoryFactory.DEFAULT.createCache(
                    cacheCapacity, highPriorityPoolRatio);
        }

        @Override
        public WriteBufferManager createWriteBufferManager(
                long writeBufferManagerCapacity, Cache cache) {
            actualWbmCapacity = writeBufferManagerCapacity;
            return RocksDBMemoryControllerUtils.RocksDBMemoryFactory.DEFAULT
                    .createWriteBufferManager(writeBufferManagerCapacity, cache);
        }
    }
}
