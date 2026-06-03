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

package org.apache.flink.state.forst;

import org.forstdb.Cache;
import org.forstdb.NativeLibraryLoader;
import org.forstdb.WriteBufferManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests to guard {@link ForStMemoryControllerUtils}. */
class ForStMemoryControllerUtilsTest {

    @BeforeEach
    void ensureRocksDbNativeLibraryLoaded(@TempDir Path temporaryFolder) throws IOException {
        NativeLibraryLoader.getInstance().loadLibrary(temporaryFolder.toFile().getAbsolutePath());
    }

    @Test
    void testCreateSharedResourcesWithExpectedCapacity() {
        long totalMemorySize = 2048L;
        double writeBufferRatio = 0.5;
        double highPriPoolRatio = 0.1;
        TestingForStMemoryFactory factory = new TestingForStMemoryFactory();
        ForStSharedResources forStSharedResources =
                ForStMemoryControllerUtils.allocateForStSharedResources(
                        totalMemorySize, writeBufferRatio, highPriPoolRatio, false, factory);
        long expectedCacheCapacity =
                ForStMemoryControllerUtils.calculateActualCacheCapacity(
                        totalMemorySize, writeBufferRatio);
        long expectedWbmCapacity =
                ForStMemoryControllerUtils.calculateWriteBufferManagerCapacity(
                        totalMemorySize, writeBufferRatio);

        assertThat(factory.actualCacheCapacity).isEqualTo(expectedCacheCapacity);
        assertThat(factory.actualWbmCapacity).isEqualTo(expectedWbmCapacity);
        assertThat(forStSharedResources.getWriteBufferManagerCapacity())
                .isEqualTo(expectedWbmCapacity);
    }

    @Test
    void testCalculateForStDefaultArenaBlockSize() {
        final long align = 4 * 1024;
        final long writeBufferSize = 64 * 1024 * 1024;
        final long expectArenaBlockSize = writeBufferSize / 8;

        // Normal case test
        assertThat(ForStMemoryControllerUtils.calculateForStDefaultArenaBlockSize(writeBufferSize))
                .as("Arena block size calculation error for normal case")
                .isEqualTo(expectArenaBlockSize);

        // Alignment tests
        assertThat(
                        ForStMemoryControllerUtils.calculateForStDefaultArenaBlockSize(
                                writeBufferSize - 1))
                .as("Arena block size calculation error for alignment case")
                .isEqualTo(expectArenaBlockSize);
        assertThat(
                        ForStMemoryControllerUtils.calculateForStDefaultArenaBlockSize(
                                writeBufferSize + 8))
                .as("Arena block size calculation error for alignment case2")
                .isEqualTo(expectArenaBlockSize + align);
    }

    @Test
    void testCalculateForStMutableLimit() {
        long bufferSize = 64 * 1024 * 1024;
        long limit = bufferSize * 7 / 8;
        assertThat(ForStMemoryControllerUtils.calculateForStMutableLimit(bufferSize))
                .isEqualTo(limit);
    }

    @Test
    void testValidateArenaBlockSize() {
        long arenaBlockSize = 8 * 1024 * 1024;
        assertThat(
                        ForStMemoryControllerUtils.validateArenaBlockSize(
                                arenaBlockSize, (long) (arenaBlockSize * 0.5)))
                .isFalse();
        assertThat(
                        ForStMemoryControllerUtils.validateArenaBlockSize(
                                arenaBlockSize, (long) (arenaBlockSize * 1.5)))
                .isTrue();
    }

    private static final class TestingForStMemoryFactory
            implements ForStMemoryControllerUtils.ForStMemoryFactory {
        private Long actualCacheCapacity = null;
        private Long actualWbmCapacity = null;

        @Override
        public Cache createCache(long cacheCapacity, double highPriorityPoolRatio) {
            actualCacheCapacity = cacheCapacity;
            return ForStMemoryControllerUtils.ForStMemoryFactory.DEFAULT.createCache(
                    cacheCapacity, highPriorityPoolRatio);
        }

        @Override
        public WriteBufferManager createWriteBufferManager(
                long writeBufferManagerCapacity, Cache cache) {
            actualWbmCapacity = writeBufferManagerCapacity;
            return ForStMemoryControllerUtils.ForStMemoryFactory.DEFAULT.createWriteBufferManager(
                    writeBufferManagerCapacity, cache);
        }
    }
}
