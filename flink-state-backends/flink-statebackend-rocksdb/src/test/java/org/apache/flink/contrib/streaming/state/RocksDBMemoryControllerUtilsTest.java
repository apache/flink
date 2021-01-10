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

package org.apache.flink.contrib.streaming.state;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.rocksdb.Cache;
import org.rocksdb.LRUCache;
import org.rocksdb.NativeLibraryLoader;
import org.rocksdb.WriteBufferManager;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.powermock.api.mockito.PowerMockito.when;

/** Tests to guard {@link RocksDBMemoryControllerUtils}. */
@RunWith(PowerMockRunner.class)
@PrepareForTest(RocksDBMemoryControllerUtils.class)
public class RocksDBMemoryControllerUtilsTest {

    @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Before
    public void ensureRocksDbNativeLibraryLoaded() throws IOException {
        NativeLibraryLoader.getInstance()
                .loadLibrary(temporaryFolder.newFolder().getAbsolutePath());
    }

    @Test
    public void testCreateSharedResourcesWithExpectedCapacity() {
        PowerMockito.mockStatic(RocksDBMemoryControllerUtils.class);
        final AtomicLong actualCacheCapacity = new AtomicLong(0L);
        final AtomicLong actualWbmCapacity = new AtomicLong(0L);

        when(RocksDBMemoryControllerUtils.allocateRocksDBSharedResources(
                        anyLong(), anyDouble(), anyDouble()))
                .thenCallRealMethod();

        when(RocksDBMemoryControllerUtils.calculateActualCacheCapacity(anyLong(), anyDouble()))
                .thenCallRealMethod();

        when(RocksDBMemoryControllerUtils.calculateWriteBufferManagerCapacity(
                        anyLong(), anyDouble()))
                .thenCallRealMethod();

        // because PowerMockito cannot mock on native static method easily,
        // we introduce `createCache` and `createWriteBufferManager` wrappers here.
        when(RocksDBMemoryControllerUtils.createCache(anyLong(), anyDouble()))
                .thenAnswer(
                        (Answer<LRUCache>)
                                invocation -> {
                                    Object[] arguments = invocation.getArguments();
                                    actualCacheCapacity.set((long) arguments[0]);
                                    return (LRUCache) invocation.callRealMethod();
                                });

        when(RocksDBMemoryControllerUtils.createWriteBufferManager(anyLong(), any(Cache.class)))
                .thenAnswer(
                        (Answer<WriteBufferManager>)
                                invocation -> {
                                    Object[] arguments = invocation.getArguments();
                                    actualWbmCapacity.set((long) arguments[0]);
                                    return (WriteBufferManager) invocation.callRealMethod();
                                });

        long totalMemorySize = 2048L;
        double writeBufferRatio = 0.5;
        double highPriPoolRatio = 0.1;
        RocksDBSharedResources rocksDBSharedResources =
                RocksDBMemoryControllerUtils.allocateRocksDBSharedResources(
                        totalMemorySize, writeBufferRatio, highPriPoolRatio);
        long expectedCacheCapacity =
                RocksDBMemoryControllerUtils.calculateActualCacheCapacity(
                        totalMemorySize, writeBufferRatio);
        long expectedWbmCapacity =
                RocksDBMemoryControllerUtils.calculateWriteBufferManagerCapacity(
                        totalMemorySize, writeBufferRatio);

        assertThat(actualCacheCapacity.get(), is(expectedCacheCapacity));
        assertThat(actualWbmCapacity.get(), is(expectedWbmCapacity));
        assertThat(rocksDBSharedResources.getWriteBufferManagerCapacity(), is(expectedWbmCapacity));
    }

    @Test
    public void testCalculateRocksDBDefaultArenaBlockSize() {
        final long align = 4 * 1024;
        final long writeBufferSize = 64 * 1024 * 1024;
        final long expectArenaBlockSize = writeBufferSize / 8;

        // Normal case test
        assertThat(
                "Arena block size calculation error for normal case",
                RocksDBMemoryControllerUtils.calculateRocksDBDefaultArenaBlockSize(writeBufferSize),
                is(expectArenaBlockSize));

        // Alignment tests
        assertThat(
                "Arena block size calculation error for alignment case",
                RocksDBMemoryControllerUtils.calculateRocksDBDefaultArenaBlockSize(
                        writeBufferSize - 1),
                is(expectArenaBlockSize));
        assertThat(
                "Arena block size calculation error for alignment case2",
                RocksDBMemoryControllerUtils.calculateRocksDBDefaultArenaBlockSize(
                        writeBufferSize + 8),
                is(expectArenaBlockSize + align));
    }

    @Test
    public void testCalculateRocksDBMutableLimit() {
        long bufferSize = 64 * 1024 * 1024;
        long limit = bufferSize * 7 / 8;
        assertThat(
                RocksDBMemoryControllerUtils.calculateRocksDBMutableLimit(bufferSize), is(limit));
    }

    @Test
    public void testValidateArenaBlockSize() {
        long arenaBlockSize = 8 * 1024 * 1024;
        assertFalse(
                RocksDBMemoryControllerUtils.validateArenaBlockSize(
                        arenaBlockSize, (long) (arenaBlockSize * 0.5)));
        assertTrue(
                RocksDBMemoryControllerUtils.validateArenaBlockSize(
                        arenaBlockSize, (long) (arenaBlockSize * 1.5)));
    }
}
