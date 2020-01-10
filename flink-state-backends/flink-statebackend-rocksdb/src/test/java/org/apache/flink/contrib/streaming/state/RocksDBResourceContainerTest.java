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

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.runtime.memory.OpaqueMemoryResource;
import org.apache.flink.util.function.ThrowingRunnable;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.Cache;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.LRUCache;
import org.rocksdb.NativeLibraryLoader;
import org.rocksdb.WriteBufferManager;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyLong;

/**
 * Tests to guard {@link RocksDBResourceContainer}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(RocksDBOperationUtils.class)
public class RocksDBResourceContainerTest {

	private static boolean rocksDBLoaded = false;

	@Rule
	public final TemporaryFolder temporaryFolder = new TemporaryFolder();

	@Before
	public void ensureRocksDbNativeLibraryLoaded() throws IOException {
		if (!rocksDBLoaded) {
			NativeLibraryLoader.getInstance().loadLibrary(temporaryFolder.newFolder().getAbsolutePath());
			rocksDBLoaded = true;
		}
	}

	// ------------------------------------------------------------------------

	@Test
	public void testFreeDBOptionsAfterClose() throws Exception {
		RocksDBResourceContainer container = new RocksDBResourceContainer();
		DBOptions dbOptions = container.getDbOptions();
		assertThat(dbOptions.isOwningHandle(), is(true));
		container.close();
		assertThat(dbOptions.isOwningHandle(), is(false));
	}

	@Test
	public void testFreeMultipleDBOptionsAfterClose() throws Exception {
		RocksDBResourceContainer container = new RocksDBResourceContainer();
		final int optionNumber = 20;
		ArrayList<DBOptions> dbOptions = new ArrayList<>(optionNumber);
		for (int i = 0; i < optionNumber; i++) {
			dbOptions.add(container.getDbOptions());
		}
		container.close();
		for (DBOptions dbOption: dbOptions) {
			assertThat(dbOption.isOwningHandle(), is(false));
		}
	}

	/**
	 * Guard the shared resources will be released after {@link RocksDBResourceContainer#close()} when the
	 * {@link RocksDBResourceContainer} instance is initiated with {@link OpaqueMemoryResource}.
	 *
	 * @throws Exception if unexpected error happened.
	 */
	@Test
	public void testSharedResourcesAfterClose() throws Exception {
		OpaqueMemoryResource<RocksDBSharedResources> sharedResources = getSharedResources();
		RocksDBResourceContainer container =
			new RocksDBResourceContainer(PredefinedOptions.DEFAULT, null, sharedResources);
		container.close();
		RocksDBSharedResources rocksDBSharedResources = sharedResources.getResourceHandle();
		assertThat(rocksDBSharedResources.getCache().isOwningHandle(), is(false));
		assertThat(rocksDBSharedResources.getWriteBufferManager().isOwningHandle(), is(false));
	}

	/**
	 * Guard that {@link RocksDBResourceContainer#getDbOptions()} shares the same {@link WriteBufferManager} instance
	 * if the {@link RocksDBResourceContainer} instance is initiated with {@link OpaqueMemoryResource}.
	 *
	 * @throws Exception if unexpected error happened.
	 */
	@Test
	public void testGetDbOptionsWithSharedResources() throws Exception {
		final int optionNumber = 20;
		OpaqueMemoryResource<RocksDBSharedResources> sharedResources = getSharedResources();
		RocksDBResourceContainer container =
			new RocksDBResourceContainer(PredefinedOptions.DEFAULT, null, sharedResources);
		HashSet<WriteBufferManager> writeBufferManagers = new HashSet<>();
		for (int i = 0; i < optionNumber; i++) {
			DBOptions dbOptions = container.getDbOptions();
			WriteBufferManager writeBufferManager = getWriteBufferManager(dbOptions);
			writeBufferManagers.add(writeBufferManager);
		}
		assertThat(writeBufferManagers.size(), is(1));
		assertThat(writeBufferManagers.iterator().next(),
			is(sharedResources.getResourceHandle().getWriteBufferManager()));
		container.close();
	}

	/**
	 * Guard that {@link RocksDBResourceContainer#getColumnOptions()} shares the same {@link Cache} instance
	 * if the {@link RocksDBResourceContainer} instance is initiated with {@link OpaqueMemoryResource}.
	 *
	 * @throws Exception if unexpected error happened.
	 */
	@Test
	public void testGetColumnFamilyOptionsWithSharedResources() throws Exception {
		final int optionNumber = 20;
		OpaqueMemoryResource<RocksDBSharedResources> sharedResources = getSharedResources();
		RocksDBResourceContainer container =
			new RocksDBResourceContainer(PredefinedOptions.DEFAULT, null, sharedResources);
		HashSet<Cache> caches = new HashSet<>();
		for (int i = 0; i < optionNumber; i++) {
			ColumnFamilyOptions columnOptions = container.getColumnOptions();
			Cache cache = getBlockCache(columnOptions);
			caches.add(cache);
		}
		assertThat(caches.size(), is(1));
		assertThat(caches.iterator().next(),
			is(sharedResources.getResourceHandle().getCache()));
		container.close();
	}

	@Test
	public void testCreateSharedResourcesWithExpectedCapacity() throws Exception {
		PowerMockito.spy(RocksDBOperationUtils.class);
		final AtomicLong actualCacheCapacity = new AtomicLong(0L);
		// the `createCache` wrapper is introduced due to PowerMockito cannot mock on native static method easily.
		PowerMockito.when(RocksDBOperationUtils.createCache(anyLong(), anyDouble()))
			.thenAnswer((Answer<LRUCache>) invocation -> {
				Object[] arguments = invocation.getArguments();
				actualCacheCapacity.set((long) arguments[0]);
				return (LRUCache) invocation.callRealMethod();
			});

		long totalMemorySize = 2048L;
		double writeBufferRatio = 0.5;
		double highPriPoolRatio = 0.1;
		createSharedResources(totalMemorySize, writeBufferRatio, highPriPoolRatio);
		long expectedCacheCapacity = RocksDBOperationUtils.calculateActualCacheCapacity(totalMemorySize, writeBufferRatio);
		assertThat(actualCacheCapacity.get(), is(expectedCacheCapacity));
	}

	private OpaqueMemoryResource<RocksDBSharedResources> getSharedResources() {
		return createSharedResources(1024L, 0.5, 0.1);
	}

	private OpaqueMemoryResource<RocksDBSharedResources> createSharedResources(long totalMemorySize, double writeBufferRatio, double highPriPoolRatio) {
		RocksDBSharedResources rocksDBSharedResources = RocksDBOperationUtils
			.allocateRocksDBSharedResources(totalMemorySize, writeBufferRatio, highPriPoolRatio);
		return new OpaqueMemoryResource<>(rocksDBSharedResources, totalMemorySize, rocksDBSharedResources::close);
	}

	private Cache getBlockCache(ColumnFamilyOptions columnOptions) {
		BlockBasedTableConfig blockBasedTableConfig = null;
		try {
			blockBasedTableConfig = (BlockBasedTableConfig) columnOptions.tableFormatConfig();
		} catch (ClassCastException e) {
			fail("Table config got from ColumnFamilyOptions is not BlockBasedTableConfig");
		}
		Field cacheField = null;
		try {
			cacheField = BlockBasedTableConfig.class.getDeclaredField("blockCache_");
		} catch (NoSuchFieldException e) {
			fail("blockCache_ is not defined");
		}
		cacheField.setAccessible(true);
		try {
			return (Cache) cacheField.get(blockBasedTableConfig);
		} catch (IllegalAccessException e) {
			fail("Cannot access blockCache_ field.");
			return null;
		}
	}

	private WriteBufferManager getWriteBufferManager(DBOptions dbOptions) {

		Field writeBufferManagerField = null;
		try {
			writeBufferManagerField = DBOptions.class.getDeclaredField("writeBufferManager_");
		} catch (NoSuchFieldException e) {
			fail("writeBufferManager_ is not defined.");
		}
		writeBufferManagerField.setAccessible(true);
		try {
			return (WriteBufferManager) writeBufferManagerField.get(dbOptions);
		} catch (IllegalAccessException e) {
			fail("Cannot access writeBufferManager_ field.");
			return null;
		}
	}

	@Test
	public void testFreeColumnOptionsAfterClose() throws Exception {
		RocksDBResourceContainer container = new RocksDBResourceContainer();
		ColumnFamilyOptions columnFamilyOptions = container.getColumnOptions();
		assertThat(columnFamilyOptions.isOwningHandle(), is(true));
		container.close();
		assertThat(columnFamilyOptions.isOwningHandle(), is(false));
	}

	@Test
	public void testFreeMultipleColumnOptionsAfterClose() throws Exception {
		RocksDBResourceContainer container = new RocksDBResourceContainer();
		final int optionNumber = 20;
		ArrayList<ColumnFamilyOptions> columnFamilyOptions = new ArrayList<>(optionNumber);
		for (int i = 0; i < optionNumber; i++) {
			columnFamilyOptions.add(container.getColumnOptions());
		}
		container.close();
		for (ColumnFamilyOptions columnFamilyOption: columnFamilyOptions) {
			assertThat(columnFamilyOption.isOwningHandle(), is(false));
		}
	}

	@Test
	public void testFreeMultipleColumnOptionsWithPredefinedOptions() throws Exception {
		for (PredefinedOptions predefinedOptions: PredefinedOptions.values()) {
			RocksDBResourceContainer container = new RocksDBResourceContainer(predefinedOptions, null);
			final int optionNumber = 20;
			ArrayList<ColumnFamilyOptions> columnFamilyOptions = new ArrayList<>(optionNumber);
			for (int i = 0; i < optionNumber; i++) {
				columnFamilyOptions.add(container.getColumnOptions());
			}
			container.close();
			for (ColumnFamilyOptions columnFamilyOption: columnFamilyOptions) {
				assertThat(columnFamilyOption.isOwningHandle(), is(false));
			}
		}
	}

	@Test
	public void testFreeSharedResourcesAfterClose() throws Exception {
		LRUCache cache = new LRUCache(1024L);
		WriteBufferManager wbm = new WriteBufferManager(1024L, cache);
		RocksDBSharedResources sharedResources = new RocksDBSharedResources(cache, wbm);
		final ThrowingRunnable<Exception> disposer = sharedResources::close;
		OpaqueMemoryResource<RocksDBSharedResources> opaqueResource =
			new OpaqueMemoryResource<>(sharedResources, 1024L, disposer);

		RocksDBResourceContainer container = new RocksDBResourceContainer(PredefinedOptions.DEFAULT, null, opaqueResource);

		container.close();
		assertThat(cache.isOwningHandle(), is(false));
		assertThat(wbm.isOwningHandle(), is(false));
	}
}
