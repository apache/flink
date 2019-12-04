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

import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.memory.MemoryType;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.memory.MemoryManagerBuilder;
import org.apache.flink.runtime.operators.testutils.DummyEnvironment;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.LRUCache;
import org.rocksdb.WriteBufferManager;

import java.io.IOException;
import java.util.Collections;
import java.util.Deque;

/**
 * Tests to verify memory bounded for rocksDB state backend.
 */
public class RocksDBStateBackendBoundedMemoryTest {

	private static final int MEMORY_SIZE = 64 * 1024 * 1024; // 64 MiBytes

	private MemoryManager memoryManager;

	@Rule
	public final TemporaryFolder tempFolder = new TemporaryFolder();

	@Before
	public void setUp() {
		this.memoryManager = MemoryManagerBuilder
			.newBuilder()
			.setMemorySize(MemoryType.OFF_HEAP, MEMORY_SIZE)
			.build();
	}

	@After
	public void shutDown() {
		this.memoryManager.shutdown();
		Assert.assertTrue(memoryManager.getStateBackendSharedObjects().get().isEmpty());
	}

	@Test
	public void testSharedObjectsInitializeOnlyOnce() throws IOException {
		DummyEnvironment env = new DummyEnvironment();
		env.setMemoryManager(memoryManager);

		Deque<AutoCloseable> sharedObjects = memoryManager.getStateBackendSharedObjects().get();
		Assert.assertNull(sharedObjects);

		Configuration configuration = new Configuration();
		configuration.setString(RocksDBOptions.BOUNDED_MEMORY_SIZE, "128MB");
		RocksDBStateBackend originalStateBackend = new RocksDBStateBackend(tempFolder.newFolder().toURI());
		originalStateBackend.setDbStoragePath(tempFolder.newFolder().getAbsolutePath());

		RocksDBStateBackend rocksDBStateBackend1 = originalStateBackend.configure(configuration, Thread.currentThread().getContextClassLoader());
		AbstractKeyedStateBackend keyedStateBackend1 = createKeyedStateBackend(rocksDBStateBackend1, env);

		sharedObjects = memoryManager.getStateBackendSharedObjects().get();
		Assert.assertNotNull(sharedObjects);

		// we create objects as the order of LRUCache -> WriteBufferManager, and we should get them in reverse order.
		Assert.assertTrue(sharedObjects.getFirst() instanceof WriteBufferManager);
		Assert.assertTrue(sharedObjects.getLast() instanceof LRUCache);
		Assert.assertEquals(2, sharedObjects.size());

		LRUCache lruCache = (LRUCache) sharedObjects.getLast();
		WriteBufferManager writeBufferManager = (WriteBufferManager) sharedObjects.getFirst();

		RocksDBStateBackend rocksDBStateBackend2 = originalStateBackend.configure(configuration, Thread.currentThread().getContextClassLoader());
		AbstractKeyedStateBackend keyedStateBackend2 = createKeyedStateBackend(rocksDBStateBackend2, env);

		try {
			// Another keyed state backend is created but only initialized once for cache and write buffer manager.
			sharedObjects = memoryManager.getStateBackendSharedObjects().get();
			Assert.assertTrue(sharedObjects.getFirst() instanceof WriteBufferManager);
			Assert.assertTrue(sharedObjects.getLast() instanceof LRUCache);
			Assert.assertEquals(2, sharedObjects.size());
			Assert.assertEquals(lruCache, sharedObjects.getLast());
			Assert.assertEquals(writeBufferManager, sharedObjects.getFirst());
		} finally {
			keyedStateBackend1.close();
			keyedStateBackend2.close();
			// even keyed state backend closed, cache and write buffer manager would not be disposed.
			Assert.assertTrue(lruCache.isOwningHandle());
			Assert.assertTrue(writeBufferManager.isOwningHandle());
		}
	}

	private AbstractKeyedStateBackend createKeyedStateBackend(RocksDBStateBackend rocksDBStateBackend, Environment env) throws IOException {
		return rocksDBStateBackend.createKeyedStateBackend(
			env,
			env.getJobID(),
			"test_op",
			IntSerializer.INSTANCE,
			1,
			new KeyGroupRange(0, 0),
			env.getTaskKvStateRegistry(),
			TtlTimeProvider.DEFAULT,
			new UnregisteredMetricsGroup(),
			Collections.emptyList(),
			new CloseableRegistry());

	}
}
