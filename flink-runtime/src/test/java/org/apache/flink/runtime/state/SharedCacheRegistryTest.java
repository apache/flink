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
package org.apache.flink.runtime.state;

import org.apache.flink.runtime.checkpoint.CheckpointCache;
import org.apache.flink.runtime.checkpoint.SharedCacheRegistry;
import org.apache.flink.runtime.concurrent.Executors;
import org.apache.flink.runtime.state.filesystem.FileStateHandle;
import org.junit.Assert;
import org.junit.Test;

import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Tests for SharedCacheRegistry
 */
public class SharedCacheRegistryTest {

	@Test
	public void testRegister() throws Exception {
		SharedCacheRegistry registry = new SharedCacheRegistry(Executors.directExecutor());
		CheckpointCache.CacheKey key = new CheckpointCache.CacheKey(new StateHandleID("hanleId1"));
		CheckpointCache.CacheEntry entry1 = mock(CheckpointCache.CacheEntry.class);
		CheckpointCache.CacheEntry entry2 = mock(CheckpointCache.CacheEntry.class);

		registry.registerReference(key, entry1);
		registry.registerReference(key, entry2);

		TimeUnit.MILLISECONDS.sleep(500);
		verify(entry2, times(1)).discard();
		verify(entry1, times(2)).increaseReferenceCount();
	}

	@Test
	public void testConcurrencyRequest() throws Exception {

		ExecutorService executorService = java.util.concurrent.Executors.newFixedThreadPool(10);

		SharedCacheRegistry registry = new SharedCacheRegistry(Executors.directExecutor());

		CheckpointCache.CacheKey[] keys = new CheckpointCache.CacheKey[10];
		int[] referCount = new int[keys.length];
		for (int i = 0; i < keys.length; ++i) {
			keys[i]	= new CheckpointCache.CacheKey(new StateHandleID("handleId" + i));
			// for prevent unregister except
			for (int j = 0; j < 100; ++j) {
				CheckpointCache.CacheEntry entry = new CheckpointCache.CacheEntry(mock(FileStateHandle.class));
				registry.registerReference(keys[i], entry);
			}
			Assert.assertEquals(100, registry.getCacheEntry(keys[i]).getReferenceCount());
			referCount[i] = 100;
		}

		// init 100 runners
		Runnable[] runnables = new Runnable[100];
		for (int i = 0; i < runnables.length; ++i) {
			final int index = new Random().nextInt(keys.length);
			final int opType = new Random().nextInt(2);
			if (opType == 0) {
				referCount[index]--;
			} else {
				referCount[index]++;
			}
			runnables[i] = () -> {
				if (opType == 0) {
					registry.unregisterReference(keys[index]);
				} else {
					CheckpointCache.CacheEntry entry = new CheckpointCache.CacheEntry(mock(FileStateHandle.class));
					registry.registerReference(keys[index], entry);
				}
			};
		}

		// submit runners
		Future[] futures = new Future[runnables.length];
		for (int i = 0; i < runnables.length; ++i) {
			futures[i] = executorService.submit(runnables[i]);
		}

		// wait for finish
		for (int i = 0; i < futures.length; ++i) {
			futures[i].get();
		}

		// valid result
		for (int i = 0; i < keys.length; ++i) {
			if (referCount[i] == 0) {
				Assert.assertNull(registry.getCacheEntry(keys[i]));
			} else {
				Assert.assertEquals(referCount[i], registry.getCacheEntry(keys[i]).getReferenceCount());
			}
		}
	}
}
