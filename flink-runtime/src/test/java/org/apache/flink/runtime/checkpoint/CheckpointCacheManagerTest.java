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
package org.apache.flink.runtime.checkpoint;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.concurrent.Executors;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Tests for CheckpointCacheManager
 */
public class CheckpointCacheManagerTest {

	@Rule
	public final TemporaryFolder tmp = new TemporaryFolder();

	@Test
	public void testCheckpointCacheManager() throws Exception {
		CheckpointCacheManager cacheManager = new CheckpointCacheManager(new ScheduledThreadPoolExecutor(1), Executors.directExecutor(), tmp.newFolder().getAbsolutePath());
		JobID jobID1 = new JobID(1L, 1L);
		cacheManager.registerCheckpointCache(jobID1, 10000, 5);
		Assert.assertEquals(1, cacheManager.getCheckpointCacheSize());
		cacheManager.unregisterCheckpointCache(jobID1);
		TimeUnit.SECONDS.sleep(6);
		Assert.assertEquals(0, cacheManager.getCheckpointCacheSize());
	}

	@Test
	public void testCheckpointCacheRetouchFromRelease() throws Exception {

		CheckpointCacheManager cacheManager = new CheckpointCacheManager(new ScheduledThreadPoolExecutor(1), Executors.directExecutor(), tmp.newFolder().getAbsolutePath());
		JobID jobID = new JobID(1L, 1L);
		CheckpointCache cache1 = cacheManager.registerCheckpointCache(jobID, 10000, 5);

		// this should release the CheckpointCache in future.
		cacheManager.unregisterCheckpointCache(jobID);
		Assert.assertEquals(0, cache1.getReference());
		ScheduledFuture<?> clearFuture = cacheManager.getCacheClearFuture(jobID);
		TimeUnit.SECONDS.sleep(3);
		Assert.assertTrue(!clearFuture.isDone());

		// reassign a lease for CheckpointCache
		CheckpointCache cache2 = cacheManager.registerCheckpointCache(jobID, 10000, 5);
		TimeUnit.SECONDS.sleep(3);

		Assert.assertTrue(clearFuture.isCancelled());
		Assert.assertNull(cacheManager.getCacheClearFuture(jobID));
		Assert.assertEquals(cache1, cache2);
	}

	@Test
	public void testConcurrencyRequest() throws Exception {
		CheckpointCacheManager cacheManager = new CheckpointCacheManager(new ScheduledThreadPoolExecutor(1), Executors.directExecutor(), tmp.newFolder().getAbsolutePath());
		// init jobs
		int[] referenceCount = new int[10];
		JobID[] jobIDS = new JobID[10];
		for (int i = 0; i < 10; ++i) {
			jobIDS[i] = new JobID();
			for (int j = 0; j < 50; ++j) {
				cacheManager.registerCheckpointCache(jobIDS[i], 1, 1);
			}
			referenceCount[i] = 50;
		}

		// init runnables but dont submit immediately
		Runnable[] runnables = new Runnable[50];
		for (int i = 0; i < runnables.length; ++i) {
			final int index = new Random().nextInt(jobIDS.length);
			final int opType = new Random().nextInt(2);
			if (opType == 0) {
				referenceCount[index]--;
			} else {
				referenceCount[index]++;
			}
			runnables[i] = () -> {
				if (opType == 0) {
					cacheManager.unregisterCheckpointCache(jobIDS[index]);
				} else {
					cacheManager.registerCheckpointCache(jobIDS[index], 1, 1);
				}
			};
		}

		//submit all runnables
		ExecutorService executor = java.util.concurrent.Executors.newFixedThreadPool(10);
		Future[] futures = new Future[runnables.length];
		for (int i = 0; i < futures.length; ++i) {
			futures[i] = executor.submit(runnables[i]);
		}

		//wait all requests finished
		for (int i = 0; i < futures.length; ++i) {
			futures[i].get();
		}

		//valid result
		for (int i = 0; i < jobIDS.length; ++i) {
			Assert.assertEquals(referenceCount[i], cacheManager.getCheckpointCache(jobIDS[i]).getReference());
		}
	}

	@Test
	public void testShowdown() throws Exception {
		CheckpointCacheManager cacheManager = new CheckpointCacheManager(new ScheduledThreadPoolExecutor(1), Executors.directExecutor(), tmp.newFolder().getAbsolutePath());
		JobID jobID1 = new JobID(1L, 1L);
		cacheManager.registerCheckpointCache(jobID1, 10000, 5);
		cacheManager.shutdown();
		Assert.assertEquals(0, cacheManager.getCheckpointCacheSize());
	}
}
